package coordinator

import (
	"encoding/json"
	"errors"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"sync"
)

// TxnStateStore tracks (producerID, epoch) per transactional.id so
// InitProducerId can implement the epoch-fence-on-rejoin contract
// (gh #22). Apache Kafka's transactional producer relies on this:
// every reconnect of the same transactional.id returns the same
// PID with epoch+1, and any in-flight Produce from the previous
// session (still tagged with the old epoch) gets fenced by the
// idempotence check at storage.Append → ErrInvalidProducerEpoch
// → wire code 47.
//
// Persistence: a single JSON file under <dataDir>/__cluster.
// Atomic tmp + rename to keep the on-disk view consistent across
// restarts; the file is small (one entry per active txnID) so a
// full rewrite per InitProducerId is acceptable on the cold path
// transactional producers exercise.
//
// Multi-broker: this implementation is per-broker. A producer
// that reconnects to a DIFFERENT broker for the same txnID will
// get a fresh PID (epoch=0) because the new broker doesn't see
// the prior state. Apache Kafka avoids this by partitioning the
// __transaction_state topic by hash(txnID) so a deterministic
// broker is the coordinator. skafka would need a similar
// FindCoordinator(type=transaction) routing layer to match;
// tracked separately. For single-bootstrap setups (the common
// case for kafka-*.sh tools and most non-prod clients) the gap
// is invisible.
type TxnStateStore struct {
	path string

	mu    sync.Mutex
	state map[string]TxnEntry
}

// TxnEntry is the persistent record of a transactional producer.
// PID stays stable across the lifetime of the entry; only Epoch
// moves. Once Epoch saturates int16 we rotate to a fresh PID
// (the InitProducerIdHandler does the rotation; TxnStateStore
// just records what it's told).
type TxnEntry struct {
	PID   int64 `json:"pid"`
	Epoch int16 `json:"epoch"`
}

// NewTxnStateStore opens the per-cluster transactional-state file.
// dir is typically <dataDir>/__cluster. Missing file is treated
// as "no transactional producers yet" — it's created lazily on
// the first persist.
func NewTxnStateStore(dir string) (*TxnStateStore, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	s := &TxnStateStore{
		path:  filepath.Join(dir, "transactional_state.json"),
		state: make(map[string]TxnEntry),
	}
	if err := s.loadLocked(); err != nil {
		return nil, err
	}
	return s, nil
}

// GetOrAllocate is the gh #22 contract: for txnID="foo" the first
// call returns a fresh PID with epoch=0; every subsequent call
// returns the SAME PID with epoch+1.
//
// alloc supplies a fresh PID — typically the same monotonic
// counter the non-transactional InitProducerId path uses, so PIDs
// stay globally distinct on this broker. alloc is only invoked
// the first time a txnID is seen, and on epoch rotation.
//
// Concurrent callers for the same txnID serialise on s.mu, so
// two clients claiming the same transactional.id at exactly the
// same time get different epochs (one fences the other). That's
// the whole point — the fence depends on a strict monotonic
// stream of (PID, epoch) pairs.
func (s *TxnStateStore) GetOrAllocate(txnID string, alloc func() int64) (int64, int16, error) {
	if txnID == "" {
		return 0, 0, errors.New("txn state store: empty transactional id")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.state[txnID]
	if !ok {
		entry = TxnEntry{PID: alloc(), Epoch: 0}
	} else if entry.Epoch == math.MaxInt16 {
		// Epoch overflow: rotate to a fresh PID. Apache Kafka
		// emits PRODUCER_FENCED here and forces the client to
		// re-init; for skafka without a transactional fence
		// surface, allocating a new PID achieves the same
		// effect — old in-flight writes can't match the new
		// (PID, epoch) pair so they're naturally fenced.
		entry = TxnEntry{PID: alloc(), Epoch: 0}
	} else {
		entry.Epoch++
	}
	s.state[txnID] = entry
	if err := s.persistLocked(); err != nil {
		return 0, 0, err
	}
	return entry.PID, entry.Epoch, nil
}

// Snapshot returns a copy of the current in-memory state. Used by
// tests to assert persistence + rejoin behaviour without poking
// into private fields.
func (s *TxnStateStore) Snapshot() map[string]TxnEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]TxnEntry, len(s.state))
	for k, v := range s.state {
		out[k] = v
	}
	return out
}

func (s *TxnStateStore) loadLocked() error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil // fresh broker, empty state is correct
		}
		return err
	}
	return json.Unmarshal(data, &s.state)
}

func (s *TxnStateStore) persistLocked() error {
	data, err := json.Marshal(s.state)
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}
