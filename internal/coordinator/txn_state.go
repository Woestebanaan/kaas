package coordinator

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
// Persistence: sharded by hash(txnID) % numSlots, one JSON file per
// slot under <dataDir>/__cluster/txn_state/slot-N.json. Each broker
// owns only the slots routing to it under the gh #91 hash-and-alive-
// fallback in internal/broker/group_hash.go; on coordinator failover
// the new owner reads the slot file the prior owner wrote and
// continues from the same (PID, epoch) state. This closes the gh
// #108 correctness gap where a producer's preferred-slot broker
// dying caused the new owner to allocate a fresh PID with epoch=0
// and silently break the fence-on-rejoin contract.
//
// Mirrors Apache Kafka's __transaction_state internal topic:
// partition = slot, log replay = JSON file read. Skafka skips the
// log-replay step because the file is already the materialised map
// the Apache coordinator builds from compacted log records.
//
// Read-fresh-on-every-call semantics: each GetOrAllocate re-reads
// the slot file from disk before mutating, then writes back via
// atomic tmp+rename. NFS close-to-open consistency means a fresh
// os.Open sees the latest committed state from any other broker
// that recently wrote. Cost: ~2 file ops per InitProducerId (cold
// path; transactional producers init rarely).
//
// Split-brain risk: during a controller transition (~15s window)
// two brokers can both think they own a slot. Last-write-wins on
// slot-N.json. Mitigated for the common case by the controller's
// ~5s lease refresh, fully closed by the gh #108 phase 2 cross-
// broker fence broadcast which also bumps every in-flight (PID,
// epoch) on the losing broker's partitions.
type TxnStateStore struct {
	dir      string
	numSlots int

	mu sync.Mutex
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

// NewTxnStateStore opens the per-cluster transactional-state dir.
// dir is typically <dataDir>/__cluster.
//
// numSlots is the StatefulSet replica count (same value the gh #91
// PickTxnCoordinator hashes into). Pinning this to the configured
// replica count — not len(alive) — keeps slot mapping stable across
// rolling restarts; a scale-out from N→N' would re-shard, which is
// out of scope for #108 and tracked separately.
//
// Migrates the legacy single-file layout (transactional_state.json)
// on first open: every entry is hashed and written into the new
// slot-keyed shape, then the legacy file is deleted.
func NewTxnStateStore(dir string, numSlots int) (*TxnStateStore, error) {
	if numSlots <= 0 {
		return nil, fmt.Errorf("txn state store: numSlots must be > 0, got %d", numSlots)
	}
	slotDir := filepath.Join(dir, "txn_state")
	if err := os.MkdirAll(slotDir, 0o755); err != nil {
		return nil, err
	}
	s := &TxnStateStore{
		dir:      slotDir,
		numSlots: numSlots,
	}
	if err := s.migrateLegacy(dir); err != nil {
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
// Reads the slot file fresh on every call (gh #108): a producer
// rejoining after its preferred-slot broker failed over will hit
// the new coordinator, which reads the same slot-N.json the prior
// coordinator wrote and bumps from there.
//
// Concurrent callers within a single broker process serialise on
// s.mu, so two clients claiming the same transactional.id at
// exactly the same time get different epochs (one fences the
// other). Cross-broker concurrent claims are race-bounded to the
// brief controller-transition window; outside of that the gh #91
// OwnsTxn gate keeps each txn ID on a single broker.
func (s *TxnStateStore) GetOrAllocate(txnID string, alloc func() int64) (int64, int16, error) {
	if txnID == "" {
		return 0, 0, errors.New("txn state store: empty transactional id")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	slot := s.slotFor(txnID)
	state, err := s.loadSlot(slot)
	if err != nil {
		return 0, 0, err
	}

	entry, ok := state[txnID]
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
	state[txnID] = entry
	if err := s.persistSlot(slot, state); err != nil {
		return 0, 0, err
	}
	return entry.PID, entry.Epoch, nil
}

// Snapshot returns a copy of every txn entry across every slot.
// Used by tests to assert persistence + rejoin behaviour without
// poking into private fields.
func (s *TxnStateStore) Snapshot() map[string]TxnEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]TxnEntry)
	for slot := 0; slot < s.numSlots; slot++ {
		state, err := s.loadSlot(slot)
		if err != nil {
			continue
		}
		for k, v := range state {
			out[k] = v
		}
	}
	return out
}

// slotFor hashes txnID into [0, numSlots). Mirrors Apache's
// partitionFor(groupId) and skafka's broker.TxnCoordinatorSlot
// (FNV-1a 32-bit). The hash is purely local to disk-layout
// decisions; the broker-side coordinator routing uses its own
// hash in internal/broker/group_hash.go. The two hash functions
// happen to match (both FNV-1a 32 over the txnID bytes) but they
// don't have to — only the divisor (numSlots == numBrokers) and
// the deterministic mapping matter.
func (s *TxnStateStore) slotFor(txnID string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(txnID))
	return int(h.Sum32()) % s.numSlots
}

func (s *TxnStateStore) slotPath(slot int) string {
	return filepath.Join(s.dir, fmt.Sprintf("slot-%d.json", slot))
}

func (s *TxnStateStore) loadSlot(slot int) (map[string]TxnEntry, error) {
	data, err := os.ReadFile(s.slotPath(slot))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return make(map[string]TxnEntry), nil
		}
		return nil, err
	}
	state := make(map[string]TxnEntry)
	if len(data) == 0 {
		return state, nil
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("txn state store: decode slot-%d: %w", slot, err)
	}
	return state, nil
}

func (s *TxnStateStore) persistSlot(slot int, state map[string]TxnEntry) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	tmp := s.slotPath(slot) + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.slotPath(slot))
}

// migrateLegacy ingests <dataDir>/__cluster/transactional_state.json
// (the pre-#108 single-file layout) into the new slot-keyed dir
// and deletes the legacy file. Idempotent: returns nil if the
// legacy file is absent. Each entry is hashed to its slot via
// slotFor() and merged into the slot's existing map (so a warm
// broker that wrote some entries to the new layout already and
// then the legacy file resurfaces won't lose newer state).
func (s *TxnStateStore) migrateLegacy(parentDir string) error {
	legacy := filepath.Join(parentDir, "transactional_state.json")
	data, err := os.ReadFile(legacy)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	var legacyState map[string]TxnEntry
	if len(data) > 0 {
		if err := json.Unmarshal(data, &legacyState); err != nil {
			return fmt.Errorf("txn state store: decode legacy file: %w", err)
		}
	}
	for txnID, entry := range legacyState {
		slot := s.slotFor(txnID)
		state, err := s.loadSlot(slot)
		if err != nil {
			return err
		}
		// Don't overwrite a newer entry that may have landed in the
		// new layout while the legacy file lingered (race-bounded:
		// the legacy file is supposed to be deleted once, but if a
		// crash leaves it around between writes, the new-layout
		// entry's epoch will be ≥ legacy's).
		if existing, ok := state[txnID]; ok && existing.Epoch >= entry.Epoch && existing.PID == entry.PID {
			continue
		}
		state[txnID] = entry
		if err := s.persistSlot(slot, state); err != nil {
			return err
		}
	}
	return os.Remove(legacy)
}

// activeSlots returns the slot indices that currently have a file
// on disk (used by tests to inspect the on-disk shape).
func (s *TxnStateStore) activeSlots() ([]int, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	var slots []int
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "slot-") || !strings.HasSuffix(name, ".json") {
			continue
		}
		nstr := strings.TrimSuffix(strings.TrimPrefix(name, "slot-"), ".json")
		n, err := strconv.Atoi(nstr)
		if err != nil {
			continue
		}
		slots = append(slots, n)
	}
	return slots, nil
}
