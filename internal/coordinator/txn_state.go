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

// DefaultNumSlots matches Apache Kafka's
// `transaction.state.log.num.partitions=50` default. Pinning the slot
// count to a fixed cluster-wide constant — instead of the StatefulSet
// replica count — decouples the storage layout from broker scale
// operations: scaling up or down changes which broker owns each slot
// (gh #91 hash routing), but every slot file remains valid. Same
// shape Apache uses: __transaction_state has a fixed 50 partitions;
// scaling brokers shifts leadership, never the partition count.
const DefaultNumSlots = 50

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
// numSlots is a "set once at bootstrap" value. Apache enforces this
// by reading transaction.state.log.num.partitions at first cluster
// start and ignoring later changes; skafka has a softer guarantee —
// changing the value requires a re-shard pass that runs in
// migrateLayout() on every broker startup. The migration is
// idempotent: it walks every existing slot-*.json, computes each
// entry's expected slot under the current numSlots, moves any
// misplaced entry, and removes empty / out-of-range slot files.
// Best-effort during rolling upgrades: while old-version brokers
// still write to old-numSlots files, the new-version brokers' next
// startup migration catches them.
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
// numSlots ≤ 0 falls back to DefaultNumSlots (50). Pinning to a
// fixed cluster-wide constant — independent of broker count —
// keeps the storage layout stable across scale operations. Mirrors
// Apache's `transaction.state.log.num.partitions=50` default.
//
// Two migrations run on open, both idempotent:
//
//  1. Legacy single-file layout (pre-v0.1.81) — read
//     transactional_state.json, distribute entries to slot files,
//     delete the legacy file.
//  2. Slot-layout drift — re-shard any entry currently sitting in
//     slot-K.json where hash(txnID) % numSlots != K. Catches the
//     v0.1.81-v0.1.83 → v0.1.84 transition (numSlots was the
//     replica count, now pinned to 50) plus any future numSlots
//     change. Removes empty / out-of-range slot files.
func NewTxnStateStore(dir string, numSlots int) (*TxnStateStore, error) {
	if numSlots <= 0 {
		numSlots = DefaultNumSlots
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
	if err := s.migrateLayout(); err != nil {
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

// migrateLayout re-shards any entry sitting in a slot file that
// disagrees with the current numSlots — the case when an operator
// changes numSlots between boots, or when upgrading from a
// pre-v0.1.84 build that used a smaller numSlots (= broker count).
// Idempotent: running on an already-correct layout is a no-op.
//
// Five-phase algorithm:
//  1. Read every existing slot-*.json into memory.
//  2. Compute the new layout by hashing each entry under the
//     current numSlots. Higher-epoch wins on conflict (defensive
//     against the rolling-upgrade window).
//  3. Detect no-op: if the new layout is byte-identical to the
//     old, skip phases 4-5. Avoids spurious writes on warm
//     restarts.
//  4. Write every slot in the new layout. Atomic tmp+rename per
//     file. Source slots that also appear in the new layout get
//     overwritten cleanly here.
//  5. Delete source files that don't appear in the new layout
//     (either empty after relocation, or out-of-range under new
//     numSlots). Tolerates concurrent removal under rolling
//     upgrade.
//
// The phase ordering matters: writing the new layout BEFORE
// deleting source files means any concurrent reader sees a
// consistent view (either the old or the new layout, never a
// partial state with the entry deleted from old slot but not yet
// in new). Conversely, source-file deletion comes last so the
// migration is recoverable from a crash mid-write.
func (s *TxnStateStore) migrateLayout() error {
	dirEntries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}

	// Phase 1: load everything.
	oldLayout := make(map[int]map[string]TxnEntry) // slot → state
	for _, e := range dirEntries {
		name := e.Name()
		if !strings.HasPrefix(name, "slot-") || !strings.HasSuffix(name, ".json") {
			continue
		}
		nstr := strings.TrimSuffix(strings.TrimPrefix(name, "slot-"), ".json")
		n, err := strconv.Atoi(nstr)
		if err != nil {
			continue
		}
		state, err := s.loadSlot(n)
		if err != nil {
			return err
		}
		oldLayout[n] = state
	}

	// Phase 2: compute the target layout. Walk every entry from
	// every old slot, place into its expected slot under current
	// numSlots. Empty slots aren't materialised — they don't
	// generate write or delete activity.
	newLayout := make(map[int]map[string]TxnEntry)
	for _, state := range oldLayout {
		for txnID, entry := range state {
			expected := s.slotFor(txnID)
			if existing, ok := newLayout[expected][txnID]; ok && existing.Epoch >= entry.Epoch {
				continue
			}
			if newLayout[expected] == nil {
				newLayout[expected] = make(map[string]TxnEntry)
			}
			newLayout[expected][txnID] = entry
		}
	}

	// Phase 3: idempotency check. If the layout already matches,
	// skip the rewrite. The check is map-equality on (slot →
	// txnID → entry).
	if layoutsEqual(oldLayout, newLayout) {
		return nil
	}

	// Phase 4: write every slot in the new layout. Source slots
	// that also appear here get overwritten cleanly (the new
	// content already includes any entries that hashed back to
	// the same slot).
	for slot, state := range newLayout {
		if err := s.persistSlot(slot, state); err != nil {
			return err
		}
	}

	// Phase 5: delete source files that no longer participate in
	// the new layout. Either the slot has no entries hashed to it
	// under current numSlots, or the slot index is out of range
	// (≥ numSlots).
	for slot := range oldLayout {
		if _, kept := newLayout[slot]; kept && slot < s.numSlots {
			continue
		}
		if err := os.Remove(s.slotPath(slot)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

// layoutsEqual reports byte-for-byte equality between two
// (slot → state) maps. Used by migrateLayout's idempotency check.
func layoutsEqual(a, b map[int]map[string]TxnEntry) bool {
	if len(a) != len(b) {
		return false
	}
	for slot, aState := range a {
		bState, ok := b[slot]
		if !ok || len(aState) != len(bState) {
			return false
		}
		for txnID, aEntry := range aState {
			if bEntry, ok := bState[txnID]; !ok || aEntry != bEntry {
				return false
			}
		}
	}
	return true
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
