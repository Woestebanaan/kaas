package coordinator

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

// allocCounter mimics the production wiring: a monotonic counter
// that the InitProducerId handler shares with the txn store, so a
// fresh PID never collides with an outstanding non-transactional
// PID.
func allocCounter() (alloc func() int64, peek func() int64) {
	var n atomic.Int64
	n.Store(1000)
	return func() int64 { return n.Add(1) }, n.Load
}

// TestTxnStateFirstAllocReturnsEpochZero pins the gh #22 contract
// at the most fundamental level: a transactional.id never seen
// before gets a fresh PID and epoch=0.
func TestTxnStateFirstAllocReturnsEpochZero(t *testing.T) {
	dir := t.TempDir()
	s, err := NewTxnStateStore(dir, 3)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	alloc, _ := allocCounter()
	pid, epoch, err := s.GetOrAllocate("foo", alloc)
	if err != nil {
		t.Fatalf("alloc: %v", err)
	}
	if pid <= 0 {
		t.Errorf("PID=%d, want positive", pid)
	}
	if epoch != 0 {
		t.Errorf("first-call epoch=%d, want 0", epoch)
	}
}

// TestTxnStateRejoinBumpsEpoch is gh #22's headline behaviour: the
// SAME transactional.id calling InitProducerId again gets the
// SAME PID with epoch+1. Without this, the storage-layer
// idempotence fence has nothing to fence against — both sessions
// write under (PID, epoch=0).
func TestTxnStateRejoinBumpsEpoch(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewTxnStateStore(dir, 3)
	alloc, _ := allocCounter()

	pid1, ep1, _ := s.GetOrAllocate("foo", alloc)
	pid2, ep2, _ := s.GetOrAllocate("foo", alloc)
	pid3, ep3, _ := s.GetOrAllocate("foo", alloc)

	if pid1 != pid2 || pid2 != pid3 {
		t.Errorf("PIDs drifted across rejoins: %d %d %d (must be stable)", pid1, pid2, pid3)
	}
	if ep1 != 0 || ep2 != 1 || ep3 != 2 {
		t.Errorf("epochs=(%d,%d,%d), want (0,1,2)", ep1, ep2, ep3)
	}
}

// TestTxnStateDistinctIDsGetDistinctPIDs guards against a typo
// that would let two transactional.ids share a PID and dedupe
// each other out.
func TestTxnStateDistinctIDsGetDistinctPIDs(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewTxnStateStore(dir, 3)
	alloc, _ := allocCounter()

	pidA, _, _ := s.GetOrAllocate("foo", alloc)
	pidB, _, _ := s.GetOrAllocate("bar", alloc)
	if pidA == pidB {
		t.Errorf("two distinct txn.ids got the same PID %d", pidA)
	}
}

// TestTxnStatePersistsAcrossRestart confirms the JSON-on-disk
// contract: a producer that opens a new connection AFTER the
// broker pod is replaced still gets its old PID + epoch+1, not
// a fresh PID. Without this, every broker restart silently
// resets everyone's epoch to 0 and zombies survive the restart.
func TestTxnStatePersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	alloc1, _ := allocCounter()

	s1, _ := NewTxnStateStore(dir, 3)
	pid1, _, _ := s1.GetOrAllocate("foo", alloc1)

	// Fresh store on the same dir simulates a broker pod replacement.
	alloc2, _ := allocCounter()
	s2, err := NewTxnStateStore(dir, 3)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	pid2, ep2, _ := s2.GetOrAllocate("foo", alloc2)

	if pid2 != pid1 {
		t.Errorf("post-restart PID=%d, want %d (TxnStateStore did not reload)", pid2, pid1)
	}
	if ep2 != 1 {
		t.Errorf("post-restart epoch=%d, want 1 (must continue bumping from disk state)", ep2)
	}
}

// TestTxnStateEpochOverflowRotatesPID: after epoch hits int16 max
// we rotate to a fresh PID with epoch=0. Apache Kafka emits
// PRODUCER_FENCED at the same point and forces a re-init; for a
// skafka producer without that surface the rotation achieves the
// same effect — the new (PID, epoch) doesn't match anything the
// old session was using.
func TestTxnStateEpochOverflowRotatesPID(t *testing.T) {
	dir := t.TempDir()
	// numSlots=1 so "foo" is guaranteed to land in slot 0.
	s, _ := NewTxnStateStore(dir, 1)
	alloc, _ := allocCounter()

	// Hand-craft a state at the boundary so we don't have to call
	// GetOrAllocate 32k times in test.
	preExisting := TxnEntry{PID: 9999, Epoch: math.MaxInt16}
	if err := s.persistSlot(0, map[string]TxnEntry{"foo": preExisting}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	pid, ep, err := s.GetOrAllocate("foo", alloc)
	if err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if pid == 9999 {
		t.Errorf("epoch-overflow did not rotate PID (still %d)", pid)
	}
	if ep != 0 {
		t.Errorf("post-rotate epoch=%d, want 0", ep)
	}
}

// TestTxnStateConcurrentRejoinsSerialised: the whole point of the
// (PID, epoch) bump is that two clients claiming the same txnID
// at the same wall-clock instant get DIFFERENT epochs — one
// fences the other. Concurrent GetOrAllocate calls for the same
// txnID must serialise so we never hand out the same epoch twice.
func TestTxnStateConcurrentRejoinsSerialised(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewTxnStateStore(dir, 3)
	alloc, _ := allocCounter()

	const concurrency = 32
	results := make(chan int16, concurrency)
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for range concurrency {
		go func() {
			defer wg.Done()
			_, ep, err := s.GetOrAllocate("foo", alloc)
			if err != nil {
				t.Errorf("alloc: %v", err)
				return
			}
			results <- ep
		}()
	}
	wg.Wait()
	close(results)

	seen := map[int16]struct{}{}
	for ep := range results {
		if _, dup := seen[ep]; dup {
			t.Errorf("epoch %d returned twice — concurrent serialisation broke", ep)
		}
		seen[ep] = struct{}{}
	}
	if len(seen) != concurrency {
		t.Errorf("got %d unique epochs from %d concurrent calls", len(seen), concurrency)
	}
}

// TestTxnStateEmptyTxnIDRejected guards the handler-side contract:
// the empty transactional.id ("") is the wire sentinel for a
// non-transactional idempotent producer. The store must NOT
// accept it as a real key — the handler short-circuits to the
// fresh-PID path before reaching the store. Defense in depth.
func TestTxnStateEmptyTxnIDRejected(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewTxnStateStore(dir, 3)
	alloc, _ := allocCounter()
	if _, _, err := s.GetOrAllocate("", alloc); err == nil {
		t.Error("empty txn.id should return an error")
	}
}

// TestTxnStateCrossBrokerRejoinSurvivesFailover is the gh #108 headline
// test: a producer's txnID is first seen on broker A, A dies, the alive-
// set fallback routes the rejoin to broker B, and B must return the
// SAME PID with epoch+1 — not a fresh PID. This is the contract the
// per-broker single-file layout silently broke; the sharded slot files
// on the shared RWX PVC let B read what A wrote.
//
// Simulated: two TxnStateStore instances pointing at the same dir
// (same shared PVC view). brokerA writes; brokerB reads + bumps.
func TestTxnStateCrossBrokerRejoinSurvivesFailover(t *testing.T) {
	dir := t.TempDir()
	const numSlots = 3
	allocA, _ := allocCounter()
	allocB, _ := allocCounter()

	// Broker A: producer's first InitProducerId.
	brokerA, err := NewTxnStateStore(dir, numSlots)
	if err != nil {
		t.Fatalf("brokerA: %v", err)
	}
	pidA, epochA, err := brokerA.GetOrAllocate("payment-tx", allocA)
	if err != nil {
		t.Fatalf("brokerA alloc: %v", err)
	}
	if epochA != 0 {
		t.Fatalf("first alloc epoch=%d, want 0", epochA)
	}

	// Broker A goes down. Broker B (different process, same shared PVC)
	// boots and the alive-set fallback routes "payment-tx" to it.
	brokerB, err := NewTxnStateStore(dir, numSlots)
	if err != nil {
		t.Fatalf("brokerB: %v", err)
	}
	pidB, epochB, err := brokerB.GetOrAllocate("payment-tx", allocB)
	if err != nil {
		t.Fatalf("brokerB alloc: %v", err)
	}

	if pidB != pidA {
		t.Errorf("cross-broker rejoin allocated fresh PID=%d (want %d) — gh #108 contract broken", pidB, pidA)
	}
	if epochB != 1 {
		t.Errorf("cross-broker rejoin epoch=%d, want 1 (must continue from disk state)", epochB)
	}
}

// TestTxnStateMigrateLegacySingleFile pins the upgrade path: warm
// clusters running pre-#108 versions have a single transactional_state.json
// in <dataDir>/__cluster. On first open of the new sharded store, every
// entry must end up in the right slot file and the legacy file must be
// removed. Without this, a v0.1.81 broker would lose all prior txn state
// and every transactional producer's next rejoin gets a fresh PID.
func TestTxnStateMigrateLegacySingleFile(t *testing.T) {
	parent := t.TempDir()
	legacyPath := filepath.Join(parent, "transactional_state.json")
	legacy := map[string]TxnEntry{
		"txn-a": {PID: 1001, Epoch: 5},
		"txn-b": {PID: 1002, Epoch: 0},
		"txn-c": {PID: 1003, Epoch: 99},
	}
	data, _ := json.Marshal(legacy)
	if err := os.WriteFile(legacyPath, data, 0o644); err != nil {
		t.Fatalf("write legacy: %v", err)
	}

	const numSlots = 3
	s, err := NewTxnStateStore(parent, numSlots)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Legacy file must be gone.
	if _, err := os.Stat(legacyPath); !os.IsNotExist(err) {
		t.Errorf("legacy file still present after migration: %v", err)
	}

	// Every legacy entry must be readable through the new store.
	snap := s.Snapshot()
	for txnID, want := range legacy {
		got, ok := snap[txnID]
		if !ok {
			t.Errorf("legacy entry %q lost during migration", txnID)
			continue
		}
		if got.PID != want.PID || got.Epoch != want.Epoch {
			t.Errorf("legacy entry %q migrated incorrectly: got %+v, want %+v", txnID, got, want)
		}
	}
}

// TestTxnStateDefaultNumSlots: passing 0 (or any non-positive
// value) selects the cluster-wide constant DefaultNumSlots=50,
// matching Apache's transaction.state.log.num.partitions default.
// Catches a wiring slip that would silently revert the gh #108
// follow-up to per-broker-replica-count slot mapping.
func TestTxnStateDefaultNumSlots(t *testing.T) {
	dir := t.TempDir()
	s, err := NewTxnStateStore(dir, 0)
	if err != nil {
		t.Fatalf("default numSlots: %v", err)
	}
	if s.numSlots != DefaultNumSlots {
		t.Errorf("expected numSlots=%d, got %d", DefaultNumSlots, s.numSlots)
	}
	// Negative is treated the same as 0.
	s2, err := NewTxnStateStore(dir, -1)
	if err != nil {
		t.Fatalf("negative numSlots: %v", err)
	}
	if s2.numSlots != DefaultNumSlots {
		t.Errorf("expected numSlots=%d for negative, got %d", DefaultNumSlots, s2.numSlots)
	}
}

// TestTxnStateMigrateLayoutFromSmallerNumSlots is the v0.1.83 →
// v0.1.84 upgrade test: existing clusters wrote slot files keyed
// by hash(txnID) % broker_replicas (typically 3). When the new
// version pins numSlots=50, every entry needs to relocate to its
// new slot. The migration must:
//
//  1. Move every entry to its expected slot under the new numSlots.
//  2. Delete out-of-range slot files (slot index ≥ numSlots).
//  3. Preserve every (PID, epoch) — losing this would silently
//     reset transactional producers' epoch counters and break the
//     gh #22 fence-on-rejoin contract on the upgrade boundary.
func TestTxnStateMigrateLayoutFromSmallerNumSlots(t *testing.T) {
	dir := t.TempDir()
	const oldNumSlots = 3

	// Seed a v0.1.83-style on-disk layout: 3 slot files keyed by
	// hash(txnID) % 3.
	old, err := NewTxnStateStore(dir, oldNumSlots)
	if err != nil {
		t.Fatalf("seed store: %v", err)
	}
	alloc, _ := allocCounter()
	want := map[string]TxnEntry{}
	for _, txnID := range []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta"} {
		pid, epoch, err := old.GetOrAllocate(txnID, alloc)
		if err != nil {
			t.Fatalf("seed alloc %s: %v", txnID, err)
		}
		want[txnID] = TxnEntry{PID: pid, Epoch: epoch}
	}

	// Reopen with the new pinned numSlots=50 — triggers
	// migrateLayout on the existing dir.
	migrated, err := NewTxnStateStore(dir, 50)
	if err != nil {
		t.Fatalf("migrated open: %v", err)
	}
	if migrated.numSlots != 50 {
		t.Fatalf("post-migration numSlots=%d, want 50", migrated.numSlots)
	}

	// Every prior entry must be readable and unchanged.
	snap := migrated.Snapshot()
	for txnID, expected := range want {
		got, ok := snap[txnID]
		if !ok {
			t.Errorf("entry %q lost during migration", txnID)
			continue
		}
		if got.PID != expected.PID || got.Epoch != expected.Epoch {
			t.Errorf("%q migrated incorrectly: got %+v, want %+v", txnID, got, expected)
		}
	}

	// All slot files on disk must now be in [0, 50). The old
	// slot-0/1/2.json files are still valid indices under
	// numSlots=50, so they may persist if some entry still hashes
	// there — but no slot index ≥ 50 should exist. The stronger
	// invariant — every file's contents hash to its index — is
	// covered below.
	slots, err := migrated.activeSlots()
	if err != nil {
		t.Fatalf("activeSlots: %v", err)
	}
	for _, n := range slots {
		if n >= 50 {
			t.Errorf("out-of-range slot file slot-%d.json present after migration", n)
		}
	}

	// Every entry on disk must hash to its file's slot under the
	// current numSlots. Catches a partial migration that left
	// entries in their old slots.
	for _, n := range slots {
		state, err := migrated.loadSlot(n)
		if err != nil {
			t.Fatalf("loadSlot %d: %v", n, err)
		}
		for txnID := range state {
			if got := migrated.slotFor(txnID); got != n {
				t.Errorf("entry %q in slot-%d.json but hashes to slot %d", txnID, n, got)
			}
		}
	}
}

// TestTxnStateMigrateLayoutFromLargerNumSlots is the inverse of the
// upgrade test: a cluster previously running with numSlots=50 (or
// any larger value) is reopened with numSlots=3. Out-of-range slot
// files (slot index ≥ numSlots) must be removed; entries inside
// must relocate to a valid slot. Exercises the
// "outOfRange = n >= s.numSlots" branch in migrateLayout.
func TestTxnStateMigrateLayoutFromLargerNumSlots(t *testing.T) {
	dir := t.TempDir()
	const oldNumSlots = 50
	const newNumSlots = 3

	old, _ := NewTxnStateStore(dir, oldNumSlots)
	alloc, _ := allocCounter()
	want := map[string]TxnEntry{}
	for _, txnID := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		pid, epoch, err := old.GetOrAllocate(txnID, alloc)
		if err != nil {
			t.Fatalf("seed: %v", err)
		}
		want[txnID] = TxnEntry{PID: pid, Epoch: epoch}
	}

	// Confirm the seed produced at least one slot file with index
	// ≥ 3 (otherwise the test wouldn't actually exercise the
	// out-of-range branch).
	preSlots, _ := old.activeSlots()
	hasOutOfRange := false
	for _, n := range preSlots {
		if n >= newNumSlots {
			hasOutOfRange = true
			break
		}
	}
	if !hasOutOfRange {
		t.Skip("seed didn't land any entry in slot ≥ 3 — test would be vacuous")
	}

	migrated, err := NewTxnStateStore(dir, newNumSlots)
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// All entries still readable.
	snap := migrated.Snapshot()
	for txnID, expected := range want {
		got, ok := snap[txnID]
		if !ok {
			t.Errorf("entry %q lost during shrink-migration", txnID)
			continue
		}
		if !txnEntriesEqual(got, expected) {
			t.Errorf("%q changed: got %+v, want %+v", txnID, got, expected)
		}
	}

	// No slot files with index ≥ newNumSlots.
	postSlots, _ := migrated.activeSlots()
	for _, n := range postSlots {
		if n >= newNumSlots {
			t.Errorf("out-of-range slot-%d.json survived shrink-migration", n)
		}
	}
}

// TestTxnStateMigrateLayoutHandlesCorruptSlotFile guards graceful
// handling of a slot file with garbage content (e.g. a half-written
// crash, manual edit). Migration should surface a real error rather
// than silently discarding the file's entries — an operator should
// know they have a corrupt slot to investigate.
func TestTxnStateMigrateLayoutHandlesCorruptSlotFile(t *testing.T) {
	dir := t.TempDir()
	// Open + close once to create the dir layout.
	if _, err := NewTxnStateStore(dir, 3); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Drop a corrupt JSON file directly in the slot dir.
	slotDir := filepath.Join(dir, "txn_state")
	corruptPath := filepath.Join(slotDir, "slot-1.json")
	if err := os.WriteFile(corruptPath, []byte("{not json"), 0o644); err != nil {
		t.Fatalf("corrupt: %v", err)
	}
	// Reopen — migration should error rather than silently dropping
	// the corrupt entries.
	_, err := NewTxnStateStore(dir, 50)
	if err == nil {
		t.Fatal("expected error when slot file is corrupt")
	}
}

// TestTxnStateNumSlotsOne pins behaviour at the degenerate edge: a
// single-broker dev cluster (or operator that explicitly sets
// numSlots=1) collapses every txnID to slot 0. Every operation
// must still work. Catches a divide-by-zero / off-by-one in
// slotFor or the migration logic.
func TestTxnStateNumSlotsOne(t *testing.T) {
	dir := t.TempDir()
	s, err := NewTxnStateStore(dir, 1)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	alloc, _ := allocCounter()
	for _, txnID := range []string{"alpha", "beta", "gamma"} {
		pid, ep, err := s.GetOrAllocate(txnID, alloc)
		if err != nil {
			t.Fatalf("alloc %s: %v", txnID, err)
		}
		if ep != 0 {
			t.Errorf("first alloc of %s: epoch=%d, want 0", txnID, ep)
		}
		if pid <= 0 {
			t.Errorf("PID for %s = %d, want positive", txnID, pid)
		}
	}
	// Exactly one slot file should exist.
	slots, _ := s.activeSlots()
	if len(slots) != 1 || slots[0] != 0 {
		t.Errorf("expected exactly slot-0.json with numSlots=1, got %v", slots)
	}
	// Re-open with numSlots=50 — every entry hashes to its real
	// slot and slot-0 likely empties out (depends on FNV
	// distribution). Confirm at least that nothing is lost.
	pre := s.Snapshot()
	wide, err := NewTxnStateStore(dir, 50)
	if err != nil {
		t.Fatalf("widen: %v", err)
	}
	post := wide.Snapshot()
	if len(pre) != len(post) {
		t.Errorf("entry count changed across widen: %d → %d", len(pre), len(post))
	}
}

// TestTxnStateMigrateLayoutNoOpOnFreshDir: opening a fresh dir
// (no slot files at all) must not fail and must not produce
// spurious empty slot files. Catches a bug where the migration
// eagerly walks an empty dir and crashes on a nil iteration.
func TestTxnStateMigrateLayoutNoOpOnFreshDir(t *testing.T) {
	dir := t.TempDir()
	s, err := NewTxnStateStore(dir, 50)
	if err != nil {
		t.Fatalf("fresh open: %v", err)
	}
	slots, err := s.activeSlots()
	if err != nil {
		t.Fatalf("activeSlots: %v", err)
	}
	if len(slots) != 0 {
		t.Errorf("fresh dir should have 0 slot files, got %d: %v", len(slots), slots)
	}
}

// TestTxnStateMigrateLayoutPreservesEpochOnConflict: if two slot
// files happen to contain the SAME txnID (shouldn't normally
// occur, but defensive against operator mistakes — e.g. a manual
// rename or a partial migration crash that left dual writes), the
// migration must keep the higher-epoch entry. Losing the higher
// epoch would silently allow zombie writes from an older session.
func TestTxnStateMigrateLayoutPreservesEpochOnConflict(t *testing.T) {
	dir := t.TempDir()
	// Create the slot dir layout.
	if _, err := NewTxnStateStore(dir, 3); err != nil {
		t.Fatalf("seed: %v", err)
	}
	slotDir := filepath.Join(dir, "txn_state")
	// Write the SAME txnID into two different slot files with
	// different epochs. Defensive scenario: which epoch wins on
	// merge?
	older := map[string]TxnEntry{"foo": {PID: 100, Epoch: 5}}
	newer := map[string]TxnEntry{"foo": {PID: 100, Epoch: 9}}
	for _, pair := range []struct {
		path  string
		state map[string]TxnEntry
	}{
		{filepath.Join(slotDir, "slot-0.json"), older},
		{filepath.Join(slotDir, "slot-1.json"), newer},
	} {
		data, _ := json.Marshal(pair.state)
		if err := os.WriteFile(pair.path, data, 0o644); err != nil {
			t.Fatalf("seed write: %v", err)
		}
	}

	// Reopen with numSlots=50 — both slot files relocate, the
	// merge must keep the higher-epoch (9) entry.
	s, err := NewTxnStateStore(dir, 50)
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	got := s.Snapshot()["foo"]
	if got.PID != 100 || got.Epoch != 9 {
		t.Errorf("conflict resolution wrong: got %+v, want PID=100 Epoch=9 (higher epoch wins)", got)
	}
}

// TestTxnStateMigrateLayoutIsIdempotent: running migration on an
// already-correct layout must be a no-op. Catches a bug where the
// migration eagerly rewrites every file even when nothing changed
// — wasteful and risk-prone under concurrent reads.
func TestTxnStateMigrateLayoutIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	s1, _ := NewTxnStateStore(dir, 50)
	alloc, _ := allocCounter()
	for i := 0; i < 5; i++ {
		txnID := "txn-" + strconv.Itoa(i)
		if _, _, err := s1.GetOrAllocate(txnID, alloc); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	// Reopen — migration should detect "every entry already in the
	// right slot" and not move anything.
	s2, err := NewTxnStateStore(dir, 50)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	snap1 := s1.Snapshot()
	snap2 := s2.Snapshot()
	if len(snap1) != len(snap2) {
		t.Errorf("entry count diverged after idempotent re-migration: %d → %d", len(snap1), len(snap2))
	}
	for k, v := range snap1 {
		if got := snap2[k]; !txnEntriesEqual(got, v) {
			t.Errorf("idempotent migration altered %q: %+v → %+v", k, v, got)
		}
	}
}

// TestTxnStateSlotFileLayout asserts the on-disk shape: per-slot JSON
// files under <dataDir>/__cluster/txn_state/. Catches a refactor that
// accidentally puts every entry in slot-0 (defeats failover) or
// elsewhere on disk.
func TestTxnStateSlotFileLayout(t *testing.T) {
	dir := t.TempDir()
	const numSlots = 4
	s, _ := NewTxnStateStore(dir, numSlots)
	alloc, _ := allocCounter()

	// Spread enough txnIDs that we get hits in multiple slots.
	for i := 0; i < 32; i++ {
		txnID := "txn-" + strconv.Itoa(i)
		if _, _, err := s.GetOrAllocate(txnID, alloc); err != nil {
			t.Fatalf("alloc %s: %v", txnID, err)
		}
	}

	slots, err := s.activeSlots()
	if err != nil {
		t.Fatalf("activeSlots: %v", err)
	}
	if len(slots) < 2 {
		t.Errorf("expected ≥2 slot files for 32 txnIDs across 4 slots, got %d", len(slots))
	}
	for _, n := range slots {
		if n < 0 || n >= numSlots {
			t.Errorf("slot index %d out of range [0,%d)", n, numSlots)
		}
	}
}

// TestAddPartitionsRejectsEmptyTxnID pins the gh #23 input check.
// Apache Kafka's handleAddPartitionsToTransaction returns
// INVALID_REQUEST for an empty/null transactionalId; the storage
// layer surfaces a sentinel that the handler maps to the wire code.
func TestAddPartitionsRejectsEmptyTxnID(t *testing.T) {
	dir := t.TempDir()
	s, err := NewTxnStateStore(dir, 50)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := s.AddPartitions("", 1, 0, []TxnTopic{{Topic: "t", Partitions: []int32{0}}}); err == nil {
		t.Fatal("expected ErrEmptyTxnID, got nil")
	}
}

// TestAddPartitionsRejectsUnknownTxn: AddPartitions before
// InitProducerId should fail with ErrTxnUnknownProducer (handler
// maps to INVALID_PRODUCER_ID_MAPPING). Apache equivalent:
// `getTransactionState(transactionalId)` returns None.
func TestAddPartitionsRejectsUnknownTxn(t *testing.T) {
	dir := t.TempDir()
	s, err := NewTxnStateStore(dir, 50)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	err = s.AddPartitions("never-init", 100, 0, []TxnTopic{{Topic: "t", Partitions: []int32{0}}})
	if err != ErrTxnUnknownProducer {
		t.Fatalf("got %v, want ErrTxnUnknownProducer", err)
	}
}

// TestAddPartitionsRejectsPIDMismatch: the (txnID, PID) tuple must
// match the persisted entry exactly. A wrong PID could mean a stale
// session that pre-dates an epoch rotation; surface
// ErrTxnUnknownProducer so the handler returns
// INVALID_PRODUCER_ID_MAPPING (Apache parity).
func TestAddPartitionsRejectsPIDMismatch(t *testing.T) {
	dir := t.TempDir()
	s, err := NewTxnStateStore(dir, 50)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	alloc, _ := allocCounter()
	pid, epoch, err := s.GetOrAllocate("tx-pid-mismatch", alloc)
	if err != nil {
		t.Fatalf("alloc: %v", err)
	}
	err = s.AddPartitions("tx-pid-mismatch", pid+999, epoch, []TxnTopic{{Topic: "t", Partitions: []int32{0}}})
	if err != ErrTxnUnknownProducer {
		t.Fatalf("got %v, want ErrTxnUnknownProducer", err)
	}
}

// TestAddPartitionsRejectsEpochMismatch: a stale-epoch caller
// (zombie session that didn't see the rejoin) gets fenced.
// Apache equivalent: PRODUCER_FENCED.
func TestAddPartitionsRejectsEpochMismatch(t *testing.T) {
	dir := t.TempDir()
	s, err := NewTxnStateStore(dir, 50)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	alloc, _ := allocCounter()
	pid, epoch, err := s.GetOrAllocate("tx-epoch-mismatch", alloc)
	if err != nil {
		t.Fatalf("alloc: %v", err)
	}
	err = s.AddPartitions("tx-epoch-mismatch", pid, epoch-1, []TxnTopic{{Topic: "t", Partitions: []int32{0}}})
	if err != ErrTxnEpochFenced {
		t.Fatalf("got %v, want ErrTxnEpochFenced", err)
	}
}

// TestAddPartitionsHappyPathPersists: a successful add unions the
// partitions into the entry and persists the slot file.
func TestAddPartitionsHappyPathPersists(t *testing.T) {
	dir := t.TempDir()
	s, err := NewTxnStateStore(dir, 50)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	alloc, _ := allocCounter()
	pid, epoch, err := s.GetOrAllocate("tx-happy", alloc)
	if err != nil {
		t.Fatalf("alloc: %v", err)
	}
	err = s.AddPartitions("tx-happy", pid, epoch, []TxnTopic{
		{Topic: "alpha", Partitions: []int32{0, 4}},
		{Topic: "beta", Partitions: []int32{2}},
	})
	if err != nil {
		t.Fatalf("addPartitions: %v", err)
	}

	// Reopen the store from disk to confirm persistence.
	s2, err := NewTxnStateStore(dir, 50)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	snap := s2.Snapshot()
	got, ok := snap["tx-happy"]
	if !ok {
		t.Fatal("entry lost across reopen")
	}
	if got.PID != pid || got.Epoch != epoch {
		t.Errorf("(PID, epoch) = (%d, %d), want (%d, %d)", got.PID, got.Epoch, pid, epoch)
	}
	if len(got.Partitions) != 2 {
		t.Fatalf("expected 2 topic entries, got %d: %+v", len(got.Partitions), got.Partitions)
	}
}

// TestAddPartitionsIdempotentReAdd is Apache's `subsetOf` shortcut:
// re-adding a partition that's already in the entry returns nil
// without rewriting the slot file. We can't easily observe "did it
// rewrite?" from the public API, but we CAN observe that the
// entry is unchanged and a follow-up call still sees the same
// state.
func TestAddPartitionsIdempotentReAdd(t *testing.T) {
	dir := t.TempDir()
	s, err := NewTxnStateStore(dir, 50)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	alloc, _ := allocCounter()
	pid, epoch, _ := s.GetOrAllocate("tx-idem", alloc)
	additions := []TxnTopic{{Topic: "t", Partitions: []int32{0, 1, 2}}}

	if err := s.AddPartitions("tx-idem", pid, epoch, additions); err != nil {
		t.Fatalf("first add: %v", err)
	}
	// Re-add the same set — should be a no-op success.
	if err := s.AddPartitions("tx-idem", pid, epoch, additions); err != nil {
		t.Fatalf("second add (idempotent): %v", err)
	}
	// Add a partial-overlap set: one new partition (3), two already
	// present (1, 2). Should succeed and union to {0,1,2,3}.
	if err := s.AddPartitions("tx-idem", pid, epoch, []TxnTopic{
		{Topic: "t", Partitions: []int32{1, 2, 3}},
	}); err != nil {
		t.Fatalf("partial-overlap: %v", err)
	}

	got := s.Snapshot()["tx-idem"]
	if len(got.Partitions) != 1 {
		t.Fatalf("expected 1 topic entry, got %d", len(got.Partitions))
	}
	if got.Partitions[0].Topic != "t" {
		t.Fatalf("topic=%q, want t", got.Partitions[0].Topic)
	}
	want := map[int32]bool{0: true, 1: true, 2: true, 3: true}
	for _, p := range got.Partitions[0].Partitions {
		if !want[p] {
			t.Errorf("unexpected partition %d in union", p)
		}
		delete(want, p)
	}
	if len(want) != 0 {
		t.Errorf("missing partitions: %v", want)
	}
}

// TestAddPartitionsAcrossMultipleTopics confirms different topics
// in one call produce independent entries in entry.Partitions.
func TestAddPartitionsAcrossMultipleTopics(t *testing.T) {
	dir := t.TempDir()
	s, err := NewTxnStateStore(dir, 50)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	alloc, _ := allocCounter()
	pid, epoch, _ := s.GetOrAllocate("tx-multi", alloc)
	if err := s.AddPartitions("tx-multi", pid, epoch, []TxnTopic{
		{Topic: "alpha", Partitions: []int32{0}},
		{Topic: "beta", Partitions: []int32{0, 1}},
		{Topic: "gamma", Partitions: []int32{42}},
	}); err != nil {
		t.Fatalf("addPartitions: %v", err)
	}
	got := s.Snapshot()["tx-multi"]
	if len(got.Partitions) != 3 {
		t.Fatalf("expected 3 topic entries, got %d", len(got.Partitions))
	}
}

// TestEndTxnHappyCommit pins the gh #25 commit path: Ongoing →
// CompleteCommit, partition list cleared, persisted to disk.
func TestEndTxnHappyCommit(t *testing.T) {
	dir := t.TempDir()
	s, err := NewTxnStateStore(dir, 50)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	alloc, _ := allocCounter()
	pid, epoch, _ := s.GetOrAllocate("tx-commit", alloc)
	if err := s.AddPartitions("tx-commit", pid, epoch, []TxnTopic{
		{Topic: "t", Partitions: []int32{0, 1}},
	}); err != nil {
		t.Fatalf("addPartitions: %v", err)
	}
	if err := s.EndTxn("tx-commit", pid, epoch, true); err != nil {
		t.Fatalf("endTxn: %v", err)
	}

	// Reopen to confirm persistence.
	s2, err := NewTxnStateStore(dir, 50)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	got := s2.Snapshot()["tx-commit"]
	if got.State != TxnStateCompleteCommit {
		t.Errorf("state=%q, want %q", got.State, TxnStateCompleteCommit)
	}
	if len(got.Partitions) != 0 {
		t.Errorf("partitions should be cleared on commit, got %+v", got.Partitions)
	}
	if got.PID != pid || got.Epoch != epoch {
		t.Errorf("(PID, epoch) drifted: got (%d, %d), want (%d, %d)",
			got.PID, got.Epoch, pid, epoch)
	}
}

// TestEndTxnHappyAbort: same shape, abort=false → CompleteAbort.
func TestEndTxnHappyAbort(t *testing.T) {
	dir := t.TempDir()
	s, err := NewTxnStateStore(dir, 50)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	alloc, _ := allocCounter()
	pid, epoch, _ := s.GetOrAllocate("tx-abort", alloc)
	if err := s.AddPartitions("tx-abort", pid, epoch, []TxnTopic{
		{Topic: "t", Partitions: []int32{0}},
	}); err != nil {
		t.Fatalf("addPartitions: %v", err)
	}
	if err := s.EndTxn("tx-abort", pid, epoch, false); err != nil {
		t.Fatalf("endTxn: %v", err)
	}
	got := s.Snapshot()["tx-abort"]
	if got.State != TxnStateCompleteAbort {
		t.Errorf("state=%q, want %q", got.State, TxnStateCompleteAbort)
	}
	if len(got.Partitions) != 0 {
		t.Errorf("partitions should be cleared on abort, got %+v", got.Partitions)
	}
}

// TestEndTxnIdempotentCommitRetry: post-CompleteCommit retry returns
// nil (NONE) without re-writing. Mirrors Apache's
// "CompleteCommit + commit → NONE" branch.
func TestEndTxnIdempotentCommitRetry(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewTxnStateStore(dir, 50)
	alloc, _ := allocCounter()
	pid, epoch, _ := s.GetOrAllocate("tx-rep", alloc)
	_ = s.AddPartitions("tx-rep", pid, epoch, []TxnTopic{{Topic: "t", Partitions: []int32{0}}})
	if err := s.EndTxn("tx-rep", pid, epoch, true); err != nil {
		t.Fatalf("first commit: %v", err)
	}
	// Retry — same action, post-Complete state. Must succeed.
	if err := s.EndTxn("tx-rep", pid, epoch, true); err != nil {
		t.Fatalf("idempotent retry: %v", err)
	}
}

// TestEndTxnAbortAfterCommitRejected: mismatched action against a
// completed txn is INVALID_TXN_STATE. Apache's "Complete* + opposite
// action → invalid" branch.
func TestEndTxnAbortAfterCommitRejected(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewTxnStateStore(dir, 50)
	alloc, _ := allocCounter()
	pid, epoch, _ := s.GetOrAllocate("tx-mix", alloc)
	_ = s.AddPartitions("tx-mix", pid, epoch, []TxnTopic{{Topic: "t", Partitions: []int32{0}}})
	_ = s.EndTxn("tx-mix", pid, epoch, true) // commit first
	if err := s.EndTxn("tx-mix", pid, epoch, false); err != ErrTxnInvalidState {
		t.Fatalf("abort-after-commit: got %v, want ErrTxnInvalidState", err)
	}
}

// TestEndTxnNoStartedTxnRejected: EndTxn against an Empty entry
// (InitProducerId without AddPartitionsToTxn).
func TestEndTxnNoStartedTxnRejected(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewTxnStateStore(dir, 50)
	alloc, _ := allocCounter()
	pid, epoch, _ := s.GetOrAllocate("tx-unstarted", alloc)
	if err := s.EndTxn("tx-unstarted", pid, epoch, true); err != ErrTxnInvalidState {
		t.Fatalf("got %v, want ErrTxnInvalidState (no Ongoing txn to end)", err)
	}
}

// TestEndTxnRejectsEpochMismatch: stale-epoch caller gets fenced.
func TestEndTxnRejectsEpochMismatch(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewTxnStateStore(dir, 50)
	alloc, _ := allocCounter()
	pid, epoch, _ := s.GetOrAllocate("tx-fence", alloc)
	_ = s.AddPartitions("tx-fence", pid, epoch, []TxnTopic{{Topic: "t", Partitions: []int32{0}}})
	if err := s.EndTxn("tx-fence", pid, epoch-1, true); err != ErrTxnEpochFenced {
		t.Fatalf("got %v, want ErrTxnEpochFenced", err)
	}
}

// TestEndTxnRejectsPIDMismatch: wrong PID.
func TestEndTxnRejectsPIDMismatch(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewTxnStateStore(dir, 50)
	alloc, _ := allocCounter()
	pid, epoch, _ := s.GetOrAllocate("tx-wrong-pid", alloc)
	_ = s.AddPartitions("tx-wrong-pid", pid, epoch, []TxnTopic{{Topic: "t", Partitions: []int32{0}}})
	if err := s.EndTxn("tx-wrong-pid", pid+999, epoch, true); err != ErrTxnUnknownProducer {
		t.Fatalf("got %v, want ErrTxnUnknownProducer", err)
	}
}

// TestEndTxnEmptyIDRejected: input validation.
func TestEndTxnEmptyIDRejected(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewTxnStateStore(dir, 50)
	if err := s.EndTxn("", 1, 0, true); err != ErrEmptyTxnID {
		t.Fatalf("got %v, want ErrEmptyTxnID", err)
	}
}

// TestNewTxnAfterCompleteCommit: after a successful commit, a fresh
// AddPartitionsToTxn must transition state back to Ongoing for the
// next transaction. Apache's CompleteCommit → Ongoing transition.
func TestNewTxnAfterCompleteCommit(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewTxnStateStore(dir, 50)
	alloc, _ := allocCounter()
	pid, epoch, _ := s.GetOrAllocate("tx-recycle", alloc)
	_ = s.AddPartitions("tx-recycle", pid, epoch, []TxnTopic{{Topic: "t", Partitions: []int32{0}}})
	_ = s.EndTxn("tx-recycle", pid, epoch, true)
	// Second transaction reuses the same (PID, epoch).
	if err := s.AddPartitions("tx-recycle", pid, epoch, []TxnTopic{
		{Topic: "t", Partitions: []int32{5}},
	}); err != nil {
		t.Fatalf("second txn AddPartitions: %v", err)
	}
	got := s.Snapshot()["tx-recycle"]
	if got.State != TxnStateOngoing {
		t.Errorf("state=%q, want Ongoing (CompleteCommit → Ongoing on new AddPartitions)", got.State)
	}
	if len(got.Partitions) != 1 || got.Partitions[0].Partitions[0] != 5 {
		t.Errorf("new-txn partitions wrong: %+v", got.Partitions)
	}
}

// TestAddOffsetsToTxnHappyPath: tracks groupID in entry.Groups,
// transitions Empty/Complete* → Ongoing. gh #24.
func TestAddOffsetsToTxnHappyPath(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewTxnStateStore(dir, 50)
	alloc, _ := allocCounter()
	pid, epoch, _ := s.GetOrAllocate("tx-offsets", alloc)
	if err := s.AddOffsetsToTxn("tx-offsets", pid, epoch, "consumer-group-A"); err != nil {
		t.Fatalf("addOffsetsToTxn: %v", err)
	}
	got := s.Snapshot()["tx-offsets"]
	if got.State != TxnStateOngoing {
		t.Errorf("state=%q, want Ongoing", got.State)
	}
	if len(got.Groups) != 1 || got.Groups[0] != "consumer-group-A" {
		t.Errorf("groups=%+v, want [consumer-group-A]", got.Groups)
	}
}

// TestAddOffsetsToTxnIdempotent: re-adding same group is no-op.
func TestAddOffsetsToTxnIdempotent(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewTxnStateStore(dir, 50)
	alloc, _ := allocCounter()
	pid, epoch, _ := s.GetOrAllocate("tx-idem", alloc)
	_ = s.AddOffsetsToTxn("tx-idem", pid, epoch, "cg-1")
	if err := s.AddOffsetsToTxn("tx-idem", pid, epoch, "cg-1"); err != nil {
		t.Fatalf("idempotent re-add: %v", err)
	}
	got := s.Snapshot()["tx-idem"]
	if len(got.Groups) != 1 {
		t.Errorf("duplicate add added group twice: %+v", got.Groups)
	}
}

// TestAddOffsetsToTxnEmptyGroupID: gh #24 input validation.
func TestAddOffsetsToTxnEmptyGroupID(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewTxnStateStore(dir, 50)
	alloc, _ := allocCounter()
	pid, epoch, _ := s.GetOrAllocate("tx-no-group", alloc)
	if err := s.AddOffsetsToTxn("tx-no-group", pid, epoch, ""); err != ErrTxnInvalidState {
		t.Fatalf("got %v, want ErrTxnInvalidState (empty groupID)", err)
	}
}

// TestAddOffsetsToTxnEpochFenced.
func TestAddOffsetsToTxnEpochFenced(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewTxnStateStore(dir, 50)
	alloc, _ := allocCounter()
	pid, epoch, _ := s.GetOrAllocate("tx-epoch", alloc)
	if err := s.AddOffsetsToTxn("tx-epoch", pid, epoch-1, "cg"); err != ErrTxnEpochFenced {
		t.Fatalf("got %v, want ErrTxnEpochFenced", err)
	}
}

// TestEndTxnFiresOffsetHook: when commit fires, the hook receives
// each (groupID, pid, true). When abort, (groupID, pid, false).
func TestEndTxnFiresOffsetHook(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewTxnStateStore(dir, 50)
	alloc, _ := allocCounter()
	pid, epoch, _ := s.GetOrAllocate("tx-hook", alloc)
	_ = s.AddOffsetsToTxn("tx-hook", pid, epoch, "cg-A")
	_ = s.AddOffsetsToTxn("tx-hook", pid, epoch, "cg-B")

	var calls []struct {
		group  string
		pid    int64
		commit bool
	}
	s.SetTxnOffsetHook(func(group string, p int64, commit bool) {
		calls = append(calls, struct {
			group  string
			pid    int64
			commit bool
		}{group, p, commit})
	})

	if err := s.EndTxn("tx-hook", pid, epoch, true); err != nil {
		t.Fatalf("endTxn: %v", err)
	}
	if len(calls) != 2 {
		t.Fatalf("expected 2 hook calls, got %d: %+v", len(calls), calls)
	}
	gotGroups := map[string]bool{calls[0].group: true, calls[1].group: true}
	if !gotGroups["cg-A"] || !gotGroups["cg-B"] {
		t.Errorf("missing groups in hook calls: %+v", calls)
	}
	for _, c := range calls {
		if c.pid != pid || !c.commit {
			t.Errorf("call mismatch: %+v want pid=%d commit=true", c, pid)
		}
	}

	// Groups list cleared after EndTxn.
	got := s.Snapshot()["tx-hook"]
	if len(got.Groups) != 0 {
		t.Errorf("groups should be cleared post-commit: %+v", got.Groups)
	}
}

// TestEndTxnAbortFiresOffsetHookWithCommitFalse.
func TestEndTxnAbortFiresOffsetHookWithCommitFalse(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewTxnStateStore(dir, 50)
	alloc, _ := allocCounter()
	pid, epoch, _ := s.GetOrAllocate("tx-abort-hook", alloc)
	_ = s.AddOffsetsToTxn("tx-abort-hook", pid, epoch, "cg-X")

	var commits []bool
	s.SetTxnOffsetHook(func(_ string, _ int64, commit bool) {
		commits = append(commits, commit)
	})
	if err := s.EndTxn("tx-abort-hook", pid, epoch, false); err != nil {
		t.Fatalf("endTxn abort: %v", err)
	}
	if len(commits) != 1 || commits[0] {
		t.Errorf("expected one hook call with commit=false, got %+v", commits)
	}
}

// TestAbortOverdue_AgesOutCrashedProducer pins the gh #28 reaper
// contract: an Ongoing transaction whose OngoingSinceMs+timeoutMs is
// before nowMs is transitioned to CompleteAbort, its partition list
// cleared, and its epoch bumped — same fencing signal a clean
// EndTxn(abort) produces.
func TestAbortOverdue_AgesOutCrashedProducer(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewTxnStateStore(dir, 3)

	prev := nowUnixMillis
	nowUnixMillis = func() int64 { return 1_000 }
	defer func() { nowUnixMillis = prev }()

	alloc, _ := allocCounter()
	pid, epoch, err := s.GetOrAllocateWithTimeout("crashed-tx", 60_000, alloc)
	if err != nil {
		t.Fatalf("alloc: %v", err)
	}

	// Stamp OngoingSinceMs=1000 via AddPartitions.
	if err := s.AddPartitions("crashed-tx", pid, epoch, []TxnTopic{
		{Topic: "events", Partitions: []int32{0}},
	}); err != nil {
		t.Fatalf("addPartitions: %v", err)
	}

	// nowMs well past 1000 + 60000 — reaper should fire.
	got := s.AbortOverdue(1_000 + 60_000 + 1)
	if len(got) != 1 {
		t.Fatalf("AbortOverdue returned %d records, want 1: %+v", len(got), got)
	}
	rec := got[0]
	if rec.TxnID != "crashed-tx" {
		t.Errorf("aborted TxnID=%q, want %q", rec.TxnID, "crashed-tx")
	}
	if rec.PID != pid {
		t.Errorf("aborted PID=%d, want %d", rec.PID, pid)
	}
	if rec.NewEpoch != rec.OldEpoch+1 {
		t.Errorf("epoch not bumped: old=%d new=%d", rec.OldEpoch, rec.NewEpoch)
	}

	entry := s.Snapshot()["crashed-tx"]
	if entry.State != TxnStateCompleteAbort {
		t.Errorf("post-abort state=%q, want CompleteAbort", entry.State)
	}
	if entry.OngoingSinceMs != 0 {
		t.Errorf("OngoingSinceMs should be cleared, got %d", entry.OngoingSinceMs)
	}
	if len(entry.Partitions) != 0 {
		t.Errorf("Partitions should be cleared, got %+v", entry.Partitions)
	}
}

// TestAbortOverdue_SkipsActiveTxn keeps the reaper honest — a txn
// still inside its window must NOT be aborted, otherwise a slow
// producer gets nuked mid-commit.
func TestAbortOverdue_SkipsActiveTxn(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewTxnStateStore(dir, 3)

	prev := nowUnixMillis
	nowUnixMillis = func() int64 { return 1_000 }
	defer func() { nowUnixMillis = prev }()

	alloc, _ := allocCounter()
	pid, epoch, _ := s.GetOrAllocateWithTimeout("live-tx", 60_000, alloc)
	_ = s.AddPartitions("live-tx", pid, epoch, []TxnTopic{
		{Topic: "events", Partitions: []int32{0}},
	})

	// nowMs only 30s past start — well inside the 60s budget.
	if got := s.AbortOverdue(1_000 + 30_000); len(got) != 0 {
		t.Errorf("AbortOverdue fired on live txn: %+v", got)
	}
	entry := s.Snapshot()["live-tx"]
	if entry.State != TxnStateOngoing {
		t.Errorf("state changed under reaper: got %q want Ongoing", entry.State)
	}
}

// TestAbortOverdue_SkipsCompletedTxn protects against the
// reaper-fires-twice race: an already-committed/aborted txn must not
// re-trip the sweep, otherwise a subsequent re-init would see a
// surprise epoch jump.
func TestAbortOverdue_SkipsCompletedTxn(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewTxnStateStore(dir, 3)
	prev := nowUnixMillis
	nowUnixMillis = func() int64 { return 1_000 }
	defer func() { nowUnixMillis = prev }()

	alloc, _ := allocCounter()
	pid, epoch, _ := s.GetOrAllocateWithTimeout("done-tx", 60_000, alloc)
	_ = s.AddPartitions("done-tx", pid, epoch, []TxnTopic{
		{Topic: "events", Partitions: []int32{0}},
	})
	if err := s.EndTxn("done-tx", pid, epoch, true); err != nil {
		t.Fatalf("endTxn: %v", err)
	}
	priorEntry := s.Snapshot()["done-tx"]

	// Long after deadline. EndTxn cleared OngoingSinceMs, so the
	// reaper must skip even though State no longer matches Ongoing.
	if got := s.AbortOverdue(1_000_000_000); len(got) != 0 {
		t.Errorf("AbortOverdue re-fired on completed txn: %+v", got)
	}
	after := s.Snapshot()["done-tx"]
	if after.Epoch != priorEntry.Epoch || after.State != priorEntry.State {
		t.Errorf("completed txn mutated: before=%+v after=%+v", priorEntry, after)
	}
}

// TestAbortOverdue_NoTimeoutSetIsSkipped keeps pre-#28 entries (and
// any future call path that didn't carry a timeout) from getting
// aborted on the first reaper tick. The TxnEntry was written before
// the field existed and has TransactionTimeoutMs=0.
func TestAbortOverdue_NoTimeoutSetIsSkipped(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewTxnStateStore(dir, 3)
	prev := nowUnixMillis
	nowUnixMillis = func() int64 { return 1_000 }
	defer func() { nowUnixMillis = prev }()

	alloc, _ := allocCounter()
	// Note: zero timeout — caller wasn't a fresh KIP-98 client.
	pid, epoch, _ := s.GetOrAllocateWithTimeout("no-timeout-tx", 0, alloc)
	_ = s.AddPartitions("no-timeout-tx", pid, epoch, []TxnTopic{
		{Topic: "events", Partitions: []int32{0}},
	})

	if got := s.AbortOverdue(999_999_999); len(got) != 0 {
		t.Errorf("AbortOverdue fired on entry with no timeout: %+v", got)
	}
}

// TestAbortOverdue_FiresOffsetHookOnAbort verifies the cross-
// coordinator signal still goes out on a reaper-driven abort — the
// staged TxnOffsetCommit offsets must be discarded the same way
// an explicit EndTxn(abort) would discard them.
func TestAbortOverdue_FiresOffsetHookOnAbort(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewTxnStateStore(dir, 3)
	prev := nowUnixMillis
	nowUnixMillis = func() int64 { return 1_000 }
	defer func() { nowUnixMillis = prev }()

	alloc, _ := allocCounter()
	pid, epoch, _ := s.GetOrAllocateWithTimeout("reaped-tx", 1_000, alloc)
	_ = s.AddOffsetsToTxn("reaped-tx", pid, epoch, "cg-A")
	_ = s.AddOffsetsToTxn("reaped-tx", pid, epoch, "cg-B")

	var got []struct {
		group  string
		commit bool
	}
	s.SetTxnOffsetHook(func(g string, _ int64, commit bool) {
		got = append(got, struct {
			group  string
			commit bool
		}{g, commit})
	})

	s.AbortOverdue(1_000 + 1_000 + 1)
	if len(got) != 2 {
		t.Fatalf("expected 2 hook calls (cg-A, cg-B), got %d: %+v", len(got), got)
	}
	for _, c := range got {
		if c.commit {
			t.Errorf("reaper hook fired with commit=true on %q (should be abort)", c.group)
		}
	}
}
