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
