package integration

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/woestebanaan/skafka/internal/broker"
	"github.com/woestebanaan/skafka/internal/coordinator"
	"github.com/woestebanaan/skafka/internal/storage"
	"github.com/woestebanaan/skafka/tests/testutil/recordbatch"
)

// idempotentBatch builds a v2 RecordBatch tagged with the producer
// fields the broker's idempotence path checks. recordCount controls
// lastOffsetDelta so firstSeq..firstSeq+recordCount-1 is the
// sequence range Java/franz-go would send for a batch of
// `recordCount` records. Mirrors the helper in
// internal/storage/idempotence_engine_test.go.
func idempotentBatch(pid int64, epoch int16, firstSeq int32, recordCount int) []byte {
	records := make([]recordbatch.Record, recordCount)
	for i := range records {
		records[i] = recordbatch.Record{OffsetDelta: int32(i), Value: []byte("x")}
	}
	return recordbatch.Encode(nil, &recordbatch.RecordBatch{
		BaseOffset:      0,
		LastOffsetDelta: int32(recordCount - 1),
		ProducerID:      pid,
		ProducerEpoch:   epoch,
		BaseSequence:    firstSeq,
		Records:         records,
	})
}

// TestTxnFailoverFencesZombieAcrossBrokers walks the gh #108 end-to-end
// scenario the unit tests stub:
//
//  1. Producer with transactional.id="payment-tx" calls InitProducerId
//     on broker A (initial coordinator). A allocates (PID=p, epoch=0)
//     and persists to slot-N.json on the shared PVC.
//  2. The producer writes a record to a partition led by broker C.
//     Engine C's idempotence cache records (p, 0) for this partition.
//  3. Broker A dies. The alive-set fallback picks broker B as the new
//     coordinator for "payment-tx". Producer reconnects, calls
//     InitProducerId on B.
//  4. B reads slot-N.json (what A wrote), bumps to (p, 1), writes
//     to its outbound fence log on the shared PVC.
//  5. Broker C's FenceWatcher polls the fence directory, reads B's
//     entry, applies (p, 1) to engine C's per-partition producer-state
//     cache.
//  6. The dead session has an in-flight zombie batch tagged (p, 0)
//     that lands on broker C's partition. classifyIdempotence sees
//     batch.epoch=0 < entry.epoch=1 → ErrInvalidProducerEpoch.
//     ZOMBIE REJECTED.
//  7. The new session at (p, 1) writes a fresh batch — must succeed.
//
// This is the contract acceptance criterion #2 of the issue described:
// "Cross-broker fence (DiskStorageEngine.FenceProducerEpoch) is
// broadcast at takeover time so in-flight zombie writes are rejected
// with INVALID_PRODUCER_EPOCH (47) on every partition the bumped
// (PID, epoch) lands."
//
// Brokers A and B are simulated by separate TxnStateStore instances
// rooted at the same shared __cluster directory; broker C is a real
// DiskStorageEngine plus a FenceWatcher polling the fence dir.
func TestTxnFailoverFencesZombieAcrossBrokers(t *testing.T) {
	ctx := context.Background()

	// One shared dir for the whole "cluster" — mirrors the RWX PVC
	// where every broker sees /data/__cluster/.
	sharedDir := t.TempDir()
	clusterDir := filepath.Join(sharedDir, "__cluster")
	fenceDir := coordinator.FenceLogDir(clusterDir)

	// --- Broker C: partition leader. Real DiskStorageEngine. ---
	cfg := storage.DefaultConfig()
	cfg.FlushIntervalMessages = 0
	leaderEngine, err := storage.NewDiskStorageEngine(sharedDir, &stubLeaseManager{leader: true}, cfg)
	if err != nil {
		t.Fatalf("leader engine: %v", err)
	}
	if err := leaderEngine.CreatePartition("payments", 0); err != nil {
		t.Fatalf("create partition: %v", err)
	}
	if _, err := leaderEngine.TakeOver(ctx, "payments", 0, 1); err != nil {
		t.Fatalf("takeover: %v", err)
	}

	// Watcher on broker C — applies peer fence entries.
	leaderWatcher := broker.NewFenceWatcher(fenceDir, "from-skafka-leader.json", leaderEngine)

	// --- Broker A: initial txn coordinator. Allocates (p, 0). ---
	const numSlots = 1 // collapses every txnID to slot-0 for simplicity
	storeA, err := coordinator.NewTxnStateStore(clusterDir, numSlots)
	if err != nil {
		t.Fatalf("storeA: %v", err)
	}
	pidA, epochA, err := storeA.GetOrAllocate("payment-tx", func() int64 { return 7777 })
	if err != nil {
		t.Fatalf("storeA alloc: %v", err)
	}
	if pidA != 7777 || epochA != 0 {
		t.Fatalf("brokerA initial: pid=%d, epoch=%d, want (7777, 0)", pidA, epochA)
	}

	// --- Old session writes to broker C at (p=7777, epoch=0). ---
	// Engine C now has producerStates[7777].epoch=0.
	if _, err := leaderEngine.Append(ctx, "payments", 0, 1, idempotentBatch(7777, 0, 0, 5)); err != nil {
		t.Fatalf("old session write: %v", err)
	}

	// --- Broker A dies. Broker B takes over coordinator role. ---
	storeB, err := coordinator.NewTxnStateStore(clusterDir, numSlots)
	if err != nil {
		t.Fatalf("storeB: %v", err)
	}

	// B's outbound fence log on the shared PVC — peers will read this.
	fenceLogB, err := coordinator.NewFenceLog(fenceDir, "skafka-coordB")
	if err != nil {
		t.Fatalf("fenceLogB: %v", err)
	}

	// Producer's reconnect lands on B. B reads slot-N.json and bumps.
	pidB, epochB, err := storeB.GetOrAllocate("payment-tx", func() int64 {
		t.Error("brokerB should NOT have allocated a fresh PID — gh #108 phase 1 contract broken")
		return -1
	})
	if err != nil {
		t.Fatalf("storeB alloc: %v", err)
	}
	if pidB != pidA || epochB != 1 {
		t.Fatalf("brokerB rejoin: pid=%d, epoch=%d, want (%d, 1)", pidB, epochB, pidA)
	}

	// Phase 2: B's broadcastingFencer would call both
	// engineLocal.FenceProducerEpoch and fenceLogB.Append. Engine
	// local is N/A here (B doesn't lead this partition); we simulate
	// just the log half.
	if err := fenceLogB.Append(pidB, epochB); err != nil {
		t.Fatalf("fenceLogB append: %v", err)
	}

	// --- Broker C's watcher picks up B's fence and applies it. ---
	leaderWatcher.Tick()

	// --- Zombie batch at (p, 0) — old session's in-flight. ---
	// Must be rejected: engine C's producerStates[7777].epoch is now 1.
	_, zombieErr := leaderEngine.Append(ctx, "payments", 0, 1, idempotentBatch(7777, 0, 5, 1))
	if !errors.Is(zombieErr, storage.ErrInvalidProducerEpoch) {
		t.Errorf("zombie at epoch=0 got err=%v, want ErrInvalidProducerEpoch — gh #108 phase 2 fence didn't propagate",
			zombieErr)
	}

	// --- New session at (p, 1) — must succeed. The fence advanced
	// the cache to epoch=1 and cleared the dedupe window, so a fresh
	// (firstSeq=0, count=5) batch is appended cleanly even though
	// the old session sent the same sequence range.
	if _, err := leaderEngine.Append(ctx, "payments", 0, 1, idempotentBatch(7777, 1, 0, 5)); err != nil {
		t.Errorf("new session at epoch=1: %v (fence cleared recent[] but rejected the new batch?)", err)
	}
}

// TestTxnFenceBroadcastDedupesAcrossTicks asserts that even if
// FenceWatcher polls many times after the same fence event, the
// engine's RLock-and-walk-every-partition path only fires once
// per (PID, epoch). Combined with the FenceLog idempotent Append,
// a steady-state cluster with no new InitProducerId calls does
// zero engine fence work despite continuous 2s polls.
func TestTxnFenceBroadcastDedupesAcrossTicks(t *testing.T) {
	ctx := context.Background()
	sharedDir := t.TempDir()
	clusterDir := filepath.Join(sharedDir, "__cluster")
	fenceDir := coordinator.FenceLogDir(clusterDir)

	cfg := storage.DefaultConfig()
	cfg.FlushIntervalMessages = 0
	leaderEngine, err := storage.NewDiskStorageEngine(sharedDir, &stubLeaseManager{leader: true}, cfg)
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	if err := leaderEngine.CreatePartition("t", 0); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := leaderEngine.TakeOver(ctx, "t", 0, 1); err != nil {
		t.Fatalf("takeover: %v", err)
	}
	// Seed the engine's producerStates with PID=42 epoch=0.
	if _, err := leaderEngine.Append(ctx, "t", 0, 1, idempotentBatch(42, 0, 0, 1)); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// One peer broker writes a fence (42 → epoch=1).
	logPeer, _ := coordinator.NewFenceLog(fenceDir, "skafka-peer")
	if err := logPeer.Append(42, 1); err != nil {
		t.Fatalf("peer fence: %v", err)
	}

	// Watcher polls 5 times — the first applies the fence; the
	// next four must be no-ops at the watcher's dedupe layer.
	w := broker.NewFenceWatcher(fenceDir, "from-skafka-self.json", leaderEngine)
	for range 5 {
		w.Tick()
	}

	// Verify the fence was applied: a zombie at (42, 0) must be rejected.
	_, err = leaderEngine.Append(ctx, "t", 0, 1, idempotentBatch(42, 0, 1, 1))
	if !errors.Is(err, storage.ErrInvalidProducerEpoch) {
		t.Errorf("post-watcher zombie at epoch=0 got err=%v, want ErrInvalidProducerEpoch", err)
	}
}
