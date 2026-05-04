package storage

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/woestebanaan/skafka/internal/lease"
)

// neverLeaderLeases is the K8s-mode shape post-gh #75: nothing acquires
// per-partition Leases anymore, so KubernetesLeaseManager.IsLeader
// returns false for every partition. We track call counts to assert
// that the engine no longer consults this check on Append.
type neverLeaderLeases struct{ isLeaderCalls atomic.Int64 }

func (n *neverLeaderLeases) Acquire(_ context.Context, _ string, _ int32) error { return nil }
func (n *neverLeaderLeases) Release(_ string, _ int32) error                    { return nil }
func (n *neverLeaderLeases) IsLeader(_ string, _ int32) bool {
	n.isLeaderCalls.Add(1)
	return false
}
func (n *neverLeaderLeases) LeaderFor(_ string, _ int32) int32 { return -1 }
func (n *neverLeaderLeases) WatchLeaders(_ context.Context) (<-chan lease.LeaderChange, error) {
	return make(chan lease.LeaderChange), nil
}

// TestClosePartitionDropsFileHandles guards gh #76: ClosePartition
// must close the partition's open log/index file handles and remove
// it from the engine's in-memory map so the operator's finalizer can
// unlink the directory without hitting NFS .nfsXXXX silly-rename
// EBUSY. Idempotent — calling on an already-closed partition is a
// no-op.
func TestClosePartitionDropsFileHandles(t *testing.T) {
	dir := t.TempDir()
	leases := &neverLeaderLeases{}
	e, err := NewDiskStorageEngine(dir, leases, DefaultConfig())
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	if err := e.CreatePartition("t", 0); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := e.TakeOver(context.Background(), "t", 0, 1); err != nil {
		t.Fatalf("takeover: %v", err)
	}
	if _, ok := e.getPartition("t", 0); !ok {
		t.Fatal("partition should be in the engine map after TakeOver")
	}

	if err := e.ClosePartition("t", 0); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, ok := e.getPartition("t", 0); ok {
		t.Error("partition still in engine map after ClosePartition")
	}

	// Idempotent — second close is a no-op, no error.
	if err := e.ClosePartition("t", 0); err != nil {
		t.Errorf("second close: %v", err)
	}
	// And ClosePartition on a never-opened partition is also a no-op.
	if err := e.ClosePartition("nonexistent", 0); err != nil {
		t.Errorf("close on unknown partition: %v", err)
	}
}

// TestEngineAppendDoesNotConsultLeaseIsLeader guards the gh #75 / 0.1.16
// regression fix: the storage engine's Append must NOT gate on
// lease.LeaseManager.IsLeader. Post-#75, no broker acquires per-partition
// Leases, so that check returns false for every partition. If the engine
// still consulted it, every Produce would surface as
// UnknownServerException to the client — exactly the 0.1.15 production
// bug.
//
// We assert two things:
//  1. Append returns *something other than* ErrNotLeader for a valid
//     partition (TakeOver has run). Pre-fix it returned ErrNotLeader
//     unconditionally; we tolerate a parse error here because
//     constructing a valid Kafka RecordBatch would inflate the test
//     for no extra behavioural coverage.
//  2. IsLeader was never called.
func TestEngineAppendDoesNotConsultLeaseIsLeader(t *testing.T) {
	dir := t.TempDir()
	leases := &neverLeaderLeases{}
	e, err := NewDiskStorageEngine(dir, leases, DefaultConfig())
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	if err := e.CreatePartition("t", 0); err != nil {
		t.Fatalf("create partition: %v", err)
	}
	if _, err := e.TakeOver(context.Background(), "t", 0, 1); err != nil {
		t.Fatalf("takeover: %v", err)
	}

	// 27-byte all-zero buffer is the minimum parseBatchOffsets accepts:
	// baseOffset=0, lastOffsetDelta=0, no CRC validation in this path.
	// Pre-fix, the engine never reached parseBatchOffsets — it short-
	// circuited on lease.IsLeader=false. We assert that the engine now
	// runs *past* that gate. Whether Append ultimately succeeds or
	// fails on the underlying segment write doesn't matter for this
	// test; what matters is that it does NOT return ErrNotLeader.
	batch := make([]byte, 27)
	_, err = e.Append(context.Background(), "t", 0, 1, batch)
	if errors.Is(err, ErrNotLeader) {
		t.Errorf("Append returned ErrNotLeader — the lease.IsLeader gate is still in place (gh #75 regression)")
	}

	if calls := leases.isLeaderCalls.Load(); calls != 0 {
		t.Errorf("Append called lease.IsLeader %d times, want 0", calls)
	}
}
