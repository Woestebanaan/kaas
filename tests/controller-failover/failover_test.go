// Package controllerfailover exercises the v3.3 single-Lease controller
// election: when the holder releases, the next elector picks up at a
// strictly higher leaseTransitions value.
//
// Note on test fidelity. client-go/kubernetes/fake doesn't enforce real
// Lease compare-and-swap semantics — two electors against the same fake
// clientset can both conclude they own the lease. Real apiserver (envtest)
// is the only way to test contention faithfully. What we CAN test against
// fake is the sequenced handoff: broker-0 acquires, releases, then broker-1
// acquires and observes a bumped leaseTransitions. That property is the
// load-bearing failover guarantee — brokers fence stale-controller writes
// using exactly this counter — and the unit test TestElectionEpochReadsExistingLease
// in internal/controller/ covers the read side.
package controllerfailover

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"

	"github.com/woestebanaan/skafka/internal/controller"
)

// TestSequencedFailoverBumpsEpoch:
//   1. broker-0 starts an Election → OnAcquired fires at epoch E0.
//   2. broker-0 cancels its context → OnLost fires.
//   3. broker-1 starts a NEW Election against the same Lease →
//      OnAcquired fires at epoch E1 > E0.
//
// This is the core failover invariant: every controller term has a
// strictly-greater leaseTransitions value than the previous one. The
// epoch fence on assignment.json depends on it.
func TestSequencedFailoverBumpsEpoch(t *testing.T) {
	client := fake.NewSimpleClientset()
	rootCtx, rootCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer rootCancel()

	// --- broker-0 term ---
	type acquireSignal struct {
		epoch int64
	}
	a0Ack := make(chan acquireSignal, 1)
	a0LostCh := make(chan struct{}, 1)

	a0Ctx, a0Cancel := context.WithCancel(rootCtx)
	e0 := controller.New(client, "default", "broker-0",
		func(_ context.Context, epoch int64) {
			select {
			case a0Ack <- acquireSignal{epoch: epoch}:
			default:
			}
		},
		func() {
			select {
			case a0LostCh <- struct{}{}:
			default:
			}
		},
	).WithTimings(500*time.Millisecond, 300*time.Millisecond, 50*time.Millisecond)
	go func() { _ = e0.Run(a0Ctx) }()

	var e0Epoch int64
	select {
	case sig := <-a0Ack:
		e0Epoch = sig.epoch
	case <-time.After(5 * time.Second):
		t.Fatal("broker-0 never acquired the lease")
	}

	// Cancel broker-0 → it surrenders the lease (ReleaseOnCancel=true) and
	// OnLost fires.
	a0Cancel()
	select {
	case <-a0LostCh:
	case <-time.After(5 * time.Second):
		t.Fatal("broker-0 OnLost did not fire after cancel")
	}

	// --- broker-1 term ---
	a1Ack := make(chan acquireSignal, 1)
	a1Ctx, a1Cancel := context.WithCancel(rootCtx)
	defer a1Cancel()
	e1 := controller.New(client, "default", "broker-1",
		func(_ context.Context, epoch int64) {
			select {
			case a1Ack <- acquireSignal{epoch: epoch}:
			default:
			}
		},
		nil,
	).WithTimings(500*time.Millisecond, 300*time.Millisecond, 50*time.Millisecond)
	go func() { _ = e1.Run(a1Ctx) }()

	var e1Epoch int64
	select {
	case sig := <-a1Ack:
		e1Epoch = sig.epoch
	case <-time.After(10 * time.Second):
		t.Fatal("broker-1 never acquired the lease after broker-0 released")
	}

	if e1Epoch <= e0Epoch {
		t.Errorf("leaseTransitions did not advance: broker-0 epoch=%d, broker-1 epoch=%d", e0Epoch, e1Epoch)
	}
}

// TestOnLostFiresOnContextCancel is a smaller-but-explicit assertion of
// the contract that internal/controller relies on: cancelling the
// election's context drives OnLost via the underlying leaderelection
// library's stopCh. Without this guarantee, a Stop on the AssignmentLoop
// would never fire and the controller goroutines would leak across tests.
func TestOnLostFiresOnContextCancel(t *testing.T) {
	client := fake.NewSimpleClientset()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var lost atomic.Bool
	acquired := make(chan struct{}, 1)
	e := controller.New(client, "default", "broker-99",
		func(_ context.Context, _ int64) {
			select {
			case acquired <- struct{}{}:
			default:
			}
		},
		func() { lost.Store(true) },
	).WithTimings(500*time.Millisecond, 300*time.Millisecond, 50*time.Millisecond)

	runCtx, runCancel := context.WithCancel(ctx)
	go func() { _ = e.Run(runCtx) }()

	select {
	case <-acquired:
	case <-time.After(3 * time.Second):
		t.Fatal("did not acquire")
	}

	runCancel()
	deadline := time.Now().Add(3 * time.Second)
	for !lost.Load() && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if !lost.Load() {
		t.Error("OnLost did not fire within 3s of context cancel")
	}
}
