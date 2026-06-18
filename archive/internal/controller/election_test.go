package controller

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	coordv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// TestElectionAcquireAndRelease drives a single elector against a fake
// clientset and verifies that the acquired callback fires (with an epoch),
// runs while leadership is held, and the lost callback fires when the
// outer context is cancelled.
func TestElectionAcquireAndRelease(t *testing.T) {
	client := fake.NewSimpleClientset()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var acquiredEpoch atomic.Int64
	var acquiredAt atomic.Int64
	var lostAt atomic.Int64

	onAcquired := func(_ context.Context, epoch int64) {
		acquiredEpoch.Store(epoch)
		acquiredAt.Store(time.Now().UnixNano())
	}
	onLost := func() {
		lostAt.Store(time.Now().UnixNano())
	}

	e := New(client, "default", "broker-0", onAcquired, onLost).
		WithTimings(2*time.Second, 1*time.Second, 200*time.Millisecond)

	runCtx, runCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- e.Run(runCtx) }()

	// Wait until OnAcquired fires.
	deadline := time.Now().Add(5 * time.Second)
	for acquiredAt.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if acquiredAt.Load() == 0 {
		t.Fatal("OnAcquired did not fire within 5s")
	}
	// First-ever Lease creation: leaseTransitions defaults to 0 (or omitted,
	// which we treat as 0). Either is acceptable for v1 — the property we
	// care about is "epoch increases monotonically", which only matters
	// across multiple acquisitions.
	if acquiredEpoch.Load() < 0 {
		t.Errorf("acquired epoch should be >= 0, got %d", acquiredEpoch.Load())
	}

	// Cancel and verify OnLost fires.
	runCancel()
	deadline = time.Now().Add(5 * time.Second)
	for lostAt.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if lostAt.Load() == 0 {
		t.Fatal("OnLost did not fire within 5s of context cancellation")
	}

	if err := <-done; err != nil {
		t.Errorf("Run returned %v", err)
	}
}

// TestElectionEpochReadsExistingLease verifies that when there's already a
// Lease in the cluster (e.g. left by a previous controller term), the elector
// picks up the existing leaseTransitions value rather than starting from
// zero. Otherwise an "old" controller restart would silently rewind the
// epoch and brokers would mis-fence.
func TestElectionEpochReadsExistingLease(t *testing.T) {
	client := fake.NewSimpleClientset()

	// Pre-populate a Lease with leaseTransitions=42 to simulate prior history.
	prior := int32(42)
	holder := "previous-broker"
	dur := int32(1)
	now := metav1.NewMicroTime(time.Now())
	seed := &coordv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: LeaseName, Namespace: "default"},
		Spec: coordv1.LeaseSpec{
			HolderIdentity:       &holder,
			LeaseDurationSeconds: &dur,
			AcquireTime:          &now,
			RenewTime:            &now,
			LeaseTransitions:     &prior,
		},
	}
	if _, err := client.CoordinationV1().Leases("default").Create(
		context.Background(), seed, metav1.CreateOptions{},
	); err != nil {
		t.Fatalf("seed lease: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	gotEpoch := make(chan int64, 1)
	onAcquired := func(_ context.Context, epoch int64) {
		select {
		case gotEpoch <- epoch:
		default:
		}
	}

	e := New(client, "default", "broker-0", onAcquired, nil).
		WithTimings(1*time.Second, 500*time.Millisecond, 100*time.Millisecond)

	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	go func() { _ = e.Run(runCtx) }()

	select {
	case epoch := <-gotEpoch:
		// Acquiring a stale lease bumps leaseTransitions to 43 — so the
		// observed epoch should be either 42 (read of the freshly-acquired
		// value if leaderelection didn't bump yet) or 43+ (after the bump).
		// What we must NOT see is 0.
		if epoch < 42 {
			t.Errorf("expected epoch >= 42 from existing Lease, got %d", epoch)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("OnAcquired did not fire")
	}
}

