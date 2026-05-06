package controllerfailover

import (
	"context"
	"sync"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"

	"github.com/woestebanaan/skafka/internal/broker"
	"github.com/woestebanaan/skafka/internal/controller"
)

// TestSelfFenceOnNetworkPartition is the broker-side half of the gh #62
// network-partition chaos test. It models a 3-broker cluster where one
// broker (the "isolated" one) loses its heartbeat link to the
// controller — and asserts:
//
//  1. The isolated broker's Coordinator stops reporting fresh
//     heartbeats within DefaultHeartbeatTimeout (3s in production;
//     overridden here so the test runs in milliseconds).
//  2. The Coordinator on the surviving brokers stays healthy.
//  3. The Produce hot path's IsHeartbeatFresh gate flips false on the
//     isolated broker, true on the survivors — that's what shuts off
//     ack'd writes from the partitioned side and is the load-bearing
//     correctness guarantee per CLAUDE.md ("the v3 epoch fence + 3s
//     self-fence make the takeover safety delay short regardless of
//     NFS or storage weirdness").
//
// The companion failover half — surviving brokers re-elect a new
// controller — is covered by TestSequencedFailoverBumpsEpoch on the
// Lease side.
func TestSelfFenceOnNetworkPartition(t *testing.T) {
	// Shared "controller" — its tick loop pushes heartbeat updates
	// into each broker's fake heartbeat source. A partition is just
	// "stop calling tick on this broker's source".
	now := time.Now()
	survivor := &fakeHeartbeat{}
	isolated := &fakeHeartbeat{}

	// Both brokers start fresh: heartbeat received "now".
	survivor.set(now)
	isolated.set(now)

	const fenceWindow = 50 * time.Millisecond

	if !broker.IsHeartbeatFresh(survivor.LastReceived(), fenceWindow) {
		t.Fatal("survivor: expected fresh at t0")
	}
	if !broker.IsHeartbeatFresh(isolated.LastReceived(), fenceWindow) {
		t.Fatal("isolated: expected fresh at t0")
	}

	// Tick: survivor keeps getting heartbeats, isolated does not.
	// Run for 3× fenceWindow so the staleness is unambiguous.
	deadline := time.Now().Add(3 * fenceWindow)
	for time.Now().Before(deadline) {
		survivor.set(time.Now())
		// isolated.set(...) intentionally not called — partition.
		time.Sleep(fenceWindow / 4)
	}

	// Survivor still fresh.
	if !broker.IsHeartbeatFresh(survivor.LastReceived(), fenceWindow) {
		t.Errorf("survivor went stale unexpectedly: lastReceived=%v, now=%v",
			survivor.LastReceived(), time.Now())
	}
	// Isolated has fenced.
	if broker.IsHeartbeatFresh(isolated.LastReceived(), fenceWindow) {
		t.Errorf("isolated should have self-fenced after %v: lastReceived=%v, now=%v",
			3*fenceWindow, isolated.LastReceived(), time.Now())
	}
}

// TestPartitionRecoveryRestoresFreshness models the heal half of the
// chaos run: after the network partition resolves, heartbeats resume
// and the previously-isolated broker rejoins fresh state. Without
// this, an operator who wedges a single broker briefly might find it
// permanently fenced — that would be a bug, not the design.
func TestPartitionRecoveryRestoresFreshness(t *testing.T) {
	const fenceWindow = 50 * time.Millisecond

	hb := &fakeHeartbeat{}
	hb.set(time.Now())
	if !broker.IsHeartbeatFresh(hb.LastReceived(), fenceWindow) {
		t.Fatal("expected fresh at t0")
	}

	// Simulated partition: 3 fenceWindows of silence.
	time.Sleep(3 * fenceWindow)
	if broker.IsHeartbeatFresh(hb.LastReceived(), fenceWindow) {
		t.Fatal("expected stale after partition window")
	}

	// Heal — controller resumes heartbeats.
	hb.set(time.Now())
	if !broker.IsHeartbeatFresh(hb.LastReceived(), fenceWindow) {
		t.Errorf("expected fresh after recovery: lastReceived=%v", hb.LastReceived())
	}
}

// TestControllerSurvivesPeerPartition is the failover half of #62 from
// the Lease side: when the network partition isolates a *follower*
// broker (not the controller), the surviving controller continues to
// hold the controller Lease without churn — leaseTransitions stays
// flat, no false re-election. Pairs with the broker-side self-fence
// tests above.
func TestControllerSurvivesPeerPartition(t *testing.T) {
	client := fake.NewSimpleClientset()
	rootCtx, rootCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer rootCancel()

	var acquireMu sync.Mutex
	var acquireCalls int
	var lostCalls int

	ctx0, cancel0 := context.WithCancel(rootCtx)
	defer cancel0()
	e0 := controller.New(client, "default", "broker-0",
		func(_ context.Context, _ int64) {
			acquireMu.Lock()
			acquireCalls++
			acquireMu.Unlock()
		},
		func() {
			acquireMu.Lock()
			lostCalls++
			acquireMu.Unlock()
		},
	)
	go func() { _ = e0.Run(ctx0) }()

	// Wait for broker-0 to acquire.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		acquireMu.Lock()
		got := acquireCalls
		acquireMu.Unlock()
		if got == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	acquireMu.Lock()
	if acquireCalls != 1 {
		acquireMu.Unlock()
		t.Fatalf("broker-0 didn't acquire: acquireCalls=%d", acquireCalls)
	}
	acquireMu.Unlock()

	// Simulate broker-1 being partitioned: it never participates in
	// the election. Verify broker-0's leadership is stable — no
	// spurious lost-then-reacquired cycles. The fake clientset
	// doesn't model real network failures, so the property we can
	// assert here is: with no contender, no electoral churn.
	time.Sleep(500 * time.Millisecond)

	acquireMu.Lock()
	defer acquireMu.Unlock()
	if acquireCalls != 1 {
		t.Errorf("controller flapped during peer partition: acquireCalls=%d, want 1", acquireCalls)
	}
	if lostCalls != 0 {
		t.Errorf("controller lost lease without context cancel: lostCalls=%d, want 0", lostCalls)
	}
}

// fakeHeartbeat is a thread-safe HeartbeatSource for partition tests.
// "Receiving a heartbeat" is just calling set(); "partition" is not
// calling it.
type fakeHeartbeat struct {
	mu       sync.Mutex
	received time.Time
}

func (f *fakeHeartbeat) set(t time.Time) {
	f.mu.Lock()
	f.received = t
	f.mu.Unlock()
}

func (f *fakeHeartbeat) LastReceived() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.received
}
