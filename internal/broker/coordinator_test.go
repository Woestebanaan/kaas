package broker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/woestebanaan/skafka/internal/assignment"
	"github.com/woestebanaan/skafka/pkg/kafkaapi"
)

// fakeLeases is a stub ControllerWatch that lets tests pin the epoch to
// arbitrary values without spinning up a Kubernetes informer. It mimics
// only the surface the Coordinator reads.
type fakeLeases struct{ epoch atomic.Int64 }

func (f *fakeLeases) CurrentEpoch() int64  { return f.epoch.Load() }
func (f *fakeLeases) CurrentHolder() string { return "" }

// realLeases is needed because the Coordinator's leases field is a
// concrete *ControllerWatch rather than an interface. Wrap the fake in a
// real ControllerWatch by setting its atomic field directly through the
// public CurrentEpoch path used in tests below.
//
// Practical solution: tests use an actual ControllerWatch and set its
// internal epoch via direct call. We expose a small test helper.
func newPinnedWatch(epoch int64) *ControllerWatch {
	w := NewControllerWatch(nil, "default")
	w.epoch.Store(epoch)
	return w
}

func sampleAssignment(brokerID string, version int64, controllerEpoch int64) *kafkaapi.Assignment {
	return &kafkaapi.Assignment{
		ControllerEpoch:   controllerEpoch,
		AssignmentVersion: version,
		GeneratedAt:       time.Now(),
		Controller:        "controller-x",
		Brokers: []kafkaapi.BrokerAssignment{
			{ID: brokerID, Health: kafkaapi.BrokerHealthAlive, LastSeen: time.Now()},
		},
		Partitions: []kafkaapi.PartitionAssignment{
			{Topic: "events", Partition: 0, Broker: brokerID, Epoch: 5, Role: kafkaapi.PartitionRoleLeader},
			{Topic: "events", Partition: 1, Broker: "other-broker", Epoch: 5, Role: kafkaapi.PartitionRoleLeader},
		},
	}
}

func TestCoordinatorAppliesAndDispatches(t *testing.T) {
	dir := t.TempDir()
	store := assignment.NewFileStore(dir).WithPollInterval(20 * time.Millisecond)
	leases := newPinnedWatch(1)

	c := NewCoordinator("broker-7", store, leases, nil)

	var changes atomic.Int64
	c.OnAssignmentChange(func(_ context.Context, _, _ *kafkaapi.Assignment) {
		changes.Add(1)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() { _ = c.Start(ctx) }()

	// Write a fresh assignment.
	a := sampleAssignment("broker-7", 1, 1)
	if err := store.Write(ctx, a); err != nil {
		t.Fatal(err)
	}

	// Wait for the handler to fire.
	deadline := time.Now().Add(2 * time.Second)
	for changes.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if changes.Load() == 0 {
		t.Fatal("handler did not fire after assignment write")
	}

	if !c.Owns("events", 0) {
		t.Errorf("Coordinator should own events/0")
	}
	if c.Owns("events", 1) {
		t.Errorf("Coordinator should NOT own events/1 (assigned to other-broker)")
	}
	if epoch, ok := c.CurrentEpoch("events", 0); !ok || epoch != 5 {
		t.Errorf("CurrentEpoch(events,0)=(%d,%v), want (5,true)", epoch, ok)
	}
	if _, ok := c.CurrentEpoch("events", 1); ok {
		t.Errorf("CurrentEpoch(events,1) should be (0,false) — not owned")
	}

	// LeaderFor must return the assigned broker's ordinal — not "this
	// broker's view of leadership". Critical for the Metadata handler:
	// if Coordinator.LeaderFor went silent on partitions led elsewhere,
	// we'd reproduce the gh #75 split-brain symptom in the response.
	// sampleAssignment uses "broker-7" for events/0 and "other-broker"
	// for events/1. ParseOrdinalFromIdentity strips trailing dashes so
	// the ordinals are 7 and -1 respectively (-1 because "broker" is
	// not numeric — that's the unknown-broker sentinel and matches what
	// the Metadata handler expects from a missing partition).
	if got := c.LeaderFor("events", 0); got != 7 {
		t.Errorf("LeaderFor(events,0) = %d, want 7", got)
	}
	if got := c.LeaderFor("events", 1); got != -1 {
		t.Errorf("LeaderFor(events,1) = %d, want -1 (other-broker has no parseable ordinal)", got)
	}
	if got := c.LeaderFor("nonexistent", 0); got != -1 {
		t.Errorf("LeaderFor(nonexistent,0) = %d, want -1", got)
	}
}

func TestCoordinatorRejectsStaleControllerEpoch(t *testing.T) {
	dir := t.TempDir()
	store := assignment.NewFileStore(dir).WithPollInterval(20 * time.Millisecond)
	leases := newPinnedWatch(10) // current controller is on epoch 10

	c := NewCoordinator("broker-7", store, leases, nil)

	var changes atomic.Int64
	c.OnAssignmentChange(func(_ context.Context, _, _ *kafkaapi.Assignment) {
		changes.Add(1)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _ = c.Start(ctx) }()

	// A partitioned ex-controller on epoch 5 writes an assignment claiming
	// ownership for broker-7. The Coordinator must reject it.
	stale := sampleAssignment("broker-7", 99, 5)
	if err := store.Write(ctx, stale); err != nil {
		t.Fatal(err)
	}

	// Give the watcher a moment.
	time.Sleep(200 * time.Millisecond)

	if changes.Load() != 0 {
		t.Errorf("handler fired for stale-epoch assignment (%d times)", changes.Load())
	}
	if c.Owns("events", 0) {
		t.Errorf("Coordinator should NOT own anything from a stale-epoch assignment")
	}
}

func TestCoordinatorDedupesByVersion(t *testing.T) {
	dir := t.TempDir()
	store := assignment.NewFileStore(dir).WithPollInterval(20 * time.Millisecond)
	leases := newPinnedWatch(1)

	c := NewCoordinator("broker-7", store, leases, nil)

	var changes atomic.Int64
	c.OnAssignmentChange(func(_ context.Context, _, _ *kafkaapi.Assignment) {
		changes.Add(1)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _ = c.Start(ctx) }()

	a := sampleAssignment("broker-7", 1, 1)
	if err := store.Write(ctx, a); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(1 * time.Second)
	for changes.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}

	// Touch the file with the same version + same epoch — handler must NOT
	// fire again.
	before := changes.Load()
	if err := store.Write(ctx, a); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if changes.Load() != before {
		t.Errorf("handler fired for duplicate version: before=%d after=%d", before, changes.Load())
	}

	// New version → handler must fire.
	a2 := sampleAssignment("broker-7", 2, 1)
	if err := store.Write(ctx, a2); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(1 * time.Second)
	for changes.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if changes.Load() < 2 {
		t.Errorf("handler did not fire for new version (changes=%d)", changes.Load())
	}
}

func TestCoordinatorReadsExistingFileOnStart(t *testing.T) {
	// Lay down an assignment BEFORE the Coordinator starts watching. The
	// initial-load path inside Start() should pick it up without waiting
	// for a Watch tick.
	dir := t.TempDir()
	store := assignment.NewFileStore(dir).WithPollInterval(time.Hour)
	leases := newPinnedWatch(1)

	a := sampleAssignment("broker-7", 1, 1)
	if err := store.Write(context.Background(), a); err != nil {
		t.Fatal(err)
	}

	c := NewCoordinator("broker-7", store, leases, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = c.Start(ctx) }()

	// Give Start() a moment to do its initial read.
	deadline := time.Now().Add(500 * time.Millisecond)
	for !c.Owns("events", 0) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if !c.Owns("events", 0) {
		t.Fatal("Coordinator did not load assignment on Start despite file already existing")
	}
}

// hashFallthroughAssignment builds a 3-broker assignment with NO
// explicit ConsumerGroups entries — exercising the hash fallback
// path added in gh #92.
func hashFallthroughAssignment() *kafkaapi.Assignment {
	return &kafkaapi.Assignment{
		ControllerEpoch:   1,
		AssignmentVersion: 1,
		GeneratedAt:       time.Now(),
		Controller:        "skafka-0",
		Brokers: []kafkaapi.BrokerAssignment{
			{ID: "skafka-0", Health: kafkaapi.BrokerHealthAlive, LastSeen: time.Now()},
			{ID: "skafka-1", Health: kafkaapi.BrokerHealthAlive, LastSeen: time.Now()},
			{ID: "skafka-2", Health: kafkaapi.BrokerHealthAlive, LastSeen: time.Now()},
		},
		// ConsumerGroups intentionally empty — the hash fallback is
		// what we're testing.
	}
}

// TestCoordinatorOwnsGroupHashFallthrough guards gh #92's headline
// behaviour: with no explicit ConsumerGroups entry, exactly ONE
// broker out of three returns true for OwnsGroup(G), and that
// broker matches PickGroupCoordinator's prediction. Without this,
// either zero brokers (chicken-and-egg deadlock) or all three
// brokers (LocalGroupSource leak) would claim the group.
func TestCoordinatorOwnsGroupHashFallthrough(t *testing.T) {
	a := hashFallthroughAssignment()
	const groupID = "my-test-group"

	// Build a Coordinator on each broker and apply the assignment.
	coords := make(map[string]*Coordinator, 3)
	for _, brokerID := range []string{"skafka-0", "skafka-1", "skafka-2"} {
		c := NewCoordinator(brokerID, nil, nil, nil)
		c.mu.Lock()
		c.current = a
		c.mu.Unlock()
		coords[brokerID] = c
	}

	// Exactly one broker owns the group.
	owners := 0
	var ownerID string
	for id, c := range coords {
		if c.OwnsGroup(groupID) {
			owners++
			ownerID = id
		}
	}
	if owners != 1 {
		t.Errorf("OwnsGroup(%q) returned true on %d brokers, want 1", groupID, owners)
	}

	// And that owner matches the standalone hash function.
	wantOwner := PickGroupCoordinator(groupID,
		[]string{"skafka-0", "skafka-1", "skafka-2"},
		map[string]bool{"skafka-0": true, "skafka-1": true, "skafka-2": true})
	if ownerID != wantOwner {
		t.Errorf("OwnsGroup picked %q, but PickGroupCoordinator says %q (broker disagrees with helper)", ownerID, wantOwner)
	}
}

// TestCoordinatorGroupCoordinatorHashFallthrough: GroupCoordinator
// returns the hashed broker for unknown groups instead of
// (-, false). FindCoordinator delegates to this — without the
// fallthrough, every fresh group's FindCoordinator would surface
// CoordinatorNotAvailable and Java clients would retry forever.
func TestCoordinatorGroupCoordinatorHashFallthrough(t *testing.T) {
	a := hashFallthroughAssignment()
	c := NewCoordinator("skafka-0", nil, nil, nil)
	c.mu.Lock()
	c.current = a
	c.mu.Unlock()

	pick, ok := c.GroupCoordinator("fresh-group")
	if !ok {
		t.Fatal("GroupCoordinator returned not-found for an unknown group; hash fallback never fired")
	}
	if pick != "skafka-0" && pick != "skafka-1" && pick != "skafka-2" {
		t.Errorf("GroupCoordinator returned %q, want one of skafka-{0,1,2}", pick)
	}
}

// TestCoordinatorExplicitAssignmentOverridesHash pins the
// controller's override channel: an explicit ConsumerGroups entry
// wins over the hash fallback. This is the lever sticky-rebalance
// will eventually use; if we ever drop the explicit-first check,
// this test catches it.
func TestCoordinatorExplicitAssignmentOverridesHash(t *testing.T) {
	a := hashFallthroughAssignment()
	const groupID = "pinned-group"
	// Whichever broker the hash would NOT pick — pin the group there
	// to prove the explicit entry overrides.
	hashOwner := PickGroupCoordinator(groupID,
		[]string{"skafka-0", "skafka-1", "skafka-2"},
		map[string]bool{"skafka-0": true, "skafka-1": true, "skafka-2": true})
	var explicitOwner string
	for _, b := range []string{"skafka-0", "skafka-1", "skafka-2"} {
		if b != hashOwner {
			explicitOwner = b
			break
		}
	}
	a.ConsumerGroups = []kafkaapi.ConsumerGroupAssignment{
		{GroupID: groupID, Broker: explicitOwner, Epoch: 1},
	}

	for _, brokerID := range []string{"skafka-0", "skafka-1", "skafka-2"} {
		c := NewCoordinator(brokerID, nil, nil, nil)
		c.mu.Lock()
		c.current = a
		c.mu.Unlock()
		want := brokerID == explicitOwner
		if got := c.OwnsGroup(groupID); got != want {
			t.Errorf("on broker %q OwnsGroup=%v, want %v (explicit override should win over hash %q)",
				brokerID, got, want, hashOwner)
		}
	}
}

// TestCoordinatorOwnsGroupNoAliveBrokers: defense in depth — every
// broker is dead in the assignment view. OwnsGroup must return
// false for every broker (otherwise a stale broker would self-claim
// every group) and GroupCoordinator returns ok=false so
// FindCoordinator surfaces CoordinatorNotAvailable.
func TestCoordinatorOwnsGroupNoAliveBrokers(t *testing.T) {
	a := &kafkaapi.Assignment{
		ControllerEpoch:   1,
		AssignmentVersion: 1,
		GeneratedAt:       time.Now(),
		Brokers: []kafkaapi.BrokerAssignment{
			{ID: "skafka-0", Health: kafkaapi.BrokerHealthDead},
			{ID: "skafka-1", Health: kafkaapi.BrokerHealthDead},
		},
	}
	c := NewCoordinator("skafka-0", nil, nil, nil)
	c.mu.Lock()
	c.current = a
	c.mu.Unlock()

	if c.OwnsGroup("anything") {
		t.Error("OwnsGroup returned true with all brokers dead")
	}
	if _, ok := c.GroupCoordinator("anything"); ok {
		t.Error("GroupCoordinator returned ok=true with all brokers dead")
	}
}

// TestCoordinatorOwnsTxnHashFallthrough is the gh #91 sibling of
// TestCoordinatorOwnsGroupHashFallthrough. With no explicit
// txn-coordinator entry in the assignment (there is no such field
// today — the hash is the only path), exactly ONE broker out of
// three returns true for OwnsTxn(txnID), and that broker matches
// PickTxnCoordinator's prediction. Without this, gating
// InitProducerId / AddPartitionsToTxn / EndTxn on OwnsTxn would
// reject every transactional producer's first request.
func TestCoordinatorOwnsTxnHashFallthrough(t *testing.T) {
	a := hashFallthroughAssignment()
	const txnID = "my-test-txn"

	coords := make(map[string]*Coordinator, 3)
	for _, brokerID := range []string{"skafka-0", "skafka-1", "skafka-2"} {
		c := NewCoordinator(brokerID, nil, nil, nil)
		c.mu.Lock()
		c.current = a
		c.mu.Unlock()
		coords[brokerID] = c
	}

	owners := 0
	var ownerID string
	for id, c := range coords {
		if c.OwnsTxn(txnID) {
			owners++
			ownerID = id
		}
	}
	if owners != 1 {
		t.Errorf("OwnsTxn(%q) returned true on %d brokers, want 1", txnID, owners)
	}

	wantOwner := PickTxnCoordinator(txnID,
		[]string{"skafka-0", "skafka-1", "skafka-2"},
		map[string]bool{"skafka-0": true, "skafka-1": true, "skafka-2": true})
	if ownerID != wantOwner {
		t.Errorf("OwnsTxn picked %q, but PickTxnCoordinator says %q (broker disagrees with helper)", ownerID, wantOwner)
	}
}

// TestCoordinatorTxnCoordinatorHashFallthrough: TxnCoordinator
// returns the hashed broker for any txnID instead of (-, false).
// The forthcoming FindCoordinator(KeyType=transaction) handler
// (gh #91 PR 3) delegates to this — without the fallback, every
// transactional producer's discovery call would surface
// CoordinatorNotAvailable.
func TestCoordinatorTxnCoordinatorHashFallthrough(t *testing.T) {
	a := hashFallthroughAssignment()
	c := NewCoordinator("skafka-0", nil, nil, nil)
	c.mu.Lock()
	c.current = a
	c.mu.Unlock()

	pick, ok := c.TxnCoordinator("fresh-txn")
	if !ok {
		t.Fatal("TxnCoordinator returned not-found for an unknown txnID; hash fallback never fired")
	}
	if pick != "skafka-0" && pick != "skafka-1" && pick != "skafka-2" {
		t.Errorf("TxnCoordinator returned %q, want one of skafka-{0,1,2}", pick)
	}
}

// Sanity: ensure fakeLeases compiles even though we ended up not using it.
var _ = (&fakeLeases{}).CurrentEpoch
