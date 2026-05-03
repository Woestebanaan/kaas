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

// Sanity: ensure fakeLeases compiles even though we ended up not using it.
var _ = (&fakeLeases{}).CurrentEpoch
