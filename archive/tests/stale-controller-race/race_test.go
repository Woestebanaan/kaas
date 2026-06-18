// Package stalecontrollerrace exercises the v3.3 epoch fence on the
// assignment file. The race the fence is designed to resolve:
//
//  1. Controller A holds the Lease at leaseTransitions=N and queues a
//     write to /data/__cluster/assignment.json.
//  2. A is partitioned. The Lease expires. Controller B acquires at
//     leaseTransitions=N+1.
//  3. A's queued write lands on the PVC AFTER B has written.
//
// Without the epoch fence, A's stale write overwrites B's authoritative
// state. The plan §"The stale-controller race" resolves this by stamping
// every assignment.json with the writer's leaseTransitions value and
// having brokers reject any file whose epoch is behind the current Lease
// holder's epoch.
//
// This test simulates the race by writing two assignment files with
// adversarial epochs and verifying the broker.Coordinator's behaviour.
package stalecontrollerrace

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/woestebanaan/skafka/internal/assignment"
	"github.com/woestebanaan/skafka/internal/broker"
	"github.com/woestebanaan/skafka/pkg/kafkaapi"
)

func mkAssignment(controllerEpoch int64, version int64, brokerID string) *kafkaapi.Assignment {
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
		},
	}
}

// pinnedWatch returns a ControllerWatch whose CurrentEpoch is fixed at
// the given value. Lets the test simulate "broker observes the new
// Lease at epoch N+1" without spinning up real Kubernetes informers.
func pinnedWatch(epoch int64) *broker.ControllerWatch {
	w := broker.NewControllerWatch(nil, "default")
	// epoch field is unexported; use the package-level setter pattern
	// (refresh() reads from the API). For tests we instead inject via
	// a small helper exported from the broker package — see SetEpochForTest.
	broker.SetControllerWatchEpochForTest(w, epoch)
	return w
}

// TestStaleControllerWriteIgnored: an ex-controller at epoch=5 writes
// assignment.json with that stale epoch AFTER the new controller (epoch=6)
// has been elected. The broker's Coordinator must NOT apply the stale file.
func TestStaleControllerWriteIgnored(t *testing.T) {
	dir := t.TempDir()
	store := assignment.NewFileStore(dir).WithPollInterval(20 * time.Millisecond)

	leases := pinnedWatch(6) // broker has observed the new Lease at epoch 6
	c := broker.NewCoordinator("broker-0", store, leases, nil)

	var changes atomic.Int64
	c.OnAssignmentChange(func(_ context.Context, _, _ *kafkaapi.Assignment) {
		changes.Add(1)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _ = c.Start(ctx) }()

	// The ex-controller at epoch 5 commits its queued write.
	stale := mkAssignment(5, 99, "broker-0")
	if err := store.Write(ctx, stale); err != nil {
		t.Fatal(err)
	}

	// Give the watcher a beat to process.
	time.Sleep(200 * time.Millisecond)

	if changes.Load() != 0 {
		t.Errorf("Coordinator applied stale-epoch assignment (%d times); should have rejected", changes.Load())
	}
	if c.Owns("events", 0) {
		t.Errorf("Coordinator believes it owns events/0 from a stale assignment")
	}
}

// TestNewControllerOverwriteAccepted: after the stale write is rejected,
// the new controller (epoch=6) writes its own assignment. The broker
// must accept this one and observe ownership.
func TestNewControllerOverwriteAccepted(t *testing.T) {
	dir := t.TempDir()
	store := assignment.NewFileStore(dir).WithPollInterval(20 * time.Millisecond)

	leases := pinnedWatch(6)
	c := broker.NewCoordinator("broker-0", store, leases, nil)

	var changes atomic.Int64
	c.OnAssignmentChange(func(_ context.Context, _, _ *kafkaapi.Assignment) {
		changes.Add(1)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = c.Start(ctx) }()

	// First, the stale write — must be ignored.
	stale := mkAssignment(5, 99, "broker-0")
	if err := store.Write(ctx, stale); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if changes.Load() != 0 {
		t.Fatalf("stale write was unexpectedly applied")
	}

	// New controller's authoritative write at epoch=6.
	fresh := mkAssignment(6, 1, "broker-0")
	if err := store.Write(ctx, fresh); err != nil {
		t.Fatal(err)
	}

	// Wait for the handler to fire.
	deadline := time.Now().Add(2 * time.Second)
	for changes.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if changes.Load() == 0 {
		t.Fatal("Coordinator did not apply the new controller's higher-epoch write")
	}
	if !c.Owns("events", 0) {
		t.Errorf("Coordinator should own events/0 after the new controller's write")
	}
}

// TestStaleAfterNewControllerStillIgnored simulates the realistic race:
// new controller writes first (epoch=6), THEN the partitioned old
// controller's queued write lands (epoch=5). The Coordinator has already
// applied epoch=6's assignment; the late stale write must not overwrite
// the in-memory state.
func TestStaleAfterNewControllerStillIgnored(t *testing.T) {
	dir := t.TempDir()
	store := assignment.NewFileStore(dir).WithPollInterval(20 * time.Millisecond)

	leases := pinnedWatch(6)
	c := broker.NewCoordinator("broker-0", store, leases, nil)

	var changes atomic.Int64
	c.OnAssignmentChange(func(_ context.Context, _, _ *kafkaapi.Assignment) {
		changes.Add(1)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = c.Start(ctx) }()

	// New controller writes at epoch 6.
	fresh := mkAssignment(6, 1, "broker-0")
	if err := store.Write(ctx, fresh); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for changes.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if changes.Load() == 0 {
		t.Fatal("new-controller write not applied")
	}
	if !c.Owns("events", 0) {
		t.Fatal("Coordinator should own events/0 after fresh write")
	}

	// Partitioned ex-controller's queued write lands. Stamp the same
	// version as the fresh write but a stale epoch — closest analogue
	// to the actual race.
	stale := mkAssignment(5, 1, "different-broker")
	if err := store.Write(ctx, stale); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)

	// changes should NOT have advanced — coordinator rejected the stale
	// epoch and kept the previous in-memory state.
	if !c.Owns("events", 0) {
		t.Errorf("stale write reverted ownership: %+v", c.Snapshot())
	}
}
