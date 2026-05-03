package controller

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/woestebanaan/skafka/internal/assignment"
	"github.com/woestebanaan/skafka/pkg/kafkaapi"
)

// stubTopics is an in-memory TopicSource for tests.
type stubTopics struct {
	mu     sync.Mutex
	topics []TopicSpec
}

func (s *stubTopics) Topics() []TopicSpec {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]TopicSpec, len(s.topics))
	copy(out, s.topics)
	return out
}

func (s *stubTopics) Set(t []TopicSpec) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.topics = t
}

// stubBrokers is an in-memory BrokerSource for tests.
type stubBrokers struct {
	mu      sync.Mutex
	brokers []string
}

func (s *stubBrokers) AliveBrokers() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.brokers))
	copy(out, s.brokers)
	return out
}

func (s *stubBrokers) Set(b []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.brokers = b
}

// recordingMirror counts CR mirror calls.
type recordingMirror struct{ calls atomic.Int64 }

func (r *recordingMirror) Mirror(_ context.Context, _ *kafkaapi.Assignment) {
	r.calls.Add(1)
}

func TestAssignmentLoopWritesOnStart(t *testing.T) {
	dir := t.TempDir()
	store := assignment.NewFileStore(dir).WithPollInterval(20 * time.Millisecond)
	topics := &stubTopics{topics: []TopicSpec{{Name: "events", PartitionCount: 3}}}
	brokers := &stubBrokers{brokers: []string{"broker-0", "broker-1"}}
	mirror := &recordingMirror{}

	loop := NewAssignmentLoop(store, nil, mirror, topics, brokers, "broker-0")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _ = loop.Start(ctx, 7) }()

	// Wait for the initial write to land.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if a, _ := store.Read(ctx); a != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	a, err := store.Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if a.ControllerEpoch != 7 {
		t.Errorf("controllerEpoch=%d, want 7", a.ControllerEpoch)
	}
	if a.AssignmentVersion < 1 {
		t.Errorf("assignmentVersion=%d, want >= 1", a.AssignmentVersion)
	}
	if a.Controller != "broker-0" {
		t.Errorf("controller=%q, want broker-0", a.Controller)
	}
	if len(a.Partitions) != 3 {
		t.Errorf("partitions=%d, want 3", len(a.Partitions))
	}
	if mirror.calls.Load() < 1 {
		t.Errorf("mirror not called: %d", mirror.calls.Load())
	}
}

func TestAssignmentLoopVersionMonotonic(t *testing.T) {
	dir := t.TempDir()
	store := assignment.NewFileStore(dir).WithPollInterval(20 * time.Millisecond)
	topics := &stubTopics{topics: []TopicSpec{{Name: "events", PartitionCount: 1}}}
	brokers := &stubBrokers{brokers: []string{"broker-0"}}

	loop := NewAssignmentLoop(store, nil, nil, topics, brokers, "broker-0")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _ = loop.Start(ctx, 1) }()

	// Wait for initial write.
	deadline := time.Now().Add(2 * time.Second)
	for loop.Snapshot() == nil && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if loop.Snapshot() == nil {
		t.Fatal("loop did not perform initial write")
	}
	v0 := loop.Snapshot().AssignmentVersion

	// Force a recompute.
	_ = loop.UpdateAssignment(ctx, kafkaapi.AssignmentChange{
		Reason: kafkaapi.AssignmentReasonTopicCreated, Topic: "x",
	})

	deadline = time.Now().Add(2 * time.Second)
	for loop.Snapshot().AssignmentVersion <= v0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if loop.Snapshot().AssignmentVersion <= v0 {
		t.Errorf("version did not advance after Update: was %d, still %d", v0, loop.Snapshot().AssignmentVersion)
	}
}

func TestAssignmentLoopBootstrapsFromExistingFile(t *testing.T) {
	dir := t.TempDir()
	store := assignment.NewFileStore(dir).WithPollInterval(20 * time.Millisecond)

	// Pre-populate as if a previous controller had been running.
	prior := &kafkaapi.Assignment{
		ControllerEpoch:   3,
		AssignmentVersion: 100,
		GeneratedAt:       time.Now(),
		Controller:        "old-broker",
		Partitions: []kafkaapi.PartitionAssignment{
			{Topic: "events", Partition: 0, Broker: "broker-0", Epoch: 5, Role: kafkaapi.PartitionRoleLeader},
		},
	}
	if err := store.Write(context.Background(), prior); err != nil {
		t.Fatal(err)
	}

	topics := &stubTopics{topics: []TopicSpec{{Name: "events", PartitionCount: 1}}}
	brokers := &stubBrokers{brokers: []string{"broker-0"}}
	loop := NewAssignmentLoop(store, nil, nil, topics, brokers, "broker-0")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _ = loop.Start(ctx, 4) }() // new controller epoch=4 > old epoch=3

	deadline := time.Now().Add(2 * time.Second)
	for loop.Snapshot() == nil && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	a := loop.Snapshot()
	if a == nil {
		t.Fatal("loop produced no assignment")
	}
	// Version must strictly advance past the old 100.
	if a.AssignmentVersion <= 100 {
		t.Errorf("version=%d should be > 100 (old controller's last write)", a.AssignmentVersion)
	}
	// Strict-stability: existing assignment for broker-0 should be preserved.
	if a.Partitions[0].Broker != "broker-0" {
		t.Errorf("strict-stability violated: events/0 reassigned to %q", a.Partitions[0].Broker)
	}
	if a.Partitions[0].Epoch != 5 {
		t.Errorf("epoch should be preserved at 5, got %d", a.Partitions[0].Epoch)
	}
}

// stubGroups is an in-memory GroupSource for tests.
type stubGroups struct {
	mu     sync.Mutex
	groups []string
}

func (s *stubGroups) ActiveGroups() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.groups))
	copy(out, s.groups)
	return out
}

func (s *stubGroups) Set(g []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.groups = g
}

func TestAssignmentLoopWithGroupSourceEmitsConsumerGroups(t *testing.T) {
	dir := t.TempDir()
	store := assignment.NewFileStore(dir).WithPollInterval(20 * time.Millisecond)
	topics := &stubTopics{topics: []TopicSpec{{Name: "events", PartitionCount: 1}}}
	brokers := &stubBrokers{brokers: []string{"broker-0", "broker-1"}}
	groups := &stubGroups{groups: []string{"payments", "billing"}}

	loop := NewAssignmentLoop(store, nil, nil, topics, brokers, "broker-0").
		WithGroupSource(groups)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _ = loop.Start(ctx, 1) }()

	// Wait for the initial write.
	deadline := time.Now().Add(2 * time.Second)
	for loop.Snapshot() == nil && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	a := loop.Snapshot()
	if a == nil {
		t.Fatal("loop did not perform initial write")
	}

	// consumerGroups must have both groups, distributed across brokers.
	if len(a.ConsumerGroups) != 2 {
		t.Fatalf("ConsumerGroups: got %d, want 2; full: %+v", len(a.ConsumerGroups), a.ConsumerGroups)
	}
	gotIDs := map[string]bool{}
	for _, g := range a.ConsumerGroups {
		gotIDs[g.GroupID] = true
		if g.Broker != "broker-0" && g.Broker != "broker-1" {
			t.Errorf("group %s assigned to unknown broker %q", g.GroupID, g.Broker)
		}
		if g.Epoch != 1 {
			t.Errorf("group %s: epoch=%d, want 1 (fresh)", g.GroupID, g.Epoch)
		}
	}
	for _, want := range []string{"payments", "billing"} {
		if !gotIDs[want] {
			t.Errorf("ConsumerGroups missing %q", want)
		}
	}
}

func TestAssignmentLoopWithoutGroupSourceLeavesConsumerGroupsEmpty(t *testing.T) {
	// Default constructor (no WithGroupSource) → ConsumerGroups stays nil/empty.
	dir := t.TempDir()
	store := assignment.NewFileStore(dir).WithPollInterval(20 * time.Millisecond)
	topics := &stubTopics{topics: []TopicSpec{{Name: "events", PartitionCount: 1}}}
	brokers := &stubBrokers{brokers: []string{"broker-0"}}

	loop := NewAssignmentLoop(store, nil, nil, topics, brokers, "broker-0")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = loop.Start(ctx, 1) }()

	deadline := time.Now().Add(1 * time.Second)
	for loop.Snapshot() == nil && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if a := loop.Snapshot(); a != nil && len(a.ConsumerGroups) != 0 {
		t.Errorf("ConsumerGroups should be empty without GroupSource, got %+v", a.ConsumerGroups)
	}
}
