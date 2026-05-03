package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/woestebanaan/skafka/operator/api/v1alpha1"
	"github.com/woestebanaan/skafka/pkg/kafkaapi"
)

func newMirrorScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(s)
	return s
}

func sampleAssignmentForMirror() *kafkaapi.Assignment {
	return &kafkaapi.Assignment{
		ControllerEpoch:   7,
		AssignmentVersion: 42,
		GeneratedAt:       time.Date(2026, 5, 3, 12, 0, 0, 0, time.UTC),
		Controller:        "skafka-1",
		Brokers: []kafkaapi.BrokerAssignment{
			{ID: "skafka-0", Health: kafkaapi.BrokerHealthAlive, LastSeen: time.Date(2026, 5, 3, 12, 0, 0, 0, time.UTC)},
			{ID: "skafka-1", Health: kafkaapi.BrokerHealthAlive, LastSeen: time.Date(2026, 5, 3, 12, 0, 0, 0, time.UTC)},
		},
		Partitions: []kafkaapi.PartitionAssignment{
			{Topic: "events", Partition: 0, Broker: "skafka-0", Epoch: 3, Role: kafkaapi.PartitionRoleLeader},
			{Topic: "events", Partition: 1, Broker: "skafka-1", Epoch: 3, Role: kafkaapi.PartitionRoleLeader},
		},
		ConsumerGroups: []kafkaapi.ConsumerGroupAssignment{
			{GroupID: "payments", Broker: "skafka-0", Epoch: 1},
		},
	}
}

func TestK8sMirrorWritesStatus(t *testing.T) {
	// Pre-populate the CR (operator step 4 will own creation; here we
	// stand in as the operator for the test).
	cr := &v1alpha1.KafkaClusterAssignments{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster-1", Namespace: "default"},
	}
	c := fake.NewClientBuilder().
		WithScheme(newMirrorScheme()).
		WithObjects(cr).
		WithStatusSubresource(cr).
		Build()

	m := NewK8sMirror(c, "default", "cluster-1")
	m.Mirror(context.Background(), sampleAssignmentForMirror())

	var got v1alpha1.KafkaClusterAssignments
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "cluster-1"}, &got); err != nil {
		t.Fatalf("Get after Mirror: %v", err)
	}
	if got.Status.ControllerEpoch != 7 {
		t.Errorf("ControllerEpoch=%d, want 7", got.Status.ControllerEpoch)
	}
	if got.Status.AssignmentVersion != 42 {
		t.Errorf("AssignmentVersion=%d, want 42", got.Status.AssignmentVersion)
	}
	if got.Status.Controller != "skafka-1" {
		t.Errorf("Controller=%q, want skafka-1", got.Status.Controller)
	}
	if len(got.Status.Brokers) != 2 {
		t.Errorf("Brokers count=%d, want 2", len(got.Status.Brokers))
	}
	if len(got.Status.Partitions) != 2 {
		t.Errorf("Partitions count=%d, want 2", len(got.Status.Partitions))
	}
	if len(got.Status.ConsumerGroups) != 1 || got.Status.ConsumerGroups[0].GroupID != "payments" {
		t.Errorf("ConsumerGroups=%+v, want one entry for payments", got.Status.ConsumerGroups)
	}
	if got.Status.Truncated {
		t.Errorf("Truncated should be false (no truncation in this test)")
	}
}

func TestK8sMirrorMissingCRIsNoOp(t *testing.T) {
	// CR doesn't exist (operator hasn't created it yet). Mirror logs +
	// skips silently — the file is the source of truth, the CR is just
	// kubectl-debugging convenience.
	c := fake.NewClientBuilder().WithScheme(newMirrorScheme()).Build()
	m := NewK8sMirror(c, "default", "cluster-1")
	// Must not panic, must not error (signature is void anyway).
	m.Mirror(context.Background(), sampleAssignmentForMirror())

	// Confirm the CR was NOT created (Mirror doesn't take ownership of creation).
	var got v1alpha1.KafkaClusterAssignments
	err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "cluster-1"}, &got)
	if err == nil {
		t.Errorf("Mirror should not create the CR; found %+v", got)
	}
}

func TestK8sMirrorNilAssignmentIsNoOp(t *testing.T) {
	cr := &v1alpha1.KafkaClusterAssignments{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster-1", Namespace: "default"},
	}
	c := fake.NewClientBuilder().
		WithScheme(newMirrorScheme()).
		WithObjects(cr).
		WithStatusSubresource(cr).
		Build()
	m := NewK8sMirror(c, "default", "cluster-1")
	m.Mirror(context.Background(), nil)

	var got v1alpha1.KafkaClusterAssignments
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "cluster-1"}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.AssignmentVersion != 0 {
		t.Errorf("nil assignment should leave Status untouched; got AssignmentVersion=%d", got.Status.AssignmentVersion)
	}
}

func TestK8sMirrorTruncatesWhenOverThreshold(t *testing.T) {
	// Build an assignment with way more partitions than the threshold.
	a := &kafkaapi.Assignment{
		ControllerEpoch:   1,
		AssignmentVersion: 1,
		GeneratedAt:       time.Now(),
		Controller:        "skafka-0",
	}
	const total = 50
	for i := 0; i < total; i++ {
		a.Partitions = append(a.Partitions, kafkaapi.PartitionAssignment{
			Topic:     fmt.Sprintf("topic-%02d", i/5),
			Partition: int32(i % 5),
			Broker:    "skafka-0",
			Epoch:     1,
			Role:      kafkaapi.PartitionRoleLeader,
		})
	}

	cr := &v1alpha1.KafkaClusterAssignments{
		ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "default"},
	}
	c := fake.NewClientBuilder().
		WithScheme(newMirrorScheme()).
		WithObjects(cr).
		WithStatusSubresource(cr).
		Build()

	m := NewK8sMirror(c, "default", "c1").WithMaxPartitions(10)
	m.Mirror(context.Background(), a)

	var got v1alpha1.KafkaClusterAssignments
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "c1"}, &got); err != nil {
		t.Fatal(err)
	}
	if !got.Status.Truncated {
		t.Errorf("Truncated should be true; got %+v", got.Status.Truncated)
	}
	if len(got.Status.Partitions) != 10 {
		t.Errorf("Partitions count=%d, want 10", len(got.Status.Partitions))
	}
}

func TestK8sMirrorPrioritisesChangedPartitions(t *testing.T) {
	// Pre-populate the CR with a previous Status containing 6 partitions
	// at epoch 1. New assignment has 10 partitions: 3 with epoch=2 (changed),
	// 7 with epoch=1 (unchanged) — including the original 6. Truncate to 5.
	// The 3 changed partitions MUST be in the kept set; only 2 of the 7
	// unchanged make the cut.
	prev := &v1alpha1.KafkaClusterAssignments{
		ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "default"},
		Status: v1alpha1.KafkaClusterAssignmentsStatus{
			Partitions: []v1alpha1.KafkaClusterPartitionAssign{
				{Topic: "events", Partition: 0, Broker: "skafka-0", Epoch: 1},
				{Topic: "events", Partition: 1, Broker: "skafka-0", Epoch: 1},
				{Topic: "events", Partition: 2, Broker: "skafka-0", Epoch: 1},
				{Topic: "events", Partition: 3, Broker: "skafka-0", Epoch: 1},
				{Topic: "events", Partition: 4, Broker: "skafka-0", Epoch: 1},
				{Topic: "events", Partition: 5, Broker: "skafka-0", Epoch: 1},
			},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(newMirrorScheme()).
		WithObjects(prev).
		WithStatusSubresource(prev).
		Build()

	a := &kafkaapi.Assignment{
		ControllerEpoch:   1,
		AssignmentVersion: 2,
		GeneratedAt:       time.Now(),
		Controller:        "skafka-0",
	}
	// 0-2: unchanged at epoch 1; 3-5: bumped to epoch 2 (changed);
	// 6-9: new partitions (don't exist in prev → also "changed").
	for i := 0; i < 10; i++ {
		epoch := uint32(1)
		if i >= 3 && i < 6 {
			epoch = 2 // bumped
		}
		a.Partitions = append(a.Partitions, kafkaapi.PartitionAssignment{
			Topic:     "events",
			Partition: int32(i),
			Broker:    "skafka-0",
			Epoch:     epoch,
			Role:      kafkaapi.PartitionRoleLeader,
		})
	}

	m := NewK8sMirror(c, "default", "c1").WithMaxPartitions(5)
	m.Mirror(context.Background(), a)

	var got v1alpha1.KafkaClusterAssignments
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "c1"}, &got); err != nil {
		t.Fatal(err)
	}
	if !got.Status.Truncated {
		t.Fatal("expected Truncated=true")
	}
	if len(got.Status.Partitions) != 5 {
		t.Fatalf("expected 5 partitions, got %d", len(got.Status.Partitions))
	}

	// The 3 changed partitions (3, 4, 5) plus the 4 new partitions (6, 7, 8, 9)
	// are "changed" — 7 changed total. The truncation-to-5 must include 5
	// of those 7. Either way, partitions 0/1/2 (unchanged at epoch 1) MUST
	// NOT be in the kept set: they're a strictly lower-priority bucket.
	for _, p := range got.Status.Partitions {
		if p.Partition <= 2 {
			t.Errorf("unchanged partition %d should have been dropped; kept set: %+v",
				p.Partition, got.Status.Partitions)
		}
	}
}

func TestK8sMirrorNoTruncationWhenWithinThreshold(t *testing.T) {
	a := sampleAssignmentForMirror()
	cr := &v1alpha1.KafkaClusterAssignments{
		ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "default"},
	}
	c := fake.NewClientBuilder().
		WithScheme(newMirrorScheme()).
		WithObjects(cr).
		WithStatusSubresource(cr).
		Build()

	// Default threshold (8000) is way above the 2 partitions in the sample.
	m := NewK8sMirror(c, "default", "c1")
	m.Mirror(context.Background(), a)

	var got v1alpha1.KafkaClusterAssignments
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "c1"}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Truncated {
		t.Errorf("Truncated should be false for under-threshold count")
	}
	if len(got.Status.Partitions) != 2 {
		t.Errorf("Partitions count=%d, want 2", len(got.Status.Partitions))
	}
}

func TestBuildStatusZeroAssignmentProducesEmptySlices(t *testing.T) {
	// Zero-value Assignment → Status with empty (non-nil) slices.
	// Helps kubectl get -o yaml not show "null" for fields the
	// debugging UI may render.
	a := &kafkaapi.Assignment{}
	st := buildStatus(a)
	if st.Brokers == nil || st.Partitions == nil || st.ConsumerGroups == nil {
		t.Errorf("buildStatus zero: nil slices not allowed; got %+v", st)
	}
	if st.AssignmentVersion != 0 || st.ControllerEpoch != 0 {
		t.Errorf("buildStatus zero: epoch/version should be 0; got %+v", st)
	}
}
