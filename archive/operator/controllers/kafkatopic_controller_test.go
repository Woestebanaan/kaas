package controllers

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/woestebanaan/skafka/operator/api/v1alpha1"
)

// TestKafkaTopicReconcile_UsesEffectiveTopicName guards the gh #86
// contract on the operator side: when a CR has spec.topicName set
// (admin-protocol path: synthetic metadata.name + literal Kafka name
// in spec), partition directories MUST be created under the literal
// Kafka name on the PVC — that's what brokers' openPartition() reads
// from /data/<topic>/<partition>/. Using metadata.name would put the
// dirs at /data/skafka-topic-<hash>/ where no broker would look.
func TestKafkaTopicReconcile_UsesEffectiveTopicName(t *testing.T) {
	dataDir := t.TempDir()
	const realKafkaName = "MY_STREAMS_TOPIC"
	const syntheticMeta = "skafka-topic-abc123"

	scheme := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	topic := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Name: syntheticMeta, Namespace: "skafka"},
		Spec: v1alpha1.KafkaTopicSpec{
			TopicName:  realKafkaName, // synthetic metadata.name + literal Kafka name in spec
			Partitions: 3,
		},
	}
	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(topic).
		WithStatusSubresource(topic).
		Build()

	r := NewKafkaTopicReconciler(cli, dataDir)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "skafka", Name: syntheticMeta},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// Partition dirs must land at /data/<KafkaName>/<p>/, NOT under the
	// synthetic metadata.name.
	for _, p := range []string{"0", "1", "2"} {
		want := filepath.Join(dataDir, realKafkaName, p)
		if _, err := os.Stat(want); err != nil {
			t.Errorf("partition %s: expected dir at %s, got err=%v", p, want, err)
		}
	}
	// And NOT at the synthetic name.
	syntheticDir := filepath.Join(dataDir, syntheticMeta)
	if _, err := os.Stat(syntheticDir); err == nil {
		t.Errorf("partition dirs unexpectedly created at synthetic-name path %s", syntheticDir)
	}
}

// TestKafkaTopicReconcile_PassthroughForValidNames pins the common-
// case backward compatibility: a CR without spec.topicName still
// resolves to metadata.name as the on-disk path. Old CRs (pre-#86)
// keep working.
func TestKafkaTopicReconcile_PassthroughForValidNames(t *testing.T) {
	dataDir := t.TempDir()
	scheme := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	topic := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Name: "events", Namespace: "skafka"},
		Spec:       v1alpha1.KafkaTopicSpec{Partitions: 1},
	}
	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(topic).
		WithStatusSubresource(topic).
		Build()

	r := NewKafkaTopicReconciler(cli, dataDir)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "skafka", Name: "events"},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	want := filepath.Join(dataDir, "events", "0")
	if _, err := os.Stat(want); err != nil {
		t.Errorf("expected partition dir at %s, got err=%v", want, err)
	}
}
