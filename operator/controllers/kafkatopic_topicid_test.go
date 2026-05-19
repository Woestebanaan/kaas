package controllers

import (
	"context"
	"regexp"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/woestebanaan/skafka/operator/api/v1alpha1"
)

// TestGenerateTopicUUIDShape pins the gh #105 UUID format: 36-char
// canonical hyphenated, hex-only digits, RFC 4122 v4 (the high
// nibble of byte 6 is '4'; the high nibble of byte 8 is one of
// 8/9/a/b).
func TestGenerateTopicUUIDShape(t *testing.T) {
	want := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	seen := map[string]bool{}
	for i := 0; i < 50; i++ {
		u := generateTopicUUID()
		if !want.MatchString(u) {
			t.Errorf("generateTopicUUID()=%q doesn't match v4 pattern", u)
		}
		if seen[u] {
			t.Errorf("collision on %q after %d iterations — entropy looks weak", u, i)
		}
		seen[u] = true
	}
}

// TestKafkaTopicReconcile_AssignsTopicIDOnFirstReconcile pins gh #105:
// a fresh KafkaTopic CR ends its first reconcile with a non-empty
// Status.TopicID. The operator never rotates it on subsequent
// reconciles.
func TestKafkaTopicReconcile_AssignsTopicIDOnFirstReconcile(t *testing.T) {
	dataDir := t.TempDir()
	scheme := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	topic := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Name: "events", Namespace: "skafka"},
		Spec:       v1alpha1.KafkaTopicSpec{Partitions: 2},
	}
	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(topic).
		WithStatusSubresource(topic).
		Build()

	r := NewKafkaTopicReconciler(cli, dataDir)
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "skafka", Name: "events"}}

	// First reconcile assigns the UUID.
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	var got v1alpha1.KafkaTopic
	if err := cli.Get(ctx, req.NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	first := got.Status.TopicID
	if first == "" {
		t.Fatal("Status.TopicID empty after first reconcile")
	}

	// Second reconcile must NOT rotate it.
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if err := cli.Get(ctx, req.NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.TopicID != first {
		t.Errorf("TopicID rotated across reconciles: first=%q second=%q (Apache contract: stable for the topic's lifetime)",
			first, got.Status.TopicID)
	}
}
