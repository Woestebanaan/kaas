package controllers

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/woestebanaan/skafka/operator/api/v1alpha1"
)

// TestKafkaUserReconcile_MissingSecret_SetsConditionNoRetry pins gh
// #120: a KafkaUser whose spec.authentication.password.name references
// a Secret that doesn't exist must produce a Ready=False condition
// with Reason=SecretNotFound AND Reconcile must return a nil error
// so controller-runtime doesn't enter the exponential-backoff log
// spam loop.
func TestKafkaUserReconcile_MissingSecret_SetsConditionNoRetry(t *testing.T) {
	dataDir := t.TempDir()
	scheme := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	user := &v1alpha1.KafkaUser{
		ObjectMeta: metav1.ObjectMeta{Name: "alice", Namespace: "skafka"},
		Spec: v1alpha1.KafkaUserSpec{
			Authentication: v1alpha1.KafkaUserAuthentication{
				Type: "scram-sha-512",
				// gh #136 normally lets Password be nil (auto-generate); the
				// gh #120 path is the explicitly-specified-but-missing
				// Secret case, since that's where operators get burned.
				Password: &v1alpha1.SecretKeyRef{Name: "alice-creds", Key: "password"},
			},
		},
	}
	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(user).
		WithStatusSubresource(user).
		Build()

	r := NewKafkaUserReconciler(cli, dataDir, "skafka")
	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "skafka", Name: "alice"},
	})
	if err != nil {
		t.Fatalf("Reconcile returned non-nil err for missing Secret; the gh #120 fix should swallow it and rely on the Condition: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("Reconcile requested a requeue (%+v); fix should rely on owner-ref watch / periodic resync, not explicit retry", res)
	}

	var got v1alpha1.KafkaUser
	if err := cli.Get(context.Background(), types.NamespacedName{Namespace: "skafka", Name: "alice"}, &got); err != nil {
		t.Fatalf("get user: %v", err)
	}
	var ready *metav1.Condition
	for i := range got.Status.Conditions {
		c := &got.Status.Conditions[i]
		if c.Type == "Ready" {
			ready = c
			break
		}
	}
	if ready == nil {
		t.Fatalf("expected a Ready condition, got %+v", got.Status.Conditions)
	}
	if ready.Status != metav1.ConditionFalse {
		t.Errorf("Ready.Status=%s, want False", ready.Status)
	}
	if ready.Reason != "SecretNotFound" {
		t.Errorf("Ready.Reason=%q, want SecretNotFound (distinct from generic CredentialError so operators can grep)", ready.Reason)
	}
}

// TestKafkaUserReconcile_MissingSecret_RecoversWhenSecretAppears
// completes the gh #120 acceptance: once the Secret is created, the
// next Reconcile call must succeed and flip the condition to
// Ready=True. Controller-runtime's owner-reference watch normally
// triggers this in production; here we drive it manually.
func TestKafkaUserReconcile_MissingSecret_RecoversWhenSecretAppears(t *testing.T) {
	dataDir := t.TempDir()
	scheme := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	user := &v1alpha1.KafkaUser{
		ObjectMeta: metav1.ObjectMeta{Name: "alice", Namespace: "skafka"},
		Spec: v1alpha1.KafkaUserSpec{
			Authentication: v1alpha1.KafkaUserAuthentication{
				Type:     "scram-sha-512",
				Password: &v1alpha1.SecretKeyRef{Name: "alice-creds", Key: "password"},
			},
		},
	}
	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(user).
		WithStatusSubresource(user).
		Build()

	r := NewKafkaUserReconciler(cli, dataDir, "skafka")
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "skafka", Name: "alice"}}

	// First reconcile — Secret missing.
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}

	// Create the referenced Secret.
	if err := cli.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "skafka", Name: "alice-creds"},
		Data:       map[string][]byte{"password": []byte("hunter2hunter2hunter2hunter2hunt")},
	}); err != nil {
		t.Fatal(err)
	}

	// Second reconcile — should succeed and flip Ready to True.
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("second reconcile after creating secret: %v", err)
	}

	var got v1alpha1.KafkaUser
	if err := cli.Get(ctx, req.NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	var ready *metav1.Condition
	for i := range got.Status.Conditions {
		c := &got.Status.Conditions[i]
		if c.Type == "Ready" {
			ready = c
		}
	}
	if ready == nil || ready.Status != metav1.ConditionTrue {
		t.Errorf("after secret appears, Ready=%+v, want Status=True", ready)
	}
}
