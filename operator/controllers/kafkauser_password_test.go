package controllers

import (
	"context"
	"regexp"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/woestebanaan/skafka/operator/api/v1alpha1"
)

// TestGenerateAlphaNumPasswordShape pins the Strimzi-mirror password
// shape: exactly N characters, drawn from [A-Za-z0-9]. gh #136.
func TestGenerateAlphaNumPasswordShape(t *testing.T) {
	want := regexp.MustCompile(`^[A-Za-z0-9]{32}$`)
	seen := map[string]bool{}
	for i := 0; i < 50; i++ {
		pw, err := generateAlphaNumPassword(32)
		if err != nil {
			t.Fatalf("generateAlphaNumPassword: %v", err)
		}
		if !want.MatchString(pw) {
			t.Errorf("password %q doesn't match [A-Za-z0-9]{32}", pw)
		}
		seen[pw] = true
	}
	if len(seen) < 45 {
		t.Errorf("only %d distinct passwords in 50 calls — entropy looks weak", len(seen))
	}
}

// TestResolveSCRAMPasswordAutoGenerate pins the gh #136 contract: if
// spec.authentication.password is nil, the operator generates a fresh
// 32-char password on first call (output Secret not yet existing).
func TestResolveSCRAMPasswordAutoGenerate(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	user := &v1alpha1.KafkaUser{
		ObjectMeta: metav1.ObjectMeta{Name: "alice", Namespace: "default"},
		Spec: v1alpha1.KafkaUserSpec{
			Authentication: v1alpha1.KafkaUserAuthentication{Type: "scram-sha-512"},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(user).Build()
	r := &KafkaUserReconciler{Client: c, DataDir: t.TempDir(), Namespace: "default"}

	pw, err := r.resolveSCRAMPassword(context.Background(), user, "alice-kafka-credentials")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(pw) != 32 {
		t.Errorf("len(password)=%d, want 32", len(pw))
	}
	if !regexp.MustCompile(`^[A-Za-z0-9]+$`).MatchString(pw) {
		t.Errorf("password %q contains chars outside [A-Za-z0-9]", pw)
	}
}

// TestResolveSCRAMPasswordStableAcrossReconciles pins idempotency: a
// second reconcile must return the SAME password (read back from the
// operator-owned output Secret), not regenerate.
func TestResolveSCRAMPasswordStableAcrossReconciles(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	user := &v1alpha1.KafkaUser{
		ObjectMeta: metav1.ObjectMeta{Name: "alice", Namespace: "default"},
		Spec: v1alpha1.KafkaUserSpec{
			Authentication: v1alpha1.KafkaUserAuthentication{Type: "scram-sha-512"},
		},
	}
	// Output Secret pre-populated as if a prior reconcile had run.
	outSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "alice-kafka-credentials", Namespace: "default"},
		Data:       map[string][]byte{"password": []byte("StableXxxYyyZzzAaaBbbCccDddEeeFf")},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(user, outSecret).Build()
	r := &KafkaUserReconciler{Client: c, DataDir: t.TempDir(), Namespace: "default"}

	pw, err := r.resolveSCRAMPassword(context.Background(), user, "alice-kafka-credentials")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if pw != "StableXxxYyyZzzAaaBbbCccDddEeeFf" {
		t.Errorf("expected password to be read from existing output Secret, got %q", pw)
	}
}

// TestResolveSCRAMPasswordUsesInputSecret pins the legacy behaviour:
// if spec.authentication.password IS set, read the input Secret —
// auto-generate doesn't trigger.
func TestResolveSCRAMPasswordUsesInputSecret(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	user := &v1alpha1.KafkaUser{
		ObjectMeta: metav1.ObjectMeta{Name: "alice", Namespace: "default"},
		Spec: v1alpha1.KafkaUserSpec{
			Authentication: v1alpha1.KafkaUserAuthentication{
				Type:     "scram-sha-512",
				Password: &v1alpha1.SecretKeyRef{Name: "my-input", Key: "password"},
			},
		},
	}
	input := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "my-input", Namespace: "default"},
		Data:       map[string][]byte{"password": []byte("user-supplied")},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(user, input).Build()
	r := &KafkaUserReconciler{Client: c, DataDir: t.TempDir(), Namespace: "default"}

	pw, err := r.resolveSCRAMPassword(context.Background(), user, "alice-kafka-credentials")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if pw != "user-supplied" {
		t.Errorf("expected input-secret password, got %q", pw)
	}
}
