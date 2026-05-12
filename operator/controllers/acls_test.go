package controllers

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/woestebanaan/skafka/operator/api/v1alpha1"
)

func newACLScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(s)
	return s
}

// TestReconcileACLs_FromInlineSpec pins the gh #135 contract: ACLs are
// authored on the KafkaUser CR via spec.authorization.acls (Strimzi-
// style) and merged into acls.json with the on-disk capitalised
// permission ("Allow"/"Deny").
func TestReconcileACLs_FromInlineSpec(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	user := v1alpha1.KafkaUser{
		ObjectMeta: metav1.ObjectMeta{Name: "alice", Namespace: "default"},
		Spec: v1alpha1.KafkaUserSpec{
			Authentication: v1alpha1.KafkaUserAuthentication{Type: "scram-sha-512"},
			Authorization: &v1alpha1.KafkaUserAuthorization{
				Type: "simple",
				ACLs: []v1alpha1.KafkaUserACL{
					{
						Resource:   v1alpha1.KafkaUserACLResource{Type: "topic", Name: "payments", PatternType: "literal"},
						Operations: []string{"Write"},
						Type:       "allow",
					},
				},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(newACLScheme()).WithObjects(&user).Build()
	if err := reconcileACLs(ctx, c, "default", dir); err != nil {
		t.Fatalf("reconcileACLs: %v", err)
	}

	data, err := os.ReadFile(aclsPath(dir))
	if err != nil {
		t.Fatalf("read acls.json: %v", err)
	}
	var af ACLFile
	_ = json.Unmarshal(data, &af)

	if len(af.ACLs) != 1 {
		t.Fatalf("expected 1 ACL entry, got %d", len(af.ACLs))
	}
	if af.ACLs[0].Principal != "User:alice" {
		t.Errorf("principal=%q, want User:alice", af.ACLs[0].Principal)
	}
	if af.ACLs[0].Resource.Name != "payments" {
		t.Errorf("resource name=%q, want payments", af.ACLs[0].Resource.Name)
	}
	// "allow" (CR) → "Allow" (on-disk). Boundary translation pinned.
	if af.ACLs[0].Permission != "Allow" {
		t.Errorf("permission=%q, want Allow (CR 'allow' should capitalise on disk)", af.ACLs[0].Permission)
	}
}

// TestReconcileACLs_DenyLowercase asserts the lowercase-to-capitalised
// translation works for deny too — the broker AclEngine matches
// case-sensitively, so the boundary translation in aclToEntry MUST run.
func TestReconcileACLs_DenyLowercase(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	user := v1alpha1.KafkaUser{
		ObjectMeta: metav1.ObjectMeta{Name: "bob", Namespace: "default"},
		Spec: v1alpha1.KafkaUserSpec{
			Authentication: v1alpha1.KafkaUserAuthentication{Type: "tls"},
			Authorization: &v1alpha1.KafkaUserAuthorization{
				Type: "simple",
				ACLs: []v1alpha1.KafkaUserACL{
					{
						Resource:   v1alpha1.KafkaUserACLResource{Type: "topic", Name: "secret", PatternType: "literal"},
						Operations: []string{"Read"},
						Type:       "deny",
					},
				},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(newACLScheme()).WithObjects(&user).Build()
	if err := reconcileACLs(ctx, c, "default", dir); err != nil {
		t.Fatalf("reconcileACLs: %v", err)
	}
	data, _ := os.ReadFile(aclsPath(dir))
	var af ACLFile
	_ = json.Unmarshal(data, &af)

	if af.ACLs[0].Permission != "Deny" {
		t.Errorf("permission=%q, want Deny", af.ACLs[0].Permission)
	}
}

// TestReconcileACLs_DeletedUserSkipped verifies that ACLs from a CR
// mid-deletion (DeletionTimestamp set) are not emitted. Mirrors the
// pre-gh #135 deleted-skip behaviour now that the source is KafkaUser.
func TestReconcileACLs_DeletedUserSkipped(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	now := metav1.Now()
	user := v1alpha1.KafkaUser{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "carol",
			Namespace:         "default",
			DeletionTimestamp: &now,
			Finalizers:        []string{"some-finalizer"},
		},
		Spec: v1alpha1.KafkaUserSpec{
			Authentication: v1alpha1.KafkaUserAuthentication{Type: "tls"},
			Authorization: &v1alpha1.KafkaUserAuthorization{
				Type: "simple",
				ACLs: []v1alpha1.KafkaUserACL{
					{
						Resource:   v1alpha1.KafkaUserACLResource{Type: "topic", Name: "*", PatternType: "literal"},
						Operations: []string{"Read"},
						Type:       "allow",
					},
				},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(newACLScheme()).WithObjects(&user).Build()
	if err := reconcileACLs(ctx, c, "default", dir); err != nil {
		t.Fatalf("reconcileACLs: %v", err)
	}
	data, _ := os.ReadFile(aclsPath(dir))
	var af ACLFile
	_ = json.Unmarshal(data, &af)

	if len(af.ACLs) != 0 {
		t.Errorf("expected 0 ACL entries for deleted user, got %d", len(af.ACLs))
	}
}

// TestReconcileACLs_DefaultsApplied: defaults are at the CR level
// (kubebuilder), but reconcileACLs must also default missing fields at
// the boundary because the fake client doesn't apply defaulting and
// gh #135's KafkaUserACL marks PatternType + Type + Host as omitempty.
func TestReconcileACLs_DefaultsApplied(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	user := v1alpha1.KafkaUser{
		ObjectMeta: metav1.ObjectMeta{Name: "default-test", Namespace: "default"},
		Spec: v1alpha1.KafkaUserSpec{
			Authentication: v1alpha1.KafkaUserAuthentication{Type: "tls"},
			Authorization: &v1alpha1.KafkaUserAuthorization{
				ACLs: []v1alpha1.KafkaUserACL{
					{
						// PatternType + Type omitted entirely.
						Resource:   v1alpha1.KafkaUserACLResource{Type: "topic", Name: "events"},
						Operations: []string{"Read"},
					},
				},
			},
		},
	}
	c := fake.NewClientBuilder().WithScheme(newACLScheme()).WithObjects(&user).Build()
	if err := reconcileACLs(ctx, c, "default", dir); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(aclsPath(dir))
	var af ACLFile
	_ = json.Unmarshal(data, &af)

	if af.ACLs[0].Resource.PatternType != "literal" {
		t.Errorf("PatternType default not applied: got %q, want literal", af.ACLs[0].Resource.PatternType)
	}
	if af.ACLs[0].Permission != "Allow" {
		t.Errorf("Permission default not applied: got %q, want Allow", af.ACLs[0].Permission)
	}
}

func TestWriteAtomicIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/test.json"

	type payload struct{ X int }
	for i := 0; i < 5; i++ {
		if err := writeAtomic(path, payload{X: i}); err != nil {
			t.Fatalf("writeAtomic iter %d: %v", i, err)
		}
	}
	data, _ := os.ReadFile(path)
	var p payload
	_ = json.Unmarshal(data, &p)
	if p.X != 4 {
		t.Errorf("expected X=4, got %d", p.X)
	}
}
