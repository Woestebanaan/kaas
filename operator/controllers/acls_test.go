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

func TestReconcileACLs_FromKafkaAcl(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	acl := v1alpha1.KafkaAcl{
		ObjectMeta: metav1.ObjectMeta{Name: "payments-acl", Namespace: "default"},
		Spec: v1alpha1.KafkaAclSpec{
			Principal: v1alpha1.AclPrincipal{Kind: "KafkaUser", Name: "alice"},
			Rules: []v1alpha1.AclRule{
				{
					Resource:   v1alpha1.AclResource{Type: "topic", Name: "payments", PatternType: "literal"},
					Operations: []string{"Write"},
					Permission: "Allow",
				},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(newACLScheme()).WithObjects(&acl).Build()
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
}

func TestReconcileACLs_FromKafkaUserGroup(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	group := v1alpha1.KafkaUserGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "analytics-team", Namespace: "default"},
		Spec: v1alpha1.KafkaUserGroupSpec{
			Members: []string{"alice", "bob"},
			Rules: []v1alpha1.AclRule{
				{
					Resource:   v1alpha1.AclResource{Type: "topic", Name: "analytics-", PatternType: "prefix"},
					Operations: []string{"Read", "Describe"},
					Permission: "Allow",
				},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(newACLScheme()).WithObjects(&group).Build()
	if err := reconcileACLs(ctx, c, "default", dir); err != nil {
		t.Fatalf("reconcileACLs: %v", err)
	}

	data, _ := os.ReadFile(aclsPath(dir))
	var af ACLFile
	_ = json.Unmarshal(data, &af)

	// Each member gets one entry → 2 total.
	if len(af.ACLs) != 2 {
		t.Fatalf("expected 2 ACL entries (one per member), got %d", len(af.ACLs))
	}
	principals := map[string]bool{}
	for _, e := range af.ACLs {
		principals[e.Principal] = true
	}
	if !principals["User:alice"] || !principals["User:bob"] {
		t.Errorf("missing expected principals: %v", principals)
	}
}

func TestReconcileACLs_DeletedObjectsSkipped(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	now := metav1.Now()
	acl := v1alpha1.KafkaAcl{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "deleted-acl",
			Namespace:         "default",
			DeletionTimestamp: &now,
			Finalizers:        []string{"some-finalizer"},
		},
		Spec: v1alpha1.KafkaAclSpec{
			Principal: v1alpha1.AclPrincipal{Kind: "KafkaUser", Name: "carol"},
			Rules: []v1alpha1.AclRule{
				{
					Resource:   v1alpha1.AclResource{Type: "topic", Name: "*", PatternType: "literal"},
					Operations: []string{"Read"},
					Permission: "Allow",
				},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(newACLScheme()).WithObjects(&acl).Build()
	if err := reconcileACLs(ctx, c, "default", dir); err != nil {
		t.Fatalf("reconcileACLs: %v", err)
	}

	data, _ := os.ReadFile(aclsPath(dir))
	var af ACLFile
	_ = json.Unmarshal(data, &af)

	if len(af.ACLs) != 0 {
		t.Errorf("expected 0 ACL entries for deleted object, got %d", len(af.ACLs))
	}
}

func TestReconcileACLs_MergesBothSources(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	acl := v1alpha1.KafkaAcl{
		ObjectMeta: metav1.ObjectMeta{Name: "acl1", Namespace: "default"},
		Spec: v1alpha1.KafkaAclSpec{
			Principal: v1alpha1.AclPrincipal{Kind: "KafkaUser", Name: "alice"},
			Rules:     []v1alpha1.AclRule{{Resource: v1alpha1.AclResource{Type: "topic", Name: "t1", PatternType: "literal"}, Operations: []string{"Write"}, Permission: "Allow"}},
		},
	}
	group := v1alpha1.KafkaUserGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "grp1", Namespace: "default"},
		Spec: v1alpha1.KafkaUserGroupSpec{
			Members: []string{"bob"},
			Rules:   []v1alpha1.AclRule{{Resource: v1alpha1.AclResource{Type: "topic", Name: "t2", PatternType: "literal"}, Operations: []string{"Read"}, Permission: "Allow"}},
		},
	}

	c := fake.NewClientBuilder().WithScheme(newACLScheme()).WithObjects(&acl, &group).Build()
	if err := reconcileACLs(ctx, c, "default", dir); err != nil {
		t.Fatalf("reconcileACLs: %v", err)
	}

	data, _ := os.ReadFile(aclsPath(dir))
	var af ACLFile
	_ = json.Unmarshal(data, &af)

	if len(af.ACLs) != 2 {
		t.Fatalf("expected 2 merged entries, got %d", len(af.ACLs))
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
