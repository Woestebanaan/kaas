package k8s

import (
	"context"
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/woestebanaan/skafka/internal/protocol/handlers"
	v1alpha1 "github.com/woestebanaan/skafka/operator/api/v1alpha1"
)

func newACLClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
}

func userWithAuth(name string) *v1alpha1.KafkaUser {
	return &v1alpha1.KafkaUser{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "skafka"},
		Spec: v1alpha1.KafkaUserSpec{
			Authentication: v1alpha1.KafkaUserAuthentication{Type: "scram-sha-512"},
		},
	}
}

func readUser(t *testing.T, c client.Client, name string) *v1alpha1.KafkaUser {
	t.Helper()
	var u v1alpha1.KafkaUser
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "skafka", Name: name}, &u); err != nil {
		t.Fatalf("get user %s: %v", name, err)
	}
	return &u
}

// TestCreateACL_AppendsNewEntry pins the happy path: a fresh KafkaUser
// CR with no Authorization grows a new spec.authorization.acls entry
// reflecting the wire binding. Defaults are filled (Type=simple,
// patternType=literal, type=allow).
func TestCreateACL_AppendsNewEntry(t *testing.T) {
	c := newACLClient(t, userWithAuth("alice"))
	w := NewKafkaUserACLWriter(c, "skafka")

	err := w.CreateACL(context.Background(), handlers.ACLBinding{
		Principal:    "User:alice",
		ResourceType: "topic",
		ResourceName: "payments",
		PatternType:  "literal",
		Operation:    "Write",
		Permission:   "Allow",
	})
	if err != nil {
		t.Fatalf("CreateACL: %v", err)
	}

	u := readUser(t, c, "alice")
	if u.Spec.Authorization == nil {
		t.Fatal("Authorization not initialised")
	}
	if u.Spec.Authorization.Type != "simple" {
		t.Errorf("Type=%q, want simple", u.Spec.Authorization.Type)
	}
	if len(u.Spec.Authorization.ACLs) != 1 {
		t.Fatalf("expected 1 ACL, got %d", len(u.Spec.Authorization.ACLs))
	}
	entry := u.Spec.Authorization.ACLs[0]
	if entry.Resource.Name != "payments" || entry.Resource.Type != "topic" || entry.Resource.PatternType != "literal" {
		t.Errorf("resource=%+v", entry.Resource)
	}
	if entry.Type != "allow" {
		t.Errorf("type=%q, want allow (lowercased)", entry.Type)
	}
	if len(entry.Operations) != 1 || entry.Operations[0] != "Write" {
		t.Errorf("operations=%v, want [Write]", entry.Operations)
	}
}

// TestCreateACL_IdempotentOnExactDup re-runs the same CreateACL twice
// and asserts no phantom duplicate row. AdminClient retries on
// transient errors; doubling rows would corrupt the CR.
func TestCreateACL_IdempotentOnExactDup(t *testing.T) {
	c := newACLClient(t, userWithAuth("alice"))
	w := NewKafkaUserACLWriter(c, "skafka")

	b := handlers.ACLBinding{
		Principal: "User:alice", ResourceType: "topic", ResourceName: "p",
		PatternType: "literal", Operation: "Read", Permission: "Allow",
	}
	if err := w.CreateACL(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if err := w.CreateACL(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	u := readUser(t, c, "alice")
	if len(u.Spec.Authorization.ACLs) != 1 {
		t.Fatalf("expected 1 ACL after dup-create, got %d", len(u.Spec.Authorization.ACLs))
	}
	if len(u.Spec.Authorization.ACLs[0].Operations) != 1 {
		t.Errorf("operations doubled: %v", u.Spec.Authorization.ACLs[0].Operations)
	}
}

// TestCreateACL_FoldsIntoExistingEntry — same resource + permission,
// different op → the writer must append the op to the existing entry's
// Operations slice (not create a sibling row). This keeps the CR shape
// close to how operators hand-author KafkaUsers.
func TestCreateACL_FoldsIntoExistingEntry(t *testing.T) {
	c := newACLClient(t, userWithAuth("alice"))
	w := NewKafkaUserACLWriter(c, "skafka")

	for _, op := range []string{"Read", "Write"} {
		if err := w.CreateACL(context.Background(), handlers.ACLBinding{
			Principal: "User:alice", ResourceType: "topic", ResourceName: "p",
			PatternType: "literal", Operation: op, Permission: "Allow",
		}); err != nil {
			t.Fatal(err)
		}
	}
	u := readUser(t, c, "alice")
	if len(u.Spec.Authorization.ACLs) != 1 {
		t.Fatalf("expected 1 ACL row, got %d", len(u.Spec.Authorization.ACLs))
	}
	ops := u.Spec.Authorization.ACLs[0].Operations
	if len(ops) != 2 || ops[0] != "Read" || ops[1] != "Write" {
		t.Errorf("operations=%v, want [Read Write]", ops)
	}
}

// TestCreateACL_AllowAndDenyAreSeparateEntries — same resource but
// different permission must NOT fold; Allow + Deny are distinct rules.
func TestCreateACL_AllowAndDenyAreSeparateEntries(t *testing.T) {
	c := newACLClient(t, userWithAuth("alice"))
	w := NewKafkaUserACLWriter(c, "skafka")

	if err := w.CreateACL(context.Background(), handlers.ACLBinding{
		Principal: "User:alice", ResourceType: "topic", ResourceName: "p",
		PatternType: "literal", Operation: "Read", Permission: "Allow",
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.CreateACL(context.Background(), handlers.ACLBinding{
		Principal: "User:alice", ResourceType: "topic", ResourceName: "p",
		PatternType: "literal", Operation: "Read", Permission: "Deny",
	}); err != nil {
		t.Fatal(err)
	}
	u := readUser(t, c, "alice")
	if len(u.Spec.Authorization.ACLs) != 2 {
		t.Fatalf("expected 2 entries (allow + deny), got %d", len(u.Spec.Authorization.ACLs))
	}
}

// TestCreateACL_UnknownPrincipal surfaces ErrUnknownPrincipal so the
// handler can return INVALID_REQUEST rather than silently failing or
// auto-creating the CR.
func TestCreateACL_UnknownPrincipal(t *testing.T) {
	c := newACLClient(t) // no KafkaUsers
	w := NewKafkaUserACLWriter(c, "skafka")

	err := w.CreateACL(context.Background(), handlers.ACLBinding{
		Principal: "User:ghost", ResourceType: "topic", ResourceName: "p",
		PatternType: "literal", Operation: "Read", Permission: "Allow",
	})
	if !errors.Is(err, handlers.ErrUnknownPrincipal) {
		t.Fatalf("err=%v, want ErrUnknownPrincipal", err)
	}
}

// TestCreateACL_InvalidPrincipalShape rejects non-User: prefixes; the
// CR model only stores ACLs on KafkaUser, so Group:/ServiceAccount:
// can't be persisted without inventing a target.
func TestCreateACL_InvalidPrincipalShape(t *testing.T) {
	c := newACLClient(t)
	w := NewKafkaUserACLWriter(c, "skafka")

	cases := []string{"", "User:", "Group:alice", "alice", "user:lowercase"}
	for _, p := range cases {
		err := w.CreateACL(context.Background(), handlers.ACLBinding{
			Principal: p, ResourceType: "topic", ResourceName: "p",
			PatternType: "literal", Operation: "Read", Permission: "Allow",
		})
		if !errors.Is(err, handlers.ErrInvalidPrincipal) {
			t.Errorf("principal=%q: err=%v, want ErrInvalidPrincipal", p, err)
		}
	}
}

// TestDeleteACLs_PartialOpSplitsEntry — the entry has Read + Write,
// the filter matches Read only; the entry must remain with [Write].
// This is the gh #107 partial-op semantics: AdminClient deletes one op
// at a time, never the whole row.
func TestDeleteACLs_PartialOpSplitsEntry(t *testing.T) {
	u := userWithAuth("alice")
	u.Spec.Authorization = &v1alpha1.KafkaUserAuthorization{
		Type: "simple",
		ACLs: []v1alpha1.KafkaUserACL{{
			Resource:   v1alpha1.KafkaUserACLResource{Type: "topic", Name: "p", PatternType: "literal"},
			Operations: []string{"Read", "Write"},
			Type:       "allow",
		}},
	}
	c := newACLClient(t, u)
	w := NewKafkaUserACLWriter(c, "skafka")

	matched, err := w.DeleteACLs(context.Background(), handlers.ACLFilter{
		Principal: "User:alice", ResourceType: "topic", ResourceName: "p",
		PatternType: "literal", Operation: "Read", Permission: "Allow",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(matched) != 1 || matched[0].Operation != "Read" {
		t.Fatalf("matched=%+v, want one Read binding", matched)
	}
	got := readUser(t, c, "alice")
	if len(got.Spec.Authorization.ACLs) != 1 {
		t.Fatalf("entry was dropped instead of split: %+v", got.Spec.Authorization.ACLs)
	}
	ops := got.Spec.Authorization.ACLs[0].Operations
	if len(ops) != 1 || ops[0] != "Write" {
		t.Errorf("operations=%v, want [Write]", ops)
	}
}

// TestDeleteACLs_DropsEmptyEntry — when the last operation in an entry
// is removed, the whole entry goes away rather than leaving an
// Operations=[] row that the operator's reconcileACLs would silently
// drop anyway.
func TestDeleteACLs_DropsEmptyEntry(t *testing.T) {
	u := userWithAuth("alice")
	u.Spec.Authorization = &v1alpha1.KafkaUserAuthorization{
		ACLs: []v1alpha1.KafkaUserACL{{
			Resource:   v1alpha1.KafkaUserACLResource{Type: "topic", Name: "p", PatternType: "literal"},
			Operations: []string{"Read"},
			Type:       "allow",
		}},
	}
	c := newACLClient(t, u)
	w := NewKafkaUserACLWriter(c, "skafka")

	if _, err := w.DeleteACLs(context.Background(), handlers.ACLFilter{
		Principal: "User:alice", Operation: "Read",
	}); err != nil {
		t.Fatal(err)
	}
	got := readUser(t, c, "alice")
	if len(got.Spec.Authorization.ACLs) != 0 {
		t.Errorf("entry survived after last op removed: %+v", got.Spec.Authorization.ACLs)
	}
}

// TestDeleteACLs_FiltersByPrincipal — a filter scoped to User:alice
// must not touch User:bob's ACLs even when their resource shape
// matches.
func TestDeleteACLs_FiltersByPrincipal(t *testing.T) {
	alice := userWithAuth("alice")
	alice.Spec.Authorization = &v1alpha1.KafkaUserAuthorization{
		ACLs: []v1alpha1.KafkaUserACL{{
			Resource:   v1alpha1.KafkaUserACLResource{Type: "topic", Name: "p", PatternType: "literal"},
			Operations: []string{"Read"},
			Type:       "allow",
		}},
	}
	bob := userWithAuth("bob")
	bob.Spec.Authorization = &v1alpha1.KafkaUserAuthorization{
		ACLs: []v1alpha1.KafkaUserACL{{
			Resource:   v1alpha1.KafkaUserACLResource{Type: "topic", Name: "p", PatternType: "literal"},
			Operations: []string{"Read"},
			Type:       "allow",
		}},
	}
	c := newACLClient(t, alice, bob)
	w := NewKafkaUserACLWriter(c, "skafka")

	matched, err := w.DeleteACLs(context.Background(), handlers.ACLFilter{
		Principal: "User:alice", Operation: "Read",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(matched) != 1 || matched[0].Principal != "User:alice" {
		t.Fatalf("matched=%+v, want one User:alice binding", matched)
	}
	if got := readUser(t, c, "bob"); len(got.Spec.Authorization.ACLs) != 1 {
		t.Errorf("bob's ACL was touched: %+v", got.Spec.Authorization.ACLs)
	}
}

// TestListACLs_FilterAcrossUsers — list with no filter returns every
// (entry, operation) pair across every KafkaUser CR. The expansion
// (one entry with Operations=[Read,Write] → two bindings) is what
// DescribeAcls eventually folds back via groupBindingsByResource.
func TestListACLs_FilterAcrossUsers(t *testing.T) {
	alice := userWithAuth("alice")
	alice.Spec.Authorization = &v1alpha1.KafkaUserAuthorization{
		ACLs: []v1alpha1.KafkaUserACL{{
			Resource:   v1alpha1.KafkaUserACLResource{Type: "topic", Name: "p", PatternType: "literal"},
			Operations: []string{"Read", "Write"},
			Type:       "allow",
		}},
	}
	bob := userWithAuth("bob")
	bob.Spec.Authorization = &v1alpha1.KafkaUserAuthorization{
		ACLs: []v1alpha1.KafkaUserACL{{
			Resource:   v1alpha1.KafkaUserACLResource{Type: "group", Name: "g", PatternType: "literal"},
			Operations: []string{"Read"},
			Type:       "deny",
		}},
	}
	c := newACLClient(t, alice, bob)
	w := NewKafkaUserACLWriter(c, "skafka")

	out, err := w.ListACLs(context.Background(), handlers.ACLFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 3 {
		t.Fatalf("expected 3 bindings (2 alice + 1 bob), got %d: %+v", len(out), out)
	}
	// Spot check the deny case — operator-side stores it lowercase, the
	// writer must capitalise on the way out so the wire response and
	// the on-disk acls.json agree.
	var sawBobDeny bool
	for _, b := range out {
		if b.Principal == "User:bob" && b.Permission == "Deny" {
			sawBobDeny = true
		}
	}
	if !sawBobDeny {
		t.Error("expected User:bob/Deny binding in result")
	}
}

// TestListACLs_FiltersPatternType pins the gh #135 default: an entry
// authored without an explicit patternType is treated as "literal" on
// list, so a filter for PatternType="literal" must include it.
func TestListACLs_FiltersPatternType(t *testing.T) {
	u := userWithAuth("alice")
	u.Spec.Authorization = &v1alpha1.KafkaUserAuthorization{
		ACLs: []v1alpha1.KafkaUserACL{
			{
				Resource:   v1alpha1.KafkaUserACLResource{Type: "topic", Name: "p"}, // patternType blank
				Operations: []string{"Read"},
				Type:       "allow",
			},
			{
				Resource:   v1alpha1.KafkaUserACLResource{Type: "topic", Name: "pref-", PatternType: "prefix"},
				Operations: []string{"Write"},
				Type:       "allow",
			},
		},
	}
	c := newACLClient(t, u)
	w := NewKafkaUserACLWriter(c, "skafka")

	out, err := w.ListACLs(context.Background(), handlers.ACLFilter{PatternType: "literal"})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || out[0].Operation != "Read" {
		t.Errorf("literal filter returned %+v, expected one Read binding", out)
	}
	out, err = w.ListACLs(context.Background(), handlers.ACLFilter{PatternType: "prefix"})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || out[0].Operation != "Write" {
		t.Errorf("prefix filter returned %+v, expected one Write binding", out)
	}
}

// TestListACLs_SkipsDeletedUser — CRs mid-deletion must not surface
// their ACLs (mirrors the operator's reconcileACLs DeletionTimestamp
// skip in acls.go:64).
func TestListACLs_SkipsDeletedUser(t *testing.T) {
	now := metav1.Now()
	u := userWithAuth("alice")
	u.DeletionTimestamp = &now
	u.Finalizers = []string{"x"}
	u.Spec.Authorization = &v1alpha1.KafkaUserAuthorization{
		ACLs: []v1alpha1.KafkaUserACL{{
			Resource:   v1alpha1.KafkaUserACLResource{Type: "topic", Name: "p"},
			Operations: []string{"Read"},
			Type:       "allow",
		}},
	}
	c := newACLClient(t, u)
	w := NewKafkaUserACLWriter(c, "skafka")

	out, err := w.ListACLs(context.Background(), handlers.ACLFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 0 {
		t.Errorf("expected 0 bindings for deleted user, got %+v", out)
	}
}

// TestPrincipalToUserName pins the surface explicitly — the parser is
// what makes "Group:" / "ServiceAccount:" land on ErrInvalidPrincipal
// rather than silently mismatching a CR name.
func TestPrincipalToUserName(t *testing.T) {
	if n, err := principalToUserName("User:alice"); err != nil || n != "alice" {
		t.Errorf("User:alice → %q, %v", n, err)
	}
	for _, bad := range []string{"", "User:", "Group:alice", "Alice", "user:alice"} {
		if _, err := principalToUserName(bad); !errors.Is(err, handlers.ErrInvalidPrincipal) {
			t.Errorf("%q: err=%v, want ErrInvalidPrincipal", bad, err)
		}
	}
}
