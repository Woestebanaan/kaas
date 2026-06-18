package auth

import "testing"

// stubAuthorizer is a deny-everything inner authorizer so the test
// can prove SuperUserAuthorizer truly bypasses the inner. Returning
// false from the inner unconditionally means any positive result has
// to have come from the super-user short-circuit.
type stubAuthorizer struct{ called bool }

func (s *stubAuthorizer) Authorize(_ Principal, _ Resource, _ Operation) bool {
	s.called = true
	return false
}

// TestSuperUserAuthorizer_Bypass pins gh #44's super-user contract:
// a principal whose name is in the configured superUsers list bypasses
// the inner Authorizer entirely. Matches Strimzi's
// `authorization.superUsers` semantic — the most common ops escape
// hatch when an ACL accidentally locks the admin user out.
func TestSuperUserAuthorizer_Bypass(t *testing.T) {
	inner := &stubAuthorizer{}
	az := NewSuperUserAuthorizer([]string{"admin", "ops"}, inner)

	if !az.Authorize(Principal{Name: "admin", Kind: "User"},
		Resource{Type: "topic", Name: "anything", PatternType: "literal"}, OpWrite) {
		t.Error("super-user 'admin' should be allowed without consulting inner")
	}
	if inner.called {
		t.Error("inner authorizer was called for super-user — short-circuit failed")
	}
}

// TestSuperUserAuthorizer_NonSuperDelegatesToInner pins the
// complement: a non-super principal MUST go through the inner
// authorizer. This is the load-bearing path: ACL evaluation runs
// only for non-supers.
func TestSuperUserAuthorizer_NonSuperDelegatesToInner(t *testing.T) {
	inner := &stubAuthorizer{}
	az := NewSuperUserAuthorizer([]string{"admin"}, inner)

	if az.Authorize(Principal{Name: "alice", Kind: "User"},
		Resource{Type: "topic", Name: "private", PatternType: "literal"}, OpWrite) {
		t.Error("non-super principal should NOT be allowed when inner says no")
	}
	if !inner.called {
		t.Error("inner authorizer was not consulted for non-super principal")
	}
}

// TestSuperUserAuthorizer_PrincipalKindIgnored documents that the
// matcher uses principal.Name verbatim. mTLS subjects show up as
// Kind="User" Name="CN=admin" — operators are responsible for
// configuring the superUsers list with the exact strings used by
// the AuthEngine's principal-extraction layer (so mTLS users would
// configure "CN=admin", SCRAM users would configure "admin").
func TestSuperUserAuthorizer_PrincipalKindIgnored(t *testing.T) {
	inner := &stubAuthorizer{}
	az := NewSuperUserAuthorizer([]string{"CN=admin"}, inner)

	// Bare-name "admin" must NOT match — only the exact "CN=admin" does.
	if az.Authorize(Principal{Name: "admin", Kind: "User"}, Resource{Type: "topic"}, OpRead) {
		t.Error("super-user list is matched verbatim against Name; bare 'admin' should not match 'CN=admin'")
	}
	if !az.Authorize(Principal{Name: "CN=admin", Kind: "User"}, Resource{Type: "topic"}, OpRead) {
		t.Error("'CN=admin' should match the configured super-user")
	}
}
