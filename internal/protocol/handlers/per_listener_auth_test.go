package handlers

import (
	"sync"
	"testing"

	"github.com/woestebanaan/skafka/internal/auth"
	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
)

// recordingAuthorizer counts Authorize calls and returns a fixed
// verdict. Used by the cluster-wide-authz test below.
type recordingAuthorizer struct {
	mu      sync.Mutex
	calls   int
	verdict bool
}

func (a *recordingAuthorizer) Authorize(auth.Principal, auth.Resource, auth.Operation) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls++
	return a.verdict
}

// TestDeleteGroupsClusterWideAuthorizer pins gh #126: authorization
// is cluster-wide. The same Authorizer evaluates every request
// regardless of which listener accepted the connection. The legacy
// per-listener-routing test (from gh #124) was replaced with this
// after authorization moved off the per-listener AuthEngine.
//
// Both the "anon" and "authed" listener call sites consult the
// same cluster-wide Authorizer — verdict=false on the shared
// instance blocks DeleteGroups on either listener.
func TestDeleteGroupsClusterWideAuthorizer(t *testing.T) {
	denyAll := &recordingAuthorizer{verdict: false}
	h := NewDeleteGroupsHandler(nil, denyAll)

	body := encodeDeleteGroupsRequestV2(t, []string{"my-group"})

	for _, listener := range []connstate.ListenerName{"anon", "authed"} {
		out, err := h.Handle(&connstate.ConnState{Listener: listener}, 2, body)
		if err != nil {
			t.Fatalf("Handle on %q: %v", listener, err)
		}
		got := decodeDeleteGroupsResponseV2(t, out)
		if got["my-group"] != int16(codec.ErrGroupAuthorizationFailed) {
			t.Errorf("listener=%q: errCode=%d, want GroupAuthorizationFailed (cluster-wide deny)",
				listener, got["my-group"])
		}
	}
	if denyAll.calls != 2 {
		t.Errorf("cluster-wide Authorizer should be consulted once per listener; got %d calls (want 2)",
			denyAll.calls)
	}
}

// TestSuperUserAuthorizerEarlyAllow pins the superUsers contract:
// a principal matching a configured superUser name bypasses the
// inner Authorizer entirely (early-allow), regardless of what the
// inner would have decided.
func TestSuperUserAuthorizerEarlyAllow(t *testing.T) {
	inner := &recordingAuthorizer{verdict: false} // would deny anything
	su := auth.NewSuperUserAuthorizer([]string{"admin", "CN=operator"}, inner)

	// superUser principal: early-allow, inner never called.
	if !su.Authorize(auth.Principal{Name: "admin"}, auth.Resource{Type: "topic", Name: "secret"}, auth.OpRead) {
		t.Error("superUser admin denied; want allow")
	}
	if !su.Authorize(auth.Principal{Name: "CN=operator"}, auth.Resource{Type: "cluster", Name: "kafka-cluster"}, auth.OpDescribe) {
		t.Error("superUser CN=operator denied; want allow")
	}
	if inner.calls != 0 {
		t.Errorf("inner consulted for superUser principals (calls=%d, want 0)", inner.calls)
	}

	// non-superUser principal: falls through to inner (which denies).
	if su.Authorize(auth.Principal{Name: "bob"}, auth.Resource{Type: "topic", Name: "secret"}, auth.OpRead) {
		t.Error("non-superUser bob allowed; inner should have denied")
	}
	if inner.calls != 1 {
		t.Errorf("inner not consulted for non-superUser; calls=%d, want 1", inner.calls)
	}
}
