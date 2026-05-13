package handlers

import (
	"sync"
	"testing"

	"github.com/woestebanaan/skafka/internal/auth"
	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
)

// allowAuth permits everything — used to isolate the rest of the
// DeleteGroups handler logic from auth.
type allowAuth struct{}

func (allowAuth) NewSASLExchange(string) (auth.SASLExchange, error) { return nil, nil }
func (allowAuth) AuthenticateTLS(string) (auth.Principal, error)    { return auth.Principal{}, nil }
func (allowAuth) Authorize(auth.Principal, auth.Resource, auth.Operation) bool {
	return true
}
func (allowAuth) CheckProduceQuota(auth.Principal, int) int32 { return 0 }
func (allowAuth) CheckFetchQuota(auth.Principal, int) int32   { return 0 }
func (allowAuth) RequiresPreAuth() bool                       { return false }

// denyAuth records every Authorize call and lets the test program a
// per-(resource.Name, op) decision. Anything not explicitly programmed
// returns false (deny by default), matching Apache Kafka's
// allow.everyone.if.no.acl.found=false posture.
type denyAuth struct {
	mu     sync.Mutex
	calls  []auth.Resource
	allow  map[string]bool
}

func newDenyAuth() *denyAuth {
	return &denyAuth{allow: map[string]bool{}}
}

func (d *denyAuth) NewSASLExchange(string) (auth.SASLExchange, error) { return nil, nil }
func (d *denyAuth) AuthenticateTLS(string) (auth.Principal, error)    { return auth.Principal{}, nil }
func (d *denyAuth) Authorize(_ auth.Principal, r auth.Resource, _ auth.Operation) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, r)
	return d.allow[r.Name]
}
func (d *denyAuth) CheckProduceQuota(auth.Principal, int) int32 { return 0 }
func (d *denyAuth) CheckFetchQuota(auth.Principal, int) int32   { return 0 }
func (d *denyAuth) RequiresPreAuth() bool                       { return true }

// encodeDeleteGroupsRequestV2 builds a minimal flexible v2 body for
// the handler tests. Single compact array of compact strings + tagged
// fields — the simplest shape AdminClient sends.
func encodeDeleteGroupsRequestV2(t *testing.T, groups []string) []byte {
	t.Helper()
	w := codec.NewWriter()
	w.WriteCompactArray(len(groups), func() {
		for _, g := range groups {
			w.WriteCompactString(g)
		}
	})
	w.WriteEmptyTaggedFields()
	return w.Bytes()
}

// decodeDeleteGroupsResponseV2 parses the response body into a map
// of {groupID: errCode}. Throws away tagged fields (we don't need
// to inspect them).
func decodeDeleteGroupsResponseV2(t *testing.T, body []byte) map[string]int16 {
	t.Helper()
	r := codec.NewReader(body)
	if _, err := r.ReadInt32(); err != nil { // throttle
		t.Fatal(err)
	}
	out := map[string]int16{}
	if err := r.ReadCompactArray(func() error {
		gid, err := r.ReadCompactString()
		if err != nil {
			return err
		}
		errCode, err := r.ReadInt16()
		if err != nil {
			return err
		}
		out[gid] = errCode
		return r.ReadTaggedFields()
	}); err != nil {
		t.Fatal(err)
	}
	if err := r.ReadTaggedFields(); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestDeleteGroupsHandlerAuthGateRejectsUnauthorized guards the gh
// #89 ACL contract: a principal without OpDelete on the Group
// resource gets GROUP_AUTHORIZATION_FAILED (30) per group, and
// the coordinator never sees the request. Without the gate, an
// unauthenticated TLS client could clear arbitrary consumer
// groups — exactly the surface AdminClient exposes via
// `--delete --group <id>`.
func TestDeleteGroupsHandlerAuthGateRejectsUnauthorized(t *testing.T) {
	denyEng := newDenyAuth() // empty allow map → deny every group

	// nil coord: if the gate failed, the handler would dereference
	// nil and panic. The test passing means the gate caught the
	// rejection before the coordinator branch.
	h := NewDeleteGroupsHandler(nil, auth.NewSingleAuthEngine(denyEng))

	body := encodeDeleteGroupsRequestV2(t, []string{"orders", "payments"})
	out, err := h.Handle(&connstate.ConnState{}, 2, body)
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	got := decodeDeleteGroupsResponseV2(t, out)
	if got["orders"] != int16(codec.ErrGroupAuthorizationFailed) {
		t.Errorf("orders errCode=%d, want %d (GROUP_AUTHORIZATION_FAILED)",
			got["orders"], codec.ErrGroupAuthorizationFailed)
	}
	if got["payments"] != int16(codec.ErrGroupAuthorizationFailed) {
		t.Errorf("payments errCode=%d, want %d", got["payments"], codec.ErrGroupAuthorizationFailed)
	}
	// Authorize was called once per group with type=group, op=Delete.
	denyEng.mu.Lock()
	defer denyEng.mu.Unlock()
	if len(denyEng.calls) != 2 {
		t.Errorf("Authorize call count = %d, want 2", len(denyEng.calls))
	}
	for _, r := range denyEng.calls {
		if r.Type != "group" {
			t.Errorf("Authorize resource.Type=%q, want \"group\"", r.Type)
		}
	}
}

// TestDeleteGroupsHandlerAuthGatePermitsAllowed: the inverse —
// when auth grants Delete on one group but not another, only the
// allowed one reaches the coordinator. Coordinator-side error
// codes (here 16 NOT_COORDINATOR via nil coord fallback) cover
// the allowed group; the denied one gets 30 directly.
func TestDeleteGroupsHandlerAuthGatePermitsAllowed(t *testing.T) {
	denyEng := newDenyAuth()
	denyEng.allow["orders"] = true
	// "payments" stays denied.

	// nil coord branches into the "no-coordinator wired" fallback,
	// returning CoordinatorNotAvailable (15) for each allowed group.
	h := NewDeleteGroupsHandler(nil, auth.NewSingleAuthEngine(denyEng))

	body := encodeDeleteGroupsRequestV2(t, []string{"orders", "payments"})
	out, err := h.Handle(&connstate.ConnState{}, 2, body)
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	got := decodeDeleteGroupsResponseV2(t, out)
	if got["orders"] != int16(codec.ErrCoordinatorNotAvailable) {
		t.Errorf("orders errCode=%d, want %d (CoordinatorNotAvailable; coord=nil branch)",
			got["orders"], codec.ErrCoordinatorNotAvailable)
	}
	if got["payments"] != int16(codec.ErrGroupAuthorizationFailed) {
		t.Errorf("payments errCode=%d, want %d (still denied)",
			got["payments"], codec.ErrGroupAuthorizationFailed)
	}
}

// TestDeleteGroupsHandlerNoAuthEnginePassesThrough: a handler
// constructed with nil auth must not crash. Local-dev mode runs
// without an auth engine; the handler should treat that as
// "all calls allowed" and forward to the coordinator. Defense in
// depth — the broker's New() always wires AllowAllAuthEngine, but
// custom embeddings might not.
func TestDeleteGroupsHandlerNoAuthEnginePassesThrough(t *testing.T) {
	h := NewDeleteGroupsHandler(nil, nil)
	body := encodeDeleteGroupsRequestV2(t, []string{"x"})
	out, err := h.Handle(&connstate.ConnState{}, 2, body)
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	got := decodeDeleteGroupsResponseV2(t, out)
	if got["x"] != int16(codec.ErrCoordinatorNotAvailable) {
		t.Errorf("nil-auth nil-coord path: errCode=%d, want %d",
			got["x"], codec.ErrCoordinatorNotAvailable)
	}
}

// TestDeleteGroupsHandlerAllowAllPassesThrough: with permissive
// auth wired (production default in dev/test), every group reaches
// the coordinator. Same expected output as the no-auth case.
func TestDeleteGroupsHandlerAllowAllPassesThrough(t *testing.T) {
	h := NewDeleteGroupsHandler(nil, auth.NewSingleAuthEngine(allowAuth{}))
	body := encodeDeleteGroupsRequestV2(t, []string{"x", "y"})
	out, err := h.Handle(&connstate.ConnState{}, 2, body)
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	got := decodeDeleteGroupsResponseV2(t, out)
	for _, gid := range []string{"x", "y"} {
		if got[gid] != int16(codec.ErrCoordinatorNotAvailable) {
			t.Errorf("%s: errCode=%d, want %d",
				gid, got[gid], codec.ErrCoordinatorNotAvailable)
		}
	}
}
