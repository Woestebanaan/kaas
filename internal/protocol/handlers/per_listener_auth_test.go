package handlers

import (
	"sync"
	"testing"

	"github.com/woestebanaan/skafka/internal/auth"
	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
)

// recordingDenyEngine denies every Authorize call. recordingAllowEngine
// allows everything. Both record the calls they receive so the test can
// assert which engine was consulted for each conn.
type recordingEngine struct {
	mu      sync.Mutex
	calls   int
	verdict bool
}

func (e *recordingEngine) NewSASLExchange(string) (auth.SASLExchange, error) { return nil, nil }
func (e *recordingEngine) AuthenticateTLS(string) (auth.Principal, error) {
	return auth.Principal{}, nil
}
func (e *recordingEngine) Authorize(auth.Principal, auth.Resource, auth.Operation) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls++
	return e.verdict
}
func (e *recordingEngine) CheckProduceQuota(auth.Principal, int) int32 { return 0 }
func (e *recordingEngine) CheckFetchQuota(auth.Principal, int) int32   { return 0 }
func (e *recordingEngine) RequiresPreAuth() bool                       { return !e.verdict }

// TestDeleteGroupsRoutesPerListenerEngine pins gh #124: a Produce/Fetch
// or DeleteGroups on the "anon" listener consults the AllowAll engine
// (verdict=true → request passes through); the SAME request on the
// "authed" listener consults the Deny engine (verdict=false → request
// rejected with the auth-failed code). One handler, one ConnState
// listener tag, two different decisions. Demonstrates the
// User:ANONYMOUS workaround no longer applies — anonymous listeners
// don't trigger ACL evaluation at all.
func TestDeleteGroupsRoutesPerListenerEngine(t *testing.T) {
	allowOnAnon := &recordingEngine{verdict: true}
	denyOnAuthed := &recordingEngine{verdict: false}
	engines := auth.PerListenerAuthEngine{
		"anon":   allowOnAnon,
		"authed": denyOnAuthed,
	}
	h := NewDeleteGroupsHandler(nil, engines)

	body := encodeDeleteGroupsRequestV2(t, []string{"my-group"})

	// Anon listener: AllowAll engine → request passes through to the
	// (nil-coord) coordinator path, yielding ErrCoordinatorNotAvailable.
	out, err := h.Handle(&connstate.ConnState{Listener: "anon"}, 2, body)
	if err != nil {
		t.Fatalf("anon Handle: %v", err)
	}
	got := decodeDeleteGroupsResponseV2(t, out)
	if got["my-group"] != int16(codec.ErrCoordinatorNotAvailable) {
		t.Errorf("anon listener: errCode=%d, want CoordinatorNotAvailable (Allow + nil coord)",
			got["my-group"])
	}
	if allowOnAnon.calls != 1 {
		t.Errorf("anon engine consulted %d times, want 1", allowOnAnon.calls)
	}
	if denyOnAuthed.calls != 0 {
		t.Errorf("authed engine consulted on anon listener; %d calls (want 0)", denyOnAuthed.calls)
	}

	// Authed listener: Deny engine → request blocked with GROUP_AUTH_FAILED.
	out, err = h.Handle(&connstate.ConnState{Listener: "authed"}, 2, body)
	if err != nil {
		t.Fatalf("authed Handle: %v", err)
	}
	got = decodeDeleteGroupsResponseV2(t, out)
	if got["my-group"] != int16(codec.ErrGroupAuthorizationFailed) {
		t.Errorf("authed listener: errCode=%d, want GroupAuthorizationFailed (Deny)",
			got["my-group"])
	}
	if denyOnAuthed.calls != 1 {
		t.Errorf("authed engine consulted %d times, want 1", denyOnAuthed.calls)
	}
	if allowOnAnon.calls != 1 {
		t.Errorf("anon engine consulted on authed listener (calls now %d, was 1 before)",
			allowOnAnon.calls)
	}
}
