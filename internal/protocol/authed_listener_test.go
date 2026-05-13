package protocol

import (
	"testing"

	"github.com/woestebanaan/skafka/internal/connstate"
)

// TestAuthedListenerRejectsUnauthenticatedRequest pins gh #139's
// per-listener SASL gate. A request arriving on connstate.ListenerAuthed
// that hasn't completed SASL must be rejected with
// CLUSTER_AUTHORIZATION_FAILED, regardless of the global
// Dispatcher.RequireSASL flag. Pre-SASL API keys (17 SaslHandshake,
// 18 ApiVersions, 36 SaslAuthenticate) are allowed so the handshake
// itself can proceed.
func TestAuthedListenerRejectsUnauthenticatedRequest(t *testing.T) {
	d := NewDispatcher()
	// Register Metadata (api_key=3) — a non-pre-SASL API.
	d.Register(3, 0, 12, HandlerFunc(func(c *connstate.ConnState, v int16, b []byte) ([]byte, error) {
		return []byte{0, 0, 0, 0}, nil
	}))

	cases := []struct {
		name     string
		listener connstate.ListenerName
		apiKey   int16
		wantErr  bool
		wantCode int16
	}{
		{"authed + Metadata (no SASL)", connstate.ListenerAuthed, 3, true, ErrClusterAuthorizationFailed},
		{"authed + ApiVersions (pre-SASL, no SASL needed)", connstate.ListenerAuthed, 18, false, 0},
		{"authed + SaslHandshake (pre-SASL allowed)", connstate.ListenerAuthed, 17, false, 0},
		{"authed + SaslAuthenticate (pre-SASL allowed)", connstate.ListenerAuthed, 36, false, 0},
		{"internal + Metadata (anon OK)", connstate.ListenerInternal, 3, false, 0},
		{"external + Metadata (anon OK on TLS)", connstate.ListenerExternal, 3, false, 0},
	}
	// Register the pre-SASL keys + a non-pre-SASL one for the internal cases.
	for _, k := range []int16{17, 18, 36} {
		d.Register(k, 0, 12, HandlerFunc(func(c *connstate.ConnState, v int16, b []byte) ([]byte, error) {
			return []byte{0, 0, 0, 0}, nil
		}))
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state := &connstate.ConnState{Listener: tc.listener}
			resp, err := d.Dispatch(RequestHeader{APIKey: tc.apiKey, APIVersion: 0, CorrelationID: 1}, nil, state)
			if err != nil {
				t.Fatalf("dispatch: %v", err)
			}
			// Error responses have a 2-byte error code right after the
			// 4-byte correlation id (5-byte for flexible headers but
			// the api_key=3 case here isn't flexible at v=0).
			if tc.wantErr {
				// errorResponse layout: [4-byte correlation][maybe tag-buffer][2-byte errCode]
				// For api_key=3 v=0 the response header is not flexible.
				if len(resp) < 6 {
					t.Fatalf("response too short to carry an error code: %v", resp)
				}
				// Last two bytes are the error code (big-endian int16).
				gotCode := int16(resp[len(resp)-2])<<8 | int16(resp[len(resp)-1])
				if gotCode != tc.wantCode {
					t.Errorf("error code = %d, want %d (ClusterAuthorizationFailed)", gotCode, tc.wantCode)
				}
			} else {
				// Non-error responses from our stub handler are 4 bytes of body
				// (the {0,0,0,0} above) prefixed by the framed response header.
				// We just confirm Dispatch didn't return an error response form
				// matching the rejection path.
				if len(resp) == 6 && (resp[4] != 0 || resp[5] != 0) {
					t.Errorf("expected pass-through, got 2-byte error code 0x%02x%02x", resp[4], resp[5])
				}
			}
		})
	}
}
