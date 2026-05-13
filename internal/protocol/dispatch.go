package protocol

import (
	"fmt"

	"github.com/woestebanaan/skafka/internal/connstate"
)

// Handler is a broker-side API handler.
// It receives the raw request body bytes and the negotiated API version,
// and returns the raw response body bytes (without the frame length prefix).
type Handler interface {
	Handle(conn *connstate.ConnState, version int16, body []byte) ([]byte, error)
}

// HandlerFunc adapts a plain function to Handler.
type HandlerFunc func(conn *connstate.ConnState, version int16, body []byte) ([]byte, error)

func (f HandlerFunc) Handle(conn *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	return f(conn, version, body)
}

// versionRange describes the min/max API versions a handler supports.
type versionRange struct {
	min, max int16
}

type registration struct {
	handler  Handler
	versions versionRange
}

// Dispatcher routes incoming requests to the correct handler by API key.
type Dispatcher struct {
	handlers    map[int16]registration
	middleware  []Middleware // applied to handlers at Register time (see middleware.go)
	RequireSASL bool         // when true, reject non-SASL requests from unauthenticated connections
}

// preSASLKeys are API keys permitted before SASL authentication completes.
var preSASLKeys = map[int16]bool{17: true, 18: true, 36: true}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{handlers: make(map[int16]registration)}
}

// Register adds a handler for the given API key and version range.
// The handler is wrapped by every middleware registered via Use(),
// so the hot path sees a pre-chained Handler — no closure rebuild
// per Dispatch call.
func (d *Dispatcher) Register(apiKey int16, min, max int16, h Handler) {
	d.handlers[apiKey] = registration{handler: d.chain(apiKey, h), versions: versionRange{min, max}}
}

// Dispatch decodes the request header, checks version support, calls the handler,
// and returns a complete framed response ready to write to the wire.
func (d *Dispatcher) Dispatch(hdr RequestHeader, body []byte, connState *connstate.ConnState) ([]byte, error) {
	// Reject pre-auth requests when SASL is required. Two ways SASL becomes
	// required: the global Dispatcher.RequireSASL flag (set when the broker
	// is configured to enforce auth on every listener), OR the connection
	// arrived on the SASL-required authed listener (gh #139). The
	// connstate.ListenerAuthed tag is set by serveConn for the dedicated
	// authed-listener accept loop; it lets a single broker safely host
	// both an anonymous listener and an authed listener side by side.
	requireSASLForConn := d.RequireSASL || connState.Listener == connstate.ListenerAuthed
	if requireSASLForConn && !connState.SASLDone && !preSASLKeys[hdr.APIKey] {
		return errorResponse(hdr, ErrClusterAuthorizationFailed), nil
	}

	reg, ok := d.handlers[hdr.APIKey]
	if !ok {
		return errorResponse(hdr, ErrUnsupportedVersion), nil
	}
	if hdr.APIVersion < reg.versions.min || hdr.APIVersion > reg.versions.max {
		// ApiVersions (key 18) must always return a valid response so clients can
		// discover supported versions even when they send a version we don't know.
		// Use our max supported version for the response format.
		if hdr.APIKey == 18 {
			overrideHdr := hdr
			overrideHdr.APIVersion = reg.versions.max
			responseBody, err := reg.handler.Handle(connState, overrideHdr.APIVersion, body)
			if err != nil {
				return nil, fmt.Errorf("handler api_key=18: %w", err)
			}
			prefix := buildResponsePrefix(hdr.CorrelationID, false) // ApiVersions always uses ResponseHeaderV0
			return append(prefix, responseBody...), nil
		}
		return errorResponseRaw(hdr.CorrelationID, ErrUnsupportedVersion), nil
	}

	responseBody, err := reg.handler.Handle(connState, hdr.APIVersion, body)
	if err != nil {
		return nil, fmt.Errorf("handler api_key=%d: %w", hdr.APIKey, err)
	}

	flexible := flexibleResponseHeader(hdr.APIKey, hdr.APIVersion)
	prefix := buildResponsePrefix(hdr.CorrelationID, flexible)
	return append(prefix, responseBody...), nil
}

// SupportedVersions returns the [min, max] version range for each registered API key.
// Used by the ApiVersions handler.
func (d *Dispatcher) SupportedVersions() map[int16][2]int16 {
	out := make(map[int16][2]int16, len(d.handlers))
	for k, r := range d.handlers {
		out[k] = [2]int16{r.versions.min, r.versions.max}
	}
	return out
}

// ErrUnsupportedVersion is the wire error code for unsupported API version.
const ErrUnsupportedVersion int16 = 35

// ErrClusterAuthorizationFailed is returned for requests made before SASL completes.
const ErrClusterAuthorizationFailed int16 = 31

// errorResponse builds a minimal error response for a known API version.
func errorResponse(hdr RequestHeader, errCode int16) []byte {
	flexible := flexibleRequestHeader(hdr.APIKey, hdr.APIVersion)
	prefix := buildResponsePrefix(hdr.CorrelationID, flexible)
	return append(prefix, byte(errCode>>8), byte(errCode))
}

// errorResponseRaw builds a non-flexible error response (used for unknown/unsupported versions
// where we cannot know what encoding the client expects).
func errorResponseRaw(correlationID int32, errCode int16) []byte {
	prefix := buildResponsePrefix(correlationID, false)
	return append(prefix, byte(errCode>>8), byte(errCode))
}
