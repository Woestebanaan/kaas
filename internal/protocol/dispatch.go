package protocol

import (
	"errors"
	"fmt"

	"github.com/woestebanaan/skafka/internal/auth"
	"github.com/woestebanaan/skafka/internal/connstate"
)

// ErrResponseWritten is the sentinel a Handler returns when it has
// already written the response directly to the underlying connection
// (typically via the splicer in ConnState.Splicer for gh #130). The
// dispatcher propagates this verbatim; serveConn checks for it and
// skips its own writeFrame + Flush. Handler middleware still runs —
// the error is part of the Handler interface contract, not a bypass.
var ErrResponseWritten = errors.New("protocol: handler wrote response directly via splicer")

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

// SplicingHandler is an optional secondary interface that a Handler can
// implement when it can build a response with embedded file-section
// splice points. The dispatcher routes through HandleSplicing only when
// (a) the handler implements this interface AND (b) ConnState.Splicer
// reports IsKernelSplice() == true — i.e., on plaintext *net.TCPConn
// where sendfile actually saves the userspace copy. Anywhere else
// (TLS, test fakes, non-kernel-splice fallback) the standard Handle
// path runs.
//
// HandleSplicing receives the full RequestHeader (which Handle doesn't)
// because the splice path is responsible for writing its own
// correlation-ID prefix + frame length onto the wire. Returns
// ErrResponseWritten on success — the dispatcher propagates it so
// serveConn skips writeFrame.
type SplicingHandler interface {
	HandleSplicing(conn *connstate.ConnState, hdr RequestHeader, body []byte, splicer Splicer) error
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
//
// gh #124: per-listener pre-auth gating is sourced from the engines
// selector. The selector is set after construction (NewDispatcher
// runs in main.go before the broker / engine map is assembled), via
// SetAuthEngines. When nil — tests, dev-mode, byte-opacity harness —
// the gate is open: all listeners behave like anonymous-OK.
type Dispatcher struct {
	handlers   map[int16]registration
	middleware []Middleware // applied to handlers at Register time (see middleware.go)
	engines    auth.AuthEngineSelector
}

// preSASLKeys are API keys permitted before SASL authentication completes.
var preSASLKeys = map[int16]bool{17: true, 18: true, 36: true}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{handlers: make(map[int16]registration)}
}

// SetAuthEngines wires the per-listener auth selector (gh #124) used by
// the pre-SASL gate in Dispatch. Call BEFORE serving requests; the
// gate reads the field on the hot path so a nil-to-non-nil transition
// mid-flight would race. Pass auth.NewSingleAuthEngine(eng) when there
// is only one engine for the whole broker (today's main.go fallback).
func (d *Dispatcher) SetAuthEngines(sel auth.AuthEngineSelector) {
	d.engines = sel
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
	// gh #124: per-listener pre-auth gate. The engine for this
	// connection's listener decides whether non-pre-SASL APIs are
	// allowed before SASL completes — RealAuthEngine returns true
	// (deny pre-auth), AllowAllAuthEngine returns false (open).
	// When the selector itself is nil (tests, dev-mode), the gate is
	// open; the legacy global "require SASL on every listener" flag
	// is gone (#124 follow-up to gh #139). The mtls path sets
	// SASLDone=true after AuthenticateTLS so the same gate works for
	// cert-presenting clients on TLS listeners without a special case.
	if d.engines != nil {
		if eng := d.engines.For(string(connState.Listener)); eng != nil &&
			eng.RequiresPreAuth() && !connState.SASLDone && !preSASLKeys[hdr.APIKey] {
			return errorResponse(hdr, ErrClusterAuthorizationFailed), nil
		}
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

	// gh #130: if the handler implements SplicingHandler and the
	// connection has a kernel-splice-capable Splicer, route through
	// the splice path. The handler writes its own framed response
	// directly to the splicer (correlation prefix + body + records
	// bytes spliced via sendfile) and returns ErrResponseWritten on
	// success. On error or when conditions don't match, fall through
	// to the standard Handle path below.
	if sh, ok := reg.handler.(SplicingHandler); ok && connState != nil && connState.Splicer != nil {
		if sp, ok := connState.Splicer.(Splicer); ok && sp.IsKernelSplice() {
			if err := sh.HandleSplicing(connState, hdr, body, sp); err != nil {
				if errors.Is(err, ErrResponseWritten) {
					return nil, err
				}
				// Anything else: treat as a transient failure of the
				// splice path; fall back to the standard route so the
				// client gets a normal response.
			} else {
				// HandleSplicing returned nil — we treat that as
				// "handler chose not to splice" and fall through.
			}
		}
	}

	responseBody, err := reg.handler.Handle(connState, hdr.APIVersion, body)
	if err != nil {
		if errors.Is(err, ErrResponseWritten) {
			// Splicing-aware handler took over: response is already on
			// the wire. Propagate the sentinel so serveConn skips its
			// own writeFrame.
			return nil, err
		}
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
