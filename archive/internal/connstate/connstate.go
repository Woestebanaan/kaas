package connstate

import "github.com/woestebanaan/skafka/internal/auth"

// ListenerName labels which listener a connection arrived on. The
// string value matches the listener's Name field on the protocol
// Server's ListenerConfig and is used as a key in the per-listener
// auth engine map (gh #124). Names are user-chosen — the chart's
// values.yaml `listeners[].name` flows through SKAFKA_LISTENERS env
// into ListenerConfig.Name into here. The Metadata handler's
// advertised-host logic and the chart's existing "internal" /
// "external" naming convention are still in use; they're just no
// longer pinned by a Go constant.
type ListenerName string

// ConnState holds per-connection mutable state shared between the server and handlers.
// Owned by a single goroutine (serveConn) — Apache Kafka's per-connection
// "one request in flight" contract means no concurrent mutation, so no
// lock is needed.
type ConnState struct {
	ClientID      string
	Principal     *auth.Principal
	SASLDone      bool
	SASLMechanism string            // set by SaslHandshakeHandler after negotiation
	SASLState     auth.SASLExchange // nil until exchange is started
	IsTLS         bool              // true when the connection arrived on the TLS listener
	Listener      ListenerName      // which listener received this connection

	// Splicer is non-nil for connections where the server can perform a
	// kernel-side splice (sendfile(2)) on outgoing response data — today
	// that's plaintext *net.TCPConn only. Splicing-aware handlers (the
	// Fetch path, gh #130) check this field and write their response
	// directly via the splicer instead of materialising it as []byte.
	// They signal that path by returning protocol.ErrResponseWritten;
	// the server skips its own writeFrame in response. Concrete type is
	// any value that implements protocol.Splicer — ConnState carries it
	// as an interface{} only to avoid a circular import between
	// connstate and protocol. The protocol package narrows it back to
	// Splicer at use-site.
	Splicer any
}
