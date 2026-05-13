package connstate

import "github.com/woestebanaan/skafka/internal/auth"

// ListenerName labels which listener a connection arrived on.
// It determines which advertised hostname the Metadata handler returns.
type ListenerName string

const (
	ListenerInternal ListenerName = "internal" // plaintext, in-cluster clients (headless DNS)
	ListenerExternal ListenerName = "external" // TLS, external clients (per-broker hostnames)
	// ListenerAuthed (gh #139) is a plaintext-but-SASL-required listener.
	// Used by the perf bench Jobs to verify quotas fire — quotas need a
	// non-ANONYMOUS principal, which means SASL must complete before the
	// dispatcher accepts non-pre-SASL APIs. The dispatcher consults this
	// tag at request time to enforce that gate per-listener.
	ListenerAuthed ListenerName = "authed"
)

// ConnState holds per-connection mutable state shared between the server and handlers.
type ConnState struct {
	ClientID      string
	Principal     *auth.Principal
	SASLDone      bool
	SASLMechanism string            // set by SaslHandshakeHandler after negotiation
	SASLState     auth.SASLExchange // nil until exchange is started
	IsTLS         bool              // true when the connection arrived on the TLS listener
	Listener      ListenerName      // which listener received this connection
}
