package connstate

import (
	"sync"

	"github.com/woestebanaan/skafka/internal/auth"
)

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
//
// gh #132 item 2: with per-connection request pipelining, multiple
// handler goroutines may touch ConnState concurrently. Mu guards the
// mutable fields below (ClientID, SASLDone, SASLMechanism, SASLState,
// Principal). IsTLS and Listener are set once at connection accept
// time and read-only thereafter — they don't need the lock.
type ConnState struct {
	Mu            sync.Mutex
	ClientID      string
	Principal     *auth.Principal
	SASLDone      bool
	SASLMechanism string            // set by SaslHandshakeHandler after negotiation
	SASLState     auth.SASLExchange // nil until exchange is started
	IsTLS         bool              // true when the connection arrived on the TLS listener
	Listener      ListenerName      // which listener received this connection
}
