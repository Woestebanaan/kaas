package connstate

import "github.com/woestebanaan/skafka/internal/auth"

// ConnState holds per-connection mutable state shared between the server and handlers.
type ConnState struct {
	ClientID      string
	Principal     *auth.Principal
	SASLDone      bool
	SASLMechanism string          // set by SaslHandshakeHandler after negotiation
	SASLState     auth.SASLExchange // nil until exchange is started
	IsTLS         bool            // true when the connection arrived on the TLS listener
}
