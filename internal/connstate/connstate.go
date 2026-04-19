package connstate

import "github.com/woestebanaan/skafka/internal/auth"

// ConnState holds per-connection mutable state shared between the server and handlers.
type ConnState struct {
	ClientID  string
	Principal *auth.Principal // nil until SASL authentication completes
	SASLDone  bool
}
