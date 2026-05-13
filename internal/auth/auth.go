package auth

// Principal identifies an authenticated subject.
type Principal struct {
	Name string
	Kind string // "User", "ServiceAccount"
}

// Resource is the object being accessed in an authorization check.
type Resource struct {
	Type        string // "topic", "group", "cluster", "transactionalId"
	Name        string
	PatternType string // "literal", "prefix"
}

// Operation is an action performed on a resource.
type Operation string

const (
	OpRead            Operation = "Read"
	OpWrite           Operation = "Write"
	OpCreate          Operation = "Create"
	OpDelete          Operation = "Delete"
	OpAlter           Operation = "Alter"
	OpDescribe        Operation = "Describe"
	OpDescribeConfigs Operation = "DescribeConfigs"
	OpAlterConfigs    Operation = "AlterConfigs"
)

// Quotas holds per-user throughput limits loaded from credentials.json.
type Quotas struct {
	ProducerByteRate  *int64
	ConsumerByteRate  *int64
	RequestPercentage *int32
}

// SASLExchange is the per-connection state machine for one SASL mechanism.
// Created once per connection after SaslHandshake; Step is called for each
// SaslAuthenticate message.
type SASLExchange interface {
	// Step processes the next client message and returns the server's response.
	// When done=true, authentication is complete and Principal() is valid.
	Step(clientMsg []byte) (serverMsg []byte, done bool, err error)
	Principal() Principal
}

// AuthEngine authenticates connections and authorizes operations.
type AuthEngine interface {
	// NewSASLExchange returns a fresh exchange for the given mechanism name.
	NewSASLExchange(mechanism string) (SASLExchange, error)
	// AuthenticateTLS authenticates a TLS connection by its peer certificate CN.
	AuthenticateTLS(cn string) (Principal, error)
	// Authorize checks whether principal may perform op on resource.
	Authorize(principal Principal, resource Resource, operation Operation) bool
	// CheckProduceQuota deducts bytes from the principal's producer bucket.
	// Returns ThrottleTimeMs (0 = no throttle).
	CheckProduceQuota(principal Principal, bytes int) int32
	// CheckFetchQuota deducts bytes from the principal's consumer bucket.
	CheckFetchQuota(principal Principal, bytes int) int32
	// RequiresPreAuth reports whether the dispatcher must reject non-
	// pre-SASL API requests until SASL completes. RealAuthEngine returns
	// true; AllowAllAuthEngine returns false (anonymous-OK listener).
	// Used by the dispatcher to gate per-connection access without
	// consulting a global "require SASL" flag.
	RequiresPreAuth() bool
}

// AuthEngineSelector picks the AuthEngine that handles a connection
// based on the listener it arrived on. A skafka broker can host several
// listeners with different auth policies side by side (anonymous +
// SCRAM, mTLS-external + SCRAM-internal, …). The selector keeps that
// decision out of the handlers — they just call
// `selector.For(conn.Listener).Authorize(...)` and the right engine
// services the request.
type AuthEngineSelector interface {
	// For returns the AuthEngine assigned to the listener. Implementations
	// must always return a non-nil engine; an unknown listener falls back
	// to a default (typically the anonymous-OK AllowAllAuthEngine).
	For(listener string) AuthEngine
}

// SingleAuthEngine wraps a single AuthEngine and ignores the listener
// argument. Preserves the pre-per-listener-auth behaviour for tests and
// the dev-mode boot path that has no listener-config plumbing.
type SingleAuthEngine struct {
	Engine AuthEngine
}

// NewSingleAuthEngine returns a selector that always yields the same engine.
func NewSingleAuthEngine(e AuthEngine) AuthEngineSelector {
	return SingleAuthEngine{Engine: e}
}

// For returns the wrapped engine regardless of listener.
func (s SingleAuthEngine) For(_ string) AuthEngine { return s.Engine }

// PerListenerAuthEngine maps a listener name to its assigned AuthEngine.
// An entry keyed by "" acts as the default for any listener not found
// in the map — wire it to AllowAllAuthEngine when the broker hosts
// any anonymous listener, so unknown / untagged connections don't
// surprise-deny.
type PerListenerAuthEngine map[string]AuthEngine

// For returns the engine assigned to the listener, falling back to the
// "" entry on miss. Returns nil only if neither the listener nor ""
// has an entry — callers should treat that as a config bug.
func (m PerListenerAuthEngine) For(listener string) AuthEngine {
	if e, ok := m[listener]; ok {
		return e
	}
	return m[""]
}
