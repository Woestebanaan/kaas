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
// The byte-rate fields are PER BROKER (KIP-13 semantics): with N brokers
// the effective cluster-wide ceiling is N × the configured value.
type Quotas struct {
	ProducerMaxByteRatePerBroker *int64
	ConsumerMaxByteRatePerBroker *int64
	RequestPercentage            *int32
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
// `selector.For(conn.Listener).NewSASLExchange(...)` and the right
// engine services the SASL handshake.
//
// gh #126: post-split, the selector is consulted ONLY for the
// authentication-side concerns (SASL handshake, mTLS principal
// extraction, RequiresPreAuth). Authorization moves to a separate
// cluster-wide Authorizer (see below) that runs on every connection
// regardless of listener.
type AuthEngineSelector interface {
	// For returns the AuthEngine assigned to the listener. Implementations
	// must always return a non-nil engine; an unknown listener falls back
	// to a default (typically the anonymous-OK AllowAllAuthEngine).
	For(listener string) AuthEngine
}

// Authorizer evaluates whether a principal may perform op on resource.
// gh #126: authorization is cluster-wide (mirrors Strimzi's
// `Kafka.spec.kafka.authorization`). The same Authorizer runs
// regardless of which listener accepted the connection — anonymous
// listeners no longer bypass ACLs; instead, the listener establishes
// principal=ANONYMOUS and the Authorizer evaluates ACLs against that
// principal, falling through to deny unless `User:ANONYMOUS` has an
// allow ACL or is in superUsers.
type Authorizer interface {
	// Authorize returns true iff principal is allowed to op on
	// resource. Implementations should:
	//   1. Short-circuit allow if principal matches a configured
	//      superUser.
	//   2. Otherwise evaluate the configured ACL set.
	Authorize(principal Principal, resource Resource, op Operation) bool
}

// QuotaChecker is the cluster-wide quota path. gh #126 splits this
// off from the per-listener AuthEngine so quotas (per-principal
// throughput limits) apply uniformly regardless of which listener
// the producer/consumer connected on.
type QuotaChecker interface {
	// CheckProduceQuota returns ThrottleTimeMs (0 = no throttle).
	CheckProduceQuota(principal Principal, bytes int) int32
	// CheckFetchQuota returns ThrottleTimeMs (0 = no throttle).
	CheckFetchQuota(principal Principal, bytes int) int32
}

// AllowAllAuthorizer is the no-authorization mode — every Authorize
// call returns true. Used when `authorization` is omitted from the
// cluster config (matches Strimzi's "no authorization property = no
// restrictions" semantic). Quotas still run; they're orthogonal.
type AllowAllAuthorizer struct{}

// NewAllowAllAuthorizer returns an Authorizer that permits everything.
func NewAllowAllAuthorizer() Authorizer { return AllowAllAuthorizer{} }

// Authorize always returns true.
func (AllowAllAuthorizer) Authorize(Principal, Resource, Operation) bool { return true }

// NoQuotaChecker is the no-throttle mode — every Check returns 0.
// Used when no listener has SCRAM/mTLS auth wired (anonymous-only
// brokers; ANONYMOUS has no quota config to enforce against).
type NoQuotaChecker struct{}

// NewNoQuotaChecker returns a QuotaChecker that never throttles.
func NewNoQuotaChecker() QuotaChecker { return NoQuotaChecker{} }

// CheckProduceQuota always returns 0.
func (NoQuotaChecker) CheckProduceQuota(Principal, int) int32 { return 0 }

// CheckFetchQuota always returns 0.
func (NoQuotaChecker) CheckFetchQuota(Principal, int) int32 { return 0 }

// SuperUserAuthorizer wraps an inner Authorizer with a superUsers
// early-allow check. Principal names matching a configured superUser
// bypass ACL evaluation entirely (Strimzi `authorization.superUsers`
// semantic). Matches verbatim against principal.Name — callers must
// configure with `CN=foo` for mTLS subjects or bare names for SCRAM
// principals.
type SuperUserAuthorizer struct {
	supers map[string]struct{}
	inner  Authorizer
}

// NewSuperUserAuthorizer wraps inner with a set of superUser
// principal names. nil inner is invalid — callers should pass
// NewAllowAllAuthorizer when authorization is disabled (in which
// case the SuperUserAuthorizer is redundant; main.go can skip it).
func NewSuperUserAuthorizer(supers []string, inner Authorizer) *SuperUserAuthorizer {
	set := make(map[string]struct{}, len(supers))
	for _, s := range supers {
		set[s] = struct{}{}
	}
	return &SuperUserAuthorizer{supers: set, inner: inner}
}

// Authorize short-circuits to true for superUsers; otherwise
// delegates to the inner Authorizer.
func (s *SuperUserAuthorizer) Authorize(p Principal, r Resource, op Operation) bool {
	if _, ok := s.supers[p.Name]; ok {
		return true
	}
	return s.inner.Authorize(p, r, op)
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
