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
}
