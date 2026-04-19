package auth

import "context"

type Credentials struct {
	Mechanism string
	Username  string
	Payload   []byte
}

type Principal struct {
	Name string
	Kind string // "User", "ServiceAccount"
}

type Resource struct {
	Type        string // "topic", "group", "cluster", "transactionalId"
	Name        string
	PatternType string // "literal", "prefix"
}

type Operation string

const (
	OpRead             Operation = "Read"
	OpWrite            Operation = "Write"
	OpCreate           Operation = "Create"
	OpDelete           Operation = "Delete"
	OpAlter            Operation = "Alter"
	OpDescribe         Operation = "Describe"
	OpDescribeConfigs  Operation = "DescribeConfigs"
	OpAlterConfigs     Operation = "AlterConfigs"
)

type AuthEngine interface {
	Authenticate(ctx context.Context, creds Credentials) (Principal, error)
	Authorize(principal Principal, resource Resource, operation Operation) bool
}
