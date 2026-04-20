package lease

import "context"

type LeaderChange struct {
	Topic     string
	Partition int32
	LeaderID  int32
}

type LeaseManager interface {
	Acquire(ctx context.Context, topic string, partition int32) error
	Release(topic string, partition int32) error
	IsLeader(topic string, partition int32) bool
	// LeaderFor returns the node ordinal of the current leader, or -1 if unknown.
	LeaderFor(topic string, partition int32) int32
	WatchLeaders(ctx context.Context) (<-chan LeaderChange, error)
}

// CoordinatorLeaseManager manages one Kubernetes Lease per consumer group.
// The lease holder IS the coordinator for that group.
type CoordinatorLeaseManager interface {
	AcquireCoordinator(ctx context.Context, groupID string) error
	ReleaseCoordinator(groupID string) error
	IsCoordinator(groupID string) bool
	// CoordinatorFor returns the node ordinal of the current coordinator, or -1 if unknown.
	CoordinatorFor(groupID string) int32
	// WaitForCoordinator blocks until any broker holds the coordinator lease for groupID,
	// or the context is cancelled. Returns true if a coordinator became known.
	WaitForCoordinator(ctx context.Context, groupID string) bool
}
