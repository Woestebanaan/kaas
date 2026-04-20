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
