package lock

type PartitionLock interface {
	Lock(topic string, partition int32) error
	Unlock(topic string, partition int32) error
	IsLocked(topic string, partition int32) bool
}
