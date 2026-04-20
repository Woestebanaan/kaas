package k8s

import (
	"fmt"
	"hash/fnv"
)

// Preferred returns true if this broker should attempt to acquire the given partition,
// based on FNV-32a hash modulo broker count. This is a hint only — the Kubernetes Lease
// TTL arbitrates actual ownership, so two brokers competing for the same partition is safe.
func Preferred(topic string, partition int32, selfOrdinal int32, numBrokers int) bool {
	if numBrokers <= 0 {
		return true
	}
	h := fnv.New32a()
	_, _ = fmt.Fprintf(h, "%s/%d", topic, partition)
	return int32(h.Sum32()%uint32(numBrokers)) == selfOrdinal
}
