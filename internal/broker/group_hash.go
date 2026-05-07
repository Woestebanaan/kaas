package broker

import (
	"hash/fnv"
	"sort"
)

// GroupCoordinatorSlot returns the deterministic preferred-coordinator
// index for groupID under a fixed broker count. Mirrors Apache Kafka's
//
//	partitionFor(groupId) = abs(hashCode(groupId)) % offsetsTopicPartitionCount
//
// (GroupMetadataManager.scala:318) — pure function of (groupID, numBrokers),
// requiring zero per-group registration.
//
// Hash: FNV-1a 32-bit. Apache Kafka uses Java's String.hashCode() — that
// matters there because the choice is exposed to wire-compatible client
// reimplementations of the algorithm. Skafka clients receive the
// coordinator broker via the FindCoordinator response and don't compute
// it locally, so any deterministic 32-bit hash works as long as every
// broker uses the same one. FNV-1a is in the Go stdlib, deterministic
// across architectures, no external dep.
//
// numBrokers must be the StatefulSet replica count, NOT len(alive).
// Holding the divisor constant is what keeps existing groups stable
// across rolling restarts — if we mod by the alive set size, every
// pod up/down event reshuffles ~(N-1)/N of all groups. Apache Kafka
// gets stability for free because numPartitions(__consumer_offsets)
// is set at topic-create time and is effectively immutable; we
// emulate that by holding numBrokers fixed.
func GroupCoordinatorSlot(groupID string, numBrokers int) int {
	if numBrokers <= 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(groupID))
	// fnv.Sum32() is uint32, modding by a positive int is always
	// non-negative; no abs() guard needed (unlike Apache Kafka's
	// Utils.abs which protects against Integer.MIN_VALUE in Java's
	// signed hashCode).
	return int(h.Sum32() % uint32(numBrokers))
}

// PickGroupCoordinator returns the broker ID assigned to coordinate
// groupID. brokers is the FULL set known to the cluster (sorted by
// stable identity, e.g. StatefulSet ordinal); alive maps each broker
// ID to its liveness.
//
// Decision tree:
//  1. Compute slot = hash(groupID) % len(brokers).
//  2. If brokers[slot] is alive, return it (the stable preferred case).
//  3. Otherwise, fall back to a deterministic alternate from the alive
//     subset — slot % len(aliveSorted). Stable across multiple invocations
//     of the same alive snapshot, so a consumer rejoining during a
//     transient broker outage doesn't ping-pong.
//
// Returns "" when no broker is alive (caller surfaces
// CoordinatorNotAvailable; client retries FindCoordinator).
func PickGroupCoordinator(groupID string, brokers []string, alive map[string]bool) string {
	if len(brokers) == 0 {
		return ""
	}
	sortedBrokers := append([]string(nil), brokers...)
	sort.Strings(sortedBrokers)
	slot := GroupCoordinatorSlot(groupID, len(sortedBrokers))
	if alive[sortedBrokers[slot]] {
		return sortedBrokers[slot]
	}
	// Fallback: among alive brokers, pick at slot-modulo-len-of-alive.
	var aliveSorted []string
	for _, b := range sortedBrokers {
		if alive[b] {
			aliveSorted = append(aliveSorted, b)
		}
	}
	if len(aliveSorted) == 0 {
		return ""
	}
	return aliveSorted[slot%len(aliveSorted)]
}
