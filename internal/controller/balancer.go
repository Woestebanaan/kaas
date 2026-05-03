package controller

import (
	"hash/fnv"
	"sort"

	"github.com/woestebanaan/skafka/pkg/kafkaapi"
)

// TopicSpec is the controller's view of an entry in the cluster's topic
// catalog: how many partitions a topic has. The catalog itself is sourced
// from the KafkaTopic CRD watcher (wired in step 8b).
type TopicSpec struct {
	Name           string
	PartitionCount int32
}

// GroupSpec is the controller's view of an active consumer group. The
// only attribute the balancer needs is the group ID — partition count
// has no analogue for groups (each group is a single coordinated unit).
// HeartbeatServer.ActiveGroups is the canonical GroupSource in v1; tests
// pass an in-memory stub.
type GroupSpec struct {
	GroupID string
}

// Balance computes a partition-to-broker assignment under v1 strict-stability
// rules:
//
//  1. Preserve every existing (topic, partition, broker) tuple where the
//     broker is still in the alive set. Stable assignments minimise log
//     migration, which is the dominant cost on a shared PVC.
//  2. For partitions that lost their broker (or are newly created), pick
//     a fresh broker via rendezvous (highest-random-weight) hashing keyed
//     on (topic, partition, broker_id). This gives a deterministic,
//     evenly-distributed placement without coordination.
//  3. Bump the partition epoch by one whenever the assigned broker
//     changes — the new owner uses the bumped epoch for its segment files
//     and the per-batch fence.
//
// brokers is the list of broker IDs the controller currently considers
// alive. Caller is responsible for the alive/dead determination
// (heartbeat freshness, drain state, etc.).
//
// Returns nil if brokers is empty — there's nothing to assign.
func Balance(
	prev *kafkaapi.Assignment,
	brokers []string,
	topics []TopicSpec,
) []kafkaapi.PartitionAssignment {
	if len(brokers) == 0 {
		return nil
	}

	// Sort broker IDs for deterministic rendezvous tiebreaking.
	alive := append([]string(nil), brokers...)
	sort.Strings(alive)
	aliveSet := make(map[string]struct{}, len(alive))
	for _, b := range alive {
		aliveSet[b] = struct{}{}
	}

	// Build a fast lookup for prior assignments: "topic/partition" → entry.
	prevMap := map[string]kafkaapi.PartitionAssignment{}
	if prev != nil {
		for _, p := range prev.Partitions {
			prevMap[partitionKey(p.Topic, p.Partition)] = p
		}
	}

	out := make([]kafkaapi.PartitionAssignment, 0, totalPartitions(topics))
	for _, ts := range topics {
		for partition := int32(0); partition < ts.PartitionCount; partition++ {
			key := partitionKey(ts.Name, partition)
			pa, hadPrev := prevMap[key]
			if hadPrev {
				if _, ok := aliveSet[pa.Broker]; ok {
					// Stable: keep existing assignment + epoch.
					out = append(out, pa)
					continue
				}
			}
			broker := rendezvousPick(ts.Name, partition, alive)
			epoch := uint32(1)
			if hadPrev {
				epoch = pa.Epoch + 1 // bump on reassignment
			}
			out = append(out, kafkaapi.PartitionAssignment{
				Topic:     ts.Name,
				Partition: partition,
				Broker:    broker,
				Epoch:     epoch,
				Role:      kafkaapi.PartitionRoleLeader,
			})
		}
	}
	return out
}

// rendezvousPick is highest-random-weight hashing: hash(topic, partition,
// broker) for each broker, return the broker with the highest hash.
// Deterministic given the same (topic, partition, brokers); minimal churn
// when brokers come or go (only the partitions that hashed to the
// affected broker need to move).
func rendezvousPick(topic string, partition int32, brokers []string) string {
	if len(brokers) == 0 {
		return ""
	}
	var (
		bestBroker string
		bestHash   uint64
	)
	for _, b := range brokers {
		h := rendezvousHash(topic, partition, b)
		if bestBroker == "" || h > bestHash {
			bestBroker = b
			bestHash = h
		}
	}
	return bestBroker
}

// rendezvousHash combines (topic, partition, broker) into a single uint64.
// FNV-1a is fine here — we don't need cryptographic strength, just a
// uniform distribution and sub-microsecond evaluation. The hash is part
// of placement logic, not stored on disk, so the algorithm can change
// across versions without a migration.
func rendezvousHash(topic string, partition int32, broker string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(topic))
	h.Write([]byte{0})
	// 4-byte big-endian partition.
	h.Write([]byte{
		byte(partition >> 24),
		byte(partition >> 16),
		byte(partition >> 8),
		byte(partition),
	})
	h.Write([]byte{0})
	h.Write([]byte(broker))
	return h.Sum64()
}

// partitionKey is duplicated from internal/broker/coordinator.go because
// importing across that boundary forms an unwanted dependency direction.
// Both functions must produce identical keys; if they drift, ownership
// lookups will silently miss.
func partitionKey(topic string, partition int32) string {
	buf := make([]byte, 0, len(topic)+12)
	buf = append(buf, topic...)
	buf = append(buf, '/')
	buf = appendInt32(buf, partition)
	return string(buf)
}

func appendInt32(dst []byte, v int32) []byte {
	if v == 0 {
		return append(dst, '0')
	}
	if v < 0 {
		dst = append(dst, '-')
		v = -v
	}
	var stack [10]byte
	n := 0
	for v > 0 {
		stack[n] = byte('0' + v%10)
		v /= 10
		n++
	}
	for i := n - 1; i >= 0; i-- {
		dst = append(dst, stack[i])
	}
	return dst
}

func totalPartitions(topics []TopicSpec) int {
	n := 0
	for _, t := range topics {
		n += int(t.PartitionCount)
	}
	return n
}

// BalanceGroups computes a consumer-group-to-broker assignment under the
// same v1 strict-stability + rendezvous-hash rules as Balance:
//
//  1. If a group is currently assigned to a still-alive broker, keep it.
//     Avoids spurious group-state migrations on a recompute.
//  2. Otherwise, rendezvous-hash to a fresh broker keyed on the group ID.
//  3. Bump the per-group epoch by one whenever the assigned broker
//     changes — gives clients a monotonic counter to correlate state
//     transitions with, even though v1 doesn't yet use it for fencing.
//
// Returns nil if brokers is empty.
func BalanceGroups(
	prev *kafkaapi.Assignment,
	brokers []string,
	groups []GroupSpec,
) []kafkaapi.ConsumerGroupAssignment {
	if len(brokers) == 0 {
		return nil
	}

	alive := append([]string(nil), brokers...)
	sort.Strings(alive)
	aliveSet := make(map[string]struct{}, len(alive))
	for _, b := range alive {
		aliveSet[b] = struct{}{}
	}

	prevMap := map[string]kafkaapi.ConsumerGroupAssignment{}
	if prev != nil {
		for _, g := range prev.ConsumerGroups {
			prevMap[g.GroupID] = g
		}
	}

	out := make([]kafkaapi.ConsumerGroupAssignment, 0, len(groups))
	for _, gs := range groups {
		ga, hadPrev := prevMap[gs.GroupID]
		if hadPrev {
			if _, ok := aliveSet[ga.Broker]; ok {
				out = append(out, ga)
				continue
			}
		}
		broker := rendezvousPickGroup(gs.GroupID, alive)
		epoch := uint32(1)
		if hadPrev {
			epoch = ga.Epoch + 1
		}
		out = append(out, kafkaapi.ConsumerGroupAssignment{
			GroupID: gs.GroupID,
			Broker:  broker,
			Epoch:   epoch,
		})
	}
	return out
}

// rendezvousPickGroup is the group analogue of rendezvousPick: hash each
// broker against the group ID and pick the highest. Group has no
// partition concept, so the hash key is just (group_id, broker_id).
func rendezvousPickGroup(groupID string, brokers []string) string {
	if len(brokers) == 0 {
		return ""
	}
	var (
		best     string
		bestHash uint64
	)
	for _, b := range brokers {
		h := groupHash(groupID, b)
		if best == "" || h > bestHash {
			best = b
			bestHash = h
		}
	}
	return best
}

func groupHash(groupID, broker string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(groupID))
	h.Write([]byte{0})
	h.Write([]byte(broker))
	return h.Sum64()
}
