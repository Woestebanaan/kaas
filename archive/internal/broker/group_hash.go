package broker

import (
	"hash/fnv"
	"sort"
)

// coordinatorSlot is the deterministic-preferred-broker hash used by
// every coordinator-routing path (group, txn). Pure function of (key,
// numBrokers), requiring zero per-key registration. Apache's analogue:
//
//	partitionFor(key) = abs(hashCode(key)) % numTopicPartitions
//
// where the topic in question is `__consumer_offsets` for groups
// (GroupMetadataManager.scala:318) or `__transaction_state` for txns
// (TransactionStateManager.scala). Skafka has neither topic — it hashes
// the key directly into the broker set.
//
// Hash: FNV-1a 32-bit. Apache uses Java's String.hashCode() — that
// matters there because the choice is exposed to wire-compatible client
// reimplementations of the algorithm. Skafka clients receive the
// coordinator broker via the FindCoordinator response and don't compute
// it locally, so any deterministic 32-bit hash works as long as every
// broker uses the same one. FNV-1a is in the Go stdlib, deterministic
// across architectures, no external dep.
//
// numBrokers must be the StatefulSet replica count, NOT len(alive).
// Holding the divisor constant is what keeps existing assignments
// stable across rolling restarts — if we mod by the alive set size,
// every pod up/down event reshuffles ~(N-1)/N of all keys. Apache
// gets stability for free because numPartitions(__consumer_offsets) /
// numPartitions(__transaction_state) is set at topic-create time and
// is effectively immutable; we emulate that by holding numBrokers
// fixed at the StatefulSet replica count.
func coordinatorSlot(key string, numBrokers int) int {
	if numBrokers <= 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	// fnv.Sum32() is uint32, modding by a positive int is always
	// non-negative; no abs() guard needed (unlike Apache's
	// Utils.abs which protects against Integer.MIN_VALUE in Java's
	// signed hashCode).
	return int(h.Sum32() % uint32(numBrokers))
}

// GroupCoordinatorSlot returns the deterministic preferred-coordinator
// index for groupID. Public wrapper around coordinatorSlot — kept for
// gh #92 callsites and API stability. See coordinatorSlot for the
// hash + numBrokers-divisor invariants.
func GroupCoordinatorSlot(groupID string, numBrokers int) int {
	return coordinatorSlot(groupID, numBrokers)
}

// TxnCoordinatorSlot returns the deterministic preferred-coordinator
// index for transactionalID. Sibling of GroupCoordinatorSlot; same
// hash and same "numBrokers stays fixed" invariant. Used by gh #91 to
// route InitProducerId / AddPartitionsToTxn / EndTxn requests for a
// given transactional.id to a single broker that owns its state.
func TxnCoordinatorSlot(transactionalID string, numBrokers int) int {
	return coordinatorSlot(transactionalID, numBrokers)
}

// pickCoordinator is the shared "preferred broker, fall back through
// the alive subset deterministically" decision used by PickGroup-
// Coordinator and PickTxnCoordinator. brokers is the FULL set known
// to the cluster (sorted by stable identity, e.g. StatefulSet
// ordinal); alive maps each broker ID to its liveness.
//
// Decision tree:
//  1. Compute slot = hash(key) % len(brokers).
//  2. If brokers[slot] is alive, return it (the stable preferred case).
//  3. Otherwise, fall back to a deterministic alternate from the alive
//     subset — slot % len(aliveSorted). Stable across multiple
//     invocations of the same alive snapshot, so a client rejoining
//     during a transient broker outage doesn't ping-pong.
//
// Returns "" when no broker is alive (caller surfaces
// CoordinatorNotAvailable; client retries FindCoordinator).
func pickCoordinator(key string, brokers []string, alive map[string]bool) string {
	if len(brokers) == 0 {
		return ""
	}
	sortedBrokers := append([]string(nil), brokers...)
	sort.Strings(sortedBrokers)
	slot := coordinatorSlot(key, len(sortedBrokers))
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

// PickGroupCoordinator returns the broker ID assigned to coordinate
// groupID. See pickCoordinator for the algorithm.
func PickGroupCoordinator(groupID string, brokers []string, alive map[string]bool) string {
	return pickCoordinator(groupID, brokers, alive)
}

// PickTxnCoordinator returns the broker ID assigned to coordinate
// transactionalID. Sibling of PickGroupCoordinator using the same
// hash + fallback shape — gh #91 routing for InitProducerId /
// AddPartitionsToTxn / EndTxn so a transactional producer's state
// lives on exactly one broker (without which a reconnect to a
// different broker for the same txnID gets a fresh PID with epoch=0
// and zombie writes can't be fenced).
func PickTxnCoordinator(transactionalID string, brokers []string, alive map[string]bool) string {
	return pickCoordinator(transactionalID, brokers, alive)
}
