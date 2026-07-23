# Consumer-group coordination

Deterministic hash routing of group coordination, two-tier ownership via `assignment.json`, and group takeover on assignment change.

In Apache Kafka, "which broker coordinates group G?" is a storage
question: `partitionFor(groupId)` hashes the group ID into the internal
`__consumer_offsets` topic, and whoever leads the resulting partition is
the group coordinator. Group metadata and committed offsets are records
in that topic; coordinator failover is partition-leadership failover.

kaas has no `__consumer_offsets` topic. It is one of the two internal
topics kaas replaces with plain JSON files on the shared volume — the
third substitution from the [introduction](../introduction.md); the
other is `__transaction_state`, covered in
[Transactions & idempotence](./transactions.md). Committed offsets live
in one file per group, and coordinator routing hashes the group ID
directly into the broker set instead of into a topic's partitions.

## The coordinator is a pure function

Routing is a stateless computation over the group ID, the broker set,
and the alive set — no lookup table, no election, no I/O:

- **Hash**: FNV-1a (32-bit) over the group ID, modulo the number of
  brokers. Clients never compute the coordinator themselves — they ask
  via `FindCoordinator`, exactly as against Apache Kafka — so any
  deterministic hash works as long as every broker computes the same
  one.
- **Stable divisor**: the modulus is pinned to the **full broker set**
  the controller knows about — including draining and dead brokers —
  *not* the alive count. Holding the divisor constant keeps existing
  assignments stable across rolling restarts; modding by the alive
  count would reshuffle roughly (N−1)/N of all groups on every pod
  up/down event.
- **Preferred-slot-down fallback**: when the hashed broker isn't alive,
  a deterministic alternate is picked from the alive subset, so
  coordination keeps working through a rolling restart.

The same machinery routes transaction coordination:
`hash(transactional.id)` picks the transaction coordinator, which is
what gates cross-broker transaction handling (see
[Transactions & idempotence](./transactions.md)).

## Two-tier ownership

A broker answers "do I coordinate group G?" in two tiers:

1. **Explicit entries win**: if the controller's `assignment.json`
   carries a `consumerGroups[]` entry for G, that broker is the
   coordinator. This is the controller's group-balancing output — and
   the forward-compat lever for sticky rebalancing.
2. **Hash fallback** otherwise: the pure function above, over the full
   broker set.

For stable broker sets the two tiers converge, so the hash is the
load-bearing path in steady state.

## Group takeover and the orphan sweep

When the assignment changes — a broker joins, leaves, or dies — every
broker reconciles the set of groups it coordinates in two passes:

1. **prev→next diff** — groups this broker *lost* are dropped from
   memory. Gained groups are not eagerly migrated: the new
   coordinator's first `JoinGroup` creates the group lazily and loads
   its persisted offsets from the group's file on the shared volume.
2. **Orphan sweep** — any group still held in memory that the *current*
   assignment doesn't route here is evicted, regardless of what the
   diff said.

The sweep is what keeps memory bounded across alive-set churn, and it
is what keeps `kafka-consumer-groups.sh --list` honest: the AdminClient
unions `ListGroups` across all brokers, so a single broker holding one
forgotten in-memory group would make a deleted group reappear
cluster-wide.

Ownership also filters the read side: `ListGroups` on a broker only
returns groups it currently coordinates, and `DescribeGroups` for a
group owned elsewhere answers `NOT_COORDINATOR` — a stale entry on a
non-coordinator broker is never visible to clients.

## Where group state lives

- **Membership, generation, protocol state**: in-memory on the
  coordinator broker, exactly as in Apache Kafka. It is lost on
  coordinator failover — consumers rejoin and rebalance, which is
  what a coordinator change looks like to clients of Apache Kafka too.
- **Committed offsets**: one JSON file per group,
  `/data/__cluster/__consumer_offsets/<groupID>.json`, on the shared
  volume. Durable across failover: where an Apache Kafka coordinator
  replays its `__consumer_offsets` partition to materialize offsets,
  the new kaas coordinator simply reads the same file — the file *is*
  the materialized state.
- **Transactional offsets** are staged in a pending layer keyed by
  `(group ID, producer ID)` and only become visible to `OffsetFetch`
  when the transaction commits — see
  [Transactions & idempotence](./transactions.md).

## Implementation notes (for contributors)

- Routing: `crates/kaas-broker/src/group_hash.rs` — pure functions over
  `(key, brokers, alive)`, no state, no I/O (gh #92). The same hash
  gates txn-slot ownership (gh #91).
- The coordinator manager's group-assignment source is **hot-swapped**
  from `bins/kaas/src/cluster.rs` after the broker `Coordinator` boots;
  the bootstrap source is an always-true local stub for the brief
  window before the cluster runtime is up (tests substitute their own).
  Don't unwire the swap: an earlier attempt (v0.1.52) hit the
  chicken-and-egg where strict coordinator checks blocked fresh-group
  bootstrap, and was reverted in v0.1.53 (gh #92).
- Takeover: `GroupTakeoverDriver`
  (`crates/kaas-broker/src/group_takeover.rs`) runs the prev→next diff
  and the orphan sweep; the sweep fixed the gh #89 stale-`--list`
  symptom. Lazy group creation is `Manager::get_or_create`.
- Read-side ownership filtering: `Manager::list_groups()` /
  `describe_groups()` in `crates/kaas-coordinator/src/manager.rs`.
- State: membership/generation in `crates/kaas-coordinator/src/group.rs`;
  offset persistence in `crates/kaas-coordinator/src/offset_store.rs`.
