# Consumer-group coordination

Deterministic hash routing of group coordination, two-tier ownership via `assignment.json`, and group takeover on assignment change.

Apache Kafka answers "which broker coordinates group G?" with partition
leadership of the internal `__consumer_offsets` topic
(`partitionFor(groupId)`). kaas has no `__consumer_offsets` topic — offsets
live in per-group JSON files on the shared volume — so it hashes directly
into the broker set instead (gh #92).

## coordinator-of-G is a pure function

`crates/kaas-broker/src/group_hash.rs` implements the routing as pure
functions over `(key, brokers, alive)` — no state, no I/O:

- **Hash**: FNV-1a 32-bit over the group ID, `mod num_brokers`. Clients never
  compute the coordinator themselves (they always ask via `FindCoordinator`),
  so any deterministic hash works as long as every broker computes the same
  one.
- **Stable divisor**: `num_brokers` is pinned to the **full broker set** the
  controller knows about — including draining and dead brokers — *not*
  `len(alive)`. Holding the divisor constant keeps existing assignments
  stable across rolling restarts; modding by the alive count would reshuffle
  roughly (N−1)/N of all groups on every pod up/down event.
- **Preferred-slot-down fallback**: when the hashed broker isn't alive, a
  deterministic alternate is picked from the alive subset, so coordination
  keeps working through a rolling restart.

The same machinery routes transaction coordination: `hash(transactional.id)`
picks the txn-slot owner under gh #91, which is what gates cross-broker
transaction handling (see [Transactions & idempotence](./transactions.md)).

## Two-tier ownership

The broker `Coordinator` answers `owns_group(G)` in two tiers:

1. **Explicit entries win**: if `assignment.json.consumerGroups[]` carries an
   entry for G, that broker is the coordinator. This is the controller's
   group-balancing output — and the forward-compat lever for sticky
   rebalancing.
2. **Hash fallback** otherwise: `coordinator_slot(G, full_broker_set)`.

For stable broker sets the two tiers converge, so the hash is the
load-bearing path in steady state.

One wiring subtlety worth knowing before touching boot code: the coordinator
manager's group-assignment source is **hot-swapped** from
`bins/kaas/src/cluster.rs` after the broker `Coordinator` boots. The
bootstrap source is an always-true local stub used during the brief window
before the cluster runtime is up; tests substitute their own source. Don't
unwire the swap — an earlier attempt (v0.1.52) hit the chicken-and-egg where
strict coordinator checks blocked fresh-group bootstrap, and was reverted in
v0.1.53 (gh #92).

## Group takeover and the orphan sweep

`GroupTakeoverDriver` (`crates/kaas-broker/src/group_takeover.rs`) runs on
every assignment change and does two things:

1. **prev→next diff** — groups this broker *lost* are dropped from memory.
   Gained groups are not eagerly migrated: the new coordinator's first
   `JoinGroup` creates the group via `Manager::get_or_create`, which lazily
   loads the persisted offsets from
   `/data/__cluster/__consumer_offsets/<groupID>.json`.
2. **Orphan sweep** — any group still in `Manager::local_groups()` that the
   *current* assignment doesn't route here is evicted, regardless of what the
   diff said.

The sweep is what keeps memory bounded across alive-set churn, and it fixed
the gh #89 symptom where `kafka-consumer-groups --list` showed stale entries:
the AdminClient unions `ListGroups` across brokers, so a single broker
holding a forgotten in-memory group made it reappear cluster-wide.

Ownership also filters the read side: `Manager::list_groups()` and
`describe_groups()` (`crates/kaas-coordinator/src/manager.rs`) only return
groups the broker currently coordinates — `DescribeGroups` for a group owned
elsewhere answers `NOT_COORDINATOR`, and `ListGroups` simply omits it.

## Where group state lives

- **Membership / generation / protocol state**: in-memory on the coordinator
  broker (`crates/kaas-coordinator/src/group.rs`). Lost on coordinator
  failover — consumers rejoin and rebalance, which matches what a Kafka
  coordinator change looks like to clients.
- **Committed offsets**: `/data/__cluster/__consumer_offsets/<groupID>.json`
  on the shared volume (`crates/kaas-coordinator/src/offset_store.rs`).
  Durable across failover; the new coordinator reads the same file.
- **Transactional offsets** are staged in a pending layer keyed by
  `(groupID, PID)` and only become visible to `OffsetFetch` when the
  transaction commits — see [Transactions &
  idempotence](./transactions.md).
