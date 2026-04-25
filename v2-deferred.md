# skafka v2-deferred features

Tracked features that will not ship in v1. v1 targets the read/write/admin
surface needed for general Kafka clients and admin UIs (kafbat-ui, kafka CLI
tools); v2 is "Streams-compatible / production-grade" and is the home for the
items below.

This file is a breadcrumb, not a roadmap — the cost-benefit of each item
should be re-evaluated when there's actual demand.

## How items end up here

Every entry has a **surfaced via** field — the concrete signal that proved we
need the API in question (a client error, a CLI tool failure, a UI feature
that disabled itself, etc.). If an item is here without a surfaced-via, it's
speculative; treat it as "remove or replace with evidence" rather than "build."

---

## 1. Transactional / idempotent producer surface

The Kafka transactional protocol is the single biggest deferred area.
Implementing it well requires a producer-id state machine, transaction
coordinator, segment-level abort markers, and exactly-once semantics on the
fetch side. Several months of work, only useful if v2 also delivers Streams.

### APIs

| Key | Name | Purpose |
|-----|------|---------|
| 22  | InitProducerId | Issue / fence producer IDs for idempotent + transactional producers |
| 24  | AddPartitionsToTxn | Register a partition with an in-progress transaction |
| 25  | AddOffsetsToTxn | Register a consumer-group offset commit with a transaction |
| 26  | EndTxn | Commit / abort a transaction |
| 27  | WriteTxnMarkers | Replicate commit/abort markers (controller-driven) |
| 28  | TxnOffsetCommit | Commit consumer offsets transactionally |
| 61  | DescribeProducers | Admin / UI: list active producer IDs per partition |
| 65  | DescribeTransactions | Admin / UI: list ongoing transactions |

### Workarounds available now

- **Java clients** (modern KafkaProducer defaults `enable.idempotence=true`)
  must be reconfigured `enable.idempotence=false`. For kafbat-ui this lives
  in `kafka.clusters[].producerProperties` (see
  `k3s-cluster/apps/kafbat-ui/values.yaml`).
- The skafka smoke test hardcodes `--producer-property enable.idempotence=false`
  for the same reason; see `scripts/smoke-test.sh`.

### Surfaced via

- kafbat-ui v1.5.0 "Produce Message" form. The Java producer initialises
  itself as idempotent, calls InitProducerId (22), gets UNSUPPORTED_VERSION
  back, and transitions to a fatal error state. Stack:
  `KafkaProducer.send → TransactionManager.maybeFailWithError →
  UnsupportedVersionException: The node does not support INIT_PRODUCER_ID`.

---

## 2. Log compaction

`cleanup.policy=compact` topics — needed for `__consumer_offsets`-style use
cases beyond the basic offset store, and for Kafka Streams' KTable backing
topics. Implementing log compaction touches segment cleanup, partial-segment
rewriting, and tombstone semantics.

### Surfaced via

Not yet — speculative. Will surface the moment a Streams workload is pointed
at skafka. Kafka Streams creates compacted internal topics automatically and
fails fast if the broker rejects the `compact` cleanup policy.

---

## 3. Kafka Streams API set

Kafka Streams uses a strict superset of the v1 surface. Beyond transactions
(item 1) it relies on:

- Idempotent producer for state-store backing topics (covered by item 1).
- Compacted topics for KTable changelogs (covered by item 2).
- `OffsetsForLeaderEpoch (23)` — used during partition reassignments and
  rebalance to truncate divergent logs. Skafka's RF=1 / shared-storage
  model arguably makes this trivial, but the API still has to be wired.
- `DeleteRecords (21)` — Streams-style topic cleanup also uses this.
  See item 5.

Track Streams compatibility as a single milestone once items 1 and 2 land.
Don't pull individual Streams APIs in piecemeal.

### Surfaced via

Not yet — speculative.

---

## 4. Multi-broker replication APIs

Skafka delegates durability to the CSI layer (CephFS / JuiceFS), so RF=1 is
an architectural invariant — see `internal/protocol/handlers/admin.go`
(`brokerConfigs`). The following APIs are unimplementable as long as that
holds, and would only become relevant if skafka grows in-broker replication:

- `AlterPartitionReassignments (45)`
- `ListPartitionReassignments (46)`
- `ElectLeaders (43)` — strictly speaking the K8s Lease layer already
  performs leader election; this API would just expose a "force re-election"
  knob to admin clients.

### Surfaced via

Not yet — and unlikely to, since RF=1 means UIs and tools that call these
APIs typically degrade gracefully.

---

## 5. DeleteRecords (21)

Tier 3 of `kafbat-support-plan.md`. Mutates `logStartOffset` and triggers
segment deletion before retention would normally drop them. Exists in v1
scope but is the most invasive Tier item — moves into v2 if v1 ships before
it lands.

### Surfaced via

`kafbat-ui` "Clear Messages" UI button on the topic page. UI hides the
button when the cluster doesn't advertise the API in `ApiVersions`.

---

## 6. Per-resource config storage (AlterConfigs)

`AlterConfigs (33)` and `IncrementalAlterConfigs (44)` are not in this list
because they're being stubbed in v1 (return `INVALID_CONFIG`). Real config
storage — per-topic / per-broker overrides persisted on the PVC and watched
by the broker — is v2.

### Surfaced via

Not yet, beyond the stub. Will surface as soon as someone tries to change a
topic's `retention.ms` from kafbat-ui and expects it to stick.

---

## 7. DescribeCluster (60)

Newer cluster info API. Modern clients fall back to Metadata when it's
missing, so the practical impact is low. Adding it is cheap (one handler,
~30 lines) and would flip kafbat's "Controller Type: ZooKeeper" label to
something honest. Defer until item-1 (transactions) lands; not worth a
release on its own.

### Surfaced via

`kafbat-ui` brokers page shows `Controller Type: ZooKeeper` (kafbat default
when DescribeCluster is unavailable). Cosmetic.

---

## When to close items

Move an item from this file back to `kafbat-support-plan.md` (or wherever it
ends up being implemented) the moment the cost-benefit shifts —
e.g. someone files an issue, a real user is blocked, Streams becomes a
target, or the architectural assumption (RF=1, single broker) changes.
