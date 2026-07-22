# The volume pool: log dirs & placement

A single RWX volume gives the whole cluster exactly one provisioned I/O
budget. Brokers scale compute, egress, cache, and coordination — never
durable-write throughput, because underneath every broker is the same
filesystem. On the cloud filers kaas targets (FSx for NetApp ONTAP, Azure
NetApp Files, Azure Files provisioned), throughput is provisioned **per
volume** — so a *pool* of volumes is the substrate scaling axis, and
per-topic volume choice is how load is spread and tiered.

kaas expresses this in Apache Kafka's own vocabulary: **one pool volume =
one log dir** (KIP-113). `DescribeLogDirs` reports every member;
`kafka-log-dirs.sh` works unmodified.

## Pool membership

Members are **named** RWX volumes declared in the chart
(`storage.pool[]`), mounted on every broker and on the operator at
`/vols/<name>`, and advertised through the `KAAS_LOG_DIRS` env JSON. The
data volume is always a member under the reserved name `default`. Names —
not indices — go into placement records, so removing a member never
renumbers the rest.

Growing the pool is a chart edit + rolling restart: a capacity operation,
same cadence as adding brokers. `CreateTopics` never waits on
provisioning — new partitions land on volumes that already exist. Every
member must pass the same [RWX substrate contract](./nfs-substrate.md);
a pool mixing NFS and CephFS members is legal, a pool member with weaker
semantics is not.

## Per-topic binding: the eligible-set

`KafkaTopic.spec.storage.volumes` names the log dirs a topic's
partitions may land on. One field, three cases:

- `volumes: [premium]` — pinning: hard isolation, one volume's budget.
- `volumes: [bulk-1, bulk-2]` — partitions stripe round-robin across
  the set.
- unset — the **default set**: `default` plus every member with
  `defaultEligible: true`. A member with `defaultEligible: false` is
  **reserved**: it only receives topics that name it explicitly, which
  keeps auto-created topics (Streams repartition/changelog arrive
  casually) off premium substrate.

Unknown names fail the reconcile loudly (`Ready=False`,
`UnknownLogDir`) — a typo must not silently place data.

## Placement is creation-sticky; drift is surfaced, never auto-fixed

The operator's `KafkaTopic` reconciler
(`crates/kaas-operator-controllers/src/kafkatopic_controller.rs`)
assigns a log dir to each partition **once**, when the partition first
exists, and records it in `status.volumeAssignments`. Editing
`spec.storage.volumes` never moves existing partitions: new partitions
(from expansion) follow the new set, and partitions sitting outside it
keep serving where they are, counted in `status.partitionsOutsideSpec`.
There is no reconciler-driven data movement — an inter-volume move is a
raw copy on a substrate with no replication layer, so it only ever
happens through an explicit migration (the `AlterReplicaLogDirs` path,
phase 3).

The flow of placement truth:

```text
reconciler (round-robin over eligible set)
  → KafkaTopic.status.volumeAssignments
    → broker topic watch (kube_watchers)
      → TopicRegistry (the engine's PlacementResolver)
        → DiskStorageEngine partition path resolution
```

The engine's fallback is deliberately safe: an unplaced partition, an
unknown log-dir name, or a not-yet-stashed assignment all resolve to the
`default` log dir — resolution can never make a partition unopenable
(`crates/kaas-storage/src/disk.rs`).

## What follows the partition, what doesn't

Segment files, manifest, producer-state snapshot, and the recovery
checkpoint live inside the partition dir and follow it to its volume.
Topic-level files (`.config.json`, `.topic-id.json`) are written to
**every root hosting the topic's partitions** — the gh #219 incarnation
check and the orphan sweep run per root, so a deleted topic is reclaimed
wherever its partitions were placed.

Leadership and placement are independent axes: every broker reaches
every volume (they're all RWX mounts), so binding a topic to one volume
constrains where its *data* lives, never which broker leads it. This is
a property JBOD-on-local-disk Kafka cannot have.

## Cordon & migration (the drain runbook)

Removing a pool member (or moving a hot topic off a shared volume) is a
three-step, explicitly operator-driven flow — nothing here happens
automatically:

1. **Cordon the member**: set `cordoned: true` on its `storage.pool[]`
   entry and upgrade. KIP-1066 semantics — the member stops receiving
   *new* partition placements (even from topics that name it
   explicitly); every existing partition keeps serving in place.
2. **Move the partitions off** with `AlterReplicaLogDirs` (API 34) —
   the destination is the target log dir's *path* as reported by
   `kafka-log-dirs.sh --describe`. Per partition, the leader closes it,
   fresh-copies the directory to the target volume, flips
   `status.volumeAssignments` (the durable placement record), and
   reclaims the source. Producers see a brief retriable
   `LEADER_NOT_AVAILABLE` window during the copy; a failed flip rolls
   the copy back, so data location and placement record never diverge.
   Watch progress in `DescribeLogDirs` output — the cordoned dir's
   partition list shrinks to empty — and in the per-log-dir capacity
   gauges (`kaas.log.dir.usable.bytes`).
3. **Remove the member** from `storage.pool[]` once it hosts nothing,
   and delete its PVC.

A topic whose `spec.storage.volumes` still names the removed member
keeps its `Ready=False InvalidVolumeBinding` status until the spec is
updated — placement records pointing at a vanished member resolve to
the `default` dir (the engine's fail-safe), which is why draining
*before* removal matters.

## Backend guidance

- **FSx ONTAP / ANF** (incl. manual-QoS capacity pools): each member is
  an independently provisioned throughput budget — the pool multiplies
  substrate bandwidth. The target case.
- **EFS Elastic**: the filesystem scales its own throughput and bills
  per byte; a pool adds mounts and cost with no spreading gain — use
  the pool for *tiering* only.
- **CephFS (Rook)**: qualifies with no filer required; measure MDS and
  OSD-journal latency before promoting a member to a hot tier.
- **Single-filer homelab NFS**: members share one filer's budget; the
  pool is for layout/tier testing only.
