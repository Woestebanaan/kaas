# skafka — Architecture

Narrative + diagrams. For per-feature reference detail, see
[`CLAUDE.md`](./CLAUDE.md); for the Rust rewrite plan and per-phase
detail, see [`rewrite.md`](./rewrite.md) and the
[`phase-N.md`](./phase-0.md) docs.

---

## At a glance

skafka is a from-scratch Apache Kafka 3.7 wire-compatible broker that
runs on Kubernetes. Three deliberate divergences from Apache:

- **No KRaft.** Cluster-wide controller election uses a Kubernetes
  `Lease` instead of a Raft quorum.
- **No replication / ISR.** Single-writer-per-partition on shared
  RWX storage (typically NFS); the broker is a thin wrapper around
  files on disk.
- **No `__transaction_state` internal topic.** Transaction-coordinator
  state lives in slot-sharded JSON files on the same shared volume.

Apache-Kafka clients (Java, librdkafka, franz-go) talk to skafka
unchanged. Kubernetes is the only "control plane" — there is no peer
gossip protocol, no replicated state machine.

```
                                          ┌────────────────────┐
                                          │  Kubernetes API    │
                                          │  (Leases, CRDs,    │
                                          │   Services, RBAC)  │
                                          └─────────┬──────────┘
                                                    │
                       ┌────────────────────────────┼────────────────────────────┐
                       │                            │                            │
                       │  reconcile CRs             │  watch Lease + CRs         │
                       │  (operator pod)            │  (broker pods)             │
                       │                            │                            │
                       ▼                            ▼                            │
              ┌────────────────┐         ┌────────────────┐                      │
              │ skafka         │         │ skafka brokers │                      │
              │ operator       │         │ (StatefulSet,  │                      │
              │ (Deployment)   │         │  N replicas)   │                      │
              └────────┬───────┘         └───┬─────────┬──┘                      │
                       │                     │         │                         │
                       │ writes              │ append  │ heartbeat gRPC          │
                       │ credentials.json    │ Read    │ (to controller broker)  │
                       │ acls.json           │         │                         │
                       │ partition dirs      │         │                         │
                       ▼                     ▼         │                         │
                  ┌─────────────────────────────────┐  │                         │
                  │ Shared RWX PVC (NFSv4)          │  │                         │
                  │ /data/__cluster/                │  │                         │
                  │   assignment.json               │  │                         │
                  │   credentials.json              │  │                         │
                  │   acls.json                     │  │                         │
                  │   txn_state/slot-*.json         │  │                         │
                  │   fence_log/from-skafka-*.json  │  │                         │
                  │   __consumer_offsets/<g>.json   │  │                         │
                  │ /data/<topic>/<partition>/      │  │                         │
                  │   {epoch:08x}-{base:020d}.log   │  │                         │
                  │   {epoch:08x}-{base:020d}.index │  │                         │
                  │   manifest.json                 │  │                         │
                  │   producer-state.snapshot       │  │                         │
                  │ /data/<topic>/.config.json      │  │                         │
                  └─────────────────────────────────┘  │                         │
                                                      │                         │
   Apache-Kafka clients ──────────────────────────────┘                         │
   (Java, librdkafka, franz-go)                                                 │
       Produce / Fetch / Metadata / SASL / etc.                                 │
                                                                                │
   Strimzi / Apache Kafka 3.7 ←─── parity matrix in skafka-migration-parity ────┘
```

---

## Process topology

### Broker — `cmd/skafka` (Go) / `bins/skafka` (Rust)

`StatefulSet` with stable pod ordinals (`skafka-0`, `skafka-1`, …). Each
pod is a single broker process that:

- Listens for client traffic on the listeners declared via the
  `SKAFKA_LISTENERS` JSON env. The Helm chart synthesises one entry per
  `.Values.listeners[]` item.
- Listens for peer heartbeats on `:9094` (gRPC, controller-bound).
- Exposes `/healthz` on `:8080` (HTTP, kubelet probes + diagnostics).
- Mounts the shared RWX PVC at `/data`. Every broker sees every other
  broker's segment files.

Brokers are **runtime-independent of the operator**: once the broker
process is up, K8s API outages don't block the Produce/Fetch hot path.
The hot path makes zero K8s API calls.

### Operator — `cmd/skafka-operator` (Go) / `bins/skafka-operator` (Rust)

`Deployment`, single replica. Reconciles four CRDs into on-disk config
files (auth + topics) and Kubernetes plumbing (TLS Certs + Routes):

| CRD                        | Owner action                                  |
|----------------------------|-----------------------------------------------|
| `KafkaCluster`             | External listener plumbing, cert-manager      |
|                            | Certificates, Gateway TLSRoutes               |
| `KafkaTopic`               | `/data/<topic>/<partition>/` directories,     |
|                            | `Status.TopicID` UUID (KIP-516)               |
| `KafkaUser`                | `credentials.json` + `acls.json` entries      |
| `KafkaClusterAssignments`  | Read-only mirror written by the controller    |
|                            | broker for `kubectl` debugging                |

The operator does **not** sit on the data path. Brokers serve traffic
even if the operator is crash-looping. The operator is a startup +
admission component.

### Shared substrate — RWX PVC

NFSv4 in production (`csi-driver-nfs`), local-path for single-node dev.
Requirements: **same-directory rename atomicity**, **fsync durability**,
**close-to-open consistency**. The architecture leans on these properties
in three places: the manifest tmp+rename dance, the assignment.json
epoch-prefix swap, and the txn-state slot file close-to-open consistency.

---

## Data plane: a Produce request, end-to-end

```
Client ──[Produce v9]──> Broker A's listener socket
                                │
                                ▼
                        sk-codec::frame::read_frame      ─── length-prefix
                                │                            framing
                                ▼
                        sk-codec::headers::decode         ─── api_key=0,
                                │                            api_version=9
                                ▼
                        sk-protocol::dispatch::dispatch   ─── route by
                                │                            (api_key, listener)
                                ▼
                        produce_handler                   ─── auth + quota
                                │                            checks
                                ▼
                Coordinator::owns(topic, partition) ?
                                │
                  ┌─────────────┴─────────────┐
                  no                          yes
                  │                           │
                  ▼                           ▼
        NOT_LEADER_OR_FOLLOWER       StorageEngine::append(
        (client retries → leader)         topic, partition,
                                          epoch, acks,
                                          batch_bytes)   ◄── Bytes,
                                          │                  not Record
                                          ▼
                                  Partition::inner.lock()
                                          │
                                          │  ┌──────────────────────────┐
                                          │  │ classify idempotence     │
                                          │  │ (PID + epoch + seq)      │
                                          │  └──────────────────────────┘
                                          │  ┌──────────────────────────┐
                                          │  │ maybe roll_fast(segment) │
                                          │  └──────────────────────────┘
                                          │  ┌──────────────────────────┐
                                          │  │ active.append_batch(raw) │ ── pwrite
                                          │  └──────────────────────────┘
                                          │  ┌──────────────────────────┐
                                          │  │ ArcSwap::store(snapshot) │
                                          │  └──────────────────────────┘
                                          │  ┌──────────────────────────┐
                                          │  │ if pending >= flush_int: │
                                          │  │   flush.req_tx.send(())  │ ── coalesce
                                          │  └──────────────────────────┘
                                          ▼
                                  drop inner lock
                                          │
                              acks == -1 ?│
                                  ┌───────┴───────┐
                                  no              yes
                                  │               │
                                  │               ▼
                                  │       flush.cond.wait()
                                  │               │
                                  │       (committer task in
                                  │        parallel runs
                                  │        log.sync_all() under
                                  │        spawn_blocking +
                                  │        timeout watchdog,
                                  │        then notifies)
                                  ▼               │
                                  ◄───────────────┘
                                  │
                                  ▼
                  Encode ProduceResponse(base_offset, error_code=0)
                                  │
                                  ▼
                  sk-codec::frame::write_frame ──> client
```

**Byte-opacity callouts.** Three places this path touches the
RecordBatch bytes:

1. `sk-codec` produces `records: Option<bytes::Bytes>` in the decoded
   request — zero-copy slice into the frame buffer. **Records bytes are
   never parsed.**
2. `sk-storage::idempotence::parse_batch_producer_info` reads the
   57-byte batch header for PID / epoch / sequence — header only, no
   records.
3. `sk-storage::segment::parse_batch_offsets` reads the 43-byte head
   for `(base_offset, last_offset_delta, max_timestamp)` — header only.

After step 3, `active.append_batch` calls `Fs::write_at(raw, log_size)`
— the same opaque `&[u8]` the client sent lands verbatim on disk. The
log file IS the wire format.

---

## Data plane: a Fetch request, end-to-end

Fetch is structurally simpler than Produce because the response has no
intermediate state:

```
Client ──[Fetch v12]──> Broker A
                            │
                            ▼  decode + dispatch (same as Produce)
                            │
                fetch_handler::handle
                            │
                            ▼  Coordinator::owns? — same fence as Produce
                            │
                StorageEngine::read_segment_ref(
                    topic, partition, fetch_offset, max_bytes)
                            │
                            ▼
                    ┌───────────────────┐
                    │ active segment    │  ── splice path
                    │   or closed seg ──┤
                    └───────────────────┘
                            │ returns (File, offset, length, cleanup)
                            ▼
                    sendfile(socket, file, offset, length)
                            │
                            ▼  Response framing is built around the splice:
                            │  [4-byte size][response header][per-partition
                            │   header (encoded)][records bytes (spliced)]
                            ▼
                       client receives
```

Two response-shape facts:

- **Stateless fetch sessions** (gh #4). The broker returns `SessionID=0`
  on every Fetch response. Apache's documented contract for "broker
  doesn't support sessions" — clients fall back to full Fetch data per
  request. CPU cost is fine at skafka's scale; KIP-227 caching is a
  future optimisation, not a correctness gap.
- **Read-committed isolation** (gh #31). For transactional reads, the
  handler clamps `max_offset` to the partition's last stable offset
  (LSO) so a Fetch sees `acks=all` data only after the producer's
  `EndTxn` markers land.

`Fetch` byte-opacity is symmetric to Produce: the response carries
opaque `Bytes` straight off disk via splice; the handler never decodes
a record.

---

## Control plane: controller election + `assignment.json`

skafka has no peer gossip protocol. The cluster's state machine is:

```
                     ┌──────────────────────────────┐
                     │ Kubernetes Lease             │
                     │   name: skafka-controller    │
                     │   spec.holderIdentity: "skafka-0"
                     │   spec.leaseTransitions: N    │  ◄── epoch source
                     └────────────┬─────────────────┘
                                  │
                  ┌───────────────┼────────────────┐
                  │               │                │
              skafka-0        skafka-1         skafka-2
              (controller)    (peer)           (peer)
                  │               │                │
                  │ writes        │ reads          │ reads
                  ▼               ▼                ▼
            assignment.json  (fsnotify + poll every 1 s)
            on /data/__cluster/

            { "epoch": N,         ◄── matches Lease.spec.leaseTransitions
              "partitions": [
                { "topic": "T", "partition": 0, "leader": "skafka-0", ... },
                ...
              ],
              "consumerGroups": [ ... ]
            }
```

The **controller is just a broker with the `skafka-controller` Lease**.
Its extra responsibilities:

- Observes peer brokers via heartbeat gRPC (`proto/heartbeat.proto`).
- Computes partition + consumer-group assignments
  (`internal/controller/balancer.go`).
- Writes `assignment.json` epoch-prefixed (tmp + atomic rename). Stale
  epochs are rejected by `Coordinator::set_assignment`.
- Mirrors the assignment into a `KafkaClusterAssignments` CR for
  `kubectl get kafkaclusterassignments` diagnostics.

When does it recompute?

| Trigger                          | Wired via                                          |
|----------------------------------|----------------------------------------------------|
| First win of controller Lease    | initial recompute                                  |
| `KafkaTopic` CR add/modify/delete| `TopicWatcher` → `clusterRuntime.NotifyTopicChange`|
| Broker joins/leaves alive set    | `watchBrokerSet` polls `BrokerSource.AliveBrokers` |

Non-controller brokers watch `assignment.json` via fsnotify + 1 s poll
(`internal/broker/coordinator.go`). The `TakeoverDriver` /
`GroupTakeoverDriver` opens or relinquishes partitions in the storage
engine when ownership changes. There is no per-partition Lease — the
singleton `skafka-controller` Lease is the only K8s coordination
primitive on the hot path.

---

## Storage architecture

```
/data/__cluster/                  ── cluster-wide files
    assignment.json
    credentials.json
    acls.json
    txn_state/
        slot-00.json              ── 50 slots, hash(transactional_id) % 50
        slot-01.json
        ...
    fence_log/
        from-skafka-0.json        ── one per broker; cross-broker producer
        from-skafka-1.json           epoch fence broadcast (gh #108 phase 2)
        ...
    __consumer_offsets/
        <group_id>.json           ── per-group offset file

/data/<topic>/
    .config.json                  ── operator-written; retention / segment
                                     bytes / compaction knobs

/data/<topic>/<partition>/
    manifest.json                 ── { epoch, highWatermark, logStartOffset }
    producer-state.snapshot       ── idempotent-producer dedupe window
    00000005-00000000000000000000.log   ── epoch=5, base_offset=0
    00000005-00000000000000000000.index ──   8-byte (rel_offset, file_pos)
    00000005-00000000000000001000.log   ── epoch=5, base_offset=1000
    ...
```

### Per-segment layout

Each segment is a pair of files. The filename carries both the
**leader epoch** and the **base offset** so a stale ex-leader's writes
can never physically collide with a new leader's segment:

```
{epoch:08x}-{base_offset:020d}.log     ── append-only log of v2 RecordBatches
{epoch:08x}-{base_offset:020d}.index   ── sparse offset index, 8 bytes/entry
```

The index is **sparse**: one entry every `index.interval.bytes` of log
data (4 KiB default). Each entry is `(rel_offset:i32_be, file_pos:i32_be)`.
`searchIndex` does a binary search returning the closest entry ≤ the
target offset; the caller scans forward in the log from there. Mmap of
the index is feature-gated behind `mmap` (the one unsafe-code carve-out
in the workspace).

### Single-FD ownership (gh #76)

**Only the partition's current leader holds open log + index FDs.**
Followers' segment state is meta-only (`size, base_offset, epoch` from
the filename). When ownership migrates:

- New leader's `take_over` calls `ActiveSegment::open_handles` —
  materialises the `Box<dyn FileWrite>` for log + index.
- Old leader's `relinquish` calls `ActiveSegment::close_handles` —
  drops the FDs.

Why it matters: on NFS, `os.remove(open_file)` silly-renames the file
into a `.nfsXXXX` entry that pins the parent directory until every FD
across all clients is closed. Without the single-FD rule, the leader's
segment cleanup (retention, DeleteRecords, segment roll) silly-renames
instead of freeing disk, and the operator's `unlinkat` on the partition
dir loops on EBUSY when a `KafkaTopic` is deleted.

### Manifest + producer snapshot

`manifest.json` is the source of truth for `(epoch, hwm, log_start)` on
partition open. Written via tmp + fsync + rename (NFSv4 same-directory
rename atomicity). Updated on: segment roll, `Relinquish`,
`take_over`, the cleaner's `log_start` advance. **Not** updated on
every append — the manifest can lag in-memory by up to one segment;
`recoverSegment` reconciles on takeover by scanning the active segment
forward to the first malformed batch boundary.

`producer-state.snapshot` is the idempotent-producer dedupe window
serialised next to the manifest. Written on segment roll and
`Relinquish`. Restored on `take_over` before append starts. Without
it, broker restart loses the 5-batch ring per PID and in-flight
retries get `OUT_OF_ORDER_SEQUENCE_NUMBER` even though the cached
state would have classified them as duplicates.

---

## Concurrency model: inside one Partition

The hot path is heavily optimised for shared-NFS substrates where every
COMMIT round-trip dominates. The design uses three primitives:

- A `parking_lot::Mutex<PartitionInner>` for the write path (the rare
  critical section).
- An `ArcSwap<ReadSnapshot>` for the read-path observation channel —
  lock-free HWM / log_start reads.
- A single `tokio::task` per partition (the **committer**) that fsyncs
  the log outside the mutex.

```
       ┌─────────────────── Partition ───────────────────┐
       │                                                  │
       │  pub fn append(...)                              │
       │      │                                           │
       │      ▼ lock()                                    │
       │  ┌──────────── inner ────────────┐               │
       │  │ active: ActiveSegment         │               │
       │  │ closed: Vec<SegmentMeta>      │               │
       │  │ epoch, next_write_seq, ...    │               │
       │  │ producer_states               │               │
       │  └───────────────┬───────────────┘               │
       │      │           │                               │
       │      │           │  store(Arc::new(snapshot))    │
       │      │           ▼                               │
       │      │    ┌──────────────┐                       │
       │      │    │ ArcSwap<...> │ ─── pub fn high_water │
       │      │    └──────────────┘     pub fn log_start  │
       │      │           ▲                               │
       │      │           │  load()       (no lock)       │
       │      │           │                               │
       │      ▼ send(())  │                               │
       │  ┌─────────────────────────────┐                 │
       │  │ flush.req_tx                │                 │
       │  │ (mpsc, capacity 1)          │                 │
       │  └────────────┬────────────────┘                 │
       │               ▼                                  │
       │  ┌─────────── committer task ──────────────┐     │
       │  │  loop:                                  │     │
       │  │    req_rx.recv().await                  │     │
       │  │    snapshot flush_seq + writeSeq under  │     │
       │  │      lock; clone the log FD              │     │
       │  │    drop lock                             │     │
       │  │    spawn_blocking(|| log.sync_all())     │     │
       │  │      with tokio::time::timeout(30s)      │     │
       │  │    on success → flush.cond.notify_waiters│     │
       │  │    on timeout → inner.flush_err = Stalled│     │
       │  │      (sticky; partition is dead)         │     │
       │  └──────────────────────────────────────────┘     │
       │                                                  │
       │  acks == -1 path:                                │
       │    after dropping inner.lock(), append() awaits  │
       │    flush.cond.wait_until(flush_seq <= completed) │
       └──────────────────────────────────────────────────┘
```

Three properties that make this work:

1. **Group commit (gh #82).** N concurrent appenders to the same
   partition share one fsync round-trip. Under high contention,
   per-Append latency is `O(1)` fsync + queueing, not `O(N)`.
2. **Lock-free reads (gh #134).** `HighWatermark()` /
   `LogStartOffset()` callbacks never block on the partition mutex.
   Before the `ArcSwap` mirror, a stuck NAS fsync held the mutex for
   the watchdog deadline, the OTel gauge callback stalled, and all
   skafka metrics vanished from Prometheus until the stall cleared.
3. **Fsync watchdog (gh #95).** `tokio::time::timeout` around
   `spawn_blocking(|| sync_all)` so a hung NFS server doesn't pin the
   committer indefinitely. On timeout, `flush_err` is set sticky and
   the partition fails fast on the next `Append`; the orphaned
   `spawn_blocking` task drains in the background when the kernel
   eventually returns.

---

## Idempotent producer + transactions

Apache 3.0+ producers enable idempotence by default, so every
`kafka-console-producer` invocation hits this path. Four layers,
all on the shared PVC:

| Layer                       | Storage                                   |
|-----------------------------|-------------------------------------------|
| `InitProducerId`            | in-memory PID counter; epoch++ on rejoin  |
| Per-partition dedupe        | `ProducerStates` map, 5-batch ring per PID|
| Snapshot persistence        | `producer-state.snapshot` next to manifest|
| Per-`transactional.id` state| `txn_state/slot-N.json`                   |

Transactional state lives in slot-sharded JSON files
(`txn_state/slot-N.json`, default 50 slots matching Apache's
`transaction.state.log.num.partitions`). On coordinator failover, the
new owner reads the same slot file off the shared PVC — close-to-open
consistency means the file IS the materialised state, no log replay
needed.

End-of-transaction semantics (`EndTxn`):

```
Producer ──[EndTxn(commit=true)]──> Txn coordinator broker
                                          │
                                          │  CompleteCommit transition
                                          ▼
                                  TxnStateStore.set(txn_id, {
                                      state: CompleteCommit,
                                      partitions: [],
                                      ongoing_since_ms: None,
                                  })
                                          │
                                          ▼
                                  TxnOffsetHook fires
                                          │
                                  ┌───────┴────────┐
                                  │                │
                                  ▼                ▼
                       OffsetStore.commit_pending  WriteTxnMarkers RPC
                       on each recorded group     to peer brokers
                       (commits the staged        owning data partitions
                        offsets atomically        (gh #114 — cross-broker
                        with the producer's       case)
                        EndTxn)
```

For full state-machine detail see CLAUDE.md's "Transaction coordinator
state machine" section.

---

## Crate dependency graph (Rust workspace)

```
                ┌──────────────────┐
                │   sk-codec       │   wire frames, primitives, CRC32C,
                │                  │   tagged fields, per-API codecs
                └────────┬─────────┘
                         │
                ┌────────▼─────────┐         ┌──────────────────┐
                │   sk-protocol    │ ◄──────  │   sk-auth        │
                │                  │          │                  │
                │ dispatch, server │          │ SCRAM, mTLS,     │
                │ bring-up,        │          │ ACLs, quotas     │
                │ handlers/*       │          │                  │
                └────────┬─────────┘          └──────────────────┘
                         │
                ┌────────▼─────────┐
                │   sk-broker      │ ◄──┐
                │                  │    │
                │ Broker glue,     │    │
                │ Coordinator,     │    │
                │ Takeover         │    │
                └─────┬──────┬──┬──┘    │
                      │      │  │       │
       ┌──────────────┘      │  │       │ uses for CRD types
       │                     │  │       │
       ▼                     ▼  ▼       │
┌──────────────┐    ┌──────────────┐   │
│  sk-storage  │    │ sk-coordinator│  │
│              │    │              │   │
│ Engine,      │    │ consumer-grp │   │
│ partitions,  │    │ + txn        │   │
│ idempotence, │    │              │   │
│ segments     │    └──────────────┘   │
└──────────────┘                       │
                                       │
                ┌──────────────────┐   │
                │   sk-controller  │ ──┤
                │                  │   │
                │ Election,        │   │
                │ balancer,        │   │
                │ assignment       │   │
                │ writer           │   │
                └──────────────────┘   │
                                       │
                ┌──────────────────┐   │
                │   sk-k8s         │ ──┤
                │                  │   │
                │ BrokerRegistry,  │   │
                │ TopicWatcher,    │   │
                │ CR writers       │   │
                └────────┬─────────┘   │
                         │             │
                         ▼             │
              ┌──────────────────┐    │
              │ sk-operator-api  │ ───┘
              │                  │
              │ CRD types        │
              │ (kube-derive)    │
              └────────┬─────────┘
                       │
                       ▼
            ┌──────────────────────────┐
            │ sk-operator-controllers  │
            │                          │
            │ reconcilers              │
            └──────────────────────────┘

           bins/skafka          bins/skafka-operator
           (broker entrypoint)  (operator entrypoint)
```

`sk-observability` is depended on by everything that emits metrics,
traces, or `/healthz`; it isn't on the diagram to keep the layout
readable. `sk-test-harness` carries the byte-opacity test fixtures and
the `recordbatch` helper — the **only** place in the workspace where a
decoded-record representation is allowed to live.

---

## Rewrite status

The workspace is mid-port from Go to Rust. The Go tree lives under
`archive/` and is frozen — bugfixes only, no new feature work. The
Rust port lands phase-by-phase per [`rewrite.md`](./rewrite.md):

| Phase | Scope                                     | Status         |
|-------|-------------------------------------------|----------------|
| 0     | Workspace bootstrap, CI, Dockerfiles      | **shipped**    |
| 1     | Wire codec (sk-codec)                     | initial slice  |
|       |                                           | shipped; rest  |
|       |                                           | in gh #153–155 |
| 2     | Storage engine (sk-storage)               | **in progress**|
|       |                                           | (gh #156–159)  |
| 3     | Single-broker server (sk-protocol, bin)   | unstarted      |
| 4     | Auth (sk-auth)                            | unstarted      |
| 5     | Coordinator & assignment                  | unstarted      |
| 6     | Transactions, idempotence, advanced       | unstarted      |
| 7     | Operator                                  | unstarted      |
| 8     | Observability, parity validation          | unstarted      |
| 9     | Cutover (Go → Rust default)               | unstarted      |

Tracker: [gh #143](https://github.com/Woestebanaan/skafka/issues/143).
The chart, CRDs, scripts, and `proto/heartbeat.proto` do **not** move
during the port — the Rust port reuses them verbatim.

---

## Non-goals (the "why we don't do X" section)

Each of these is a deliberate choice with a real cost to flipping:

- **KRaft / metadata quorum.** Apache replaced ZooKeeper with a Raft-
  based controller quorum. skafka leans on Kubernetes Leases instead.
  Reasons: (a) the K8s API server IS our metadata store; reinventing it
  in-process duplicates that role for no operational benefit. (b)
  brokers come and go via StatefulSet scaling, and Lease holderIdentity
  + leaseTransitions encode "current controller + monotonic epoch"
  exactly the way we need. (c) Raft adds significant code surface and
  a peer gossip protocol that the rest of the broker has no use for.

- **Replication / ISR.** Apache replicates each partition across N
  brokers. skafka writes a single copy to shared RWX storage and lets
  the substrate handle durability. Reasons: (a) ISR replication is
  what makes a multi-broker Kafka cluster operationally complex —
  preferred leader election, under-replicated alerts, controlled
  shutdown coordination. We trade that complexity for the NFS server's
  complexity. (b) Modern NFS / object-store substrates already
  replicate at the storage layer; doing it again in-broker is wasted
  work. (c) Single-writer-per-partition lets us split-brain-prevent by
  filename construction (epoch-prefixed segments) instead of by RPC
  fencing.

- **`__transaction_state` internal topic.** Apache persists txn-
  coordinator state as records in a special internal topic. skafka
  uses slot-sharded JSON files on the shared PVC. Reasons: (a) we don't
  have replication, so the internal-topic-as-state approach buys
  nothing. (b) close-to-open consistency on NFS means the slot file IS
  the materialised state — no log replay on coordinator failover.
  (c) JSON is debuggable: a stuck transaction is `cat slot-N.json`.

- **Tiered storage / S3 backend.** Apache 3.6+ ships KIP-405 tiered
  storage. skafka has no remote tier today. Reasons: (a) the NFS
  substrate is itself a "near-tier" with cheap-ish bulk storage. (b)
  KIP-405 doubles the cleanup state machine; deferring keeps the
  storage engine small. The API surfaces (`EARLIEST_LOCAL_TIMESTAMP`,
  `EARLIEST_PENDING_UPLOAD_OFFSET`) are explicitly skipped — clients
  only request them when configured for remote tiers.

If a "parity" task lands that implicitly requires any of the above,
flag it rather than adding the underlying machinery.

---

## Where to go next

- Per-feature reference: [`CLAUDE.md`](./CLAUDE.md).
- Rust rewrite plan: [`rewrite.md`](./rewrite.md) and the
  [`phase-N.md`](./phase-0.md) phase docs.
- Helm chart docs: [`deploy/helm/skafka/README.md`](./deploy/helm/skafka/README.md).
- Release procedure: [`RELEASING.md`](./RELEASING.md).
