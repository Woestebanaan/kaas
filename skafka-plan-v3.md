# SharedKafka (skafka): Kafka on Plain RWX Storage — Claude Code Implementation Plan
# Version 3.3 — Bytes-Are-Opaque Architecture, Performance Roadmap

> **Changes from v3.2:** Added the bytes-are-opaque architectural
> constraint as a v1 design requirement. The broker treats RecordBatch
> payloads as opaque bytes throughout the read and write paths;
> validation happens at the batch CRC level only; the on-disk segment
> format is byte-identical to the wire format. This is the load-bearing
> design decision that determines whether skafka can match Apache
> Kafka's per-broker throughput. Compression delegation falls out of
> this for free; zero-copy `sendfile(2)` and kTLS become straightforward
> v1.5 optimizations rather than retrofits. New "Performance Roadmap"
> section enumerates v1.5/v2 optimizations enabled by this foundation.
>
> Assignment-on-PVC, controller-based coordination, and the
> single-writer safety model are unchanged from v3.2.

## Project Overview

Build an open-source, Apache Kafka protocol-compatible broker that uses a
single shared ReadWriteMany PVC as its storage backend, enabling replication
factor 1 while retaining durability through the storage layer itself. The PVC
must support **NFSv4-class semantics**: atomic rename, fsync durability, and
close-to-open consistency. No cross-node byte-range or file locks required.
All coordination — leader election, broker membership, and partition
assignment — uses Kubernetes Lease + heartbeat + a single shared file. No
custom consensus, no ZooKeeper, no KRaft.

This plan covers two release milestones:

- **v1 (MVP):** Core produce/consume, consumer groups, auth, CRD management.
  Targets teams who need Kafka semantics on-prem on top of any RWX volume.
  Kafka Streams is NOT supported in v1.

- **v2 (Streams-compatible):** Transactions, exactly-once semantics, log
  compaction, and the remaining API keys required by Kafka Streams.

**Working name:** `skafka`
**Language:** Go 1.22+
**License:** Apache 2.0
**Target deployment:** Kubernetes with any ReadWriteMany PVC providing
NFSv4-class semantics (csi-driver-nfs, AWS EFS, Azure Files, GCP Filestore,
Longhorn-NFS, Rook-Ceph CephFS, JuiceFS).
**Scale envelope:** 3-50 brokers per cluster (storage-bound on most providers).
Assignment file scales to ~100,000 partitions before controller recompute
becomes the binding constraint.

---

## Design Principles

1. **Delegate to Kubernetes for coordination, to the PVC for state.**
   Lease for elections. Heartbeats for liveness. Shared filesystem for
   anything that scales with partition count.

2. **All brokers are identical.** No broker/controller split. One pod
   is elected controller via a single Lease.

3. **Shared storage is the source of truth — for data and for control
   metadata.** RF=1. No external coordination service for assignment.

4. **Single writer per partition, by construction.** Controller's
   assignment is the authoritative writer identity. Epoch-tagged
   immutable segments make stale-leader writes physically harmless.

5. **Coarse-grained Kubernetes coordination.** One Lease per cluster.
   Etcd writes happen on broker churn, not on every partition heartbeat.

6. **Epoch-fenced everywhere.** Both data-plane segments and control-plane
   assignment are tagged with monotonic epochs anchored to Kubernetes
   counters (`leaseTransitions`).

7. **Declarative everything.** Topics, users, ACLs are Kubernetes resources.

8. **End-to-end TLS with per-broker advertised hostnames.** Standard
   `NOT_LEADER_FOR_PARTITION` retry.

9. **The broker is a byte mover, not a byte interpreter.** RecordBatch
   payloads are opaque bytes throughout the broker. Validation happens
   at the batch CRC level only. The broker never decompresses,
   re-serializes, or inspects individual records. On-disk segment
   format is byte-identical to the wire format. This is what enables
   per-broker throughput to match Apache Kafka's; it is non-negotiable
   v1 design, not a future optimization.

---

## The Single-Writer Safety Model

Unchanged from v3.0/v3.1/v3.2.

### Core invariant

Each segment file is owned by exactly one leader epoch. A leader never
appends to another leader's segment file. On takeover, the new leader
creates a fresh segment under its own epoch.

```
{epoch:08x}-{base_offset:020d}.log
{epoch:08x}-{base_offset:020d}.index
{epoch:08x}-{base_offset:020d}.timeindex
```

### Self-fencing

Each broker keeps an atomic timestamp updated on every heartbeat PING
received from the controller. The Append path checks heartbeat freshness
before each batch. A broker that has lost connectivity stops acking
writes within `heartbeatTimeout` (3s default).

### Takeover sequence (controller-driven)

When the controller assigns partition P to broker B at epoch N:

1. Controller writes new assignment to `/data/__cluster/assignment.json`
   (atomic via tmp + rename).
2. Controller pushes `ASSIGNMENT_CHANGED` via heartbeat to all brokers.
3. B re-reads the assignment file and validates `controllerEpoch` against
   the current Lease. Stale files are rejected.
4. B waits the safety delay (default 2s).
5. B lists existing segments, identifies prior leader's last segment,
   scans backward for last well-formed CRC, seals it, writes `.recovery`
   sidecar.
6. B creates fresh segment `{N:08x}-{recovery_offset+1:020d}.log`.
7. B reports READY in next heartbeat. Partition is writable.

Total observed latency: ~150-400ms graceful, 4-5s hard-crash.

---

## Cluster Coordination via Controller

(Unchanged from v3.2. Brief recap; full detail in v3.2 if needed.)

A single Kubernetes Lease elects the cluster controller. The controller
writes the partition-to-broker assignment as a JSON file on the shared
PVC, atomically replaced via tmp + rename. Brokers read the file on
startup, on heartbeat-pushed `ASSIGNMENT_CHANGED` notifications, and via
1s mtime-polling as a safety net. The file is fenced by the controller's
`leaseTransitions` epoch — brokers reject files written by stale
controllers.

Assignment file format:

```json
{
  "controllerEpoch": 43,
  "assignmentVersion": 12847,
  "generatedAt": "2026-05-02T10:14:32Z",
  "controller": "skafka-1",
  "brokers": [...],
  "partitions": [
    {"topic": "payment-events", "partition": 0, "broker": "skafka-1", "epoch": 8, "role": "leader"},
    ...
  ]
}
```

`KafkaClusterAssignments` CR survives as a best-effort read-only debug
mirror that the controller writes fire-and-forget after each authoritative
file write.

---

## Bytes-Are-Opaque Architecture

This section is the architectural foundation for skafka's per-broker
throughput. Read it before writing any code in Phase 2 or Phase 3.

### The principle

The broker treats RecordBatch payloads as opaque bytes throughout the
read and write paths. Apache Kafka has used this design since 2014 and
it's why it hits 250-500 MB/s per broker on commodity hardware. Building
skafka any other way would cap per-broker throughput at maybe 100-200
MB/s due to user-space byte copying alone — too low to be competitive.

### What this means concretely

**Produce path:**
- Accept the RecordBatch as `[]byte` from the wire.
- Validate the batch CRC32C against the batch bytes (cheap; CRC is
  computed over the compressed payload, no decompression needed).
- Append the byte slice directly to the active segment file.
- Never iterate individual Records.
- Never decompress.
- Never re-serialize.

**Fetch path:**
- Read a byte range from the segment file.
- Wrap it in a Fetch response frame — fixed-size header plus the raw
  segment bytes.
- Send. The bytes the producer wrote are the bytes the consumer receives,
  byte-for-byte.
- Compression is handled by the consumer, not the broker.

**On-disk format == wire format:**
- A segment file is a concatenation of RecordBatch wire bytes, exactly
  as received from producers.
- `kafka-dump-log.sh` reads them directly. No conversion needed.
- Any Kafka client library can parse segment files.

### What this enables

- **Compression delegation comes for free.** A producer with
  `compression.type=snappy` ships compressed batches; the broker stores
  them compressed; the consumer decompresses. Broker CPU spent on
  compression: zero. This matches Apache Kafka's `compression.type=producer`
  default behavior.

- **Zero-copy `sendfile(2)` becomes a clean v1.5 optimization.** The
  Fetch path can use `sendfile(2)` to copy from page cache directly to
  socket, bypassing user space. ~500 lines of work because the
  architectural foundation (raw bytes from disk to wire) is already
  correct.

- **kTLS becomes feasible.** Zero-copy through TLS sockets via Linux
  kernel TLS. Without byte-opacity, kTLS doesn't help — you're already
  re-encoding bytes in user space anyway.

### What violates byte-opacity (do not do this)

- Decoding RecordBatch into a struct with `[]Record`, then writing those
  records to disk. This re-serializes — broker spends CPU on protocol
  encoding work the producer already did.
- Decompressing batches to validate individual records. Validation is
  at the batch level via CRC32C; individual record validation is the
  consumer's job.
- Allocating per-record buffers. Per-batch byte slices are fine; per-
  record allocations destroy throughput at scale.
- Re-encoding batches when forwarding through Fetch responses. Bytes
  in == bytes out.

### Tests that catch byte-opacity violations

- **Byte-identical round-trip:** produce a batch (compressed, multiple
  records, with headers), fetch it back, assert the response payload
  bytes equal the original request payload bytes byte-for-byte. This
  single test catches the most common regressions.
- **CPU profile under load:** the produce hot path should show CRC32C
  prominently. If it shows `snappy.Decode`, `gzip.NewReader`, or
  `RecordBatch.Encode`, the broker is doing work it shouldn't. Fix it.
- **Allocation profile:** `go test -bench -benchmem` on the produce hot
  path. Per-batch allocations are acceptable; per-record allocations
  are a regression.

### The architectural mental model for Claude Code

When implementing Produce, do NOT think:
> Decode each Record from the batch. Validate. Build a Go struct. Write
> the struct to disk.

Instead think:
> Validate the batch CRC. Append the batch bytes to the segment file.
> Done.

When implementing Fetch, do NOT think:
> Read records from disk. Build a response with []Record. Encode.

Instead think:
> Locate the byte range in segment files matching the requested offset
> range. Wrap with response frame headers. Send.

The broker is a byte mover. Records are an interpretation only the
producer and consumer need.

---

## Performance & Capacity

### What bounds per-broker throughput

In order: NIC bandwidth, then CPU, rarely storage backend.

Per produced byte: 1 byte IN from client + 1 byte OUT to NFS = 2 bytes
of NIC. Per consumed byte (page-cache hit): 1 byte OUT to client.

The numbers below assume the bytes-are-opaque architecture is correctly
implemented. A byte-touching design would cut these in half.

### Per-broker producer throughput by AKS VM size

| VM size | NIC | Per-broker produce | 3-broker cluster |
|---|---|---|---|
| D8s_v5 | 1.56 GB/s | 400-500 MB/s | 1.2-1.5 GB/s |
| D16s_v5 | 1.56 GB/s | 400-500 MB/s | 1.2-1.5 GB/s |
| D32s_v5 | 2.0 GB/s | 550-650 MB/s | 1.6-1.9 GB/s |
| D64s_v5 | 3.75 GB/s | 900 MB/s-1.2 GB/s | 2.7-3.6 GB/s |
| D96s_v5 | 4.4 GB/s | 1.0-1.4 GB/s | 3.0-4.2 GB/s |

Apache Kafka with RF=3 on the same hardware is roughly 1/3 to 1/2 of
the per-broker numbers above due to the replication tax.

### Storage backend ceilings

| Backend | Aggregate cap | Saturation broker count (D32s_v5) |
|---|---|---|
| Azure Files Premium NFS (provisioned v2) | 5 GB/s | 8 brokers |
| Azure NetApp Files Standard | ~16 GB/s | 25 brokers |
| Azure NetApp Files Ultra | ~25 GB/s | 38 brokers |
| AWS EFS Provisioned | 10+ GB/s | 15 brokers |
| GCP Filestore Enterprise | up to 26 GB/s | 40 brokers |
| Self-hosted NFS, 25 GbE NVMe | ~3 GB/s | 5 brokers |
| Rook-Ceph (10-node, NVMe, 25 GbE) | ~5-10 GB/s | 8-15 brokers |

### Latency

NFS fsync round-trip on Azure Files Premium: 2-5ms. Local Premium SSD:
<1ms. skafka p99 produce latency: 5-15ms vs Apache Kafka's tuned 2-8ms.

### Recommended landing zones

**Small / typical:** 3 brokers on D8s_v5, Azure Files Premium NFS
provisioned at 1.5-2 GB/s, aggregate throughput 1.2-1.5 GB/s.

**High-throughput:** 6 brokers on D32s_v5, Azure Files Premium provisioned
at 5 GB/s (or Azure NetApp Files Standard for headroom), aggregate
throughput 3-4 GB/s.

**Maximum sensible:** 12 brokers on D64s_v5, Azure NetApp Files Ultra
provisioned at 15-25 GB/s, aggregate throughput 10-15 GB/s.

### Hard scale ceilings

| Constraint | Limit | Notes |
|---|---|---|
| Storage backend throughput | Provider-specific | Real ceiling for almost all deployments |
| Partitions per cluster | ~100,000 | Controller assignment recompute time becomes binding |
| Brokers per cluster | ~200-300 | Single-controller scale |
| Heartbeat connections | ~500 (controller) | gRPC server connection ceiling |

For improvements past the storage backend ceiling, see Performance Roadmap.

---

## Performance Roadmap (v1.5 and Beyond)

The v1 architecture is designed to enable specific optimizations later
without architectural changes. None of these are v1 work, but the v1
design must not foreclose any of them.

### v1.5 — same architecture, optimization passes

1. **Zero-copy Fetch via `sendfile(2)`** — the Fetch path can use
   `sendfile(2)` to copy from page cache directly to socket on the
   plaintext listener. Doubles consumer throughput (from ~800 MB/s to
   ~1.5 GB/s per broker on D8s_v5). ~500 lines plus careful response
   framing. Already enabled by byte-opacity. Internal listener only;
   external (TLS) listener requires kTLS (next item).

2. **kTLS for zero-copy through TLS sockets** — Linux 4.13+ supports
   moving TLS state into the kernel. Allows `sendfile(2)` through TLS
   sockets. Required for the external listener since most consumer
   traffic goes through it. ~500 lines plus deployment kernel-version
   validation. AKS Ubuntu kernels are recent enough; document the
   requirement.

3. **Pooled buffers for codec** — `sync.Pool` for protocol-level
   buffers reduces GC pressure under high RPS. 5-15% throughput
   improvement at high request rates. ~100 lines.

4. **Direct I/O for write path** — `O_DIRECT` for segment writes
   bypasses local page cache, reducing memory pressure under
   produce-heavy workloads. 20-30% throughput improvement on writes.
   ~300-500 lines for alignment handling.

5. **Batch tuning documentation** — recommended client config:
   `batch.size=1048576`, `linger.ms=10-50`, `compression.type=snappy`
   or `lz4`. 2-3× producer throughput improvement at no broker cost.
   Documentation work.

6. **NFS mount tuning guide** — `nconnect=8`, `rsize/wsize=4MB`,
   kernel page-cache tuning, with measured numbers per provider.

### v2 — additive features

7. **Multi-PVC sharding** — partition topics across multiple PVCs so
   cluster aggregate throughput scales beyond a single backend's
   ceiling. Moves the cluster-throughput cap from 5 GB/s (single Azure
   Files share) to N × 5 GB/s. ~2000-3000 lines, real design work for
   the operator and assignment logic. The biggest practical scaling
   improvement available without changing skafka into a different
   product.

8. **Tiered storage to object store** — old segments offloaded to
   S3/Azure Blob/GCS via KIP-405 protocol. Reduces shared-PVC capacity
   requirement; enables long retention without provisioning massive
   shares. Aligns with Apache Kafka's tiered storage feature. v2 or v3.

### Beyond v2 (would change skafka into a different product)

9. **Object-storage-native backend** — replace shared filesystem with
   S3/Azure Blob/GCS as primary storage. Removes the storage backend
   throughput ceiling entirely. Same architectural shape as WarpStream,
   Bufstream, AutoMQ. Substantial rewrite of the storage engine; not a
   continuation of skafka v1's design.

### Honest framing for sizing

skafka v1 in its planned form is designed for clusters needing 1-5 GB/s
aggregate. With v1.5 optimizations, 5-15 GB/s is achievable. Beyond
that, the architecture has to change — multi-PVC for incremental
scaling, or object storage for unlimited scaling. Users hitting these
ceilings should plan accordingly.

---

## Failure Domains and Recovery Times

| Failure | Producer recovery | Notes |
|---|---|---|
| Single broker, graceful (SIGTERM) | ~500ms | Controller updates assignment.json + heartbeat-pushed refresh |
| Single broker, hard crash | 4-5s | Heartbeat detection (3s) + safety delay (2s) |
| Single controller, graceful | 0s data plane, ~2s control plane | Lease released cleanly; brokers reconnect |
| Single controller, hard crash | 0s data plane, 5-15s control plane | Lease election dominates; data plane self-fences if controller failover takes >heartbeatTimeout, then resumes |
| Single AZ failure | 8-15s for affected partitions | Controller failover + partition reassignment |
| Cluster network partition (broker isolated) | 4-5s for affected partitions | Self-fencing kicks in at heartbeat timeout |
| Two-AZ failure (etcd quorum loss) | Data plane keeps running until heal | New assignments paused; existing leaders self-fence after heartbeat timeout |
| Storage backend unavailable | Full cluster stall until storage recovers | RF=1 means no fallback; backups are the recovery path |
| Stale-controller write (file race) | None visible | Brokers reject stale-epoch files; new controller overwrites |

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        Kubernetes                                 │
│                                                                   │
│  ┌──────────────────────────────────────────────────────────────┐│
│  │  coordination.k8s.io/v1 Lease (one total)                    ││
│  │    skafka-controller → skafka-1 (leaseTransitions=43)        ││
│  └──────────────────────────────────────────────────────────────┘│
│                                                                   │
│  ┌────────────────────┐   ┌──────────────────────────────────┐   │
│  │  CRDs              │   │  Kubernetes Secrets              │   │
│  │  KafkaCluster      │   │  (credentials, TLS certs)        │   │
│  │  KafkaTopic        │   └──────────────────────────────────┘   │
│  │  KafkaUser         │                                           │
│  │  KafkaUserGroup    │   ┌──────────────────────────────────┐   │
│  │  KafkaAcl          │   │  KafkaClusterAssignments         │   │
│  └────────────────────┘   │  (best-effort kubectl debug      │   │
│                           │   mirror only — not authoritative)│   │
│                           └──────────────────────────────────┘   │
│                                                                   │
│  ┌──────▼─────┐    ┌────────┐    ┌────────┐                     │
│  │ skafka-0   │    │skafka-1│    │skafka-2│                     │
│  │            │   ┌┤ broker ├┐   │        │                     │
│  │  broker    │   ││ + ctrlr││   │ broker │                     │
│  └────────────┘   │└────────┘│   └────────┘                     │
│        ▲           │   gRPC   │       ▲                          │
│        │           │  heartbt │       │                          │
│        └───────────┴──────────┴───────┘                          │
│                    1s PING, 3s timeout                            │
│                    push: ASSIGNMENT_CHANGED, LEAVING              │
│                                                                   │
│  Bytes-are-opaque hot paths:                                      │
│   Produce:  client TCP → CRC validate → segment write             │
│   Fetch:    segment read → response framing → client TCP          │
│   No decode/re-encode in the broker.                              │
│                                                                   │
│  ┌──────────────────────┴──────────────────────────────────────┐ │
│  │  skafka-headless (ClusterIP: None) — in-cluster DNS         │ │
│  └──────────────────────┬──────────────────────────────────────┘ │
│                         │                                         │
│               ┌─────────▼────────────┐                           │
│               │  Single ReadWriteMany│                           │
│               │  PVC                 │                           │
│               │  /data/                                          │
│               │    __cluster/                                    │
│               │      assignment.json     ← cluster state         │
│               │      assignment.json.tmp ← write staging         │
│               │      acls.json                                   │
│               │      credentials.json                            │
│               │    topic-A/                                      │
│               │      partition-0/                                │
│               │        00000007-...      ← raw RecordBatch bytes │
│               │        00000008-...        (wire format == disk) │
│               └──────────────────────┘                           │
└──────────────────────────────────────────────────────────────────┘
```

---

## CRD Surface Area

Five CRDs. The first four are user-facing; `KafkaClusterAssignments`
is best-effort controller-mirrored output for debugging.

(Schemas for KafkaTopic, KafkaUser, KafkaUserGroup, KafkaAcl unchanged
from prior versions. KafkaClusterAssignments is debug-mirror-only with
`status.truncated` flag if exceeds CR size limit.)

---

## Kubernetes Primitives Used

| Primitive | Replaces |
|---|---|
| StatefulSet ordinal index | Broker ID assignment |
| coordination.k8s.io/v1 Lease (1 only) | Controller election + epoch source |
| client-go leaderelection lib | Custom consensus |
| Shared PVC + atomic rename | Per-partition Leases (v3.0) and assignment CR (v3.1) |
| gRPC heartbeat streams | Per-broker liveness |
| KafkaClusterAssignments CR | kubectl-visible debug mirror (non-authoritative) |
| EndpointSlice watch | Cluster membership broadcast |
| PodDisruptionBudget | min-ISR availability |
| ReadinessGates | Custom warmup |
| Init containers | Partition directory init |
| Projected volumes | Config + secret injection |
| CRDs (5x) | Admin API |
| Kubernetes Secrets | Credential storage |
| Gateway API TLSRoute | Kafka-aware proxy tier |
| Per-broker Service | External addressability |
| cert-manager Certificate | Per-broker cert rotation |

---

## Project Layout

```
skafka/
├── cmd/
│   ├── skafka/                 # Broker binary (includes controller code path)
│   ├── skafka-operator/        # Operator binary
│   ├── skafka-fsync-check/     # Storage backend fsync durability validator
│   └── skafka-failover-probe/  # Heartbeat latency calibration tool
├── internal/
│   ├── protocol/               # Kafka wire protocol (hand-rolled)
│   │   ├── server.go
│   │   ├── dispatch.go
│   │   ├── codec/              # Header/frame encoding only — never touches batch payloads
│   │   │   ├── reader.go
│   │   │   ├── writer.go
│   │   │   ├── types.go        # Header types only; RecordBatch is []byte
│   │   │   └── api/
│   │   └── handlers/           # Byte-opaque handlers
│   │       ├── produce.go      # validates batch CRC, appends bytes
│   │       ├── fetch.go        # reads byte range, wraps in response
│   │       ├── metadata.go
│   │       ├── consumer_group.go
│   │       └── admin.go
│   ├── storage/                # Single-writer engine, raw RecordBatch bytes
│   │   ├── engine.go           # Append([]byte), Read() returns []byte
│   │   ├── segment.go          # mmap or buffered read; writes are append-only []byte
│   │   ├── index.go
│   │   ├── recovery.go
│   │   ├── manifest.go
│   │   └── cleaner.go
│   ├── controller/
│   │   ├── controller.go
│   │   ├── election.go
│   │   ├── assignment.go
│   │   ├── balancer.go
│   │   ├── heartbeat_server.go
│   │   └── mirror.go
│   ├── broker/
│   │   ├── heartbeat_client.go
│   │   ├── assignment_watch.go
│   │   ├── assignment_poll.go
│   │   ├── controller_watch.go
│   │   ├── self_fence.go
│   │   └── takeover.go
│   ├── auth/
│   ├── coordinator/
│   └── k8s/
├── operator/
│   ├── controllers/
│   └── api/
├── pkg/
│   └── kafkaapi/
├── proto/
│   └── heartbeat.proto
├── deploy/
├── tests/
│   ├── unit/
│   ├── integration/
│   ├── nfs-fault/
│   ├── controller-failover/
│   ├── stale-controller-race/
│   ├── byte-opacity/           # New in v3.3 — byte-identical round-trip tests
│   └── kafka-compat/
├── Dockerfile
├── Dockerfile.operator
└── Makefile
```

There is no `internal/lock/` package, no `internal/lease/` for
per-partition leases. The codec types do NOT include a decoded
`RecordBatch` struct — RecordBatch is `[]byte` everywhere it's
handled by skafka. The codec handles request/response *frames* (with
fixed-size headers); RecordBatch payloads are passed through opaquely.

---

## Phase 1: Foundation (Week 1–2)

(Same as v3.2 — module init, kubebuilder scaffolding, core interfaces,
RBAC, CI matrix.)

```
3. Core interfaces:

   // StorageEngine — operates on raw batch bytes. Never decodes RecordBatch.
   type StorageEngine interface {
       // Append takes opaque batch bytes (already CRC-validated by caller).
       Append(ctx context.Context, topic string, partition int32,
              epoch uint32, batchBytes []byte) (baseOffset int64, err error)

       // Read returns raw segment bytes covering the requested range.
       // Caller is responsible for any framing.
       Read(ctx context.Context, topic string, partition int32,
            startOffset int64, maxBytes int) (rawBytes []byte, err error)

       HighWatermark(topic string, partition int32) (int64, error)
       LogStartOffset(topic string, partition int32) (int64, error)
       CreatePartition(topic string, partition int32) error
       DeletePartition(topic string, partition int32) error
       TakeOver(ctx context.Context, topic string, partition int32,
                epoch uint32) (recoveryOffset int64, err error)
       Relinquish(topic string, partition int32) error
   }

   // AssignmentStore, Controller, BrokerCoordinator, AuthEngine —
   // unchanged from v3.2.
```

The StorageEngine signature change vs prior versions makes byte-opacity
the default at the type-system level. There is no `[]Record` parameter
that would tempt an implementer toward decoding.

---

## Phase 2: Kafka Protocol Layer (Week 2–4)

**Goal:** Hand-rolled broker-side Kafka wire protocol codec, TCP server,
request handlers — all observing the bytes-are-opaque architecture.

### Why hand-rolled

Franz-go and Sarama are client libraries with client-shaped assumptions.
Their code re-encodes RecordBatches as part of normal client operation,
which is the wrong default for a broker. We use them as reference
material only — never imported in `internal/`.

### TCP server (internal/protocol/server.go)

- Listen on configurable port (default 9092 plaintext, 9093 TLS).
- Goroutine per connection.
- Read request frame:
  ```
  [total_length:4][api_key:2][api_version:2][correlation_id:4]
  [client_id_len:2][client_id:N][tagged_fields?][body...]
  ```
- Write response frame:
  ```
  [total_length:4][correlation_id:4][tagged_fields?][body...]
  ```
- Connection state: authenticated Principal, client_id, negotiated
  API versions.

### Codec primitives (internal/protocol/codec/)

`reader.go` and `writer.go` implement primitives for frame headers only:
varint, uvarint, compact strings, compact arrays, tagged fields, fixed-
width integers. **They do not decode RecordBatch.**

When a Produce request body is parsed, the codec extracts the topic
name, partition number, and the batch bytes — but the batch is treated
as `[]byte` and passed through. The codec extracts metadata about the
batch from a small fixed-size header (offsets, count, CRC location)
without parsing individual records.

### RecordBatch wire format (just the header that the broker needs)

Every RecordBatch starts with a 61-byte fixed header:

```
baseOffset            int64    (8 bytes)
batchLength           int32    (4 bytes)
partitionLeaderEpoch  int32    (4 bytes)
magic                 int8     (1 byte)   — must be 2
crc                   uint32   (4 bytes)  — Castagnoli; covers everything after
attributes            int16    (2 bytes)
lastOffsetDelta       int32    (4 bytes)
baseTimestamp         int64    (8 bytes)
maxTimestamp          int64    (8 bytes)
producerId            int64    (8 bytes)
producerEpoch         int16    (2 bytes)
baseSequence          int32    (4 bytes)
recordCount           int32    (4 bytes)
[opaque records...]
```

The broker reads this header to learn:
- Where the CRC is and what range to validate it over.
- How many records are in the batch (for metrics).
- The `lastOffsetDelta` (to compute the next `baseOffset`).
- The compression type (from `attributes`, but only for telemetry —
  the broker does not act on it).

The broker does NOT decode the records. Records are bytes after the
header.

### Per-API codec files (internal/protocol/codec/api/)

Implement minimum version range needed:

- API key 0 Produce v3-9
- API key 1 Fetch v4-13
- API key 2 ListOffsets v1-7
- API key 3 Metadata v1-12
- API key 8 OffsetCommit v2-8
- API key 9 OffsetFetch v1-8
- API key 10 FindCoordinator v0-4
- API key 11 JoinGroup v2-9
- API key 12 Heartbeat v0-4
- API key 13 LeaveGroup v0-4
- API key 14 SyncGroup v0-5
- API key 15 DescribeGroups v0-5
- API key 16 ListGroups v0-4
- API key 17 SaslHandshake v0-1
- API key 18 ApiVersions v0-3
- API key 19 CreateTopics v0-7
- API key 20 DeleteTopics v0-6
- API key 29 DescribeAcls v0-3
- API key 30 CreateAcls v0-3
- API key 31 DeleteAcls v0-3
- API key 36 SaslAuthenticate v0-2

For Produce and Fetch specifically, the codec handles the request/response
*envelope* but the RecordBatch payload is sliced out as `[]byte` for
Produce and constructed as `[]byte` for Fetch.

### Produce handler (internal/protocol/handlers/produce.go)

```go
// Pseudocode for the byte-opaque Produce handler.
func handleProduce(ctx context.Context, req *ProduceRequest, conn *Conn) (*ProduceResponse, error) {
    resp := &ProduceResponse{}

    for _, topicData := range req.TopicData {
        for _, partitionData := range topicData.PartitionData {
            // partitionData.RecordSet is []byte — the entire RecordBatch as wire bytes.
            batchBytes := partitionData.RecordSet

            // 1. Validate batch header is well-formed (61 bytes minimum).
            if len(batchBytes) < 61 {
                resp.AddError(topicData.Topic, partitionData.Partition, CorruptRecord)
                continue
            }

            // 2. Validate batch CRC32C over batchBytes[21:].
            // hash/crc32 with Castagnoli — automatically uses CLMUL on x86.
            expectedCRC := binary.BigEndian.Uint32(batchBytes[17:21])
            actualCRC := crc32.Checksum(batchBytes[21:], crc32cTable)
            if expectedCRC != actualCRC {
                resp.AddError(topicData.Topic, partitionData.Partition, CorruptRecord)
                continue
            }

            // 3. Authorize.
            if !conn.AuthEngine.Authorize(conn.Principal,
                                          Resource{Topic: topicData.Topic},
                                          OpWrite) {
                resp.AddError(topicData.Topic, partitionData.Partition, TopicAuthorizationFailed)
                continue
            }

            // 4. Check broker leadership and heartbeat freshness (atomic loads).
            if !coordinator.Owns(topicData.Topic, partitionData.Partition) {
                resp.AddError(topicData.Topic, partitionData.Partition, NotLeaderForPartition)
                continue
            }
            if !coordinator.IsHeartbeatFresh() {
                resp.AddError(topicData.Topic, partitionData.Partition, NotLeaderForPartition)
                continue
            }
            epoch, _ := coordinator.CurrentEpoch(topicData.Topic, partitionData.Partition)

            // 5. Append the batch bytes to the segment file.
            //    The storage engine never inspects batchBytes.
            baseOffset, err := storage.Append(ctx, topicData.Topic, partitionData.Partition,
                                              epoch, batchBytes)
            if err != nil {
                resp.AddError(topicData.Topic, partitionData.Partition, err)
                continue
            }

            resp.AddSuccess(topicData.Topic, partitionData.Partition, baseOffset)
        }
    }

    return resp, nil
}
```

**No decompression. No record iteration. No re-encoding.** The broker
reads the batch header (61 bytes), validates the CRC, and either appends
or rejects the bytes.

### Fetch handler (internal/protocol/handlers/fetch.go)

```go
// Pseudocode for the byte-opaque Fetch handler.
func handleFetch(ctx context.Context, req *FetchRequest, conn *Conn) (*FetchResponse, error) {
    resp := &FetchResponse{}

    for _, topicReq := range req.TopicReqs {
        for _, partReq := range topicReq.PartitionReqs {
            // Authorize.
            if !conn.AuthEngine.Authorize(conn.Principal,
                                          Resource{Topic: topicReq.Topic},
                                          OpRead) {
                resp.AddError(topicReq.Topic, partReq.Partition, TopicAuthorizationFailed)
                continue
            }

            // Read raw bytes from the segment file.
            // The bytes returned are a sequence of complete RecordBatches,
            // exactly as they appear on disk (which is exactly as they
            // appeared on the wire when produced).
            rawBytes, err := storage.Read(ctx, topicReq.Topic, partReq.Partition,
                                          partReq.FetchOffset, partReq.PartitionMaxBytes)
            if err != nil { /* handle */ continue }

            // The fetch response framing is added by the codec.
            // rawBytes goes into the response without modification.
            resp.AddPartitionData(topicReq.Topic, partReq.Partition, rawBytes)
        }
    }

    return resp, nil
}
```

**The `rawBytes` returned from storage are RecordBatch wire bytes.
They are written to the response frame as-is.** The consumer
deserializes them.

### ApiVersions handler

Must be correct or clients refuse to connect. Returns the supported
min/max version for every implemented API key. Clients negotiate down
to the highest mutually-supported version.

### CRC32C validation

- Castagnoli polynomial (`hash/crc32` with `crc32.MakeTable(crc32.Castagnoli)`).
- NOT IEEE polynomial (the Go default). Wrong polynomial = silent
  corruption.
- On modern x86 + ARM, Go's hash/crc32 automatically uses CLMUL/CRC32
  instructions. Verify with `go test -bench`.
- Test against known-good byte sequences from the Kafka protocol spec.

### Idempotent producer fields

Modern Kafka clients default to `enable.idempotence=true` and send
`producerId` and `baseSequence` in batch headers. v1 must accept these
fields without erroring (they live in the 61-byte header that the
broker reads). v1 does not deduplicate based on them — full duplicate
detection arrives in v2 with the transaction coordinator.

### Compatibility tests (tests/kafka-compat/)

Use franz-go, segmentio/kafka-go, kcat as TEST CLIENTS only. Never
import in `internal/`. Tests verify that real clients can produce and
consume against skafka with realistic configurations including
compression, idempotence, and consumer groups.

### Critical: what the codec MUST NOT do

- The codec MUST NOT have a `RecordBatch` struct that decodes individual
  records. The 61-byte header is the only structured part of a batch
  the broker handles; everything past byte 61 is `[]byte`.
- The codec MUST NOT decompress batches. Compression is per-batch and
  the consumer's responsibility.
- The codec MUST NOT re-encode batches when serving Fetch responses.
  Bytes from storage go directly into response frames.
- The handlers MUST NOT iterate individual Records on the produce or
  fetch hot paths.

---

## Phase 3: Storage Engine (Week 3–6)

**Goal:** Log segment reads/writes with epoch-tagged immutable segment
files on any RWX volume providing NFSv4-class semantics. Segments
contain raw RecordBatch wire bytes — the on-disk format is byte-
identical to what producers send and consumers receive.

### Filesystem layout

```
/data/
  __cluster/
    assignment.json
    assignment.json.tmp
    acls.json
    credentials.json
  __consumer_offsets/
    partition-{N}/
      manifest.json
      {epoch:08x}-{base_offset:020d}.log
  {topic}/
    partition-{N}/
      manifest.json
      {epoch:08x}-{base_offset:020d}.log         # raw RecordBatch bytes
      {epoch:08x}-{base_offset:020d}.index       # sparse offset → file position
      {epoch:08x}-{base_offset:020d}.timeindex
      {epoch:08x}-{base_offset:020d}.log.sealed
      {epoch:08x}-{base_offset:020d}.recovery
```

### On-disk segment format

A segment file is a concatenation of complete RecordBatch wire-format
byte sequences. For example, a segment containing three batches looks
like:

```
[ batch1: 61-byte header + records bytes ]
[ batch2: 61-byte header + records bytes ]
[ batch3: 61-byte header + records bytes ]
```

This is identical to what Apache Kafka writes. `kafka-dump-log.sh`
works against skafka segments without modification.

### Index files

`{epoch:08x}-{base_offset:020d}.index` contains entries:

```
[ relative_offset: int32 ][ file_position: int32 ]
```

Sparse: one entry per `indexIntervalBytes` (default 4096 bytes). On a
fetch, binary-search the index to find the nearest entry ≤ the
requested offset, then linear-scan from that file position by reading
batch headers until reaching the target offset.

The index is a broker-private optimization. It's regenerated from the
.log file if missing or corrupt on startup.

### Append flow

```go
// internal/storage/engine.go
func (e *engine) Append(ctx ctx, topic string, partition int32, epoch uint32,
                        batchBytes []byte) (int64, error) {
    p := e.openPartition(topic, partition)

    // Defense in depth — coordinator already checked, but verify epoch
    // matches the active segment's epoch.
    if p.activeSegment.epoch != epoch {
        return 0, ErrEpochMismatch
    }

    // The broker's only job here is to append batchBytes to the file.
    // No decoding. No re-encoding.
    baseOffset := p.activeSegment.nextOffset
    if err := p.activeSegment.appendBytes(batchBytes); err != nil {
        return 0, err
    }

    // Update the index if we're past the next interval.
    if p.activeSegment.bytesSinceLastIndex() >= e.config.IndexIntervalBytes {
        // Read the lastOffsetDelta from the just-written batch header.
        lastOffsetDelta := readLastOffsetDelta(batchBytes)
        p.activeSegment.appendIndex(baseOffset + int64(lastOffsetDelta), filePosition)
    }

    // Update next offset for the next batch.
    p.activeSegment.nextOffset = baseOffset + int64(readLastOffsetDelta(batchBytes)) + 1

    if p.activeSegment.size >= e.config.SegmentBytes {
        p.rollSegment(epoch)
    }
    if e.flushPolicy.ShouldFlush() {
        p.activeSegment.fdatasync()
        p.updateManifest(p.activeSegment.nextOffset - 1)
    }
    return baseOffset, nil
}
```

### Read flow

```go
func (e *engine) Read(ctx ctx, topic string, partition int32,
                      startOffset int64, maxBytes int) ([]byte, error) {
    p := e.openPartition(topic, partition)

    // Find the segment containing startOffset.
    seg := p.findSegmentForOffset(startOffset)

    // Use the index to find the nearest file position ≤ startOffset.
    indexEntry := seg.findIndexEntry(startOffset)
    pos := indexEntry.filePosition

    // Linear-scan batch headers from pos until we find the batch
    // containing startOffset.
    pos = seg.scanForBatch(pos, startOffset)

    // Read up to maxBytes starting at pos.
    // Round down to a complete batch boundary.
    rawBytes, err := seg.readRange(pos, maxBytes)
    if err != nil { return nil, err }
    rawBytes = truncateToCompleteBatch(rawBytes)

    return rawBytes, nil
}
```

The returned `[]byte` is a sequence of complete RecordBatches in wire
format. It goes directly into a Fetch response.

### TakeOver flow

(Unchanged from v3.0/v3.1/v3.2 — list segments, identify prior leader's
last segment, scan backward for last well-formed batch via CRC, seal,
write `.recovery` sidecar, create fresh segment with new epoch.)

### Per-partition manifest

(Unchanged from prior versions — atomic JSON file with current epoch,
high watermark, log start offset. Atomic write via tmp + rename in
same directory.)

### NFS operational requirements

(Unchanged from v3.2 — NFSv4.1+, sync export, hard mount, nconnect=8,
acregmax=1.)

### Retention cleaner

Background goroutine, leader-only. Lists segments older than
`retentionMs` by maxTimestamp from segment header (not mtime — mtime is
unreliable on NFS). Deletes oldest segments. Never deletes the active
segment. Runs every 5 minutes.

### inotify on config files

`acls.json` and `credentials.json` are watched via `fsnotify`. On NFS,
fsnotify falls back to polling (~1s interval). Acceptable for config
files.

---

## Phase 4: Cluster Controller and Broker Coordinator (Week 4–6)

(Unchanged from v3.2. Single-Lease controller election, gRPC heartbeat
streams, file-based assignment store, epoch fencing on assignment file
reads, heartbeat-pushed `ASSIGNMENT_CHANGED` plus 1s mtime polling
fallback. Refer to v3.2 plan for full pseudocode if needed.)

---

## Phase 5: Consumer Group Coordinator (Week 6–7)

(Unchanged from v3.2. Consumer groups assigned to brokers via the same
controller assignment mechanism; assignment file holds them alongside
topic partitions. `__consumer_offsets` partitions are stored on the
shared PVC like any other topic and follow the byte-opaque storage
model.)

---

## Phase 6: Operator (Week 7–8)

(Unchanged from v3.2. controller-runtime / kubebuilder scaffold;
KafkaTopic, KafkaUser, KafkaUserGroup, KafkaAcl, KafkaCluster
controllers; operator does NOT participate in heartbeat or assignment.)

---

## Phase 7: Authentication Engine (Week 8)

(Unchanged from v3.0/v3.1/v3.2. SCRAM-SHA-512, mTLS, Kubernetes SA JWT.
ACLs in `/data/__cluster/acls.json`, polled by brokers.)

---

## Phase 8: Kubernetes Deployment (Week 8–9)

(Unchanged from v3.2. Helm values include controller, partition, and
storage tuning. NOTES.txt prints failover characteristics and storage
requirements. Tier 1 RWX providers: csi-driver-nfs, AWS EFS, Azure
Files Premium NFS, Azure NetApp Files, GCP Filestore, Longhorn-NFS,
Rook-Ceph CephFS.)

---

## Phase 9: External Access via Per-Broker TLS Passthrough (Week 9–10)

(Unchanged from v3.0/v3.1/v3.2. Per-broker hostnames, Gateway TLSRoute
SNI passthrough, cert-manager Certificate, no custom router, no leader
forwarding.)

Note for v3.3: the external listener does not yet use kTLS in v1.
Bytes that flow out through the TLS listener are encoded by the broker
process before TLS encryption (necessarily — TLS state lives in user
space). v1.5 will introduce kTLS to enable `sendfile(2)` through the
TLS socket. The v1 architecture is correct (byte-opacity preserved
all the way to the TLS socket); v1.5 just changes where the bytes are
encrypted.

---

## Phase 10: Observability (Week 10)

```
Prometheus metrics (port 9090/metrics):

# Throughput
skafka_produce_records_total{topic}
skafka_produce_bytes_total{topic}
skafka_fetch_records_total{topic, consumer_group}
skafka_fetch_bytes_total{topic, consumer_group}
skafka_produce_batches_total{topic, compression}        # new in v3.3 — batch counts by compression type
skafka_produce_batch_size_bytes{topic}                  # histogram

# Storage
skafka_partition_high_watermark{topic, partition}
skafka_storage_write_latency_seconds{topic}
skafka_storage_read_latency_seconds{topic}
skafka_storage_fsync_latency_seconds
skafka_segment_count{topic, partition}
skafka_recovery_duration_seconds{topic, partition}

# Coordination (from v3.2)
skafka_is_controller{broker}
skafka_controller_failovers_total
skafka_controller_failover_duration_seconds
skafka_assignment_version{}
skafka_assignment_changes_total
skafka_assignment_file_writes_total{result}
skafka_assignment_file_write_latency_seconds
skafka_assignment_file_size_bytes
skafka_assignment_pushes_total
skafka_assignment_polls_total{change_detected}
skafka_stale_assignments_rejected_total
skafka_assignment_cr_mirror_writes_total{result}
skafka_heartbeat_rtt_seconds{broker}
skafka_heartbeat_misses_total{broker}
skafka_self_fence_events_total{broker}
skafka_broker_count_alive
skafka_broker_count_assigned
skafka_takeover_duration_seconds{topic, partition}
skafka_takeover_safety_delay_seconds{topic, partition}

# Byte-opacity sanity (new in v3.3)
skafka_codec_record_decode_total                        # MUST stay at zero in steady state
skafka_codec_batch_reencode_total                       # MUST stay at zero in steady state
# Both metrics are tripwires — if they ever increment, code is violating
# byte-opacity. Alert in production.

# CRC validation
skafka_produce_crc_failures_total{topic}                # batch-level CRC mismatches

# Leadership
skafka_partition_leader{topic, partition}
skafka_partition_epoch{topic, partition}

# NFS / storage
skafka_storage_estale_total
skafka_storage_open_retries_total
skafka_storage_fsync_errors_total

# Consumer groups
skafka_consumer_group_lag{topic, partition, consumer_group}
skafka_consumer_group_members{consumer_group}
skafka_consumer_group_rebalances_total{consumer_group}

# Auth
skafka_auth_success_total{mechanism}
skafka_auth_failure_total{mechanism, reason}
skafka_acl_deny_total{principal, resource_type}
skafka_quota_throttle_total{principal}

# External
skafka_external_connections_total{mode, broker_id}
skafka_tls_handshakes_total{broker, result}
skafka_cert_reload_total{broker}
skafka_not_leader_returned_total{topic, partition}
```

```
/healthz endpoint:
{
  "status": "ok",
  "broker_id": "skafka-0",
  "is_controller": false,
  "controller_id": "skafka-1",
  "controller_epoch": 43,
  "heartbeat_rtt_ms": 12,
  "heartbeat_age_ms": 230,
  "assignment_version": 12847,
  "assignment_age_ms": 450,
  "partitions_led": 4,
  "partitions_assigned": 4,
  "partitions_recovering": 0
}

Grafana panels (additions in v3.3):
- Produce batch size distribution (helps tune client batch.size)
- Produce compression breakdown (verify clients are compressing)
- Codec tripwire counters (skafka_codec_record_decode_total etc. —
  should be flat lines at zero)
```

---

## Critical Constraints for Claude Code

1. **Single writer per partition is enforced by the controller's
   assignment + epoch-tagged segment filenames + heartbeat-based
   self-fencing.** No flock. No `internal/lock/` package.

2. **Every segment file is owned by exactly one epoch.** Filename:
   `{epoch:08x}-{base_offset:020d}.log`.

3. **Cluster assignment is authoritatively a file at
   `/data/__cluster/assignment.json`.** The
   `KafkaClusterAssignments` CR is a best-effort fire-and-forget
   debug mirror only. Brokers must never read from the CR.

4. **Every assignment.json file is fenced by `controllerEpoch`,**
   sourced from the `skafka-controller` Lease's `leaseTransitions`
   field. Brokers must validate `file.controllerEpoch >= currentLeaseEpoch`
   on every read.

5. **assignment.json writes are atomic via tmp + rename within
   `/data/__cluster/`.** Always fsync the .tmp file before rename.

6. **Brokers refresh assignment via heartbeat-pushed
   `ASSIGNMENT_CHANGED` (primary) and 1s mtime polling (safety net).**
   30s full-read fallback covers NFS mtime quirks.

7. **The controller is one elected broker.** All brokers run the same
   binary. Controller code path activates only when this broker holds
   the `skafka-controller` Lease.

8. **Per-partition Leases do not exist.** All partition coordination
   flows through the assignment file.

9. **Append checks three things in order, all atomic loads:** (a) does
   the local assignment cache say this broker leads this partition,
   (b) is the heartbeat to the controller fresh, (c) does the active
   segment's epoch match the assignment.

10. **TakeOver requires a safety delay before the first write.** Default
    2s. Justified by self-fencing within heartbeat timeout.

11. **Self-fencing is synchronous and lock-free** (atomic int64 read).

12. **Manifest writes, ACL writes, credential writes, and assignment
    writes all use the tmp + rename atomic-replace pattern.** Same
    directory only.

13. **Hand-roll the protocol codec.** Use franz-go and Sarama as
    reference only.

14. **Test with real Kafka clients.** franz-go and kafka-go imported in
    `tests/` only.

15. **NFSv3 is unsupported.** Detect at startup and refuse to run on
    NFSv3 mounts.

16. **Idempotent producers (`enable.idempotence=true`):** v1 accepts
    fields without erroring; full duplicate detection in v2.

17. **Graceful shutdown sequence on SIGTERM:**
    1. If controller: write final assignment.json, push LEAVING to
       brokers, mirror to CR, release Lease, exit.
    2. Send LEAVING to controller via heartbeat.
    3. Stop accepting new connections.
    4. Drain in-flight requests (with timeout).
    5. Flush + fsync all active segments.
    6. Update manifests with final high watermark.
    7. Update ReadinessGate to False.
    8. Exit 0.

18. **Per-broker hostnames are static.** Helm-rendered at install time.

19. **TLS terminates at the broker pod.** Gateway is L4 passthrough.

20. **The KafkaClusterAssignments CR is allowed to be incomplete or
    stale.** It is purely for `kubectl get` debugging.

21. **The broker treats RecordBatch payloads as opaque bytes.**
    Validation happens at the batch CRC32C level only. The broker
    NEVER decompresses, re-serializes, or inspects individual records
    within a batch. The on-disk segment format is byte-identical to
    the wire format. This is the architectural foundation for
    per-broker throughput; treating it as a future optimization will
    cap throughput at uncompetitive levels.

22. **The codec must not contain a decoded `RecordBatch` struct with
    a `[]Record` field.** The 61-byte batch header is the only
    structured part of a RecordBatch the broker handles. Everything
    past byte 61 is `[]byte`. Adding a Record-decoding code path —
    even one not used on the hot path — invites someone to call it
    later and break byte-opacity.

23. **The Produce hot path must be allocation-bounded per batch, not
    per record.** Allocating per-batch slices is fine; allocating per
    record kills throughput at scale. CPU profiles must show CRC32C
    prominently and `snappy.Decode`/`gzip.NewReader` not at all.

24. **The Fetch path passes segment bytes directly into response
    framing.** Do not re-encode. Do not re-validate CRC (the producer
    already validated when it wrote; CRC is stable on disk).

---

## Testing Strategy

```
Unit tests:
- Protocol codec for frame headers (request/response envelopes only)
- Codec primitives: varint, uvarint, compact strings/arrays, tagged fields
- CRC32C: validate against known-good byte sequences from the Kafka spec
- ApiVersions: response matches implemented version ranges
- RecordBatch header parsing: read 61-byte header, locate CRC range
- Storage: write → read with byte-identical equality across multiple
  epochs in single partition
- TakeOver: prior segment scanned, sealed, new segment created
- Append: epoch mismatch returns ErrEpochMismatch
- Self-fence: stale heartbeat causes Append to return NOT_LEADER
- Controller election, heartbeat protocol, AssignmentStore
- Balancer, manifest atomic update
- ACL engine, SCRAM, KafkaUser/Acl controllers
- Per-broker external hostname Metadata correctness
- Cert hot-reload via fsnotify

Byte-opacity tests (tests/byte-opacity/, NEW in v3.3):
- Produce a batch with snappy compression → fetch → assert response
  payload bytes equal request payload bytes (byte-for-byte).
- Produce a batch with gzip compression → fetch → byte-identical.
- Produce a batch with lz4 compression → fetch → byte-identical.
- Produce a batch with no compression, multiple records, with headers
  → fetch → byte-identical.
- Produce 1000 batches → fetch them all → entire fetch response
  payload is byte-identical concatenation of original requests.
- CPU profile under load: assert that snappy.Decode, gzip.NewReader,
  flate.NewReader, lz4.NewReader, and RecordBatch encoding/decoding
  do NOT appear in the produce hot path's CPU profile (>1%).
- Allocation profile under load: assert no per-record allocations on
  the produce hot path; per-batch allocations only.
- Tripwire metric assertions: skafka_codec_record_decode_total and
  skafka_codec_batch_reencode_total stay at zero through full
  test suite execution.

Integration tests (kind + csi-driver-nfs default):
- Single broker: 10,000 records produce + consume + verify order
- Three brokers: produce + consume across all leaders
- Graceful broker shutdown: ~500ms producer recovery measured
- Hard broker crash: 4-5s recovery measured
- Recovery: SIGKILL mid-write, CRC-truncation at boundary
- TLS passthrough + SNI
- External Metadata response carries per-broker hostnames
- NOT_LEADER redirect handled transparently
- Consumer group with rebalance
- ACL enforcement
- KafkaTopic, KafkaUser CRDs end-to-end
- Large message (10MB)
- Retention-based segment deletion
- PodDisruptionBudget honored
- Scale brokers 3→5: partitions rebalance
- Cert rotation <5s
- Assignment file size at 8000+ partitions: no degradation
- Real client compatibility: produce with franz-go default config
  (idempotent + snappy compression), fetch with kafka-go, verify
  records arrive intact

Controller-failover tests (tests/controller-failover/):
(Unchanged from v3.2.)

Stale-controller race tests (tests/stale-controller-race/):
(Unchanged from v3.2.)

NFS fault injection tests (tests/nfs-fault/):
(Unchanged from v3.2.)

Kafka compatibility tests (test clients only):
- franz-go produce + consume + idempotent producer + snappy compression
- segmentio/kafka-go produce + consume
- kcat produce + consume
- kafka-verifiable-producer + consumer
- kafka-consumer-groups.sh, kafka-topics.sh
- kafka-dump-log.sh against skafka segment files
  (verifies on-disk format == Apache Kafka's)
```

---

## MVP Definition (What "Done" Looks Like)

- [ ] kafka-console-producer/-consumer end-to-end
- [ ] Metadata response carries per-broker hostnames
- [ ] TLS passthrough + SNI verified
- [ ] Graceful broker shutdown: ~500ms recovery measured
- [ ] Hard broker kill: 4-5s recovery measured
- [ ] Hard controller kill: data plane uninterrupted, control plane <15s
- [ ] AZ-failure simulation: cluster recovers <15s
- [ ] Kafka client `NOT_LEADER_FOR_PARTITION` retry handled
- [ ] Consumer group rebalance with consumer kill
- [ ] KafkaCluster CRD deploys all resources
- [ ] KafkaTopic, KafkaUser, KafkaAcl CRDs end-to-end
- [ ] cert-manager Certificate rotates without pod restart
- [ ] Helm chart deploys on csi-driver-nfs in one command
- [ ] Helm chart deploys on AWS EFS in one command
- [ ] Helm chart deploys on Azure Files Premium NFS in one command
- [ ] Two-leaders fault injection produces no torn records
- [ ] Recovery from kill-9-during-produce truncates at CRC boundary
- [ ] cmd/skafka-fsync-check passes
- [ ] cmd/skafka-failover-probe runs and reports recommendations
- [ ] Stale-controller race test: stale assignment file rejected via
      epoch fence
- [ ] Assignment.json size scales to 50,000 partitions without
      degradation
- [ ] CR mirror failure tolerated
- [ ] assignment.json.tmp orphan cleanup verified
- [ ] Etcd write rate <10/s with 1000 partitions
- [ ] **Byte-identical round-trip: produce a snappy-compressed batch
      with multiple records and headers, fetch it, assert response
      payload bytes equal request payload bytes byte-for-byte**
- [ ] **CPU profile under produce load shows CRC32C prominently and
      shows zero time in compression libraries (snappy, gzip, lz4,
      flate)**
- [ ] **`skafka_codec_record_decode_total` and
      `skafka_codec_batch_reencode_total` are flat lines at zero
      throughout the integration test suite**
- [ ] **kafka-dump-log.sh reads skafka segment files without error**
- [ ] Prometheus metrics expose all v3.3 metrics
- [ ] Grafana dashboard shows produce batch size distribution and
      compression breakdown
- [ ] README documents bytes-are-opaque architecture, file-based
      control plane, epoch fencing, NFS mount options

---

## Open Questions to Resolve in Phase 1

1. **fsync durability across supported RWX providers** —
   `cmd/skafka-fsync-check` validates per-PR.

2. **Heartbeat timeout calibration under load** —
   `cmd/skafka-failover-probe` measures p99.9 RTT.

3. **NFS mtime resolution and attribute caching** — verify per-provider
   that `acregmax=1` produces sub-second freshness.

4. **Controller balancer algorithm** — strict-stability for v1.

5. **__consumer_offsets in v1** — retention-based deletion + in-memory
   snapshot.

6. **Operator PVC access** — operator pod mounts the same shared PVC.

7. **Minimum API version range** — Kafka 2.6+ clients.

8. **DNS strategy for per-broker hostnames** — wildcard or explicit.

9. **Certificate strategy for per-broker hostnames** — wildcard or
   SAN-per-broker.

10. **Idempotent producer support in v1** — accept fields, no dedup.

11. **Dedicated controller mode** — deferred to v4.

12. **mmap vs read for segment file access** (NEW in v3.3) — for
    Fetch responses, do we mmap segment files (page cache shared
    with OS, zero-copy possible later via sendfile) or `read()`
    into pooled buffers? mmap is preferred because it sets up the
    v1.5 sendfile optimization with no rework, but NFS mmap behavior
    varies by client. Verify per-provider that mmap reads correctly
    serve fetch traffic at expected throughput. Default: mmap with
    fallback to `pread()` on platforms where mmap is problematic.

The v3.1 questions about per-partition Lease scaling and v3.2
questions about CR size limits are resolved.

---

## v2 Roadmap: Kafka Streams Compatibility

(Unchanged from v3.0/v3.1/v3.2. Transactions, EOS, log compaction,
remaining API keys for Streams. The bytes-are-opaque architecture
extends naturally — transaction control batches are also opaque to
the broker; the broker reads the `attributes` field in the batch
header to identify control batches but does not decode the records
within.)

---

## Local Development Setup

(Same as v3.2.)

```
make test-byte-opacity            # new in v3.3
```

---

## Migration Guide: Strimzi → skafka

(Unchanged from v3.0/v3.1/v3.2.)

---

## Security Hardening

(Unchanged from v3.1/v3.2.)

---

## Disaster Recovery and Backup

(Unchanged from v3.1/v3.2 — including the small post-restore note
about controllerEpoch reconciliation.)

---

## Summary of Changes from v3.2

| Area | v3.2 | v3.3 |
|---|---|---|
| Architectural principle: byte handling | Implicit (no guidance) | Explicit: bytes-are-opaque, articulated as Design Principle 9 and Critical Constraints 21-24 |
| StorageEngine.Append signature | (took []Record or implicit) | Takes `batchBytes []byte` directly |
| StorageEngine.Read return | (returned records) | Returns `[]byte` (raw segment bytes) |
| Codec types | Could include decoded RecordBatch | RecordBatch payload is `[]byte` everywhere; only 61-byte header is parsed |
| Compression handling | Unspecified | Delegated entirely to client; broker never compresses or decompresses |
| Segment file format | "Match Apache Kafka's format" | Exactly the wire format, byte-identical |
| Future zero-copy | Implicit | Explicit Performance Roadmap section, with `sendfile(2)` and kTLS as v1.5 work |
| Future scale-out | Implicit | Multi-PVC sharding and tiered storage as v2 work |
| Tripwire metrics | None | `skafka_codec_record_decode_total`, `skafka_codec_batch_reencode_total` |
| Tests | Functional only | Plus byte-identical round-trip, CPU/allocation profiling, kafka-dump-log compat |

The single-writer safety model, controller-based coordination,
file-based assignment, storage engine layout, protocol codec API
coverage, operator, auth engine, external access, and DR strategy are
all unchanged from v3.2. The v3.3 redesign is contained entirely in
how the broker handles batch payloads — and the decision to make that
handling rule explicit at the architectural level instead of leaving it
to implementation discretion.

The architectural soul: delegate to Kubernetes for what Kubernetes does
well, delegate to the shared filesystem for what filesystems do well,
delegate to the producer and consumer for what they do well (encoding,
compression, record-level interpretation). The broker's job is to
authenticate, authorize, validate batch integrity, route, and persist
bytes — nothing else.