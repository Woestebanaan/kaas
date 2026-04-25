# SharedKafka (skafka): Kafka on Shared Storage — Claude Code Implementation Plan
# Version 2.6 — Kubernetes-Native, Hand-Rolled Codec, Per-Broker TLS Passthrough, v2 Roadmap

## Project Overview

Build an open-source, Apache Kafka protocol-compatible broker that uses a single
shared ReadWriteMany PVC as its storage backend, enabling replication factor 1
while retaining durability through the storage layer itself. The PVC's filesystem
must honour BSD `flock(2)` cluster-wide (CephFS, JuiceFS — see Target Deployment
above). All coordination (leader election, cluster membership, metadata, access
control) is delegated to Kubernetes — no custom consensus layer, no ZooKeeper,
no KRaft.

This plan covers two release milestones:

- **v1 (MVP):** Core produce/consume, consumer groups, auth, CRD management.
  Targets teams who need Kafka semantics on-prem without cloud object storage.
  Kafka Streams is NOT supported in v1.

- **v2 (Streams-compatible):** Transactions, exactly-once semantics, log
  compaction, and the remaining API keys required by Kafka Streams. Doubles
  the protocol surface but makes skafka a full Kafka replacement.

**Working name:** `skafka`
**Language:** Go 1.22+
**License:** Apache 2.0
**Target deployment:** Kubernetes with any ReadWriteMany PVC whose filesystem
propagates BSD `flock(2)` cluster-wide (CephFS, JuiceFS, etc.). NFS is **not**
supported for multi-broker today — `flock(2)` is node-local on most NFS clients,
and the `NFSLock` placeholder is advisory-only (logs a warning, never wired in).
Single-broker deployments are safe on any RWX or RWO volume because the
Kubernetes Lease alone arbitrates writers.

---

## Design Principles

1. **Delegate to Kubernetes, don't reinvent it.** Leader election, failure
   detection, cluster membership, config management — Kubernetes already solved
   these. Use the primitives it provides.

2. **All brokers are identical.** No broker/controller split. No special roles.
   Every pod runs the same binary.

3. **Shared storage is the source of truth.** The PVC provides durability.
   Kafka-level replication is unnecessary and disabled (RF=1).

4. **Lock before write, always.** The Kubernetes Lease AND the filesystem lock
   must both be held before a single byte is written.

5. **Declarative everything.** Topics, users, ACLs are all Kubernetes resources.
   No shell scripts, no custom admin APIs.

6. **End-to-end TLS with per-broker advertised hostnames.** Each broker pod
   has its own external hostname (`broker-N.kafka.example.com`). A Gateway
   (TLSRoute passthrough, SNI-matched) or per-broker Services routes each SNI
   hostname to its specific pod without terminating TLS. No intermediate proxy
   ever holds the private key. Standard Kafka client retry handles
   `NOT_LEADER_FOR_PARTITION` — the client reconnects to the correct broker's
   hostname using the Metadata it already has.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        Kubernetes                                 │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │  coordination.k8s.io/v1 Leases (one per partition)          │ │
│  │  partition-topicA-0 → broker-1                              │ │
│  │  partition-topicA-1 → broker-0                              │ │
│  │  partition-topicB-0 → broker-2  ...                         │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                                                                   │
│  ┌────────────────────┐   ┌──────────────────────────────────┐   │
│  │  CRDs              │   │  Kubernetes Secrets              │   │
│  │  KafkaCluster      │   │  (credentials, TLS certs)        │   │
│  │  KafkaTopic        │   └──────────────────────────────────┘   │
│  │  KafkaUser         │                                           │
│  │  KafkaUserGroup    │   ┌──────────────────────────────────┐   │
│  │  KafkaAcl          │   │  EndpointSlices (broker list)    │   │
│  └────────────────────┘   └──────────────────────────────────┘   │
│                                                                   │
│  External clients (TLS + SNI)                                    │
│    broker-0.kafka.example.com:9093                               │
│    broker-1.kafka.example.com:9093                               │
│    broker-2.kafka.example.com:9093                               │
│         │                                                         │
│  ┌──────▼──────────────────────────────────────────────────────┐ │
│  │  Gateway (TLSRoute passthrough) OR LoadBalancer Service     │ │
│  │  — SNI hostname selects which broker pod                    │ │
│  │  — Gateway never terminates TLS; never sees plaintext       │ │
│  └──────┬──────────────────────────────────────────────────────┘ │
│         │  per-SNI routing                                        │
│    ┌────┴────┐      ┌─────────┐      ┌─────────┐                │
│    │broker-0 │      │broker-1 │      │broker-2 │                │
│    │  pod    │      │  pod    │      │  pod    │                │
│    │TLS ends │      │TLS ends │      │TLS ends │                │
│    │  here   │      │  here   │      │  here   │                │
│    └────┬────┘      └────┬────┘      └────┬────┘                │
│         │                │                │                      │
│         │  Wrong-leader produce? Broker returns                  │
│         │  NOT_LEADER_FOR_PARTITION. Client refreshes            │
│         │  Metadata, reconnects to actual leader using its own   │
│         │  hostname. Standard Kafka protocol retry — no custom   │
│         │  forwarding, no proxy.                                  │
│         │                                                         │
│  ┌──────┴──────────────────────────────────────────────────────┐ │
│  │  skafka-headless (ClusterIP: None) — in-cluster DNS         │ │
│  │  skafka-N.skafka-headless.<ns>.svc.cluster.local:9092       │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                          │                                        │
│               ┌──────────────────────┐                           │
│               │  Single ReadWriteMany│                           │
│               │  PVC (CephFS)        │                           │
│               │  /data/              │                           │
│               │    __cluster/        │                           │
│               │    topic-A/          │                           │
│               │    topic-B/  ...     │                           │
│               └──────────────────────┘                           │
└──────────────────────────────────────────────────────────────────┘
```

**Key property:** external access uses standard Kafka semantics, not a custom
proxy. Each broker pod has its own external hostname; clients see per-broker
addresses in Metadata and follow the protocol's built-in retry flow when they
hit a wrong leader. TLS terminates at the broker pod — a single wildcard or
SAN-per-broker certificate covers all hostnames, cert-manager handles rotation,
and fsnotify-based hot-reload means rotations require no pod restarts. The
shared PVC doesn't change this path — it matters for storage durability, not
for external addressing.

---

## CRD Surface Area

Four CRDs form the entire management interface. No kubectl-exec, no admin
scripts, no separate admin port.

### `KafkaTopic`

```yaml
apiVersion: skafka.io/v1alpha1
kind: KafkaTopic
metadata:
  name: payment-events
  namespace: kafka
spec:
  partitions: 12
  config:
    retentionMs: 604800000       # 7 days
    segmentBytes: 1073741824     # 1GB
    cleanupPolicy: delete        # or compact
status:
  conditions:
    - type: Ready
      status: "True"
  partitionCount: 12
```

### `KafkaUser`

```yaml
apiVersion: skafka.io/v1alpha1
kind: KafkaUser
metadata:
  name: payments-service
  namespace: kafka
spec:
  authentication:
    # Option A: SCRAM-SHA-512 (password-based)
    type: scram-sha-512
    password:
      secretRef:
        name: payments-kafka-password
        key: password

    # Option B: mTLS (certificate-based)
    # type: tls
    # certificateRef:
    #   name: payments-kafka-cert

    # Option C: Kubernetes ServiceAccount JWT (most cloud-native)
    # type: kubernetes-serviceaccount
    # serviceAccountRef:
    #   name: payments-service-sa
    #   namespace: payments

  quotas:
    producerByteRate: 10485760   # 10MB/s
    consumerByteRate: 20971520   # 20MB/s
    requestPercentage: 25

status:
  conditions:
    - type: Ready
      status: "True"
  secret: payments-service-kafka-credentials
```

### `KafkaUserGroup`

```yaml
apiVersion: skafka.io/v1alpha1
kind: KafkaUserGroup
metadata:
  name: analytics-team
  namespace: kafka
spec:
  members:
    - payments-service
    - inventory-service
    - reporting-service
  rules:
    - resource:
        type: topic
        name: analytics-
        patternType: prefix
      operations: [Read, Describe]
      permission: Allow
```

### `KafkaAcl`

```yaml
apiVersion: skafka.io/v1alpha1
kind: KafkaAcl
metadata:
  name: payments-service-acls
  namespace: kafka
spec:
  principal:
    kind: KafkaUser
    name: payments-service
  rules:
    - resource:
        type: topic
        name: payment-events
        patternType: literal
      operations: [Write, DescribeConfigs]
      permission: Allow
    - resource:
        type: topic
        name: inventory-updates
        patternType: literal
      operations: [Read, Describe]
      permission: Allow
    - resource:
        type: group
        name: payments-consumer-group
        patternType: literal
      operations: [Read]
      permission: Allow
    - resource:
        type: topic
        name: "*"
        patternType: literal
      operations: [Delete]
      permission: Deny
status:
  aclCount: 4
  conditions:
    - type: Ready
      status: "True"
```

---

## Kubernetes Primitives Used (and What They Replace)

| Kubernetes Primitive           | Replaces                                      |
|-------------------------------|-----------------------------------------------|
| StatefulSet ordinal index      | Broker ID assignment + registration           |
| coordination.k8s.io/v1 Lease  | KRaft/ZooKeeper partition leader election     |
| client-go leaderelection lib   | Custom consensus implementation               |
| EndpointSlice watch            | Heartbeat / cluster membership protocol       |
| PodDisruptionBudget            | Kafka min-ISR availability guarantee          |
| ReadinessGates                 | Custom broker warmup / readiness logic        |
| Init containers                | Partition directory initialization            |
| Projected volumes              | Config + secret injection                     |
| CRDs (5x)                      | Admin API, topic/user/ACL/cluster management  |
| Kubernetes Secrets             | Credential storage                            |
| HPA + custom metrics           | Manual cluster scaling                        |
| Gateway API TLSRoute (SNI-matched) | Kafka-aware proxy tier + routing logic    |
| Per-broker Service (pod-name)  | Per-broker NodePorts / external addressability |
| cert-manager Certificate       | Per-broker certificate rotation scripts       |

---

## Project Layout

```
skafka/
├── cmd/
│   ├── skafka/              # Broker binary
│   └── skafka-operator/     # Operator binary
├── internal/
│   ├── protocol/            # Kafka wire protocol — fully hand-rolled
│   │   ├── server.go        # TCP server, connection lifecycle
│   │   ├── dispatch.go      # API key → handler routing
│   │   ├── codec/           # Broker-side encoder/decoder
│   │   │   ├── reader.go    # Binary read primitives (varint, string, array)
│   │   │   ├── writer.go    # Binary write primitives
│   │   │   ├── types.go     # Shared wire types (RecordBatch, Header, etc.)
│   │   │   └── api/         # One file per API key
│   │   │       ├── produce.go        # API key 0
│   │   │       ├── fetch.go          # API key 1
│   │   │       ├── list_offsets.go   # API key 2
│   │   │       ├── metadata.go       # API key 3
│   │   │       ├── offset_commit.go  # API key 8
│   │   │       ├── offset_fetch.go   # API key 9
│   │   │       ├── find_coordinator.go # API key 10
│   │   │       ├── join_group.go     # API key 11
│   │   │       ├── heartbeat.go      # API key 12
│   │   │       ├── leave_group.go    # API key 13
│   │   │       ├── sync_group.go     # API key 14
│   │   │       ├── describe_groups.go# API key 15
│   │   │       ├── list_groups.go    # API key 16
│   │   │       ├── api_versions.go   # API key 18
│   │   │       ├── create_topics.go  # API key 19
│   │   │       ├── delete_topics.go  # API key 20
│   │   │       ├── sasl_handshake.go # API key 17
│   │   │       ├── sasl_authenticate.go # API key 36
│   │   │       ├── describe_acls.go  # API key 29
│   │   │       ├── create_acls.go    # API key 30
│   │   │       └── delete_acls.go    # API key 31
│   │   └── handlers/        # Business logic (uses codec types)
│   │       ├── produce.go
│   │       ├── fetch.go
│   │       ├── metadata.go
│   │       ├── consumer_group.go
│   │       └── admin.go
│   ├── storage/             # Shared filesystem engine
│   │   ├── engine.go        # StorageEngine interface + impl
│   │   ├── segment.go       # Segment read/write
│   │   ├── index.go         # Offset index
│   │   └── cleaner.go       # Retention / compaction
│   ├── lease/               # Kubernetes Lease management
│   │   ├── manager.go       # Acquire/release/watch leases
│   │   └── fencer.go        # Fencing token enforcement
│   ├── lock/                # Filesystem-level locking
│   │   ├── flock.go         # CephFS (flock syscall)
│   │   └── nfs.go           # NFS fallback (advisory file-based)
│   ├── auth/                # Authentication + authorization
│   │   ├── scram.go         # SCRAM-SHA-512
│   │   ├── tls.go           # mTLS
│   │   ├── serviceaccount.go# Kubernetes SA JWT validation
│   │   └── acl.go           # ACL enforcement engine
│   ├── coordinator/         # Consumer group state machine
│   │   ├── group.go
│   │   └── offset.go
│   └── k8s/                 # Kubernetes client wrappers
│       ├── broker.go        # Self-identification, EndpointSlice watch
│       └── metadata.go      # CRD watch → in-memory state
├── operator/
│   ├── controllers/
│   │   ├── kafkacluster_controller.go  # Owns StatefulSet, listeners (TLSRoute + LB Service)
│   │   ├── kafkatopic_controller.go
│   │   ├── kafkauser_controller.go
│   │   ├── kafkausergroup_controller.go
│   │   └── kafkaacl_controller.go
│   └── api/
│       └── v1alpha1/        # CRD Go types (generated)
├── pkg/
│   └── kafkaapi/            # Public broker API types (not wire types)
├── deploy/
│   ├── helm/                # Helm chart
│   ├── crds/                # CRD YAML manifests
│   ├── rbac/                # ClusterRole for broker + operator
│   └── grafana/             # Dashboard JSON
├── tests/
│   ├── unit/
│   ├── integration/         # Requires k8s + CephFS
│   └── kafka-compat/        # Real Kafka client tests
├── Dockerfile               # Broker image
├── Dockerfile.operator      # Operator image
└── Makefile
```

---

## Phase 1: Foundation (Week 1–2)

**Goal:** Project skeleton, CI, CRD definitions, core interfaces.

```
1. Initialize Go module
   go mod init github.com/yourorg/skafka

2. Generate CRD scaffolding with kubebuilder:
   kubebuilder init --domain skafka.io --repo github.com/yourorg/skafka
   kubebuilder create api --group skafka --version v1alpha1 --kind KafkaTopic
   kubebuilder create api --group skafka --version v1alpha1 --kind KafkaUser
   kubebuilder create api --group skafka --version v1alpha1 --kind KafkaUserGroup
   kubebuilder create api --group skafka --version v1alpha1 --kind KafkaAcl
   make generate && make manifests

3. Core interfaces to define upfront (before any implementation):

   // StorageEngine — shared filesystem reads/writes
   type StorageEngine interface {
       Append(ctx context.Context, topic string, partition int32,
              records []Record) (baseOffset int64, err error)
       Read(ctx context.Context, topic string, partition int32,
            startOffset int64, maxBytes int) ([]Record, error)
       HighWatermark(topic string, partition int32) (int64, error)
       LogStartOffset(topic string, partition int32) (int64, error)
       CreatePartition(topic string, partition int32) error
       DeletePartition(topic string, partition int32) error
   }

   // LeaseManager — Kubernetes Lease per partition
   type LeaseManager interface {
       Acquire(ctx context.Context, topic string, partition int32) error
       Release(topic string, partition int32) error
       IsLeader(topic string, partition int32) bool
       WatchLeaders(ctx context.Context) (<-chan LeaderChange, error)
   }

   // PartitionLock — filesystem-level write fence
   type PartitionLock interface {
       Lock(topic string, partition int32) error
       Unlock(topic string, partition int32) error
       IsLocked(topic string, partition int32) bool
   }

   // AuthEngine — authentication + ACL enforcement
   type AuthEngine interface {
       Authenticate(ctx context.Context, creds Credentials) (Principal, error)
       Authorize(principal Principal, resource Resource,
                 operation Operation) bool
   }

4. RBAC for broker ServiceAccount:
   - get/list/watch: Leases, EndpointSlices, KafkaTopic, KafkaUser,
     KafkaUserGroup, KafkaAcl, Secrets (own namespace only)
   - create/update: Leases (for partition leadership)
   - get/patch: own Pod (for ReadinessGate updates)

5. CI (GitHub Actions):
   - go vet, golangci-lint, go test ./...
   - make manifests → verify no CRD drift
   - Build multi-arch Docker images (amd64, arm64) on tag
   - Integration test job: kind + Rook-Ceph, run full test suite
```

---

## Phase 2: Kafka Protocol Layer (Week 2–4)

**Goal:** A hand-rolled broker-side Kafka wire protocol codec, TCP server,
and request handlers for all Priority 1 API keys.

### Why hand-rolled?

Franz-go and Sarama are client libraries. Their protocol encoding is designed
around a client's request/response flow and makes assumptions that are wrong
or awkward on the server side. A broker-side codec is ~2,000-3,000 lines of
straightforward Go and gives full control over every byte — which matters when
debugging protocol compatibility issues. Use franz-go and Sarama source code
as *reference*, not as a dependency.

Reference material for implementation:
- https://kafka.apache.org/protocol (official spec)
- https://github.com/twmb/franz-go (read, do not import)
- https://github.com/IBM/sarama (read, do not import)

```
1. TCP server (internal/protocol/server.go)
   - Listen on configurable port (default 9092)
   - Goroutine per connection, context-aware
   - Read request frame:
     [total_length:4][api_key:2][api_version:2][correlation_id:4]
     [client_id_len:2][client_id:N][tagged_fields?][body...]
   - Write response frame:
     [total_length:4][correlation_id:4][tagged_fields?][body...]
   - Optional TLS listener on port 9093
   - Connection-level state: authenticated Principal, client_id,
     negotiated API versions

2. Binary codec primitives (internal/protocol/codec/reader.go,
   internal/protocol/codec/writer.go)

   Reader must implement:
     ReadInt8, ReadInt16, ReadInt32, ReadInt64
     ReadUvarint, ReadVarint             ← Kafka uses both
     ReadString, ReadNullableString      ← length-prefixed
     ReadCompactString                   ← uvarint-prefixed (newer APIs)
     ReadBytes, ReadNullableBytes
     ReadArray(fn)                       ← int32 count + repeated decode
     ReadCompactArray(fn)                ← uvarint count (newer APIs)
     ReadTaggedFields                    ← flexible version APIs

   Writer must implement the symmetric set.

   NOTE: Kafka has TWO array encodings (legacy int32 and compact uvarint)
   and TWO string encodings. Which one is used depends on the API version.
   This is the main source of bugs — get it right in the primitives and
   every API implementation follows.

3. Shared wire types (internal/protocol/codec/types.go)

   RecordBatch:
     baseOffset        int64
     batchLength       int32
     partitionLeaderEpoch int32
     magic             int8    ← must be 2 for current Kafka
     crc               uint32  ← CRC32C of everything after this field
     attributes        int16   ← compression, timestamp type, etc.
     lastOffsetDelta   int32
     baseTimestamp     int64
     maxTimestamp      int64
     producerId        int64
     producerEpoch     int16
     baseSequence     int32
     records           []Record

   Record (within a batch):
     attributes        int8
     timestampDelta    varint
     offsetDelta       varint
     key               []byte  ← nullable
     value             []byte  ← nullable
     headers           []Header

   Header:
     key               string
     value             []byte

   ErrorCode: typed int16 with constants for all Kafka error codes
   (NONE=0, UNKNOWN_TOPIC=3, NOT_LEADER=6, etc. — full list in spec)

4. Per-API codec files (internal/protocol/codec/api/*.go)

   Each file implements two functions:
     Decode{ApiName}Request(r *Reader, version int16) (*{ApiName}Request, error)
     Encode{ApiName}Response(w *Writer, resp *{ApiName}Response, version int16)

   Version parameter is critical — request fields change between versions.
   Implement the minimum version range needed for each API:

   API key 0  Produce:          v3–v9
   API key 1  Fetch:            v4–v13
   API key 2  ListOffsets:      v1–v7
   API key 3  Metadata:         v1–v12
   API key 8  OffsetCommit:     v2–v8
   API key 9  OffsetFetch:      v1–v8
   API key 10 FindCoordinator:  v0–v4
   API key 11 JoinGroup:        v2–v9
   API key 12 Heartbeat:        v0–v4
   API key 13 LeaveGroup:       v0–v4
   API key 14 SyncGroup:        v0–v5
   API key 15 DescribeGroups:   v0–v5
   API key 16 ListGroups:       v0–v4
   API key 17 SaslHandshake:    v0–v1
   API key 18 ApiVersions:      v0–v3
   API key 19 CreateTopics:     v0–v7
   API key 20 DeleteTopics:     v0–v6
   API key 29 DescribeAcls:     v0–v3
   API key 30 CreateAcls:       v0–v3
   API key 31 DeleteAcls:       v0–v3
   API key 36 SaslAuthenticate: v0–v2

5. ApiVersions handler — must be correct or clients refuse to connect:
   Return the supported min/max version for every implemented API key.
   Clients use this to negotiate which version to use for all subsequent
   requests. If this is wrong, every other API will fail in subtle ways.

6. CRC32C validation on RecordBatch:
   - Verify CRC on Produce requests (reject malformed batches)
   - Compute CRC on Fetch responses
   - Use hash/crc32 with Castagnoli polynomial (not IEEE)
   - This is a common source of bugs — test with known-good byte sequences

7. Request dispatch (internal/protocol/dispatch.go):
   - Map api_key → handler function
   - Decode request using codec
   - Call handler (business logic, uses StorageEngine / LeaseManager)
   - Encode response using codec
   - All handlers must be goroutine-safe

8. Compatibility test suite (tests/kafka-compat/):
   Use franz-go and kafka-go as TEST CLIENTS ONLY — do not import
   their internal packages. Point them at a running skafka broker.
   - franz-go client: produce 1000 records, consume all, verify order
   - segmentio/kafka-go client: same test
   - kcat (kafkacat): produce and consume via CLI
   - All must pass before merging any protocol change
```

---

## Phase 3: Shared Storage Engine (Week 3–5)

**Goal:** Log segment reads/writes to a shared RWX PVC (filesystem must honour
BSD `flock(2)` cluster-wide — CephFS, JuiceFS) with exclusive write access
enforced by both Kubernetes Lease and filesystem lock.

```
1. Filesystem layout on PVC:
   /data/
     __cluster/
       acls.json               ← operator writes, brokers inotify-watch
       credentials.json        ← hashed credentials (SCRAM salts + hashes)
     {topic}/
       {partition}/
         {base_offset}.log     ← record data
         {base_offset}.index   ← sparse offset→position index
         {base_offset}.timeindex
         .leader-epoch         ← epoch of broker that created each segment
         .lock                 ← BSD flock(2) target file (must be honoured cross-node)

2. Segment file format: match Apache Kafka exactly.
   Reason: kafka-dump-log.sh and other tooling work out of the box.

3. Two-lock safety model (BOTH must be held to write):

   Lock A — Kubernetes Lease (internal/lease/manager.go):
   - Use k8s.io/client-go/tools/leaderelection
   - One Lease object per partition:
     name: "partition-{topic}-{partition}"
     namespace: same as broker StatefulSet
   - LeaseDuration: 15s (tunable to 5s minimum)
   - RenewDeadline: 10s
   - RetryPeriod: 2s
   - Callbacks:
     OnStartedLeading → acquire Lock B, mark partition writable
     OnStoppedLeading → release Lock B, mark partition read-only,
                        flush and sync current segment
     OnNewLeader      → update in-memory routing table

   Lock B — Filesystem lock (internal/lock/):
   - syscall.Flock(fd, LOCK_EX | LOCK_NB) on the partition's .lock file.
     Requires the underlying filesystem to honour BSD flock(2) cross-node
     (CephFS, JuiceFS). This is the only path wired into cmd/skafka.
   - Held as long as the Kubernetes Lease is held.
   - Released immediately when the Lease is lost.
   - NFS note: `flock(2)` is node-local on most NFS clients, so the lock
     does not protect against a writer on another node. internal/lock/nfs.go
     ships an advisory placeholder that logs a warning and is never selected
     automatically; safe NFS support would require fcntl/POSIX locks via
     rpc.statd/lockd and is not implemented today.

   Append() pseudocode:
     if !leaseManager.IsLeader(topic, partition):
         return ErrNotLeader
     if !partitionLock.IsLocked(topic, partition):
         return ErrLockNotHeld   // defense in depth
     writeToSegment(records)
     updateIndex()
     if flushPolicy.ShouldFlush():
         file.Sync()

4. Leader takeover sequence:
   a. New leader wins Kubernetes Lease
   b. OnStartedLeading fires
   c. New leader acquires filesystem lock
   d. New leader reads .leader-epoch file
   e. If epoch on disk < current epoch: scan backward from end of
      .log file, find last complete record (CRC check), truncate rest
   f. Write new epoch to .leader-epoch
   g. Mark partition writable, update high watermark

5. Segment writer:
   - Active segment: sequential append only
   - Flush policy (configurable):
     * Every N records (default: 1000)
     * Every M bytes (default: 4MB)
     * Every T milliseconds (default: 500ms)
     * On graceful shutdown: always flush + sync
   - Roll to new segment when active exceeds segmentBytes
   - Index entry written every indexIntervalBytes (default: 4096)

6. Segment reader:
   - Binary search .index for nearest offset <= requested
   - Seek .log file to indexed position
   - Scan forward to exact requested offset
   - Return records up to maxBytes

7. inotify watch on acls.json and credentials.json:
   - Use fsnotify library
   - Debounce 100ms after last event before reloading
   - Reload ACL engine and credential store without restart
   - Log reload events at INFO level

8. Retention cleaner (background goroutine, leader only):
   - Check segments older than retentionMs
   - Delete oldest segments, never the active segment
   - Non-leaders skip — only leader runs cleaner
   - Run every 5 minutes
```

---

## Phase 4: Kubernetes Lease Manager (Week 4–5)

**Goal:** Partition leader election via Kubernetes Leases, cluster membership
via EndpointSlice, broker identity via StatefulSet ordinal.

```
1. Broker identity (internal/k8s/broker.go):
   - Read own pod name from MY_POD_NAME env var (downward API)
   - Broker ID = ordinal suffix: broker-0 → 0, broker-1 → 1, etc.
   - Broker host = {pod-name}.{headless-svc}.{namespace}.svc.cluster.local
   - No registration protocol needed. Pod name IS the identity.

2. Partition assignment on startup:
   - List all KafkaTopic CRDs
   - For each topic+partition, attempt Lease acquisition
   - Use consistent hashing as preferred assignment hint
   - Target: even partition distribution across available brokers
   - Rebalancing: on new broker join (new EndpointSlice entry),
     voluntarily release some Leases to allow redistribution

3. EndpointSlice watcher:
   - Watch headless service EndpointSlice
   - Maintain in-memory map: broker_id → {host, port, ready}
   - Used by Metadata API handler to report cluster topology
   - On broker disappearance: Lease TTL expires naturally

4. Lease manager implementation:
   - Embed k8s.io/client-go/tools/leaderelection
   - One leaderelection.LeaderElector goroutine per partition
   - LeaderElectionRecord stored in Lease object
   - Identity: "{pod-name}-{leader-epoch}" for fencing

5. Startup ReadinessGate:
   Custom condition: "skafka.io/PartitionsReady"
   - False on pod start
   - True only when broker holds Leases for all assigned partitions
     AND filesystem locks are held for all of them
   - Pod only joins headless service when gate is True
   - Implementation: PATCH own Pod .status.conditions via k8s client

6. Init container (runs before broker starts):
   name: partition-init
   image: same as broker (with --init flag)
   task:
   - Mount shared PVC
   - For each KafkaTopic CRD: ensure partition directories exist
   - mkdir -p idempotently for all topic/partition paths
   - Exit 0 on success, non-zero blocks broker startup
   Reason: eliminates race between broker start and directory creation
   on freshly provisioned PVC
```

---

## Phase 5: Consumer Group Coordinator (Week 5–6)

**Goal:** Standard Kafka consumer group protocol over shared storage.

```
1. Coordinator election:
   - Kubernetes Lease: "coordinator-{group-id}"
   - Lease holder IS the coordinator for that group
   - FindCoordinator: return Lease holder's broker info
   - No holder: return COORDINATOR_NOT_AVAILABLE, client retries

2. Group state machine per consumer group:
   Empty → PreparingRebalance → CompletingRebalance → Stable → Dead

3. In-memory group state (coordinator broker only):
   - Members: {member_id → {client_id, session_timeout, protocols}}
   - GenerationId: increments on each rebalance
   - LeaderMemberId: first member to JoinGroup
   - ProtocolName: agreed assignment strategy

4. API handlers:
   JoinGroup:    add member, start rebalance timer if first member
   SyncGroup:    leader sends assignments, coordinator distributes
   Heartbeat:    reset member session timeout timer
   LeaveGroup:   remove member, trigger rebalance if Stable
   OffsetCommit: write to __consumer_offsets on shared PVC
   OffsetFetch:  read from __consumer_offsets (in-memory cache + PVC)

5. __consumer_offsets:
   - Created automatically on first OffsetCommit
   - Stored on shared PVC like any other topic
   - 50 partitions (configurable)
   - Compacted (keep only latest offset per key)

6. Session timeout:
   - Default: 30s (configurable per consumer)
   - Background timer per member
   - On expiry: remove member, trigger rebalance
```

---

## Phase 6: Operator (Week 6–7)

**Goal:** Reconcile CRDs into filesystem state and Kubernetes Secrets.
Target: ~400-600 lines of Go total across all four controllers.

```
1. Build with controller-runtime (kubebuilder generated scaffold)

2. KafkaTopic controller:
   - On create: create partition directories on shared PVC (via
     a Job that mounts the PVC, or direct mount in operator pod)
   - On update (partition count increase): create new partition dirs
   - On delete: Job to remove data directory from PVC
   - Status: update .status.partitionCount and .status.conditions

3. KafkaUser controller:

   For scram-sha-512:
   - Read password from referenced Secret (or generate one)
   - Compute SCRAM-SHA-512: generate salt, compute StoredKey+ServerKey
   - Write hashed credentials to /data/__cluster/credentials.json
     (atomic: write .tmp, then os.Rename to credentials.json)
   - Create/update output Secret with username + plaintext password
   - Status: Ready=True, reference output Secret name

   For tls:
   - Create cert-manager CertificateRequest
   - Wait for signing, write cert+key to output Secret
   - Add CN to credentials.json as valid TLS identity

   For kubernetes-serviceaccount:
   - Record ServiceAccount reference in credentials.json
   - Broker validates SA JWT via TokenReview API at runtime
   - No Secret needed — pod identity IS the credential

4. KafkaUserGroup controller:
   - Expand group membership into effective ACL rules
   - Merge into /data/__cluster/acls.json atomically
   - Rewrite entire file on any membership or rule change
   - Triggers inotify reload in all brokers automatically

5. KafkaAcl controller:
   - Validate principal exists (KafkaUser or KafkaUserGroup)
   - Merge all KafkaAcl objects into /data/__cluster/acls.json
   - Atomic write (tmp + rename)
   - Status: Ready=True with aclCount

6. acls.json format:
   {
     "version": 1,
     "acls": [
       {
         "principal": "User:payments-service",
         "resource": {
           "type": "topic",
           "name": "payment-events",
           "patternType": "literal"
         },
         "operations": ["Write", "DescribeConfigs"],
         "permission": "Allow"
       }
     ]
   }

7. RBAC for operator ServiceAccount:
   - Full CRUD: KafkaTopic, KafkaUser, KafkaUserGroup, KafkaAcl
   - Create/update/delete: Secrets (own namespace)
   - Create: CertificateRequests (if cert-manager enabled)
   - Create: Jobs (for topic deletion / directory creation)
   - get/patch: Leases (inspect partition leaders)
```

---

## Phase 7: Authentication Engine (Week 7)

**Goal:** SASL/SCRAM, mTLS, and Kubernetes ServiceAccount auth.

```
1. SASL negotiation (SaslHandshake + SaslAuthenticate):
   Supported mechanisms: SCRAM-SHA-512, PLAIN (dev/testing only)

2. SCRAM-SHA-512:
   - Server reads hashed credentials from in-memory cache
     (loaded from /data/__cluster/credentials.json)
   - Standard RFC 5802 exchange
   - No plaintext passwords stored on PVC (salted hashes only)

3. mTLS:
   - TLS listener on port 9093
   - Require and verify client certificate
   - Extract CN from cert as principal name
   - Validate CN exists in credentials.json as TLS user

4. Kubernetes ServiceAccount JWT:
   - Client sends SA JWT in SASL PLAIN password field
   - Broker calls TokenReview API: k8s validates the token
   - Extract ServiceAccount name+namespace from TokenReview response
   - Principal = "ServiceAccount:{namespace}/{name}"
   - ACLs reference this principal format

5. ACL enforcement (internal/auth/acl.go):
   - Load acls.json into memory on start and on inotify event
   - Per request: check principal + resource + operation against rules
   - Deny takes precedence over Allow (same as Kafka)
   - Wildcard and prefix pattern matching
   - Decision cache: 5s TTL to avoid per-request JSON scanning
   - Log denied requests at WARN with principal + resource

6. Quota enforcement:
   - Per-user token bucket for producerByteRate and consumerByteRate
   - On over-quota: return ThrottleTimeMs in response (Kafka standard)
   - Quotas read from credentials.json (written by operator)
```

---

## Phase 8: Kubernetes Deployment (Week 8)

**Goal:** Production-ready Helm chart with all Kubernetes primitives wired up.

```
1. Dockerfiles:
   Broker (Dockerfile):
     FROM golang:1.22-alpine AS builder
     COPY . .
     RUN CGO_ENABLED=0 go build -o /skafka ./cmd/skafka
     FROM gcr.io/distroless/static-debian12
     COPY --from=builder /skafka /skafka
     ENTRYPOINT ["/skafka"]
     Target size: <20MB

   Operator (Dockerfile.operator):
     Same pattern, different binary: ./cmd/skafka-operator

2. StatefulSet spec highlights:
   replicas: 3
   serviceName: skafka-headless

   initContainers:
   - name: partition-init
     image: same as broker
     args: ["--init"]
     volumeMounts:
     - name: data
       mountPath: /data

   containers:
   - name: broker
     readinessGates:
     - conditionType: "skafka.io/PartitionsReady"
     env:
     - name: MY_POD_NAME
       valueFrom: {fieldRef: {fieldPath: metadata.name}}
     - name: MY_NAMESPACE
       valueFrom: {fieldRef: {fieldPath: metadata.namespace}}
     livenessProbe:
       tcpSocket: {port: 9092}
       initialDelaySeconds: 30
     readinessProbe:
       httpGet: {path: /healthz, port: 8080}

   volumes:
   - name: data
     persistentVolumeClaim:
       claimName: skafka-shared-data   # single PVC, all brokers
   - name: config
     projected:
       sources:
       - configMap: {name: skafka-config}
       - secret: {name: skafka-tls-cert}   # if TLS enabled

3. Single shared PVC:
   apiVersion: v1
   kind: PersistentVolumeClaim
   metadata:
     name: skafka-shared-data
   spec:
     accessModes: [ReadWriteMany]
     storageClassName: ceph-filesystem
     resources:
       requests:
         storage: 500Gi

4. PodDisruptionBudget:
   maxUnavailable: 1
   Ensures Kubernetes never evicts >1 broker simultaneously.
   Equivalent to Kafka's min.insync.replicas safety guarantee.
   No code required — pure Kubernetes configuration.

5. Helm values (key ones):
   replicaCount: 3

   storage:
     className: ceph-filesystem     # or nfs-client
     size: 500Gi
     accessMode: ReadWriteMany
     mountPath: /data

   auth:
     enabled: true
     mechanisms: [SCRAM-SHA-512]
     tls:
       enabled: false
       certManagerIssuer: ""

   lock:
     backend: flock                  # or nfs (advisory file-based)

   config:
     logRetentionHours: 168
     logSegmentBytes: 1073741824
     numPartitions: 3
     defaultReplicationFactor: 1    # KEY: RF=1 is safe with shared storage
     minInsyncReplicas: 1

   lease:
     durationSeconds: 15
     renewDeadlineSeconds: 10
     retryPeriodSeconds: 2

   resources:
     requests:
       cpu: 500m
       memory: 1Gi
     limits:
       cpu: 2
       memory: 4Gi

   autoscaling:
     enabled: false
     minReplicas: 3
     maxReplicas: 10
     targetConsumerLagMessages: 100000  # HPA custom metric

6. StorageClass prerequisites (document in README):

   CephFS (recommended — supports flock reliably):
     provisioner: rook-ceph.cephfs.csi.ceph.com
     parameters:
       fsName: myfs
       pool: myfs-replicated
     allowVolumeExpansion: true

   NFS (advisory lock only):
     provisioner: nfs.csi.k8s.io
     WARNING: flock() is unreliable over NFS. A network partition
     could allow split-brain writes. Use CephFS for production.
```

---

## Phase 9: External Access via Per-Broker TLS Passthrough (Week 8–9)

**Goal:** Expose skafka to external clients with end-to-end TLS — no
intermediate proxy ever holds the private key, no custom protocol-aware router,
no forwarding logic. Each broker pod has its own externally-reachable hostname.
Standard Kafka client retry semantics handle leader changes; the Kafka protocol
was designed for exactly this case.

### The architectural trade-off

Two alternative designs were considered and rejected:

1. **Custom stateless router** (`skafka-router` Go binary) — rejected because
   it terminated TLS at an intermediate proxy and added ~600 lines of code
   to solve a problem the Kafka protocol already solves.

2. **Single external hostname + broker-side leader forwarding** — all brokers
   advertise `kafka.example.com`, non-leader brokers forward Produce requests
   to the lease holder over the internal network. Rejected because leader
   forwarding is redundant with the Kafka client's native
   `NOT_LEADER_FOR_PARTITION` retry — as long as Metadata gives clients
   per-broker addresses, the client library handles the retry cleanly. Adding
   forwarding on top of that just hides protocol behaviour the client is
   already built to handle.

Phase 9 adopts the third option: **per-broker advertised hostnames** (similar
in shape to Strimzi's TLS SNI approach, but with skafka's shared-storage
architecture simplifying everything downstream). Per-broker hostnames cost
slightly more YAML (N TLSRoutes + N Services) but leave the Kafka protocol
untouched and eliminate every single runtime moving part added by the other
two options.

### Architecture after Phase 9

```
External client (TLS + SNI)
        │
        ▼
Gateway (TLSRoute passthrough) OR LoadBalancer
        │
        ├─ SNI = broker-0.kafka.example.com → broker-0 pod
        ├─ SNI = broker-1.kafka.example.com → broker-1 pod
        └─ SNI = broker-2.kafka.example.com → broker-2 pod
                        │
                  (TLS terminates at the broker pod)
                        │
                  Wrong-leader produce → NOT_LEADER_FOR_PARTITION.
                  Client refreshes Metadata, reconnects to actual leader
                  using that leader's own hostname. Handled entirely by
                  the Kafka client library.

Internal clients skip the Gateway — headless Service DNS directly:
  skafka-N.skafka-headless.<ns>.svc.cluster.local:9092 (plaintext)
```

### Implementation outline

```
1. Broker TLS listener (internal/protocol/server.go + tls.go)

   Each broker runs two simultaneous listeners:
     - PLAINTEXT :9092 (internal, headless Service)
     - TLS :9093 (external, Gateway / LB, client-facing)

   The TLS listener uses a certificate watched via fsnotify — cert-manager
   rotation is picked up without a pod restart. TLS 1.3 minimum.

2. Per-broker advertised hostnames

   Each broker computes its external hostname from its StatefulSet ordinal +
   the cluster's hostname pattern:

     EXTERNAL_ADVERTISED_HOST = fmt.Sprintf(pattern, ordinal)
     (e.g. pattern = "broker-%d.kafka.example.com" → "broker-2.kafka.example.com")

   The hostname is known at pod creation time (no LB-IP wait, no operator-
   driven patch, no rolling restart). Helm renders the pattern into the
   StatefulSet env; the broker substitutes its ordinal at startup using the
   apps.kubernetes.io/pod-index downward-API label (Kubernetes 1.28+).

   Metadata responses on the external listener include each broker's own
   per-broker hostname. On the internal listener the broker advertises the
   headless DNS name.

3. Operator reconciliation (KafkaCluster controller)

   When listeners.external.enabled = true:
     a. Create/update cert-manager Certificate covering all broker hostnames
        (and the optional bootstrap hostname) in a single Secret
     b. Create N Services, one per broker, selecting the specific pod via
        statefulset.kubernetes.io/pod-name
     c. Create N TLSRoutes, one per broker, matching by SNI hostname, backed
        by the per-broker Service
     d. Update KafkaCluster.status.bootstrapServers with the list of
        broker-N.kafka.example.com:9093 addresses

   There is no post-creation patching of the StatefulSet. Hostnames are
   static from install time.

4. Gateway API TLSRoute (passthrough, SNI-matched)

   apiVersion: gateway.networking.k8s.io/v1alpha2
   kind: TLSRoute
   spec:
     hostnames: [broker-0.kafka.example.com]   # ← per-broker
     rules:
       - backendRefs:
           - name: skafka-broker-0             # ← Service selecting only pod-0
             port: 9093
     parentRefs:
       - name: skafka-gateway
         sectionName: kafka-tls                # TLS mode: Passthrough

   The Gateway reads the SNI hostname from ClientHello, picks the matching
   TLSRoute, forwards raw TLS bytes to the selected pod. Never terminates TLS.
   Never holds the private key.

   If Gateway API is not installed, per-broker LoadBalancer Services are a
   functional alternative (one public IP per broker) at higher cloud cost.

5. DNS requirements

   Two supported patterns:
     (a) Wildcard DNS: *.kafka.example.com → <Gateway LB IP>
         Simplest. One record covers all current and future brokers.

     (b) Explicit per-broker records: broker-0.kafka.example.com → <LB IP>
         One A/AAAA record per broker. More verbose; suitable for operators
         that avoid wildcards.

   Both work with the same Gateway/TLSRoute setup.

6. Certificate requirements

   Two supported patterns:
     (a) Wildcard cert: *.kafka.example.com
     (b) SAN-per-broker: broker-0.kafka.example.com,
                         broker-1.kafka.example.com,
                         broker-2.kafka.example.com,
                         plus optional bootstrap hostname

   cert-manager issues either; SAN-per-broker is preferred when scaling
   events are rare (fewer SANs = shorter issuance).

7. Helm values

   listeners:
     internal:
       port: 9092

     external:
       enabled: false                             # opt-in
       port: 9093
       hostnamePattern: broker-%d.kafka.example.com
       bootstrapHostname: ""                     # optional convenience

       tls:
         certManager:
           enabled: true
           issuerRef:
             name: letsencrypt-prod
             kind: ClusterIssuer

       gateway:
         enabled: true
         gatewayRef:
           name: skafka-gateway
           namespace: kafka

8. Internal client path (unchanged from Phase 8)

   In-cluster clients skip the Gateway entirely. They use the broker headless
   Service DNS directly:

     bootstrap.servers = skafka-0.skafka-headless.kafka.svc.cluster.local:9092,
                         skafka-1.skafka-headless.kafka.svc.cluster.local:9092,
                         skafka-2.skafka-headless.kafka.svc.cluster.local:9092

   Internal traffic is plaintext (cluster network is trusted). SASL and ACL
   enforcement apply normally on the internal listener.

9. Testing

   End-to-end in a real cluster (kind + Rook-Ceph + Envoy Gateway):
   - TLS passthrough connectivity: openssl s_client + -servername shows the
     correct broker cert (SNI routed correctly)
   - Produce + consume via external listener: franz-go bootstraps on one
     broker hostname, learns the others via Metadata, produces + consumes
     10,000 records
   - Metadata response carries per-broker hostnames (broker-0.kafka...,
     broker-1.kafka..., broker-2.kafka...) — not headless DNS, not a single
     shared host
   - NOT_LEADER redirect handled transparently: produce to broker-0's
     hostname for a partition led by broker-2 → client receives
     NOT_LEADER_FOR_PARTITION → client reconnects to broker-2.kafka... →
     succeeds, all without application-level handling
   - Certificate hot-reload: rotate the cert-manager-issued Secret; verify
     new TLS connections use the new cert within 5s, no pod restart
   - Broker failover: kubectl delete pod mid-produce → client reconnects to
     the surviving brokers via their own hostnames → partitions migrate via
     Lease → no data loss
   - Internal client unaffected when external listener is toggled
```

### What is NOT in Phase 9 (deliberately)

- No custom router binary — no `cmd/skafka-router`, no `internal/router`
- No leader forwarding — the Kafka protocol's NOT_LEADER retry is used as designed
- No Metadata response rewriting in flight — brokers advertise the correct
  host directly based on which listener received the request
- No operator-driven env var patching after pod startup — hostnames are known
  at install time via the Helm-rendered pattern

---

## Phase 10: Observability (Week 9–10)

```
1. Prometheus metrics (port 9090/metrics):

   # Throughput
   skafka_produce_records_total{topic}
   skafka_produce_bytes_total{topic}
   skafka_fetch_records_total{topic, consumer_group}
   skafka_fetch_bytes_total{topic, consumer_group}

   # Storage
   skafka_partition_high_watermark{topic, partition}
   skafka_partition_log_start_offset{topic, partition}
   skafka_partition_size_bytes{topic, partition}
   skafka_storage_write_latency_seconds{topic}   (histogram)
   skafka_storage_read_latency_seconds{topic}    (histogram)
   skafka_storage_fsync_latency_seconds          (histogram)

   # Leadership
   skafka_partition_leader{topic, partition}     # 1 if this broker leads
   skafka_lease_acquisition_total
   skafka_lease_loss_total

   # Consumer groups
   skafka_consumer_group_lag{topic, partition, consumer_group}
   skafka_consumer_group_members{consumer_group}
   skafka_consumer_group_rebalances_total{consumer_group}

   # Auth
   skafka_auth_success_total{mechanism}
   skafka_auth_failure_total{mechanism, reason}
   skafka_acl_deny_total{principal, resource_type}
   skafka_quota_throttle_total{principal}

   # External access
   skafka_external_connections_total{mode, broker_id}
   skafka_external_connection_errors_total{mode, broker_id, reason}

   # External TLS listener (per-broker, emitted by broker pods)
   skafka_tls_handshakes_total{broker, result}
   skafka_tls_handshake_errors_total{broker, reason}
   skafka_cert_reload_total{broker}
   skafka_not_leader_returned_total{topic, partition}  (client was redirected)

2. Structured logging (log/slog, JSON in production):
   - Request log: principal, api_key, topic, partition, latency, error
   - Leader change: topic, partition, old_leader, new_leader, epoch
   - ACL reload: file, acl_count, duration
   - Auth events: WARN on failure, DEBUG on success

3. /healthz endpoint (port 8080):
   Returns 200:
   {
     "status": "ok",
     "broker_id": 1,
     "partitions_led": 4,
     "partitions_assigned": 4,
     "leases_held": 4
   }
   Returns 503 if partitions_led < partitions_assigned

4. Grafana dashboard (deploy/grafana/skafka-dashboard.json):
   - Produce/consume throughput (bytes/s and records/s)
   - Storage write/read latency p50/p99
   - Consumer group lag per topic
   - Partition leadership map
   - Auth failure rate
   - Lease acquisition/loss events
```

---

## Critical Constraints for Claude Code

1. **Both locks before any write.**
   Append() must verify IsLeader() AND IsLocked() before writing.
   Write a test that verifies a non-leader returns ErrNotLeader.

2. **Hand-roll the protocol codec — do not import client libraries.**
   Franz-go and Sarama are client libraries with client-side assumptions.
   Use them as reference source only. The codec lives entirely in
   internal/protocol/codec/ and is purpose-built for server-side use.
   Test it with known-good byte sequences from the Kafka protocol spec.

3. **Atomic writes for acls.json and credentials.json.**
   Always write to .tmp file, then os.Rename() to final path.
   Rename is atomic on POSIX/CephFS. This prevents partial reads.

4. **Leader epoch on every segment.**
   On takeover, new leader must truncate partial records from previous
   leader before accepting writes. CRC-check last record boundary.

5. **Test with real Kafka clients.**
   Integration tests must use franz-go or kafka-go, not internal types.

6. **Document NFS limitations prominently.**
   README, Helm chart NOTES.txt, and operator logs must warn that NFS
   provides advisory-only write exclusivity — not suitable for production.

7. **Graceful shutdown sequence on SIGTERM:**
   1. Stop accepting new connections
   2. Drain in-flight requests (with timeout)
   3. Flush + fsync all active segments
   4. Release filesystem locks
   5. Release Kubernetes Leases explicitly (don't wait for TTL)
   6. Update ReadinessGate to False
   7. Exit 0

8. **Never cache Metadata beyond one Lease renewal interval.**
   Maintain a live Lease watch. Stale leader info causes client errors.

9. **Use the Kafka protocol's native NOT_LEADER retry — do not hide it.**
   Kafka clients already handle `NOT_LEADER_FOR_PARTITION` by refreshing
   Metadata and reconnecting to the correct broker. Metadata responses must
   give clients per-broker addresses so this retry can converge. Hiding the
   error behind a custom router or broker-to-broker forwarding is redundant
   work that the protocol already solves.

10. **Every broker's advertised hostname is known at install time.**
    Each broker's external hostname comes from a Helm-rendered pattern
    substituted with the pod ordinal. No runtime env-var patching, no rolling
    restart after LoadBalancer IP assignment, no operator waiting on
    `.status.loadBalancer.ingress`. Hostnames are static; DNS, certificates,
    and TLSRoutes are created alongside the StatefulSet.

11. **TLS terminates at the broker pod, never at an intermediate proxy.**
    The Gateway / LoadBalancer is L4 passthrough with SNI-based routing only.
    No proxy holds the private key. Certificate rotation flows through
    cert-manager directly to a Secret mounted on every broker; the broker
    hot-reloads via fsnotify without a pod restart.

---

## Testing Strategy

```
Unit tests (go test ./...):
  - Protocol codec: encode/decode round-trips for all Priority 1 API keys
  - Codec primitives: varint, uvarint, compact string, array, tagged fields
  - CRC32C: validate against known-good byte sequences from Kafka spec
  - ApiVersions: verify response matches implemented version ranges
  - RecordBatch: encode → decode round-trip, CRC validation
  - Storage: write → read, segment rolling, index seek, retention cleanup
  - Two-lock model: Append() rejects non-leader; Append() rejects when
    filesystem lock not held
  - ACL engine: allow/deny, wildcard matching, deny-over-allow precedence
  - SCRAM: full RFC 5802 exchange with standard test vectors
  - Leader epoch: partial write detection and truncation on takeover
  - KafkaUser controller: SCRAM hash computation, Secret creation
  - KafkaAcl controller: acls.json merge, atomic write
  - Per-broker external hostname: Metadata on external listener advertises
    broker-N.kafka.example.com; on internal listener advertises headless DNS
  - Broker cert hot-reload: fsnotify detects Secret update, TLS picks new cert
    within 5s without pod restart
  - NOT_LEADER handled by client library: no custom forwarding or retry logic
    in broker or router

Integration tests (kind + Rook-Ceph in CI):
  - Single broker: produce 10,000 records, consume all, verify order
  - Three brokers: produce to partition leaders, consume all
  - Leader failover: SIGKILL broker-1, verify new leader within
    leaseDurationSeconds, verify no data loss, consumer continues
  - TLS passthrough + SNI: openssl s_client with -servername picks the
    correct broker's cert (Gateway does not terminate TLS)
  - External Metadata response: per-broker hostnames (broker-0.kafka...,
    broker-1.kafka..., broker-2.kafka...), never internal DNS
  - NOT_LEADER redirect: produce via broker-0 hostname for a partition led
    by broker-2; Kafka client automatically retries against
    broker-2.kafka.example.com and succeeds
  - Consumer group: 3 consumers, 9 partitions, verify even distribution
  - Consumer group rebalance: add consumer mid-stream
  - ACL: user without Write ACL gets TOPIC_AUTHORIZATION_FAILED
  - KafkaTopic CRD: create, verify partition dirs on PVC
  - KafkaUser (SCRAM): create user, produce with credentials
  - KafkaUser (k8s SA): produce with SA JWT, verify auth succeeds
  - Large message: single 10MB record, produce + consume
  - Retention: produce beyond retentionMs, verify old segments deleted
  - PodDisruptionBudget: verify kubectl drain respects maxUnavailable
  - Scale brokers 3→5: verify client sees no disruption, partitions
    rebalance to new brokers automatically via Lease redistribution
  - Certificate rotation: rotate cert-manager-issued Secret; verify new TLS
    connections use the rotated cert within 5s, no pod restart required

Kafka compatibility tests (franz-go and kafka-go used as TEST CLIENTS only
— imported in tests/, never in internal/):
  - franz-go client: produce + consume, verify correctness
  - segmentio/kafka-go client: produce + consume, verify correctness
  - kcat (kafkacat): produce and consume via CLI
  - kafka-verifiable-producer + kafka-verifiable-consumer
  - kafka-consumer-groups.sh --describe
  - kafka-topics.sh --create / --list / --describe / --delete
```

---

## MVP Definition (What "Done" Looks Like)

- [ ] kafka-console-producer produces to skafka using a SINGLE bootstrap address
- [ ] kafka-console-consumer consumes all messages in order from same address
- [ ] Metadata response (external listener) carries per-broker hostnames
      `broker-0.kafka.example.com`, etc. — no internal DNS leaks
- [ ] TLS passthrough + SNI verified end-to-end: `openssl s_client` with
      `-servername` returns the correct broker's cert, no intermediate proxy
      in the chain
- [ ] 3-broker cluster survives kubectl delete pod broker-1 with no data loss
- [ ] Kafka client library handles `NOT_LEADER_FOR_PARTITION` transparently
      using per-broker hostnames — no custom forwarding in the data path
- [ ] Consumer group rebalances correctly when a consumer is killed
- [ ] KafkaCluster CRD deploys StatefulSet, LoadBalancer Service, TLSRoute,
      cert-manager Certificate, PVC
- [ ] KafkaTopic CRD creates topic and partition directories on PVC
- [ ] KafkaUser (SCRAM) CRD: authenticated client can produce/consume
- [ ] KafkaUser (k8s SA) CRD: pod with SA can produce without credentials
- [ ] KafkaAcl CRD denies unauthorized access with correct error code
- [ ] KafkaUserGroup applies shared ACLs to all members
- [ ] cert-manager-issued Certificate rotates without pod restart
      (broker reloads via fsnotify within 5s)
- [ ] Helm chart deploys on Rook-Ceph cluster in one command
- [ ] Prometheus metrics endpoint returns data for brokers including
      TLS handshake counts and per-broker cert rotations
- [ ] Grafana dashboard shows throughput, consumer lag, and TLS handshake
      latency p99
- [ ] README documents architecture, NFS limitations, CephFS requirement,
      and explains the per-broker-hostname addressing scheme (DNS + cert
      patterns)

---

## Open Questions to Resolve in Phase 1

1. **CephFS fsync cross-node visibility.**
   Does CephFS fsync() guarantee another node sees the data immediately?
   Verify with a Rook-Ceph test: write on node A, fsync, read on node B.
   This must pass before writing the storage engine.

2. **Lease count at scale.**
   1,000 topics x 12 partitions = 12,000 Lease objects. Test with a
   large topic count early. If problematic: batch into one Lease per
   broker containing the partition list in the spec.

3. **Partial write recovery.**
   Broker crashes mid-record-batch → partial record in segment. Design:
   last N bytes of each record contain a CRC32. On takeover, scan
   backward from end of segment, find last complete record, truncate.

4. **__consumer_offsets compaction.**
   Full log compaction is expensive at scale. Consider an in-memory
   offset store with periodic snapshot to PVC instead.

5. **Operator PVC access.**
   Operator needs to write acls.json and credentials.json to shared PVC.
   Options:
   (a) Mount same PVC in operator pod — simplest, couples operator to storage
   (b) Write via a Kubernetes Job that mounts the PVC — cleaner separation
   (c) Store in ConfigMap, brokers read from there — cleanest but 1MB limit
   Recommend (a) for MVP, document the coupling, revisit for v1.0.

6. **Minimum API version range to support.**
   The codec implements specific version ranges per API key. Too narrow
   and common clients won't connect. Too wide and the implementation
   surface grows. Recommended starting point: target compatibility with
   Kafka 2.6+ clients (released 2020), which covers the vast majority
   of production deployments. Verify by running franz-go and kafka-go
   with their default settings — both will negotiate the highest mutually
   supported version automatically via ApiVersions.

7. **DNS strategy for per-broker hostnames.**
   Two supported patterns:
   (a) Wildcard DNS: `*.kafka.example.com → <Gateway LB IP>`. One record,
       zero changes when brokers scale up. Simplest to operate; most
       organisations already manage wildcard zones.
   (b) Explicit records: one A/AAAA per broker. More verbose; suitable for
       environments that disallow wildcards for compliance or security
       reasons (e.g. strict DNSSEC posture).
   Both work with the same TLSRoute + Certificate setup. Recommend (a) for
   operational simplicity; document (b) as an explicit fallback.

8. **Certificate strategy for per-broker hostnames.**
   Two supported patterns:
   (a) Wildcard cert: `*.kafka.example.com`. Issued once; unaffected by
       broker scaling. Matches the wildcard DNS pattern.
   (b) SAN-per-broker cert: `broker-0.kafka.example.com`,
       `broker-1.kafka.example.com`, ..., plus the optional bootstrap
       hostname. More explicit; requires reissue when broker count changes.
   Let's Encrypt and most public CAs support both. If scaling the broker
   StatefulSet is frequent, prefer (a); if it is rare, (b) leaves a smaller
   attack surface if the private key is ever compromised.

---

## v2 Roadmap: Kafka Streams Compatibility

v2 targets full Kafka Streams support. This roughly doubles the implementation
scope — estimated 18–24 weeks total from project start, or 10–14 weeks of
incremental work on top of the completed v1.

Nothing in the v1 architecture blocks v2. The codec is designed to add API
keys incrementally. The storage engine needs compaction added but not
redesigned. The two-lock safety model extends naturally to transaction fencing.

### What Kafka Streams Requires From the Broker

Kafka Streams relies on four capabilities not present in v1:

1. **Transactions and exactly-once semantics (EOS)**
   Streams writes to output topics and commits consumer offsets atomically.
   A crash mid-transaction must leave the log in a state that is correctly
   identified as uncommitted and fenced from read_committed consumers.

2. **Log compaction**
   Changelog topics backing KTables and aggregations use cleanup.policy=compact.
   The broker must periodically remove superseded records, keeping only the
   latest value per key.

3. **Producer fencing**
   Zombie producers (old instances still running after a restart) must be
   fenced via producer epochs so their writes are rejected.

4. **Transaction isolation in Fetch**
   Consumers using read_committed isolation must not see records from
   open or aborted transactions. The Fetch response must respect the
   Last Stable Offset (LSO) rather than the High Watermark.

---

### v2 Phase 11: Transaction Coordinator (Week 11–14)

**Goal:** Implement the full Kafka transaction protocol, including producer
epoch management, fencing, two-phase commit, and transaction markers.

```
1. New API keys to implement in codec (internal/protocol/codec/api/):
   InitProducerId        (22) — assign producer ID + epoch
   AddPartitionsToTxn    (24) — register partitions in a transaction
   AddOffsetsToTxn       (25) — include consumer group offsets in txn
   EndTxn                (26) — commit or abort a transaction
   WriteTxnMarkers       (27) — write COMMIT/ABORT markers to logs
   TxnOffsetCommit       (28) — atomically commit offsets within a txn

2. Transaction coordinator state (per producer ID):

   ProducerState:
     producerId          int64     ← globally unique, assigned by coordinator
     producerEpoch       int16     ← incremented on each InitProducerId
     state               enum      ← Empty | Ongoing | PrepareCommit |
                                      PrepareAbort | CompleteCommit |
                                      CompleteAbort | Dead
     transactionTimeout  int32
     partitions          set       ← partitions enrolled in current txn
     startTimestamp      int64

   Stored in: /data/__cluster/transactions/{producer_id}.json
   Atomic writes (tmp + rename) same as acls.json pattern.

3. Producer ID assignment:
   - Maintain a monotonic counter in /data/__cluster/producer_id_seq
   - Each InitProducerId call: read counter, increment, write back atomically
   - Use CAS (compare-and-swap via file locking) to avoid duplicate IDs
   - Producer epoch starts at 0, increments each InitProducerId for same ID

4. Zombie fencing:
   - On InitProducerId: increment epoch for that producer ID
   - On any produce request: verify epoch in RecordBatch matches current epoch
   - If epoch is stale: return INVALID_PRODUCER_EPOCH error
   - This prevents old producer instances from writing after restart

5. Two-phase commit protocol:

   Phase 1 — Prepare:
   - EndTxn(commit=true) received
   - Write PREPARE_COMMIT marker to transaction log
   - Update producer state to PrepareCommit
   - Send WriteTxnMarkers to all enrolled partitions

   Phase 2 — Complete:
   - All partitions acknowledge COMMIT marker written
   - Write COMPLETE_COMMIT to transaction log
   - Update producer state to CompleteCommit / Empty
   - Advance Last Stable Offset on each enrolled partition

   Abort path mirrors commit path with ABORT markers.

6. Transaction log:
   - Special topic: __transaction_state
   - 50 partitions (configurable), stored on shared PVC
   - Compacted (keep only latest state per producer ID)
   - Each broker that is transaction coordinator for a producer ID
     leads the relevant __transaction_state partition

7. Transaction coordinator election:
   - Same mechanism as consumer group coordinator: Kubernetes Lease
   - Lease name: "txn-coordinator-{producer_id % num_partitions}"
   - Lease holder IS the transaction coordinator for that producer ID

8. Coordinator for Kubernetes Lease:
   Use existing lease/manager.go infrastructure — no new coordination
   mechanism needed. Transactions piggyback on the same Lease pattern
   already used for partition leadership and group coordination.

9. Transaction timeout handling:
   - Background goroutine checks ongoing transactions
   - If transaction exceeds transactionTimeout: abort automatically
   - Write ABORT markers, update producer state
   - Default timeout: 60s (configurable)
```

---

### v2 Phase 12: Transaction-Aware Fetch (Week 14–15)

**Goal:** Fetch responses respect transaction isolation level. Consumers
using read_committed do not see records from open or aborted transactions.

```
1. Last Stable Offset (LSO):
   LSO = offset of the first message of the oldest open transaction
         on this partition (or high watermark if no open transactions)

   Consumers with isolation_level=1 (read_committed) may only read
   up to LSO, not up to high watermark.

   Track per-partition: set of (producer_id, first_offset) for open txns.
   Update on:
     AddPartitionsToTxn → add entry
     EndTxn + markers written → remove entry, advance LSO

2. ABORT markers in Fetch response:
   Aborted transaction batches must still appear in the Fetch response
   as AbortedTransaction entries in the response header, so consumers
   can skip them. Do not silently drop aborted batches — clients need
   the offset range to know what to skip.

3. Fetch API version update:
   isolation_level field was added in Fetch v4.
   Ensure codec handles this field and passes it to the storage engine
   read path. Non-transactional consumers (isolation_level=0) continue
   reading up to high watermark as before.

4. RecordBatch attribute flags:
   Bit 4 of attributes: isTransactional
   Bit 5 of attributes: isControlBatch (COMMIT/ABORT markers)
   Storage engine must preserve these flags exactly as written.
   Read path must filter based on isolation level and control batch type.
```

---

### v2 Phase 13: Log Compaction (Week 15–18)

**Goal:** Support cleanup.policy=compact topics, required for Kafka Streams
changelog topics that back KTables and aggregations.

```
1. Compaction trigger:
   - Partition leader runs compaction (non-leaders skip — same as retention)
   - Trigger conditions (configurable per topic):
     * min.cleanable.dirty.ratio exceeded (default: 0.5)
     * min.compaction.lag.ms exceeded (default: 0)
   - Background goroutine, runs every compactionCheckIntervalMs (default: 15s)

2. Compaction algorithm:
   a. Build key→latest_offset map by scanning all non-active segments
   b. For each segment (oldest first):
      - Read each record
      - If key exists in map with a HIGHER offset: this record is superseded
        → write null/skip to compacted output segment
      - If key's latest offset == this record's offset: keep it
      - If record has null value (tombstone): keep if within retention,
        drop if older than delete.retention.ms
   c. Replace original segments with compacted segments (atomic rename)
   d. Rebuild index files for compacted segments

3. Shared storage compaction safety:
   Only the partition leader (Kubernetes Lease holder) runs compaction.
   Compaction writes to temporary files (.compact suffix), then atomically
   renames into place. If the broker loses the Lease mid-compaction:
   - New leader sees .compact temp files on takeover
   - New leader deletes incomplete .compact files and restarts compaction
   - The original segments are untouched until atomic rename completes
   This ensures compaction is always crash-safe.

4. Tombstone handling:
   Records with null value are tombstones (key deletions).
   Tombstones must be retained for at least delete.retention.ms before
   being dropped, so downstream consumers can process the deletion event.
   Track tombstone timestamps in a separate index file per segment.

5. KafkaTopic CRD update for compaction:
   spec:
     config:
       cleanupPolicy: compact           # or delete, or compact,delete
       minCompactionLagMs: 0
       deleteRetentionMs: 86400000      # 24h tombstone retention
       minCleanableDirtyRatio: "0.5"

6. __consumer_offsets compaction:
   This topic has been using an in-memory cache in v1 (open question #4).
   In v2, implement proper compaction for __consumer_offsets and
   __transaction_state. Both are internally compacted topics.
   Replace the in-memory snapshot approach with true log compaction.
```

---

### v2 Phase 14: Remaining API Keys for Streams (Week 18–19)

**Goal:** Implement the API keys Kafka Streams uses for administration,
monitoring, and internal topic management.

```
New API keys to add to codec:

   DescribeConfigs    (32) — Streams reads topic configs at startup
   AlterConfigs       (33) — Streams may update topic retention settings
   CreatePartitions   (37) — Streams creates repartition topics with
                             specific partition counts
   DescribeLogDirs    (35) — used by monitoring and Streams internals
   DeleteGroups       (42) — cleanup of Streams application consumer groups

Internal topic auto-creation:
   Streams creates internal topics automatically:
   - {app_id}-KSTREAM-AGGREGATE-STATE-STORE-0000000001-repartition
   - {app_id}-KSTREAM-AGGREGATE-STATE-STORE-0000000001-changelog
   These must be creatable via the existing CreateTopics API with
   cleanup.policy=compact for changelog topics.

   The KafkaTopic CRD operator should NOT interfere with Streams-managed
   internal topics. Distinguish by prefix convention or annotation.
   Streams topics created via the protocol (not CRD) should be marked
   with an internal=true flag in the partition metadata.
```

---

### v2 Phase 15: Streams Integration Testing (Week 19–21)

**Goal:** Validate that real Kafka Streams applications work end-to-end
against skafka without modification.

```
Test applications to run against skafka v2:

1. Word count (stateless + stateful):
   - Kafka Streams word count example
   - Verifies: KTable materialization, changelog topics, compaction,
     repartitioning, consumer group coordination

2. Exactly-once word count:
   - Same application with processing.guarantee=exactly_once_v2
   - Verifies: transaction protocol, producer fencing, LSO-aware fetch,
     atomic offset commit

3. Join application:
   - Stream-table join
   - Verifies: multiple internal topics, multiple consumer groups,
     state store restoration from changelog

4. Failure recovery:
   - Kill a Streams instance mid-processing
   - Verify: no duplicate output records (EOS), state store restored
     correctly from changelog, new instance picks up from committed offset

5. Zombie fencing:
   - Start two instances of the same Streams app
   - Kill one, let the other continue
   - Verify: old instance's in-flight records rejected with
     INVALID_PRODUCER_EPOCH

Test infrastructure:
   - Streams applications run as Kubernetes Jobs alongside skafka
   - Input data generated by a producer Job
   - Output verified by a consumer Job
   - All managed by a Kuttl (Kubernetes test tool) test suite
```

---

### v2 API Key Coverage Summary

| Category | v1 API Keys | v2 Additional Keys | Total |
|---|---|---|---|
| Produce/Consume | Produce, Fetch, ListOffsets | — | 3 |
| Metadata | Metadata, ApiVersions | DescribeLogDirs | 3 |
| Consumer Groups | FindCoordinator, JoinGroup, SyncGroup, Heartbeat, LeaveGroup, OffsetCommit, OffsetFetch | DeleteGroups | 8 |
| Admin | CreateTopics, DeleteTopics, DescribeGroups, ListGroups, DescribeAcls, CreateAcls, DeleteAcls | DescribeConfigs, AlterConfigs, CreatePartitions | 10 |
| Auth | SaslHandshake, SaslAuthenticate | — | 2 |
| Transactions | — | InitProducerId, AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, WriteTxnMarkers, TxnOffsetCommit | 6 |
| **Total** | **22** | **9** | **31** |

Note: Full Kafka protocol has 84 API keys. 31 covers all common workloads
including Kafka Streams. The remaining 53 are legacy, rarely-used, or
broker-internal replication APIs that skafka does not need (it has no
inter-broker replication protocol by design).

---

### v2 Storage Engine Changes Summary

Changes required to internal/storage/ for v2 — all additive, no rewrites:

```
engine.go:
  + AppendTransactional() — validates producer epoch before write
  + LastStableOffset() — tracks open transactions per partition
  + AbortedTransactions() — returns aborted txn ranges for Fetch response

segment.go:
  + preserves isTransactional and isControlBatch RecordBatch flags
  + reads control batches (COMMIT/ABORT markers) separately from data

cleaner.go (major addition):
  + compaction logic (currently only has retention/delete)
  + tombstone tracking and expiry
  + compaction safety on shared filesystem (tmp file + atomic rename)
  + compaction ratio calculation and trigger logic

new file: transaction_index.go
  + per-partition index of open transaction offsets
  + used to calculate LSO
  + persisted to .txnindex file alongside .log and .index files
```

---

### v2 Definition of Done

- [ ] Kafka Streams word count example runs successfully end-to-end
- [ ] Word count with exactly_once_v2 produces correct results
- [ ] Zombie fencing: stale producer writes rejected with INVALID_PRODUCER_EPOCH
- [ ] Compacted topics retain only latest value per key
- [ ] Tombstones respected for delete.retention.ms then dropped
- [ ] __consumer_offsets and __transaction_state use true compaction (not snapshot)
- [ ] Streams internal topics created automatically without CRD intervention
- [ ] Broker crash mid-transaction: new leader correctly identifies and fences
- [ ] read_committed fetch does not return records from open transactions
- [ ] All v1 MVP checklist items still pass (no regressions)

---

## Local Development Setup (for Claude Code)

**Goal:** Get a working skafka cluster running locally in under 10 minutes,
without needing a full CephFS cluster. Used for all development and unit
testing.

```
1. Prerequisites:
   - Docker Desktop or Colima (for kind)
   - kind v0.23+
   - kubectl, helm, go 1.22+

2. Local cluster with simulated shared storage:

   # Create kind cluster
   kind create cluster --config=hack/kind-config.yaml

   # kind-config.yaml mounts a host directory into all nodes,
   # simulating shared storage without CephFS:
   kind: Cluster
   apiVersion: kind.x-k8s.io/v1alpha4
   nodes:
   - role: control-plane
   - role: worker
     extraMounts:
     - hostPath: /tmp/skafka-data
       containerPath: /mnt/skafka-data
   - role: worker
     extraMounts:
     - hostPath: /tmp/skafka-data
       containerPath: /mnt/skafka-data
   - role: worker
     extraMounts:
     - hostPath: /tmp/skafka-data
       containerPath: /mnt/skafka-data

   # All three worker nodes mount the SAME host path.
   # This simulates a shared ReadWriteMany filesystem for local dev.
   # NOTE: host filesystem flock() semantics apply — not CephFS.
   # This is sufficient for protocol and logic testing but does NOT
   # test CephFS-specific flock behaviour.

3. Deploy skafka locally:
   make build-images          # builds broker + operator images
   kind load docker-image ... # load into kind
   make deploy-local          # applies CRDs, RBAC, KafkaCluster CR

   # deploy-local uses a local-dev overlay that:
   # - Uses hostPath PV instead of CephFS StorageClass
   # - Sets replicaCount: 1 for faster iteration
   # - Disables external access (internal only)
   # - Sets log level DEBUG

4. Makefile targets for Claude Code to use:

   make test-unit              # go test ./... (no cluster needed)
   make test-integration       # requires kind cluster
   make test-compat            # kafka client compatibility tests
   make lint                   # golangci-lint
   make generate               # controller-gen CRD manifests + deepcopy
   make build                  # compile broker + operator binaries
   make build-images           # docker buildx build
   make deploy-local           # deploy to kind
   make destroy-local          # tear down kind cluster
   make port-forward           # kubectl port-forward broker-0 9092:9092

5. Hot reload for broker development:
   # Build and reload broker binary without rebuilding image:
   make build && kubectl cp ./bin/skafka kafka/skafka-0:/skafka \
     && kubectl exec kafka/skafka-0 -- kill -SIGTERM 1
   # Pod restarts with new binary from shared filesystem
   # Useful for fast iteration on protocol handlers

6. Running a single integration test:
   go test ./tests/integration/... -run TestLeaderFailover -v \
     -kubeconfig ~/.kube/config -namespace kafka

7. Local Kafka client testing (no cluster needed for protocol unit tests):
   # Start broker in standalone mode (no Kubernetes, no CephFS):
   ./bin/skafka --standalone --data-dir /tmp/skafka-standalone
   # Broker runs with in-memory lock and local filesystem
   # Useful for rapid protocol iteration

   # Then test with kcat:
   kcat -b localhost:9092 -t test -P <<< "hello world"
   kcat -b localhost:9092 -t test -C -e
```

---

## Migration Guide: Strimzi → skafka

**Who this is for:** Teams currently running Strimzi who want to evaluate or
migrate to skafka. This is a full data migration — plan for a maintenance
window or use a parallel-run approach.

```
Prerequisites:
- skafka deployed and healthy (all brokers in Ready state)
- MirrorMaker 2 or kcat available for data migration
- Both clusters accessible from the same network

Migration strategy: parallel run with MirrorMaker 2

This is the lowest-risk approach. Strimzi and skafka run simultaneously.
MirrorMaker 2 replicates topics from Strimzi to skafka. You switch producers
first, then consumers, then decommission Strimzi.

Step 1 — Deploy skafka alongside Strimzi:
  - Use a different namespace (e.g., kafka-new)
  - Create matching KafkaTopic CRDs for each Strimzi topic
  - Verify skafka is healthy before proceeding

Step 2 — Start MirrorMaker 2 replication:
  Deploy MirrorMaker 2 pointed at Strimzi (source) → skafka (target).
  MirrorMaker 2 replicates all topics including __consumer_offsets,
  which carries committed consumer offsets across.

  Key MirrorMaker 2 config:
    source.cluster.alias: strimzi
    target.cluster.alias: skafka
    topics: .*                         # replicate all topics
    groups: .*                         # replicate all consumer groups
    sync.topic.configs.enabled: true   # sync topic retention settings
    replication.factor: 1              # RF=1 on skafka (shared storage)

Step 3 — Verify replication lag:
  Monitor consumer lag on the MirrorMaker 2 consumer group.
  Wait until lag is consistently near zero before proceeding.
  Use: kafka-consumer-groups.sh --bootstrap-server skafka:9092 --describe

Step 4 — Recreate users and ACLs:
  For each Strimzi KafkaUser, create a matching skafka KafkaUser CRD.
  For each Strimzi KafkaUser ACL, create a matching skafka KafkaAcl CRD.
  Credentials will differ (different secrets) — update application config.

Step 5 — Switch producers:
  Update producer bootstrap.servers to point to skafka.
  Roll producers one deployment at a time.
  Monitor skafka produce metrics to verify traffic is flowing.
  Keep MirrorMaker 2 running to catch any in-flight messages.

Step 6 — Switch consumers:
  Update consumer bootstrap.servers to point to skafka.
  Because __consumer_offsets was mirrored, consumers resume from
  their last committed offset on skafka — no data is reprocessed.
  Roll consumers one deployment at a time.

Step 7 — Decommission MirrorMaker 2 and Strimzi:
  Once all producers and consumers are on skafka and stable for
  at least 24 hours, stop MirrorMaker 2.
  Delete the Strimzi Kafka CR and clean up its PVCs.

Notable differences to communicate to application teams:
  - bootstrap.servers changes (new hostname/port)
  - Credentials change (new Secrets from KafkaUser CRD)
  - replication.factor must be 1 (RF>1 is silently accepted but wastes space)
  - kafka.consumer.group.id prefix: MirrorMaker 2 adds cluster alias prefix
    to mirrored group IDs. Adjust consumer group names if needed.
  - TLS: if switching from TLSRoute to TCPRoute, clients drop TLS config

Topic naming with MirrorMaker 2:
  By default MirrorMaker 2 renames topics: source.topic → strimzi.topic
  To avoid renaming, set: replication.policy.class=IdentityReplicationPolicy
  This keeps topic names identical on both clusters.
```

---

## Security Hardening

**Goal:** Production-ready security posture. All items below should be
completed before deploying skafka in a production environment.

```
1. Kubernetes NetworkPolicy:

   Deny all ingress/egress by default, then allow explicitly:

   # Allow producers/consumers → brokers on 9092
   apiVersion: networking.k8s.io/v1
   kind: NetworkPolicy
   metadata:
     name: skafka-broker-ingress
     namespace: kafka
   spec:
     podSelector:
       matchLabels:
         app: skafka
     policyTypes: [Ingress]
     ingress:
     - ports:
       - port: 9092   # Kafka protocol (internal)
       - port: 9093   # Kafka protocol (TLS, if enabled)
       - port: 8080   # healthz
       - port: 9090   # metrics

   # Allow broker → Kubernetes API server (for Lease operations)
   # Allow broker → CephFS MDS (metadata server)
   # Allow operator → Kubernetes API server
   # Deny all other ingress/egress

   Implement as Helm templates, off by default, enabled with:
     networkPolicy.enabled: true

2. Pod Security Standards:
   Apply restricted PSS to the kafka namespace:
     labels:
       pod-security.kubernetes.io/enforce: restricted
       pod-security.kubernetes.io/audit: restricted

   Broker pod must comply:
   - runAsNonRoot: true
   - runAsUser: 1000
   - readOnlyRootFilesystem: true (except /data mount and /tmp)
   - allowPrivilegeEscalation: false
   - drop ALL capabilities
   - seccompProfile: RuntimeDefault

3. Secret rotation:
   KafkaUser SCRAM credentials: operator regenerates SCRAM hashes when
   the referenced Secret changes. Applications rotate by updating the
   Secret — no broker restart needed (credentials.json reloaded via inotify).

   TLS certificates via cert-manager: automatic rotation before expiry.
   Broker reloads TLS config on cert renewal — no restart needed.

4. Audit logging:
   All authenticated Kafka API requests logged at INFO with:
   - principal, client_id, api_key, topic/group, source IP, result
   - Denied ACL checks logged at WARN
   Forward audit logs to your SIEM via standard log aggregation.

5. Encryption at rest:
   CephFS supports encryption at rest via dm-crypt / LUKS on OSDs.
   Enable in Rook-Ceph configuration — transparent to skafka.
   For NFS: use encrypted NFS (NFSv4 with Kerberos) or encrypt at the
   storage layer.

6. mTLS for internal broker communication:
   In v1, internal broker-to-broker communication uses plaintext
   (there is no replication traffic, only Lease-watch API calls).
   For organizations requiring encryption in transit everywhere, enable
   TLS on the internal listener and use cert-manager to issue internal
   certificates signed by a cluster CA.

7. RBAC least privilege — verify with audit:
   Run kubectl auth can-i --list --as=system:serviceaccount:kafka:skafka
   Verify broker SA cannot: create/delete Secrets outside kafka namespace,
   read other namespaces' resources, access node-level APIs.

8. Image supply chain:
   - Pin all image digests in Helm chart (not just tags)
   - Sign images with cosign in CI pipeline
   - Scan images with Trivy in CI — block on CRITICAL CVEs
   - Use distroless base image (already in Dockerfile)
   - Enable Kubernetes image pull policy: Always in production
```

---

## Disaster Recovery and Backup

**Goal:** Define recovery procedures for the most likely failure scenarios.
CephFS provides storage-layer redundancy, but operational mistakes and
catastrophic failures still need a recovery path.

```
Failure scenarios and recovery procedures:

1. Single broker pod failure (most common):
   Recovery: automatic. Kubernetes reschedules the pod. New pod acquires
   Leases via the lease/manager.go re-election loop. Clients experience
   failover latency equal to leaseDurationSeconds.
   No operator action required. Monitor skafka_lease_acquisition_total.

2. All broker pods down simultaneously (e.g., node drain, rolling update):
   Recovery: automatic on pod restart. Because all data is on the shared
   PVC, brokers restart clean with no catch-up needed.
   Rolling restart: use kubectl rollout restart statefulset/skafka.
   PodDisruptionBudget prevents >1 pod going down during voluntary disruption.

3. Single CephFS OSD failure:
   Recovery: handled by Ceph automatically (replication factor ≥ 2 on OSDs).
   Ceph backfills the failed OSD's data to surviving OSDs.
   skafka is unaffected during this process — reads/writes continue.
   Monitor Ceph health: ceph status.

4. CephFS MDS (metadata server) failure:
   Recovery: Ceph automatically promotes standby MDS.
   skafka may experience a brief pause (~5-30s) while MDS failover completes.
   Produce requests time out and are retried by clients. No data loss.

5. Complete CephFS cluster failure (catastrophic):
   This is the most serious scenario — all skafka data is inaccessible.
   Recovery depends on backup strategy (see below).
   Until CephFS recovers, skafka is fully unavailable.

6. Accidental topic deletion:
   The KafkaTopic controller creates a deletion Job when a KafkaTopic CRD
   is deleted. To prevent accidental deletion, add a Kubernetes finalizer
   to the KafkaTopic CRD — deletion requires explicitly removing the
   finalizer first.
   Enable with: kafkaTopic.deletionProtection: true in Helm values.

Backup strategy:

Option A — CephFS snapshots (recommended):
   Rook-Ceph supports VolumeSnapshots of CephFS volumes.
   Schedule snapshots via a VolumeSnapshotSchedule (Rook-Ceph feature)
   or a Kubernetes CronJob that creates VolumeSnapshot objects.

   Snapshot frequency recommendation:
   - Every 6 hours for active clusters
   - Retain 7 daily snapshots, 4 weekly snapshots

   Restore procedure:
   a. Scale skafka StatefulSet to 0 replicas
   b. Delete PVC skafka-shared-data
   c. Create new PVC from snapshot:
      kubectl apply -f pvc-from-snapshot.yaml
   d. Scale StatefulSet back to 3 replicas
   e. Verify: produce and consume a test message

   Recovery time: minutes (snapshot restore is near-instant on CephFS)
   Recovery point: last snapshot (up to 6 hours of data loss)

Option B — MirrorMaker 2 to a second cluster:
   Run a second skafka cluster (or any Kafka-compatible broker) as a
   standby. MirrorMaker 2 replicates all topics continuously.
   On primary failure: redirect producers/consumers to standby.
   Recovery point: near-zero (MirrorMaker 2 lag, typically seconds)
   Recovery time: minutes (DNS/config change)
   Cost: 2x infrastructure

Option C — Topic-level backup with kafka-backup tools:
   Use kafka-backup or similar tooling to export topic data to object
   storage (S3/MinIO) on a schedule. Restore individual topics from backup.
   Useful for compliance/audit requirements or accidental deletion recovery.
   Not suitable as primary DR strategy (too slow for full cluster recovery).

Backup testing:
   Restore procedure must be tested quarterly. Add a quarterly reminder
   to the project's issue tracker. An untested backup is not a backup.
```