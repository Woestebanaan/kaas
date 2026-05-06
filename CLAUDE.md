# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Parity target & non-goals

skafka targets **Apache Kafka 3.7** for wire-protocol and Kafka Streams parity. Behaviour is verified against the matrix tracked in the [skafka-migration-parity](https://github.com/users/Woestebanaan/projects/2) GitHub project. When a feature is ambiguous, default to "match Apache Kafka 3.7" rather than inventing skafka-specific semantics.

- **Deferred (intended later, not now):** tiered storage / S3 backend. Skip the tiered-storage-only API surfaces (e.g. `EARLIEST_LOCAL_TIMESTAMP`, `EARLIEST_PENDING_UPLOAD_OFFSET`) — clients only request them when configured for remote tiers.
- **Non-goals (off the table by design):** KRaft / metadata quorum (skafka uses K8s Leases instead), and replication / ISR (single-writer-per-partition). Don't add either; flag if a "parity" task implicitly requires them.

## Common commands

```bash
go build ./...              # build all binaries
go test ./...               # run all unit tests
go test ./internal/storage  # run one package
go test -run TestX ./...    # run one test by name
go vet ./...
make manifests              # regenerate CRD YAMLs from operator/api/* AND mirror into deploy/helm/skafka/crds/
make proto                  # regenerate gRPC stubs from proto/heartbeat.proto (needs `buf`; install via `make proto-tools`)
```

`go.mod` targets Go 1.26.1. `golangci-lint` is currently disabled in CI (latest release is built with Go 1.24 and refuses 1.26 modules) — `make lint` works locally if you have a matching toolchain, but don't be surprised when it fails to load.

CI (`.github/workflows/ci.yml`) runs `go vet`, `go test`, builds both Docker images, lints/templates the Helm chart, and **fails on CRD drift** — if you edit anything under `operator/api/`, run `make manifests` and commit both `deploy/crds/` and `deploy/helm/skafka/crds/`. The CI step is inlined (the runner has no `make`), but the effect is the same.

Releases are tag-driven; see `RELEASING.md`. **Always bump the patch (`v0.1.N-preview` → `v0.1.N+1-preview`), never re-cut a tag.**

`scripts/kafka-*.sh` is a per-tool integration suite that runs the Apache Kafka shell tools (`kafka-topics`, `kafka-{producer,consumer}-perf-test`, `kafka-acls`, etc.) against a live broker — *not* invoked by `go test`. Each script sources `scripts/_common.sh` for shared `BOOTSTRAP` / `KAFKA_BIN` / `skip` helpers; defaults target the in-cluster Service DNS and `/opt/kafka/bin`. Scripts that target features that are non-goals or post-3.7 (KRaft tools, share-groups, etc.) print a one-line reason and `exit 77` (the autoconf "skipped" code) so they're discoverable without pretending to test something that can't work.

## Architecture

skafka is a from-scratch Kafka-protocol-compatible broker that runs on Kubernetes. Two binaries ship in this repo:

- **`cmd/skafka`** — the broker (port 9092 plaintext, 9093 TLS, 8080 health, 9094 inter-broker heartbeat gRPC).
- **`cmd/skafka-operator`** — a controller-runtime operator that reconciles 6 CRDs into on-disk config files (auth/topics) and Kubernetes plumbing (TLS routes, etc.).

There are also two helper binaries for tests/diagnostics: `cmd/skafka-failover-probe` and `cmd/skafka-fsync-check`.

### Brokers are runtime-independent of the operator

This is the most important architectural fact and is easy to misread from the directory layout. The operator is a **startup/admission** component, not a hot-path dependency:

- Operator manages 6 CRDs in `operator/api/v1alpha1/`: `KafkaCluster` (external listener plumbing), `KafkaTopic` (partition dir creation), `KafkaUser` / `KafkaACL` / `KafkaUserGroup` (auth — materialized to files under `/data/__cluster/`), and `KafkaClusterAssignments` (read-only debug mirror, written fire-and-forget by the controller broker; brokers never read it).
- Brokers read `KafkaTopic` CRs at startup (and watch them for new topics / partition expansion), but the read is non-fatal — a missing/unreachable API server only blocks new topic creation, never serving of existing topics.
- The Produce/Fetch hot path makes **zero K8s API calls**. Ownership lookups are in-memory.

**Operator reconcilers do NOT use cleanup finalizers.** Earlier versions had `skafka.io/{topic,user,acl,usergroup,kafkacluster}-cleanup` finalizers that drained on CR delete; ArgoCD's parallel cascade-delete deadlocked when the operator pod went down before its CRs. Cleanup is now reconcile-time (best-effort `os.RemoveAll` / credentials-file edit when `Get` returns `NotFound`) plus a leader-elected startup sweep (`controllers.SweepTopics`, `controllers.SweepCredentials`) that drops orphan dirs / stale credential entries the reconciler missed. Owned external resources (Certificates, Services, TLSRoutes) carry `OwnerReferences` so K8s GC handles cluster-CR deletion.

If you find yourself adding a runtime dependency from broker → operator (e.g. a watch on a CR that blocks request handling), stop — that's an architectural change.

### Controller broker, leases, and the authoritative assignment file

The "controller" is **a broker that holds the `skafka-controller` Lease**, not a separate process. Its responsibilities:

- Observes peer brokers via gRPC heartbeats (`proto/heartbeat.proto`, `pkg/heartbeatpb/`, `internal/controller/heartbeat_server.go`).
- Computes partition + consumer-group assignments (`internal/controller/balancer.go`, `assignment.go`).
- Writes `/data/__cluster/assignment.json` on the shared RWX PVC. The file is epoch-prefixed by `leaseTransitions`; brokers reject writes with stale epochs (this is what `tests/stale-controller-race` verifies).
- Mirrors the assignment to a `KafkaClusterAssignments` CR for `kubectl` debugging only.

`assignment.json` is the **single source of truth for partition leadership** (gh #75 cleanup, v0.1.15+). The Metadata response, the produce/fetch ownership check, and `/healthz`'s `partitions_led` all source from it via `*broker.Coordinator` (`Coordinator.Owns` / `Coordinator.LeaderFor`). There is no per-partition Lease — only the singleton `skafka-controller` Lease used for cluster-wide controller election. The per-partition Lease infrastructure under `internal/lease/` is dev-mode-only (`LocalLeaseManager` always says "yes, I lead") and a vestigial `KubernetesLeaseManager` that nothing acquires from anymore.

The controller recomputes the assignment when:
- It first wins the controller Lease (initial recompute).
- A `KafkaTopic` CR is added / modified / deleted — wired via `clusterRuntime.NotifyTopicChange` from the topic-watcher's onEvent (gh #74).
- A broker joins or leaves the alive set — wired via `watchBrokerSet` polling `BrokerSource.AliveBrokers()` every 2 s (gh #77).

Non-controller brokers watch `assignment.json` via fsnotify + 1s poll (`internal/broker/coordinator.go`, `internal/fsutil/filewatch.go`); the `TakeoverDriver` / `GroupTakeoverDriver` registered on `Coordinator.OnAssignmentChange` opens or relinquishes partitions in the storage engine to match.

Local-dev mode is selected when `MY_POD_NAME` is unset; in that case `dataDir == ""` also flips storage to in-memory (see the branch in `cmd/skafka/main.go`). The v3 cluster runtime is not started in dev mode — produce and Metadata fall back to `LocalLeaseManager` paths that always treat self as leader of every partition.

### Storage hot path & file-handle ownership

The broker's storage engine is heavily optimised for shared-NFS substrates where every NFS COMMIT round-trip is the dominant cost. The current Produce path (post-perf rework, gh #80/#81/#82) issues **one NFS COMMIT per group of concurrent batches**, not per record — `flushLocked` is gone; per-partition committer goroutines drain a `flushReqCh` and run one `logFile.Sync()` per cycle while concurrent Appenders wait on a `sync.Cond`. The index file is **not** fsynced on the hot path (rebuildable on takeover via `rebuildIndex`); the manifest is **not** rewritten per Produce — it's persisted only on partition open / takeover / segment roll / cleaner advance, so its `HighWatermark` can lag in-memory by up to one segment. `recoverSegment` runs at takeover time and reconciles. The `SKAFKA_FLUSH_INTERVAL_MESSAGES` env var (default 1 = honest acks=all) is the durability/throughput dial — mirrors Apache Kafka's `log.flush.interval.messages`.

Segment roll splits the work: the in-memory swap (`rollFast`: log fsync + create new active + pointer swap) runs under `ps.mu`; the deferred finalize (index fsync, close old, manifest write) runs in a goroutine. `DeleteRecords` (API key 21) drives `logStart` advance and reclaims the active segment when the purge covers it (`logStart >= HighWatermark`).

**Only the partition's current leader holds open log/index file descriptors** (gh #76 follow-up, v0.1.39+). `openPartition` at startup statifies segments without `OpenFile`. `TakeoverDriver` (registered on `Coordinator.OnAssignmentChange`) calls `storage.TakeOver` which `openHandles()` before recovery; `Relinquish` calls `closeHandles()`. This makes leader-side `os.Remove` (segment retention, DeleteRecords, segment roll cleanup) actually free disk on NFS rather than silly-renaming the file because peer brokers held it open. The committer goroutine snapshots the `*os.File` pointer (not the segment) so a concurrent Relinquish can't race the Sync into a nil deref.

### KafkaTopic delete on NFS

When a `KafkaTopic` CR is deleted, `metadata.deletionTimestamp` goes non-nil. The topic-watcher fires `TopicDeleted` *immediately* (rather than waiting for the K8s `Deleted` event after finalizers clear); the broker calls `engine.ClosePartition` for each partition to drop its open log + index file handles on the leader broker (followers don't have them open per the previous section). Without this, NFS silly-renames the open files into `.nfsXXXX` entries that EBUSY the operator's `unlinkat` on the parent directory forever (gh #76). The topic-watcher also routes the initial reconcile through `processEvent` so brokers coming up while CRs are already mid-deletion close their handles on startup, not just on watch events.

### Code map

- `internal/protocol/` — Kafka wire protocol. `codec/` (frames, primitives, CRC32C, per-API request/response types under `codec/api/`), `dispatch.go`, `server.go` (TCP listener + TLS), and `handlers/` (one file per API: `produce.go`, `fetch.go`, `metadata.go`, `consumer_group.go`, `list_offsets.go`, `admin.go`, `sasl.go`, `api_versions.go`).
- `internal/storage/` — `DiskStorageEngine` with segment files, manifest, watcher, and cleaner. Single-writer enforcement is `BrokerCoordinator.Owns` + epoch-prefixed segment filenames (the old per-partition flock was removed in Phase 4). See "Storage hot path & file-handle ownership" above for the group-commit / lazy-open / async-roll-finalize semantics — those are easy to miss if you read the engine code in isolation.
- `internal/coordinator/` — Kafka consumer-group coordinator (group state, offset commits). Offsets persisted under `dataDir`. Group ownership comes from a `GroupAssignmentSource`; the runtime variant is wired through the controller assignment, not per-group Leases.
- `internal/broker/` — broker glue: `Broker` struct, the on-broker `Coordinator` (assignment.json watcher), `controller_watch.go` (1s poll of controller Lease for current epoch), `self_fence.go`, `takeover.go`, `group_takeover.go`, `heartbeat_client.go`.
- `internal/controller/` — controller-side logic (election, balancer, assignment writer, heartbeat server, k8s CR mirror).
- `internal/auth/` — SCRAM-SHA-256/512, mTLS principal extraction, ACL evaluation, quotas. Loads from `/data/__cluster/credentials.json` and `acls.json` (written by the operator) with hot-reload via `ClusterFileWatcher`. Toggle off with `SKAFKA_AUTH_DISABLED=true`.
- `internal/k8s/` — broker-side K8s helpers: `BrokerRegistry` (watches the headless service for peer endpoints), `BrokerIdentity` (parses the ordinal out of the StatefulSet pod name), `TopicWatcher` (fires `TopicDeleted` on `deletionTimestamp` so the broker can close handles before the operator's finalizer reconciles), `ReadinessUpdater` (satisfies the `skafka.io/PartitionsReady` readiness gate once partition directories have been created on the PVC).
- `internal/observability/` — OTLP metrics + tracing bootstrap (push-mode to Prometheus's native OTLP receiver), `/healthz` HTTP handler with rich runtime state, `byteopacity.go` tripwire counters.
- `operator/api/v1alpha1/` — CRD types (kubebuilder annotations live here; `make manifests` regenerates `deploy/crds/*.yaml` and mirrors them to the Helm chart).
- `operator/controllers/` — one reconciler per CRD. Topic/user/ACL reconcilers materialize state to files in `/data/__cluster/` on the shared PVC.
- `tests/` — multi-package integration suites. `byte-opacity/` (codec tripwire), `controller-failover/`, `stale-controller-race/`, `kafka-compat/` (mTLS, SCRAM, cert rotation, external listener), `integration/` (consumer group, disk storage). These are real Go tests but assume a richer environment than `go test ./...` provides — read each package's setup before running.

### Storage layout

Everything cluster-wide lives under `/data/__cluster/` on the shared RWX PVC: `assignment.json` (controller-written, broker-read), `acls.json`, `credentials.json` (operator-written, broker-read with hot-reload), and the `skafka-controller` Lease lives in K8s (not on the PVC).

Per-partition data lives at `/data/<topic>/<partition>/` with epoch-prefixed segment filenames so a stale leader's late writes can't corrupt a new leader's log.

### Helm chart & deployment

`deploy/helm/skafka/` is the source of truth for production config (replicas, controller-Lease tuning, storage class, image repos). The chart bundles its CRDs in `crds/` (auto-generated by `make manifests`). Helm intentionally does not upgrade CRDs across releases — see the chart's `README.md` for the upgrade procedure.
