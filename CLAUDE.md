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

- **`cmd/skafka`** — the broker. Listeners are declared via the `SKAFKA_LISTENERS` JSON env (gh #126); the chart emits one entry per `.Values.listeners[]` item. Fixed ports: 8080 health, 9094 inter-broker heartbeat gRPC.
- **`cmd/skafka-operator`** — a controller-runtime operator that reconciles 4 CRDs into on-disk config files (auth/topics) and Kubernetes plumbing (TLS routes, etc.).

### Brokers are runtime-independent of the operator

This is the most important architectural fact and is easy to misread from the directory layout. The operator is a **startup/admission** component, not a hot-path dependency:

- Operator manages 4 CRDs in `operator/api/v1alpha1/`: `KafkaCluster` (external listener plumbing), `KafkaTopic` (partition dir creation), `KafkaUser` (auth + ACLs + quotas — `spec.authentication` / `spec.authorization` mirror Strimzi 1:1 since gh #135; materialized to `credentials.json` + `acls.json` under `/data/__cluster/`), and `KafkaClusterAssignments` (read-only debug mirror, written fire-and-forget by the controller broker; brokers never read it). **`spec.quotas` intentionally diverges from Strimzi**: the byte-rate fields are named `producerMaxByteRatePerBroker` / `consumerMaxByteRatePerBroker` (vs Strimzi's `producerByteRate` / `consumerByteRate`) to make the per-broker semantics (Apache Kafka 3.7 / KIP-13) legible at the CR level. With N brokers the effective cluster-wide ceiling is N × the configured value — same semantics Strimzi/Apache Kafka have, just named honestly. The pre-gh #135 `KafkaACL` and `KafkaUserGroup` CRs are gone — ACLs are authored inline on each KafkaUser's `spec.authorization.acls` list. To grant the same rule to N principals, repeat the ACL on each of their KafkaUser CRs (no group abstraction; the Strimzi-pattern trade).
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

### Idempotent producer (gh #12, #22, #30)

The Java producer enables idempotence by default since Kafka 3.0, so every `kafka-console-producer` / `kafka-verifiable-producer` invocation hits this path. Three layers of state, all on the shared PVC:

- **InitProducerId handler** (`internal/protocol/handlers/init_producer_id.go`, API key 22, v0–v4). For non-transactional producers (empty `transactional.id`): hands out a fresh PID from a monotonic counter seeded at boot, epoch=0. For transactional producers: looks up the txn ID in `TxnStateStore` and returns the **same PID with epoch+1** on every reconnect — the gh #22 fence-on-rejoin contract.
- **Per-partition sequence tracking** (`internal/storage/idempotence.go`). `partitionState.producerStates` is a `map[int64]*producerEntry` with a 5-batch ring buffer per (PID, epoch) — mirrors Java's `max.in.flight.requests.per.connection=5`. `classifyIdempotence` runs under `ps.mu` before `appendBatch`: returns `idemDuplicate` (echo cached `baseOffset`, no log write), `idemOutOfOrder` (wire 45), `idemInvalidEpoch` (wire 47), or `idemAccept`.
- **Snapshot persistence** (`internal/storage/producer_snapshot.go`). `producer-state.snapshot` next to `manifest.json`; written on segment roll + `Relinquish` (whatever calls `persistManifestLocked`); restored on `openPartition`. Without it, broker restart loses the dedupe window and in-flight retries get OUT_OF_ORDER.
- **TxnStateStore** (`internal/coordinator/txn_state.go`). `/data/__cluster/transactional_state.json` maps `transactional.id → {PID, epoch}`. Per-broker file (multi-broker coordinator routing is filed as gh #91; producers reconnecting to a *different* broker for the same txn ID get a fresh PID).
- **Cross-partition fence on bump** (`DiskStorageEngine.FenceProducerEpoch`). Called from `InitProducerIdHandler` after every `epoch > 0` rejoin. Walks every partition, advances `producerStates[PID].epoch` and clears the dedupe window, so a zombie batch from the old session is fenced even on partitions the new session hasn't yet touched.

### Consumer-group coordinator routing (gh #92)

`coordinator-of-G` is a **pure function**: `hash(groupID) % numBrokers` with the divisor pinned to the full broker set (NOT `len(alive)`), preferred-slot-down falling back to a deterministic alternate from the alive subset. Mirrors Apache Kafka's `partitionFor(groupId)` (which leases the question to `__consumer_offsets` partition leadership); skafka has no `__consumer_offsets` topic, so we hash directly into the broker set. Implementation in `internal/broker/group_hash.go`.

`broker.Coordinator.OwnsGroup` and `GroupCoordinator` are two-tier: explicit `assignment.json.consumerGroups[]` entries win first (the controller's `BalanceGroups` writes them; this is the forward-compat lever for sticky-rebalance), hash fallback otherwise. The two converge for stable broker sets, so the hash is the load-bearing path in steady state.

`coordinator.Manager.SetGroupAssignmentSource` is a hot-swap setter called from `cluster_runtime.go` after `broker.Coordinator` boots — the bootstrap source is a `LocalGroupSource` stub (always-true) used during the brief window before the runtime is up; tests can substitute their own source. **Don't unwire the swap** without re-reading the gh #92 issue; v0.1.52 tried and hit the chicken-and-egg (strict `isCoordinator` blocks fresh-group bootstrap) and was reverted in v0.1.53.

`GroupTakeoverDriver.OnAssignmentChange` runs both a prev→next diff AND an orphan sweep (`m.groups` keys ⊆ `nextOurs`). The sweep keeps memory bounded across alive-set churn and fixes the gh #89 stale-`--list` symptom. `ListGroups` and `DescribeGroups` filter by `isCoordinator` so a stale `m.groups` entry on a non-coordinator broker isn't visible to clients via the AdminClient's union across brokers.

### Listeners, authentication, authorization (gh #124, #125, #126)

Three orthogonal axes, Strimzi 1:1:

- **`type`**: `internal` (in-cluster only) vs `external` (Gateway + cert-manager + per-broker hostnames). One listener per axis combination is normal — keep `plain` anonymous for in-cluster bench/UI traffic and add an `authed` SCRAM listener side-by-side.
- **`tls`**: `false` / `true`. `mtls` authentication implies `tls: true`; everything else is independent.
- **`authentication.type`**: `none` / `scram-sha-512` / `mtls` / `plain`. Each listener gets its own `auth.AuthEngine` selected via `AuthEngineSelector.For(listenerName)` in `internal/auth/auth.go`. Anonymous listeners use `AllowAllAuthEngine` (no SASL handshake, no principal); authenticated listeners use `RealAuthEngine` and pre-gate connections via `AuthEngine.RequiresPreAuth()` in `internal/protocol/dispatch.go` so a client must finish SASL before any non-handshake API is dispatched.

**Authentication is per-listener; authorization is cluster-wide.** That split lives in three interfaces in `internal/auth/auth.go`:

- `Authorizer.Authorize(principal, operation, resource)` — wired via `SKAFKA_AUTHORIZATION_TYPE` (`""` = none → `AllowAllAuthorizer`; `simple` = ACL-based → `RealAuthEngine`). `SKAFKA_SUPER_USERS` (comma-separated `User:foo,User:bar`) wraps the chosen authorizer in `SuperUserAuthorizer` for early-allow.
- `QuotaChecker.CheckProduceQuota` / `CheckFetchQuota` — defaults to `NoQuotaChecker`; switches to `RealAuthEngine`'s quotas when auth is enabled. Quotas fire **regardless of authorization** — they're orthogonal.

Produce/Fetch handlers route exclusively via `h.authorizer.Authorize(...)` + `h.quotas.CheckProduceQuota(...)` — they don't reach back into the per-listener engine. This is what lets `plain` (anonymous) and `authed` (SCRAM) listeners share the same ACL/quota policy.

**Per-listener Metadata advertisement (gh #125)**: each `BrokerEndpoint` carries a `ListenerPorts map[string]int32`; `addressFor` looks up the port matching the request's listener so a client that bootstrapped on :9095 gets back :9095 in the Metadata response, not :9092. Without this, an authed-listener client got routed back to the anonymous listener and looped on SCRAM retry. The listener name on the connection is propagated via `connstate.ListenerName` (free-form string — no predefined `Internal`/`External`/`Authed` constants; the chart picks the names).

**Quota debt-carry (gh #125)**: the token bucket carries negative balances forward as debt instead of clamping at 0. With clamping, N concurrent clients each saw a "full" bucket and burst at N×rate before throttle kicked in (the 16-vs-10 MiB/s gap observed under bench-perf). Removing the clamp matches KIP-13. Test: `TestQuotaMultiClientContention` in `internal/auth/quota_test.go`.

### KafkaTopic delete on NFS

When a `KafkaTopic` CR is deleted, `metadata.deletionTimestamp` goes non-nil. The topic-watcher fires `TopicDeleted` *immediately* (rather than waiting for the K8s `Deleted` event after finalizers clear); the broker calls `engine.ClosePartition` for each partition to drop its open log + index file handles on the leader broker (followers don't have them open per the previous section). Without this, NFS silly-renames the open files into `.nfsXXXX` entries that EBUSY the operator's `unlinkat` on the parent directory forever (gh #76). The topic-watcher also routes the initial reconcile through `processEvent` so brokers coming up while CRs are already mid-deletion close their handles on startup, not just on watch events.

### Code map

- `internal/protocol/` — Kafka wire protocol. `codec/` (frames, primitives, CRC32C, per-API request/response types under `codec/api/`), `dispatch.go` (per-listener pre-auth gate via `engines.For(conn.Listener).RequiresPreAuth()`), `server.go` (multi-listener TCP/TLS bring-up driven by `Config.Listeners []ListenerConfig`), and `handlers/` (one file per API: `produce.go`, `fetch.go`, `metadata.go` (per-listener port advertisement, gh #125), `consumer_group.go` (incl. DeleteGroups gh #89), `list_offsets.go`, `admin.go`, `sasl.go` (per-listener engine selector, gh #124), `api_versions.go`, `init_producer_id.go` (gh #12)).
- `internal/storage/` — `DiskStorageEngine` with segment files, manifest, watcher, and cleaner. Single-writer enforcement is `BrokerCoordinator.Owns` + epoch-prefixed segment filenames (the old per-partition flock was removed in Phase 4). `idempotence.go` + `producer_snapshot.go` carry the gh #12 idempotent-producer state — see "Idempotent producer" above. See "Storage hot path & file-handle ownership" for the group-commit / lazy-open / async-roll-finalize semantics — those are easy to miss if you read the engine code in isolation.
- `internal/coordinator/` — Kafka consumer-group coordinator (group state, offset commits). Offsets persisted under `dataDir`. Group ownership comes from a `GroupAssignmentSource` — see "Consumer-group coordinator routing" above for the gh #92 hash-fallthrough. `txn_state.go` carries the gh #22 transactional-id rejoin map.
- `internal/broker/` — broker glue: `Broker` struct, the on-broker `Coordinator` (assignment.json watcher with hash-fallthrough OwnsGroup/GroupCoordinator, gh #92), `controller_watch.go` (1s poll of controller Lease for current epoch), `self_fence.go`, `takeover.go`, `group_takeover.go` (incl. orphan sweep, gh #89), `group_hash.go` (gh #92 deterministic coordinator), `heartbeat_client.go`.
- `internal/controller/` — controller-side logic (election, balancer, assignment writer, heartbeat server, k8s CR mirror).
- `internal/auth/` — SCRAM-SHA-256/512, mTLS principal extraction, ACL evaluation, quotas. Loads from `/data/__cluster/credentials.json` and `acls.json` (written by the operator) with hot-reload via `ClusterFileWatcher`. Toggle off with `SKAFKA_AUTH_DISABLED=true`. Public interfaces (gh #124/#126): `AuthEngine` (+ `RequiresPreAuth`), `AuthEngineSelector` (per-listener engine map), `Authorizer` (cluster-wide; `AllowAllAuthorizer` / `SuperUserAuthorizer` / `RealAuthEngine`), `QuotaChecker` (`NoQuotaChecker` / `RealAuthEngine`). Quota debt-carry algorithm in `quota.go` (gh #125).
- `internal/k8s/` — broker-side K8s helpers: `BrokerRegistry` (watches the headless service for peer endpoints), `BrokerIdentity` (parses the ordinal out of the StatefulSet pod name), `TopicWatcher` (fires `TopicDeleted` on `deletionTimestamp` so the broker can close handles before the operator's finalizer reconciles), `ReadinessUpdater` (satisfies the `skafka.io/PartitionsReady` readiness gate once partition directories have been created on the PVC).
- `internal/observability/` — OTLP metrics + tracing bootstrap (push-mode to Prometheus's native OTLP receiver), `/healthz` HTTP handler with rich runtime state, `byteopacity.go` tripwire counters.
- `operator/api/v1alpha1/` — CRD types (kubebuilder annotations live here; `make manifests` regenerates `deploy/crds/*.yaml` and mirrors them to the Helm chart).
- `operator/controllers/` — one reconciler per CRD. Topic/user/ACL reconcilers materialize state to files in `/data/__cluster/` on the shared PVC.
- `tests/` — multi-package integration suites. `byte-opacity/` (codec tripwire), `controller-failover/`, `stale-controller-race/`, `kafka-compat/` (mTLS, SCRAM, cert rotation, external listener), `integration/` (consumer group, disk storage). These are real Go tests but assume a richer environment than `go test ./...` provides — read each package's setup before running.

### Storage layout

Everything cluster-wide lives under `/data/__cluster/` on the shared RWX PVC: `assignment.json` (controller-written, broker-read), `acls.json`, `credentials.json` (operator-written, broker-read with hot-reload), `transactional_state.json` (per-broker txn-id → (PID, epoch) map for gh #22), and per-group offset files under `__consumer_offsets/<groupID>.json`. The `skafka-controller` Lease lives in K8s (not on the PVC).

Per-partition data lives at `/data/<topic>/<partition>/` with epoch-prefixed segment filenames so a stale leader's late writes can't corrupt a new leader's log. Sibling files: `manifest.json` (epoch + HWM + logStartOffset) and `producer-state.snapshot` (gh #12 idempotent-producer dedupe window — see "Idempotent producer" above).

### Helm chart & deployment

`deploy/helm/skafka/` is the source of truth for production config (replicas, controller-Lease tuning, storage class, image repos). The chart bundles its CRDs in `crds/` (auto-generated by `make manifests`). Helm intentionally does not upgrade CRDs across releases — see the chart's `README.md` for the upgrade procedure.

**Listeners are a Strimzi-shape array (gh #126).** `.Values.listeners` is `[]listener` where each entry has its own `name` (free-form), `port`, `type` (`internal` / `external`), `tls`, `authentication.type`, and an optional `enabled` flag (absence = enabled — only `external` and `authed` ship as `enabled: false` defaults). Templates iterate this array to emit (a) the StatefulSet containerPorts + `SKAFKA_LISTENERS` JSON env, (b) headless + ClusterIP Service ports, (c) the NOTES.txt bootstrap-host output. Helpers in `_helpers.tpl`: `skafka.listenersJSON`, `skafka.findListener`, `skafka.firstByType`, `skafka.hasEnabledExternalListener`, `skafka.superUsersList`. **The KafkaCluster CR template still synthesizes the legacy single-listener shape** via `skafka.firstByType` for backwards-compat with the operator — a follow-up will refactor the operator side to consume the array natively.

Cluster-wide authorization lives at `.Values.authorization.{type,superUsers}` (top-level, not nested under any listener). `type: ""` (default) leaves authorization off (`AllowAllAuthorizer`); `type: simple` enables ACL enforcement via `RealAuthEngine`. `superUsers` (list of `User:foo` strings) is emitted as `SKAFKA_SUPER_USERS` and wraps whatever authorizer the broker picked in `SuperUserAuthorizer`.

**Storage substrate**: the chart accepts `storage.accessMode: ReadWriteOnce` + a local-path class for single-broker deployments (the k3s overlay does this — RWX-NFS was the source of perf-bench DeadlineExceeded errors during saturation tests). Multi-broker requires `ReadWriteMany` with NFSv4-class semantics (same-directory rename atomicity, fsync durability, close-to-open consistency); see NOTES.txt for the provider matrix.
