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

The `scripts/smoke-test.sh` end-to-end test hits a live broker in a k3s cluster — it is *not* a unit test and is not invoked by `go test`. See `scripts/README.md` for the procedure (including how to diagnose failures via `/healthz`, broker logs, Prometheus, and the `KafkaClusterAssignments` CR mirror).

## Architecture

skafka is a from-scratch Kafka-protocol-compatible broker that runs on Kubernetes. Two binaries ship in this repo:

- **`cmd/skafka`** — the broker (port 9092 plaintext, 9093 TLS, 8080 health, 9094 inter-broker heartbeat gRPC).
- **`cmd/skafka-operator`** — a controller-runtime operator that reconciles 5 CRDs into on-disk config files (auth/topics) and Kubernetes plumbing (TLS routes, etc.).

There are also two helper binaries for tests/diagnostics: `cmd/skafka-failover-probe` and `cmd/skafka-fsync-check`.

### Brokers are runtime-independent of the operator

This is the most important architectural fact and is easy to misread from the directory layout. The operator is a **startup/admission** component, not a hot-path dependency:

- Operator manages 6 CRDs in `operator/api/v1alpha1/`: `KafkaCluster` (external listener plumbing), `KafkaTopic` (partition dir creation), `KafkaUser` / `KafkaACL` / `KafkaUserGroup` (auth — materialized to files under `/data/__cluster/`), and `KafkaClusterAssignments` (read-only debug mirror, written fire-and-forget by the controller broker; brokers never read it).
- Brokers read `KafkaTopic` CRs at startup (and watch them for new topics / partition expansion), but the read is non-fatal — a missing/unreachable API server only blocks new topic creation, never serving of existing topics.
- The Produce/Fetch hot path makes **zero K8s API calls**. Ownership lookups are in-memory.

If you find yourself adding a runtime dependency from broker → operator (e.g. a watch on a CR that blocks request handling), stop — that's an architectural change.

### Controller broker, leases, and the authoritative assignment file

The "controller" is **a broker that holds the `skafka-controller` Lease**, not a separate process. Its responsibilities:

- Observes peer brokers via gRPC heartbeats (`proto/heartbeat.proto`, `pkg/heartbeatpb/`, `internal/controller/heartbeat_server.go`).
- Computes partition + consumer-group assignments (`internal/controller/balancer.go`, `assignment.go`).
- Writes `/data/__cluster/assignment.json` on the shared RWX PVC. The file is epoch-prefixed by `leaseTransitions`; brokers reject writes with stale epochs (this is what `tests/stale-controller-race` verifies).
- Mirrors the assignment to a `KafkaClusterAssignments` CR for `kubectl` debugging only.

Non-controller brokers watch `assignment.json` via fsnotify + 1s poll (`internal/broker/coordinator.go`, `internal/fsutil/filewatch.go`) and apply changes locally (`TakeoverPartition` / `RelinquishPartition` on the storage engine).

Lease-based leadership lives in `internal/lease/` (real `KubernetesLeaseManager` and a `LocalLeaseManager` for single-broker dev mode). Local-dev mode is selected when `MY_POD_NAME` is unset; in that case, `dataDir == ""` also flips storage to in-memory (see the branch in `cmd/skafka/main.go`).

### Code map

- `internal/protocol/` — Kafka wire protocol. `codec/` (frames, primitives, CRC32C, per-API request/response types under `codec/api/`), `dispatch.go`, `server.go` (TCP listener + TLS), and `handlers/` (one file per API: `produce.go`, `fetch.go`, `metadata.go`, `consumer_group.go`, `list_offsets.go`, `admin.go`, `sasl.go`, `api_versions.go`).
- `internal/storage/` — `DiskStorageEngine` with segment files, manifest, watcher, and cleaner. Single-writer enforcement is `BrokerCoordinator.Owns` + epoch-prefixed segment filenames (the old per-partition flock was removed in Phase 4).
- `internal/coordinator/` — Kafka consumer-group coordinator (group state, offset commits). Offsets persisted under `dataDir`. Group ownership comes from a `GroupAssignmentSource`; the runtime variant is wired through the controller assignment, not per-group Leases.
- `internal/broker/` — broker glue: `Broker` struct, the on-broker `Coordinator` (assignment.json watcher), `controller_watch.go` (1s poll of controller Lease for current epoch), `self_fence.go`, `takeover.go`, `group_takeover.go`, `heartbeat_client.go`.
- `internal/controller/` — controller-side logic (election, balancer, assignment writer, heartbeat server, k8s CR mirror).
- `internal/auth/` — SCRAM-SHA-256/512, mTLS principal extraction, ACL evaluation, quotas. Loads from `/data/__cluster/credentials.json` and `acls.json` (written by the operator) with hot-reload via `ClusterFileWatcher`. Toggle off with `SKAFKA_AUTH_DISABLED=true`.
- `internal/k8s/` — broker-side K8s helpers: `BrokerRegistry` (watches the headless service for peer endpoints), `BrokerIdentity` (parses the ordinal out of the StatefulSet pod name), `TopicWatcher`, `ReadinessUpdater` (satisfies the `skafka.io/PartitionsReady` readiness gate after initial partition acquisition).
- `internal/observability/` — OTLP metrics + tracing bootstrap (push-mode to Prometheus's native OTLP receiver), `/healthz` HTTP handler with rich runtime state, `byteopacity.go` tripwire counters.
- `operator/api/v1alpha1/` — CRD types (kubebuilder annotations live here; `make manifests` regenerates `deploy/crds/*.yaml` and mirrors them to the Helm chart).
- `operator/controllers/` — one reconciler per CRD. Topic/user/ACL reconcilers materialize state to files in `/data/__cluster/` on the shared PVC.
- `tests/` — multi-package integration suites. `byte-opacity/` (codec tripwire), `controller-failover/`, `stale-controller-race/`, `kafka-compat/` (mTLS, SCRAM, cert rotation, external listener), `integration/` (consumer group, disk storage). These are real Go tests but assume a richer environment than `go test ./...` provides — read each package's setup before running.

### Storage layout

Everything cluster-wide lives under `/data/__cluster/` on the shared RWX PVC: `assignment.json` (controller-written, broker-read), `acls.json`, `credentials.json` (operator-written, broker-read with hot-reload), and the `skafka-controller` Lease lives in K8s (not on the PVC).

Per-partition data lives at `/data/<topic>/<partition>/` with epoch-prefixed segment filenames so a stale leader's late writes can't corrupt a new leader's log.

### Helm chart & deployment

`deploy/helm/skafka/` is the source of truth for production config (replicas, controller-Lease tuning, storage class, image repos). The chart bundles its CRDs in `crds/` (auto-generated by `make manifests`). Helm intentionally does not upgrade CRDs across releases — see the chart's `README.md` for the upgrade procedure.
