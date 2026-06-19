# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Status: mid-rewrite from Go to Rust

skafka is being rewritten from Go to Rust. The full plan lives in [`docs/rewrite.md`](./docs/rewrite.md); per-phase detail starts with [`docs/phase-0.md`](./docs/phase-0.md). System-level architecture in [`docs/ARCHITECTURE.md`](./docs/ARCHITECTURE.md). Tracker issue: [gh #143](https://github.com/Woestebanaan/skafka/issues/143). **Read these before starting non-trivial work.**

- **Phase 0 shipped (commit `f701876`).** The Rust workspace at the repo root is real: `Cargo.toml`, `rust-toolchain.toml` (pin = 1.85), 12 lib crates under `crates/sk-*`, 2 bins under `bins/{skafka,skafka-operator}`, an `xtask` runner, vendored `protoc` via `tonic-build` in `sk-broker`. Every crate compiles, `cargo xtask ci` is green, and CI runs `rust` + `legacy-go` + `docker-rust` + `docker-go` + `helm` jobs in parallel. The crates are scaffolding only — production logic lands phase-by-phase.
- **All Go code lives under `archive/`.** Every path in the rest of this doc that names a Go source path (`cmd/...`, `internal/...`, `operator/...`, `pkg/...`, `tests/...`, `go.mod`, `Dockerfile*`, `Makefile`) should be read with an `archive/` prefix. `go build ./...` works from inside `archive/`. The Go tree is **frozen** — no new feature work; only port-blocking bugfixes.
- **`proto/`, `deploy/`, and `scripts/` stay at the root**, unchanged. The Rust port reuses them as-is (tonic-build consumes `proto/heartbeat.proto`; the Helm chart and shell integration suite are language-agnostic).
- **The architecture and behaviour sections below remain the source of truth** — they describe how skafka works on the wire, on disk, and against the K8s API. They are the spec the Rust port targets, not historical commentary.
- **Open phases.** [gh #144](https://github.com/Woestebanaan/skafka/issues/144) Phase 1 codec · [gh #145](https://github.com/Woestebanaan/skafka/issues/145) Phase 2 storage · [gh #146](https://github.com/Woestebanaan/skafka/issues/146) Phase 3 server · [gh #147](https://github.com/Woestebanaan/skafka/issues/147) Phase 4 auth · [gh #148](https://github.com/Woestebanaan/skafka/issues/148) Phase 5 coordinator · [gh #149](https://github.com/Woestebanaan/skafka/issues/149) Phase 6 transactions · [gh #150](https://github.com/Woestebanaan/skafka/issues/150) Phase 7 operator · [gh #151](https://github.com/Woestebanaan/skafka/issues/151) Phase 8 observability+parity · [gh #152](https://github.com/Woestebanaan/skafka/issues/152) Phase 9 cutover.

## Parity target & non-goals

skafka targets **Apache Kafka 3.7** for wire-protocol and Kafka Streams parity. Behaviour is verified against the matrix tracked in the [skafka-migration-parity](https://github.com/users/Woestebanaan/projects/2) GitHub project. When a feature is ambiguous, default to "match Apache Kafka 3.7" rather than inventing skafka-specific semantics.

- **Deferred (intended later, not now):** tiered storage / S3 backend. Skip the tiered-storage-only API surfaces (e.g. `EARLIEST_LOCAL_TIMESTAMP`, `EARLIEST_PENDING_UPLOAD_OFFSET`) — clients only request them when configured for remote tiers.
- **Non-goals (off the table by design):** KRaft / metadata quorum (skafka uses K8s Leases instead), replication / ISR (single-writer-per-partition), and a literal `__transaction_state` internal topic (skafka uses per-broker JSON slot files on the shared NFS PVC — see "Idempotent producer" + "Transaction coordinator state machine" below; gh #29). Don't add any of these; flag if a "parity" task implicitly requires them.

## Common commands

### Rust (at the repo root)

```bash
cargo build --workspace                                  # build every crate + both bins
cargo test  --workspace --all-features                   # run all Rust unit + integration tests
cargo test  -p sk-codec                                  # one crate
cargo test  -p sk-broker --test proto_smoke              # one integration test
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt   --check
cargo xtask ci                                           # fmt + clippy + test + release build, all in one
cargo xtask gen-proto                                    # force-rebuild sk-broker so tonic-build re-runs
cargo xtask gen-crds                                     # stub until phase 7; will write deploy/crds/
cargo xtask check-crd-drift                              # CI gate; stub until phase 7
```

`rust-toolchain.toml` pins Rust 1.85 (transitive `getrandom` needs edition 2024); `rustup` auto-installs it on first invocation. `protoc` is vendored via `protoc-bin-vendored` inside `sk-broker/build.rs` — no `apt install protobuf-compiler` needed. Generated proto code is silenced from the workspace clippy gate via a module-scope `#![allow(...)]`.

### Go (inside `archive/`)

The Go tree is **frozen**. Bugfixes only — no new feature work. All `go` and `make` invocations run from inside `archive/`:

```bash
cd archive
go build ./...              # build all binaries
go test  ./...              # run all unit tests
go test  ./internal/storage # run one package
go test  -run TestX ./...   # run one test by name
go vet   ./...
make manifests              # regenerate CRD YAMLs from operator/api/* AND mirror into ../deploy/helm/skafka/crds/
make proto                  # regenerate gRPC stubs from ../proto/heartbeat.proto (needs `buf`; install via `make proto-tools`)
```

`archive/go.mod` targets Go 1.26.1. `golangci-lint` is currently disabled in CI (latest release is built with Go 1.24 and refuses 1.26 modules) — `make lint` works locally if you have a matching toolchain, but don't be surprised when it fails to load.

### CI

`.github/workflows/ci.yml` runs five jobs in parallel:

- `rust` — `cargo fmt --check` + `cargo clippy -D warnings` + `cargo test --workspace --all-features` + `cargo build --release --workspace --bins`.
- `legacy-go` — `cd archive` then `go vet`, `go test`, `controller-gen` CRD drift check. Stays green until Phase 9 retires the Go release line.
- `docker-rust` — buildx of `bins/skafka/Dockerfile` and `bins/skafka-operator/Dockerfile`, no push.
- `docker-go` — buildx of `archive/Dockerfile` and `archive/Dockerfile.operator`, no push.
- `helm` — `helm lint deploy/helm/skafka` + `helm template`.

`.github/workflows/docker-publish.yml` is tag-triggered and currently builds **only** the Go images from `archive/`. Rust image stanzas are committed but commented out; Phase 9 flips the default flavor.

**CRD drift** — if you edit anything under `archive/operator/api/`, run `make manifests` from inside `archive/` and commit both `deploy/crds/` and `deploy/helm/skafka/crds/`. The CI step is inlined (the runner has no `make`), but the effect is the same. Once `sk-operator-api` lands in Phase 7, `xtask gen-crds` replaces `controller-gen` as the drift source.

Releases are tag-driven; see [`docs/RELEASING.md`](./docs/RELEASING.md). **Always bump the patch (`v0.1.N-preview` → `v0.1.N+1-preview`), never re-cut a tag.** The Rust port will cut from `v0.2.0-preview` per Phase 9.

`scripts/kafka-*.sh` (at the repo root, not archived) is a per-tool integration suite that runs the Apache Kafka shell tools (`kafka-topics`, `kafka-{producer,consumer}-perf-test`, `kafka-acls`, etc.) against a live broker — *not* invoked by `go test`. Each script sources `scripts/_common.sh` for shared `BOOTSTRAP` / `KAFKA_BIN` / `skip` helpers; defaults target the in-cluster Service DNS and `/opt/kafka/bin`. Scripts target the broker's wire surface, so they exercise whichever flavor (Go or Rust) is currently deployed — they don't need updating per phase. Scripts that target features that are non-goals or post-3.7 (KRaft tools, share-groups, etc.) print a one-line reason and `exit 77` (the autoconf "skipped" code) so they're discoverable without pretending to test something that can't work.

## Architecture

skafka is a from-scratch Kafka-protocol-compatible broker that runs on Kubernetes. Two binaries ship in this repo:

- **`cmd/skafka`** — the broker. Listeners are declared via the `SKAFKA_LISTENERS` JSON env (gh #126); the chart emits one entry per `.Values.listeners[]` item. Fixed ports: 8080 health, 9094 inter-broker heartbeat gRPC.
- **`cmd/skafka-operator`** — a controller-runtime operator that reconciles 4 CRDs into on-disk config files (auth/topics) and Kubernetes plumbing (TLS routes, etc.).

### Brokers are runtime-independent of the operator

This is the most important architectural fact and is easy to misread from the directory layout. The operator is a **startup/admission** component, not a hot-path dependency:

- Operator manages 4 CRDs in `operator/api/v1alpha1/`: `KafkaCluster` (external listener plumbing), `KafkaTopic` (partition dir creation; `Status.TopicID` carries a v4 UUID generated on first reconcile, surfaced by the broker on the wire — see "TopicID propagation" below; gh #105/KIP-516), `KafkaUser` (auth + ACLs + quotas — `spec.authentication` / `spec.authorization` mirror Strimzi 1:1 since gh #135; materialized to `credentials.json` + `acls.json` under `/data/__cluster/`), and `KafkaClusterAssignments` (read-only debug mirror, written fire-and-forget by the controller broker; brokers never read it). **`spec.quotas` intentionally diverges from Strimzi**: the byte-rate fields are named `producerMaxByteRatePerBroker` / `consumerMaxByteRatePerBroker` (vs Strimzi's `producerByteRate` / `consumerByteRate`) to make the per-broker semantics (Apache Kafka 3.7 / KIP-13) legible at the CR level. With N brokers the effective cluster-wide ceiling is N × the configured value — same semantics Strimzi/Apache Kafka have, just named honestly. The pre-gh #135 `KafkaACL` and `KafkaUserGroup` CRs are gone — ACLs are authored inline on each KafkaUser's `spec.authorization.acls` list. To grant the same rule to N principals, repeat the ACL on each of their KafkaUser CRs (no group abstraction; the Strimzi-pattern trade).
- Brokers read `KafkaTopic` CRs at startup (and watch them for new topics / partition expansion), but the read is non-fatal — a missing/unreachable API server only blocks new topic creation, never serving of existing topics.
- The Produce/Fetch hot path makes **zero K8s API calls**. Ownership lookups are in-memory.
- **Admin handlers DO patch `KafkaTopic` CRs** (gh #52, gh #9). `CreatePartitions` (key 37, KIP-195) and `IncrementalAlterConfigs` (key 44, KIP-339) route through `internal/k8s/topic_cr_writer.go`: `ExpandTopic` patches `spec.partitions`; `UpdateTopicConfig` patches `spec.config` per-key (SET/DELETE/APPEND/SUBTRACT). The operator then materialises the change as usual. **Broker RBAC includes `update,patch` on `kafkatopics`** — if you add another admin write path, check `deploy/helm/skafka/templates/broker-rbac.yaml` doesn't need a new verb. `IncrementalAlterConfigs` is TOPIC-only; BROKER / BROKER_LOGGER resource types return `UNSUPPORTED_VERSION` (no dynamic broker-config surface yet).

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

**Graceful SIGTERM drain (gh #61, gh #139).** `cmd/skafka/main.go`'s shutdown path runs `DiskStorageEngine.RelinquishAll()` *before* `FlushManifests`. `RelinquishAll` iterates every open partition (`splitPartKey` reverses the `topic/partition` partKey encoding, parsing from the right to handle slash-bearing topic names) and calls `Relinquish` on each — persisting the manifest one last time AND closing the active segment's file handles so the next leader doesn't hit NFS silly-rename pain on takeover. `FlushManifests` stays as defence-in-depth. **There's no controlled-shutdown RPC** yet — the controller learns the broker is gone via heartbeat timeout (gh #77) and rebalances reactively. A proactive "I'm draining, move my partitions first" hint is open follow-up.

**Compactor enforcement (gh #116).** Two per-topic config knobs that the schema already exposed but the compactor used to ignore: `min.compaction.lag.ms` (KIP-58 — segments whose `maxTimestamp` is inside the lag window are skipped; default 0 = no gate) and `delete.retention.ms` (KIP-354 — tombstones whose batch `baseTimestamp` is older than the cutoff are dropped, even if they're the latest for their key; default 0 = tombstones live forever). Tombstone-expiry granularity is per-batch in skafka (Apache is per-record).

### Idempotent producer (gh #12, #22, #30)

The Java producer enables idempotence by default since Kafka 3.0, so every `kafka-console-producer` / `kafka-verifiable-producer` invocation hits this path. Three layers of state, all on the shared PVC:

- **InitProducerId handler** (`internal/protocol/handlers/init_producer_id.go`, API key 22, v0–v4). For non-transactional producers (empty `transactional.id`): hands out a fresh PID from a monotonic counter seeded at boot, epoch=0. For transactional producers: looks up the txn ID in `TxnStateStore` and returns the **same PID with epoch+1** on every reconnect — the gh #22 fence-on-rejoin contract.
- **Per-partition sequence tracking** (`internal/storage/idempotence.go`). `partitionState.producerStates` is a `map[int64]*producerEntry` with a 5-batch ring buffer per (PID, epoch) — mirrors Java's `max.in.flight.requests.per.connection=5`. `classifyIdempotence` runs under `ps.mu` before `appendBatch`: returns `idemDuplicate` (echo cached `baseOffset`, no log write), `idemOutOfOrder` (wire 45), `idemInvalidEpoch` (wire 47), or `idemAccept`.
- **Snapshot persistence** (`internal/storage/producer_snapshot.go`). `producer-state.snapshot` next to `manifest.json`; written on segment roll + `Relinquish` (whatever calls `persistManifestLocked`); restored on `openPartition`. Without it, broker restart loses the dedupe window and in-flight retries get OUT_OF_ORDER.
- **TxnStateStore** (`internal/coordinator/txn_state.go`). Per-`transactional.id` `{PID, epoch, state, partitions, groups, ongoingSinceMs, transactionTimeoutMs}` slot-sharded across `/data/__cluster/txn_state/slot-N.json` (50 slots default, matches Apache's `transaction.state.log.num.partitions=50`). Each broker owns the slots that hash to it under gh #91 routing; on coordinator failover the new owner reads the same slot file off the shared RWX PVC (close-to-open consistency = the file IS the materialised state, no log replay needed). This is the architectural answer to gh #29 — see the "literal `__transaction_state` internal topic" non-goal up top.
- **Cross-partition fence on bump** (`DiskStorageEngine.FenceProducerEpoch`). Called from `InitProducerIdHandler` after every `epoch > 0` rejoin. Walks every partition, advances `producerStates[PID].epoch` and clears the dedupe window, so a zombie batch from the old session is fenced even on partitions the new session hasn't yet touched. gh #108 phase 2 broadcasts fences across brokers via a per-broker fence log under `/data/__cluster/fence_log/`; peer brokers' `FenceWatcher` polls and applies.

### Transaction coordinator state machine (gh #23–#28, #37)

Built on top of `TxnStateStore`. Handlers in `internal/protocol/handlers/`:

- **AddPartitionsToTxn** (key 24, `add_partitions_to_txn.go`) — `Empty/Complete*` → `Ongoing` stamps `OngoingSinceMs = time.Now().UnixMilli()` so the reaper has a deadline clock.
- **AddOffsetsToTxn** (key 25, `add_offsets_to_txn.go`) — records consumer group IDs that the txn will commit offsets to; same `Ongoing` transition stamps the clock.
- **TxnOffsetCommit** (key 28, `txn_offset_commit.go`) — stages offsets in a **pending** layer keyed by `(groupID, PID)` via `OffsetStore.StorePending`. They are NOT visible to `OffsetFetch` until commit. Group-coordinator side only (`!isCoordinator(groupID)` returns `NOT_COORDINATOR`).
- **EndTxn** (key 26, `end_txn.go`) — `Ongoing → CompleteCommit/CompleteAbort`. Clears `Partitions`, clears `OngoingSinceMs`, fires `TxnOffsetHook` (`coordinator.Manager.WireTxnOffsetHook`) on every recorded group: commit → `OffsetStore.CommitPending`; abort → `DiscardPending`. Cross-broker (txn coord ≠ group coord) returns early via `!isCoordinator(groupID)` — gh #114 (`WriteTxnMarkers` RPC) carries the remaining work for that case.
- **Timeout reaper** (`TxnStateStore.AbortOverdueOwned`, started by `broker.Broker.StartTxnTimeoutReaper`). Fires every 10s (Apache's `transaction.abort.timed.out.transaction.cleanup.interval.ms` default). Walks slots, transitions overdue `Ongoing` entries to `CompleteAbort`, bumps epoch, fires the offset hook so staged offsets are discarded. **Gates on gh #91 `OwnsTxn`** so a multi-broker cluster doesn't N-way-race on the same overdue txn.

`tests/kafka-compat/eos_v2_test.go` exercises the full KIP-447 consume-process-produce-commit round trip with franz-go against the in-process broker. Same-broker case is the gh #37 close; cross-broker still tracked under gh #114.

The kafka-compat broker (test infra) wires the txn surface differently from prod: prod constructs `TxnStateStore` inline in `Broker.registerProducerIDHandlers` when `b.store.DataDir()` is an on-disk path; tests use `Broker.UseTxnStateStore(s)` + `broker.NewLocalTxnSource` for `coordMgr.SetTxnAssignmentSource(...)` so transactional handlers register against a `MemoryStorage`-backed broker too.

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

**mTLS principal mapping (gh #43, KIP-371)**: `internal/auth/principal_mapping.go` parses Apache's `ssl.principal.mapping.rules` syntax — regex against the full subject DN with `$1`/`$2` back-references and optional `/L` / `/U` postfix flags; first matching rule wins, `DEFAULT` returns the CN. Wired via `Server.SetPrincipalMapper`; the mTLS handshake path applies the mapper to the full subject DN before calling `authEngine.AuthenticateTLS`. Parse errors bubble at startup so chart-config typos fail fast rather than silently mapping every cert to the CN.

### TopicID propagation (gh #105, KIP-516)

Every `KafkaTopic`'s `Status.TopicID` carries a v4-shape UUID, generated cryptographically in `operator/controllers/kafkatopic_controller.go` on first reconcile and **never rotated** (Apache's contract: re-created topics get distinct IDs). The state flow: operator writes `Status.TopicID` → `TopicWatcher.onEvent` calls `b.SetTopicID` → `TopicRegistry.All()` surfaces it on each `TopicSource` lookup → `Metadata` handler decodes the hyphenated form to 16 raw bytes via `decodeHyphenatedUUID` and writes them into the v10+ wire response. Empty / wrong-length values fall back to the all-zero sentinel (preserving pre-#105 behaviour for legacy CRs). `CreateTopics` v7+ carrying the UUID in its *response* is still follow-up.

### Fetch sessions (gh #4)

KIP-227 incremental fetch sessions are stateless: skafka returns **`SessionID=0` on every Fetch response** regardless of what the client sent. Echoing the client's SessionID was the pre-fix bug — clients then sent incremental deltas against state skafka didn't have and silently 'forgot' partitions from their subscription. Apache's documented contract for "broker doesn't support sessions" is `SessionID=0`, which makes clients fall back to full Fetch data per request. CPU cost is fine at skafka's scale; KIP-227 caching is a future optimisation, not a correctness gap.

### KafkaTopic delete on NFS

When a `KafkaTopic` CR is deleted, `metadata.deletionTimestamp` goes non-nil. The topic-watcher fires `TopicDeleted` *immediately* (rather than waiting for the K8s `Deleted` event after finalizers clear); the broker calls `engine.ClosePartition` for each partition to drop its open log + index file handles on the leader broker (followers don't have them open per the previous section). Without this, NFS silly-renames the open files into `.nfsXXXX` entries that EBUSY the operator's `unlinkat` on the parent directory forever (gh #76). The topic-watcher also routes the initial reconcile through `processEvent` so brokers coming up while CRs are already mid-deletion close their handles on startup, not just on watch events.

### Code map

- `internal/protocol/` — Kafka wire protocol. `codec/` (frames, primitives, CRC32C, per-API request/response types under `codec/api/`), `dispatch.go` (per-listener pre-auth gate via `engines.For(conn.Listener).RequiresPreAuth()`), `server.go` (multi-listener TCP/TLS bring-up driven by `Config.Listeners []ListenerConfig`; `SetPrincipalMapper` for gh #43), and `handlers/` (one file per API: `produce.go`, `fetch.go` (stateless `SessionID=0`, gh #4; read-committed isolation surface, gh #31), `metadata.go` (per-listener port advertisement gh #125; TopicID gh #105), `consumer_group.go` (incl. DeleteGroups gh #89), `list_offsets.go` (timestamp lookup gh #5), `admin.go`, `create_partitions.go` (key 37, gh #52, patches `KafkaTopic.Spec.Partitions`), `incremental_alter_configs.go` (key 44, gh #9, patches `KafkaTopic.Spec.Config`), `sasl.go` (per-listener engine selector, gh #124), `api_versions.go`, `init_producer_id.go` (gh #12)).
- `internal/storage/` — `DiskStorageEngine` with segment files, manifest, watcher, and cleaner. Single-writer enforcement is `BrokerCoordinator.Owns` + epoch-prefixed segment filenames (the old per-partition flock was removed in Phase 4). `idempotence.go` + `producer_snapshot.go` carry the gh #12 idempotent-producer state — see "Idempotent producer" above. See "Storage hot path & file-handle ownership" for the group-commit / lazy-open / async-roll-finalize semantics — those are easy to miss if you read the engine code in isolation.
- `internal/coordinator/` — Kafka consumer-group coordinator (group state, offset commits) plus transaction coordinator (gh #23–#28). Offsets persisted under `dataDir`. Group ownership comes from a `GroupAssignmentSource`, txn ownership from a `TxnAssignmentSource` — both backed by `*broker.Coordinator` in prod (gh #91 / #92 hash-fallthrough) and by `broker.NewLocalGroupSource` / `NewLocalTxnSource` in single-broker tests. `txn_state.go` carries the gh #22 rejoin map, the gh #23–#26 state machine, and the gh #28 `AbortOverdueOwned` timeout reaper. `fence_log.go` carries gh #108 phase 2 cross-broker fence broadcast.
- `internal/broker/` — broker glue: `Broker` struct, the on-broker `Coordinator` (assignment.json watcher with hash-fallthrough OwnsGroup/GroupCoordinator, gh #92), `controller_watch.go` (1s poll of controller Lease for current epoch), `self_fence.go`, `takeover.go`, `group_takeover.go` (incl. orphan sweep, gh #89), `group_hash.go` (gh #92 deterministic coordinator), `heartbeat_client.go`.
- `internal/controller/` — controller-side logic (election, balancer, assignment writer, heartbeat server, k8s CR mirror).
- `internal/auth/` — SCRAM-SHA-256/512, mTLS principal extraction, ACL evaluation, quotas. Loads from `/data/__cluster/credentials.json` and `acls.json` (written by the operator) with hot-reload via `ClusterFileWatcher`. Toggle off with `SKAFKA_AUTH_DISABLED=true`. Public interfaces (gh #124/#126): `AuthEngine` (+ `RequiresPreAuth`), `AuthEngineSelector` (per-listener engine map), `Authorizer` (cluster-wide; `AllowAllAuthorizer` / `SuperUserAuthorizer` / `RealAuthEngine`), `QuotaChecker` (`NoQuotaChecker` / `RealAuthEngine`). Quota debt-carry algorithm in `quota.go` (gh #125). `principal_mapping.go` carries the `ssl.principal.mapping.rules` parser (gh #43).
- `internal/k8s/` — broker-side K8s helpers: `BrokerRegistry` (watches the headless service for peer endpoints), `BrokerIdentity` (parses the ordinal out of the StatefulSet pod name), `TopicWatcher` (fires `TopicDeleted` on `deletionTimestamp` so the broker can close handles before the operator's finalizer reconciles; stashes `Status.TopicID` via `b.SetTopicID` for gh #105), `TopicCRWriter` (gh #52 / gh #9 — `ExpandTopic` patches `Spec.Partitions`, `UpdateTopicConfig` patches `Spec.Config`; needs `update,patch` on `kafkatopics` in the broker ClusterRole), `ReadinessUpdater` (satisfies the `skafka.io/PartitionsReady` readiness gate once partition directories have been created on the PVC).
- `internal/observability/` — OTLP metrics + tracing bootstrap (push-mode to Prometheus's native OTLP receiver), `/healthz` HTTP handler with rich runtime state, `byteopacity.go` tripwire counters.
- `operator/api/v1alpha1/` — CRD types (kubebuilder annotations live here; `make manifests` regenerates `deploy/crds/*.yaml` and mirrors them to the Helm chart).
- `operator/controllers/` — one reconciler per CRD. Topic/user/ACL reconcilers materialize state to files in `/data/__cluster/` on the shared PVC.
- `tests/` — multi-package integration suites. `byte-opacity/` (codec tripwire), `controller-failover/`, `stale-controller-race/`, `kafka-compat/` (mTLS, SCRAM, cert rotation, external listener), `integration/` (consumer group, disk storage). These are real Go tests but assume a richer environment than `go test ./...` provides — read each package's setup before running.

### Storage layout

Everything cluster-wide lives under `/data/__cluster/` on the shared RWX PVC: `assignment.json` (controller-written, broker-read), `acls.json`, `credentials.json` (operator-written, broker-read with hot-reload), `txn_state/slot-*.json` (gh #22 + #28 — txn coordinator state, slot-sharded; replaces Apache's `__transaction_state` internal topic, see the non-goal up top), `fence_log/from-skafka-*.json` (gh #108 phase 2 cross-broker producer-epoch fence broadcast), and per-group offset files under `__consumer_offsets/<groupID>.json`. The `skafka-controller` Lease lives in K8s (not on the PVC).

Per-partition data lives at `/data/<topic>/<partition>/` with epoch-prefixed segment filenames so a stale leader's late writes can't corrupt a new leader's log. Sibling files: `manifest.json` (epoch + HWM + logStartOffset) and `producer-state.snapshot` (gh #12 idempotent-producer dedupe window — see "Idempotent producer" above).

### Helm chart & deployment

`deploy/helm/skafka/` is the source of truth for production config (replicas, controller-Lease tuning, storage class, image repos). The chart bundles its CRDs in `crds/` (auto-generated by `make manifests`). Helm intentionally does not upgrade CRDs across releases — see the chart's `README.md` for the upgrade procedure.

**Listeners are a Strimzi-shape array (gh #126).** `.Values.listeners` is `[]listener` where each entry has its own `name` (free-form), `port`, `type` (`internal` / `external`), `tls`, `authentication.type`, and an optional `enabled` flag (absence = enabled — only `external` and `authed` ship as `enabled: false` defaults). Templates iterate this array to emit (a) the StatefulSet containerPorts + `SKAFKA_LISTENERS` JSON env, (b) headless + ClusterIP Service ports, (c) the NOTES.txt bootstrap-host output. Helpers in `_helpers.tpl`: `skafka.listenersJSON`, `skafka.findListener`, `skafka.firstByType`, `skafka.hasEnabledExternalListener`, `skafka.superUsersList`. **The KafkaCluster CR template still synthesizes the legacy single-listener shape** via `skafka.firstByType` for backwards-compat with the operator — a follow-up will refactor the operator side to consume the array natively.

Cluster-wide authorization lives at `.Values.authorization.{type,superUsers}` (top-level, not nested under any listener). `type: ""` (default) leaves authorization off (`AllowAllAuthorizer`); `type: simple` enables ACL enforcement via `RealAuthEngine`. `superUsers` (list of `User:foo` strings) is emitted as `SKAFKA_SUPER_USERS` and wraps whatever authorizer the broker picked in `SuperUserAuthorizer`.

**Storage substrate**: the chart accepts `storage.accessMode: ReadWriteOnce` + a local-path class for single-broker deployments (the k3s overlay does this — RWX-NFS was the source of perf-bench DeadlineExceeded errors during saturation tests). Multi-broker requires `ReadWriteMany` with NFSv4-class semantics (same-directory rename atomicity, fsync durability, close-to-open consistency); see NOTES.txt for the provider matrix.

## Go → Rust crate map

All Rust crates listed below are **scaffolded** (Phase 0, commit `f701876`) — each one builds, runs `cargo test`, and contains a doc-comment-only `lib.rs`. Production logic lands phase-by-phase per the **Phase** column.

| Go package (under `archive/`)    | Rust crate                       | Phase | Issue                                                  |
|----------------------------------|----------------------------------|-------|--------------------------------------------------------|
| `internal/protocol/codec`        | `crates/sk-codec`                | 1     | [gh #144](https://github.com/Woestebanaan/skafka/issues/144) |
| `internal/protocol`              | `crates/sk-protocol`             | 3     | [gh #146](https://github.com/Woestebanaan/skafka/issues/146) |
| `internal/storage`               | `crates/sk-storage`              | 2     | [gh #145](https://github.com/Woestebanaan/skafka/issues/145) |
| `internal/coordinator` (groups)  | `crates/sk-coordinator`          | 5     | [gh #148](https://github.com/Woestebanaan/skafka/issues/148) |
| `internal/coordinator` (txn)     | `crates/sk-coordinator`          | 6     | [gh #149](https://github.com/Woestebanaan/skafka/issues/149) |
| `internal/broker`                | `crates/sk-broker`               | 5     | [gh #148](https://github.com/Woestebanaan/skafka/issues/148) |
| `internal/controller`            | `crates/sk-controller`           | 5     | [gh #148](https://github.com/Woestebanaan/skafka/issues/148) |
| `internal/auth`                  | `crates/sk-auth`                 | 4     | [gh #147](https://github.com/Woestebanaan/skafka/issues/147) |
| `internal/k8s` (broker side)     | `crates/sk-k8s`                  | 5     | [gh #148](https://github.com/Woestebanaan/skafka/issues/148) |
| `internal/k8s` (CR writers)      | `crates/sk-k8s`                  | 7     | [gh #150](https://github.com/Woestebanaan/skafka/issues/150) |
| `internal/observability`         | `crates/sk-observability`        | 8     | [gh #151](https://github.com/Woestebanaan/skafka/issues/151) |
| `operator/api/v1alpha1`          | `crates/sk-operator-api`         | 7     | [gh #150](https://github.com/Woestebanaan/skafka/issues/150) |
| `operator/controllers`           | `crates/sk-operator-controllers` | 7     | [gh #150](https://github.com/Woestebanaan/skafka/issues/150) |
| `cmd/skafka`                     | `bins/skafka`                    | 3     | [gh #146](https://github.com/Woestebanaan/skafka/issues/146) |
| `cmd/skafka-operator`            | `bins/skafka-operator`           | 7     | [gh #150](https://github.com/Woestebanaan/skafka/issues/150) |
| `tests/`                         | per-crate `tests/` + `crates/sk-test-harness` | per phase | — |
| `pkg/heartbeatpb`                | tonic-build output inside `crates/sk-broker` (✅ Phase 0) | 0 | — |

`proto/`, `deploy/`, `scripts/`, and the `/data/__cluster/` layout do **not** move — the Rust port reuses them verbatim. See [`docs/rewrite.md`](./docs/rewrite.md) for phase boundaries and exit criteria, [`docs/phase-0.md`](./docs/phase-0.md) for the bootstrap decisions, and the tracker issue [gh #143](https://github.com/Woestebanaan/skafka/issues/143) for live status.
