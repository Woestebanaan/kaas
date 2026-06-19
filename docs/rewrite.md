# skafka — Rust Rewrite Plan

Target: feature parity with current Go skafka (Apache Kafka 3.7 wire protocol + Kafka Streams parity, single-writer-per-partition, K8s-native, NFS-RWX substrate). Same on-disk layout, same CRDs, same Helm chart so cutover is image-only.

## Current state

- The full Go tree (`cmd/`, `internal/`, `operator/`, `pkg/`, `tests/`, `go.mod`, `Dockerfile*`, `Makefile`) has been moved to `archive/`. It is **frozen** — no new feature work; only port-blocking bugfixes.
- `proto/`, `deploy/`, and `scripts/` remain at the repo root and are reused unchanged by the Rust port.
- The Rust workspace described below lives at the repo root alongside `archive/`. Phase 0 bootstraps it; nothing has been created yet.
- See `CLAUDE.md` (top of file + the "Go → Rust crate map" at the bottom) for the package-by-package mapping.

## Guiding rules

- **Async**: `tokio` (multi-thread, work-stealing). No `async-std`, no smol.
- **Style**: free functions + plain `struct`s by default; `trait` objects only at process-edges (storage, auth, coordinator hooks). Errors via `thiserror` enums; `anyhow` only in `main`.
- **Compactness**: one file ≈ one concept; ≤ 400 LoC/file target; reach for `iter().filter().map().collect()` before loops; prefer pattern matching over `if let` chains.
- **No `unsafe`** outside one approved boundary (memory-mapped index reads, gated behind `#[cfg(feature = "mmap")]`).
- **Crate layout**: cargo workspace; one library crate per `internal/` directory equivalent; two binaries at the root.
- **Wire-protocol fidelity**: every codec change verified against a captured byte fixture (`tests/fixtures/*.bin`) before logic tests run.

## Workspace layout

```
skafka/
  Cargo.toml                    # workspace
  crates/
    sk-codec/                   # Kafka wire frames, primitives, CRC32C, KIP-482 tagged fields
    sk-protocol/                # per-API request/response types, dispatch, server bring-up
    sk-storage/                 # DiskStorageEngine, segments, manifest, idempotence, snapshot
    sk-coordinator/             # consumer-group + txn coordinator, offset store
    sk-broker/                  # Broker glue, Coordinator (assignment.json watcher), takeover
    sk-controller/              # controller-side: balancer, assignment writer, heartbeat server
    sk-auth/                    # SCRAM-256/512, mTLS, ACLs, quotas, principal mapping
    sk-k8s/                     # broker-side: BrokerRegistry, TopicWatcher, TopicCRWriter, ReadinessUpdater
    sk-observability/           # OTLP metrics + tracing, /healthz, tripwires
    sk-operator-api/            # CRD types (kube-derive)
    sk-operator-controllers/    # one reconciler per CRD
    sk-test-harness/            # franz-rs/rdkafka harnesses, byte-opacity tripwire, fixtures
  bins/
    skafka/                     # broker
    skafka-operator/            # operator
  xtask/                        # cargo xtask: gen-crds, gen-proto, fixture-capture, fmt-check
  proto/heartbeat.proto         # unchanged; tonic-build generates into sk-broker
  deploy/                       # Helm chart + CRDs (mirrored from sk-operator-api via xtask)
  scripts/                      # kafka-*.sh integration suite (unchanged; language-agnostic)
  archive/                      # frozen Go tree — reference for porting, deleted after Phase 9
```

## Crate dependency edges (no cycles)

```
sk-codec ← sk-protocol ← sk-broker ← bins/skafka
sk-storage ← sk-broker
sk-coordinator ← sk-broker
sk-auth ← sk-protocol
sk-k8s ← sk-broker
sk-controller ← sk-broker
sk-observability ← (everyone, via re-exported macros)
sk-operator-api ← sk-operator-controllers ← bins/skafka-operator
sk-operator-api ← sk-k8s   (broker uses the CRD types for typed clients)
```

---

## Phase 0 — Bootstrap (1 week)

Detailed plan: [`phase-0.md`](./phase-0.md).

**Deliverables**

- `cargo new --vcs none` workspace **at the repo root, alongside `archive/`** — all crates listed above as empty libs.
- `rust-toolchain.toml` pinning stable (latest at start; document MSRV).
- `Cargo.toml` workspace-deps: `tokio`, `bytes`, `serde`, `serde_json`, `thiserror`, `anyhow`, `tracing`, `tracing-subscriber`, `opentelemetry`, `opentelemetry-otlp`, `kube`, `k8s-openapi`, `schemars`, `rustls`, `tokio-rustls`, `rcgen` (test certs), `tonic`, `prost`, `prost-build`, `tonic-build`, `crc32c`, `uuid`, `hmac`, `sha2`, `base64`, `regex`, `notify` (fsnotify equivalent), `dashmap`, `parking_lot`, `arc-swap`, `bytestring`.
- Dev-deps: `proptest`, `insta` (snapshot), `tokio-test`, `tempfile`, `wiremock`.
- `xtask gen-proto` + `xtask gen-crds` plumbing.
- CI: replace `.github/workflows/ci.yml` with a Rust pipeline — `cargo fmt --check`, `cargo clippy -D warnings`, `cargo test --workspace --all-features`, `cargo build --release` both bins, Helm lint, CRD-drift check. Keep a `legacy-go` job that runs `cd archive && go vet ./... && go test ./...` until the Go release line is retired in Phase 9; ditto the existing `.github/workflows/docker-publish.yml` (now building from `archive/Dockerfile*`).
- `rustfmt.toml`, `clippy.toml` (deny `unwrap_used` outside tests, deny `as_conversions`).

**Exit criteria**: empty workspace builds + tests green; CI runs in < 4 min for the empty graph.

---

## Phase 1 — Wire codec (2 weeks)

**Scope**: bit-perfect equivalent of `internal/protocol/codec/`.

**Modules in `sk-codec`**

- `primitives.rs` — `read_i8/i16/i32/i64/varint/varlong/string/compact_string/bytes/compact_bytes/uuid`, mirror writers; all return `Result<T, CodecError>`. Trait `Decode` / `Encode` with `&mut Cursor<&[u8]>` / `&mut BytesMut`. Provided impls for primitive types.
- `frame.rs` — request/response length-prefixed framing, async `tokio::io::AsyncReadExt` helpers.
- `crc.rs` — record-batch CRC32C wrapper over `crc32c` crate.
- `tagged.rs` — KIP-482 flexible tagged-field block.
- `api/` — one module per API key (mod files generated from a single declarative table in `api/registry.rs`). Each module exposes `Request`, `Response`, `ALL_VERSIONS`.
- `headers.rs` — request/response header v0/v1/v2.

**Test pattern**

- `tests/fixtures/` — raw bytes captured from Apache Kafka 3.7 + librdkafka + franz-rs. One binary file per (api_key, version, direction). Roundtrip test: decode → re-encode → byte-equal.
- `proptest` round-trip on every type.
- Cross-port: Go `byte-opacity` tripwire fixtures imported verbatim.

**Exit criteria**: every API key/version the Go broker supports decodes/encodes byte-identically against fixtures.

---

## Phase 2 — Storage engine (3 weeks)

**Scope**: `internal/storage/` equivalent, including idempotent-producer state.

**Modules in `sk-storage`**

- `engine.rs` — `DiskStorageEngine` struct + `StorageEngine` trait (the seam tests mock).
- `partition.rs` — `Partition` (was `partitionState`): active segment, manifest, producer state. `Mutex<PartitionInner>` for the rare write path; hot read path via `ArcSwap<Snapshot>`.
- `segment.rs` — segment file open / append / fsync / roll. Epoch-prefixed filenames (`<epoch>-<baseOffset>.log` / `.index`). `rollFast` (in-mem swap under lock) + `finalize_async` (index fsync + manifest write off-lock).
- `index.rs` — sparse offset index; lazily mmap'd via `memmap2` (feature-gated); rebuild on takeover.
- `manifest.rs` — `serde_json` `{epoch, hwm, log_start_offset}`. Atomic write via tempfile + `rename`.
- `committer.rs` — per-partition committer task: drains `mpsc::Receiver<FlushReq>` and calls one `Sync` per cycle; `Notify` for the appender wakeup (was `sync.Cond`).
- `idempotence.rs` — `producer_states: DashMap<i64, ProducerEntry>`; 5-batch ring buffer. `classify_idempotence` returns `enum Outcome { Duplicate(i64), OutOfOrder, InvalidEpoch, Accept }`.
- `producer_snapshot.rs` — `producer-state.snapshot` next to manifest; written on segment roll + `Relinquish`.
- `recover.rs` — `recover_segment` + `rebuild_index` on takeover.
- `takeover.rs` — `take_over(part_key)` opens fds; `relinquish(part_key)` flushes manifest + producer snapshot + closes fds.
- `cleaner.rs` — log retention (size + age) + compaction with `min.compaction.lag.ms` + `delete.retention.ms` enforcement (gh #116).
- `delete_records.rs` — `log_start` advance, active-segment reclaim.

**Concurrency model**

- One `tokio::task` per partition committer.
- Append path: `Mutex<PartitionInner>` for the critical section; `Notify` releases waiting appenders after the sync.
- Reads: lock-free via `ArcSwap` snapshot (manifest + active-segment pointer atomically swapped on roll).

**Test pattern**

- `proptest` over `(produce sequence, sync interval) -> (recovered log == produced log)`.
- Fault-injection wrapper (`FailingFs`) for fsync errors, partial writes.
- Cross-engine equivalence: replay a fixture against `MemoryStorage` and `DiskStorageEngine`, assert byte-equal log files.

**Exit criteria**: storage benchmarks within 10% of Go's per-fsync throughput on tmpfs and NFS-loopback; recovery tests pass under crash injection.

---

## Phase 3 — Single-broker server (2 weeks)

**Scope**: enough to serve Produce/Fetch/Metadata/ApiVersions to a real client. No auth, no cluster.

**Modules in `sk-protocol`**

- `server.rs` — `Server::serve(listeners, dispatcher)`. Accepts on `tokio::net::TcpListener`. Per-connection task → `handle_connection`.
- `connstate.rs` — `ConnState { listener_name, peer_addr, principal: Option<Principal> }`.
- `dispatch.rs` — `Dispatcher::dispatch(req, conn) -> Response`. Pre-auth gate stub (always allow in this phase).
- `handlers/` — one file per API. Each handler is a free `async fn handle(req, ctx) -> Result<Response>`. `ctx` is a `&HandlerCtx` aggregating `&dyn StorageEngine`, `&TopicRegistry`, etc.
  - `produce.rs`, `fetch.rs` (stateless `session_id = 0`, gh #4), `metadata.rs`, `api_versions.rs`, `init_producer_id.rs`, `list_offsets.rs` (timestamp lookup, gh #5).

**Modules in `sk-broker`**

- `broker.rs` — `Broker` struct: owns the storage engine, the in-memory `TopicRegistry`, the local lease manager.
- `local_lease.rs` — `LocalLeaseManager` (dev-mode: always "yes, I lead").
- `cli.rs` — env parsing (`SKAFKA_LISTENERS` JSON, `SKAFKA_DATA_DIR`, `SKAFKA_FLUSH_INTERVAL_MESSAGES`, etc.). `serde` deserializes the listener array directly.

**bins/skafka**

- `main.rs`: parse env → build `Broker` → `Server::serve`. Graceful SIGTERM: drain → `RelinquishAll` → exit.

**Test pattern**

- `sk-test-harness::franz` (rdkafka or franz-rs wrapper) runs against an in-process server: produce 1k records, fetch them back, byte-equal.
- `console-producer` / `console-consumer` smoke against a real binary in a tmpdir.

**Exit criteria**: `kafka-console-producer` + `kafka-console-consumer` against the Rust binary, single broker, in-memory and on-disk modes.

---

## Phase 4 — Auth (2 weeks)

**Scope**: `internal/auth/` equivalent. Per-listener authentication, cluster-wide authorization, quotas.

**Modules in `sk-auth`**

- `engine.rs` — `trait AuthEngine` (`requires_pre_auth`, `authenticate_sasl`, `authenticate_tls`). `AllowAll`, `Real`.
- `selector.rs` — `AuthEngineSelector::for_listener(name) -> Arc<dyn AuthEngine>`.
- `scram.rs` — SCRAM-SHA-256/512 server (state machine over `client_first` / `server_first` / `client_final` / `server_final`). `hmac` + `sha2` crates.
- `plain.rs` — SASL PLAIN.
- `mtls.rs` — peer-cert extraction via `tokio-rustls`; subject-DN as input to principal mapping.
- `principal_mapping.rs` — Apache `ssl.principal.mapping.rules` parser (regex + `$N` back-refs + `/L` `/U` postfix flags).
- `authorizer.rs` — `trait Authorizer`. `AllowAll`, `SuperUser(wraps inner)`, `Real(acls)`.
- `acls.rs` — load `acls.json`, evaluate `(principal, operation, resource)`. Hot-reload via `notify` on the file.
- `credentials.rs` — load `credentials.json`, hot-reload.
- `quota.rs` — `trait QuotaChecker`. `No`, `Real`. Token-bucket with debt-carry (gh #125) — bucket allowed to go negative; throttle proportional to debt.

**Wire-up in `sk-protocol`**

- `dispatch.rs` — pre-auth gate (`requires_pre_auth` blocks all non-handshake APIs).
- `handlers/sasl.rs` — `SaslHandshake` + `SaslAuthenticate`; routes by listener.
- `handlers/produce.rs` / `fetch.rs` — call `authorizer.authorize(principal, op, resource)` + `quotas.check_*`.

**Test pattern**

- SCRAM exchange byte-fixtures from Apache Kafka 3.7 (capture once with `tcpdump`).
- `proptest` round-trip on principal mapping (random subject DN, generate rule, assert output).
- Quota test: N concurrent token-bucket clients converge to `rate`, not `N × rate` (was the gh #125 bug).

**Exit criteria**: mTLS + SCRAM + PLAIN all green against an external Kafka client; ACLs deny + super-users override; quota debt-carry test passes.

---

## Phase 5 — Coordinator & assignment (3 weeks)

**Scope**: multi-broker. `internal/broker/`, `internal/controller/`, `internal/coordinator/` equivalents.

**Modules in `sk-broker`**

- `coordinator.rs` — `Coordinator { assignment: ArcSwap<Assignment>, ... }`. fsnotify + 1s poll on `/data/__cluster/assignment.json`. `Owns(topic, partition) -> bool`, `LeaderFor(...) -> BrokerID`, `OwnsGroup(group_id)`, `GroupCoordinator(group_id)` (two-tier: explicit assignment → hash fallback, gh #92).
- `group_hash.rs` — `hash(group_id) % num_brokers` with preferred-slot-down deterministic alternate.
- `takeover.rs` — `TakeoverDriver` subscribes to `Coordinator::on_assignment_change` → opens / relinquishes partitions in storage engine.
- `group_takeover.rs` — `GroupTakeoverDriver` (incl. orphan sweep, gh #89).
- `controller_watch.rs` — 1s poll of `skafka-controller` K8s Lease for current epoch.
- `self_fence.rs` — broker self-terminates if it observes a higher epoch than its own.
- `heartbeat_client.rs` — gRPC client to the current controller (epoch-bumped on Lease change).

**Modules in `sk-controller`**

- `election.rs` — acquire/renew `skafka-controller` K8s Lease.
- `heartbeat_server.rs` — tonic gRPC server (`proto/heartbeat.proto`).
- `balancer.rs` — partition + consumer-group assignment computation.
- `assignment_writer.rs` — write `/data/__cluster/assignment.json` (epoch-prefixed, atomic via tempfile + rename).
- `cr_mirror.rs` — fire-and-forget `KafkaClusterAssignments` CR update.
- `recompute.rs` — recompute on (initial Lease win, topic CR change, broker join/leave).

**Modules in `sk-coordinator`**

- `manager.rs` — `coordinator::Manager`; hot-swappable `GroupAssignmentSource` (gh #92) + `TxnAssignmentSource` (gh #91).
- `offset_store.rs` — per-group offset files under `/data/__cluster/__consumer_offsets/`; staged `pending` layer keyed by `(group_id, pid)` for txn offsets.
- `group.rs` — consumer-group state machine (Empty / PreparingRebalance / CompletingRebalance / Stable / Dead).
- `handlers/` — `find_coordinator`, `join_group`, `sync_group`, `heartbeat`, `offset_commit`, `offset_fetch`, `list_groups`, `describe_groups`, `delete_groups` (gh #89).

**Test pattern**

- `tests/controller-failover/` ported: kill the controller, assert new leader writes a higher epoch, partition leadership migrates cleanly.
- `tests/stale-controller-race/` ported: assert stale-epoch writes rejected.
- Property test: `hash(group_id) % N` is stable; with one broker down, the alternate is deterministic.

**Exit criteria**: 3-broker cluster in `kind` reassigns within 5s of a broker kill; group rebalance against `kafka-consumer-perf-test` passes.

---

## Phase 6 — Transactions, idempotence, advanced features (2 weeks)

**Scope**: gh #12, #22, #23–#30, #37, #91, #108, #114, #116.

**Modules in `sk-coordinator`**

- `txn_state.rs` — `TxnStateStore` slot-sharded across `/data/__cluster/txn_state/slot-N.json` (50 slots, matches Apache `transaction.state.log.num.partitions`). `Get`, `Put`, `AbortOverdueOwned`. Gates on `OwnsTxn` (gh #91).
- `txn_handlers/` — `init_producer_id` (epoch+1 on rejoin, gh #22), `add_partitions_to_txn`, `add_offsets_to_txn`, `txn_offset_commit` (stages via `offset_store.store_pending`), `end_txn` (`Ongoing → CompleteCommit/CompleteAbort`, fires `TxnOffsetHook`).
- `txn_reaper.rs` — 10s tick (`tokio::time::interval`); walks owned slots, transitions overdue `Ongoing → CompleteAbort`, bumps epoch, fires offset hook.
- `fence_log.rs` — cross-broker producer-epoch fence broadcast (gh #108 phase 2). Per-broker file under `/data/__cluster/fence_log/`; peer brokers poll + apply.

**Modules in `sk-storage`**

- `fence_watcher.rs` — applies inbound fences to local `producer_states`.

**Wire-up**

- `bins/skafka/main.rs` — start `txn_reaper` and `fence_watcher` after coordinator is ready.

**Test pattern**

- Port `tests/kafka-compat/eos_v2_test.go` — franz-rs (or rdkafka with `enable.idempotence=true` + `transactional.id`) consume-process-produce-commit cycle, assert exactly-once.
- Property test on `TxnStateStore`: any sequence of `{begin, add_partitions, end_commit, end_abort}` converges; reaper aborts iff `Ongoing` and overdue and owned.

**Exit criteria**: EOS test passes single-broker; multi-broker case behind feature flag pending gh #114 (`WriteTxnMarkers` RPC).

---

## Phase 7 — Operator (3 weeks)

**Scope**: `cmd/skafka-operator` + `operator/api/` + `operator/controllers/` equivalents.

**Modules in `sk-operator-api`**

- `kafkacluster.rs`, `kafkatopic.rs`, `kafkauser.rs`, `kafkaclusterassignments.rs` — `#[derive(CustomResource, Deserialize, Serialize, JsonSchema)]` via `kube-derive`. `schemars` produces the schema; `xtask gen-crds` writes `deploy/crds/*.yaml` + mirrors to Helm chart.
- KafkaUser shape mirrors Strimzi (`spec.authentication`, `spec.authorization.acls`); `KafkaACL` and `KafkaUserGroup` are intentionally absent (gh #135 closed them).
- `KafkaTopic.spec.quotas.producerMaxByteRatePerBroker` / `consumerMaxByteRatePerBroker` — keep the named-honestly per-broker semantics.
- `KafkaTopic.status.topic_id` — v4 UUID, generated cryptographically on first reconcile, never rotated (gh #105).

**Modules in `sk-operator-controllers`**

- `kafkacluster.rs` — external listener plumbing: cert-manager `Certificate`, Gateway `TLSRoute`, per-broker hostnames. `OwnerReferences` on every owned external resource (no finalizers — gh ArgoCD cascade-delete fix).
- `kafkatopic.rs` — materializes partition dirs on the PVC; reconcile-time best-effort cleanup on `Get → NotFound`; mints `Status.TopicID` on first reconcile.
- `kafkauser.rs` — materializes `credentials.json` + `acls.json` entries.
- `sweep.rs` — leader-elected startup sweep: `sweep_topics`, `sweep_credentials` drop orphans the reconciler missed.

**Wire-up via `kube` + `controller-rs`**

- Each reconciler is a `kube::runtime::Controller::new(...).run(reconcile, error_policy, ctx)`.
- Reconcile fn is a free `async fn reconcile(obj: Arc<Cr>, ctx: Arc<Ctx>) -> Result<Action>`.
- Leader election via `kube::runtime::leader_election` (or built-in `Lease` lock).

**bins/skafka-operator**

- `main.rs` — start all four controllers in `tokio::select!` with shared `kube::Client`; `tracing` + OTLP bring-up.

**Test pattern**

- `kube::Client::try_default()` against a `kind` cluster in CI.
- Reconciler property: any sequence of CR `{create, update, delete}` events converges to the same filesystem state regardless of ordering.

**Exit criteria**: existing Helm chart deploys the Rust operator + Rust broker into `kind`; the kafka-compat suite passes.

---

## Phase 8 — Observability, Helm, parity validation (2 weeks)

**Modules in `sk-observability`**

- `tracing.rs` — `tracing-subscriber` + `tracing-opentelemetry` + `opentelemetry-otlp` (gRPC push mode, same Prometheus OTLP receiver as Go).
- `metrics.rs` — push-mode OTLP meter; counters/histograms registered up-front; same metric names as Go (so Grafana dashboards keep working).
- `health.rs` — `/healthz` HTTP handler (axum); same JSON shape as Go.
- `tripwires.rs` — byte-opacity counters.

**Helm chart**

- No change to `deploy/helm/skafka/`. Image tags swap to `ghcr.io/.../skafka-rs:vX.Y.Z`.
- Env-var names unchanged (`SKAFKA_*`).
- CRD shape unchanged.
- `xtask gen-crds` mirrors `crates/sk-operator-api` → `deploy/crds/` and `deploy/helm/skafka/crds/`. CI fails on drift.

**Parity validation**

- Port `scripts/kafka-*.sh` integration suite as-is (it's shell, hits the wire — no language coupling).
- Port `tests/byte-opacity/` and `tests/kafka-compat/` test bodies; reuse the captured fixtures.
- Run `bench-compare` (skafka vs Strimzi) — must land within 5% of Go's numbers before cutover.
- Run franz-go EOS suite via `rdkafka` C extension (or franz-rs) against the Rust broker.

**Exit criteria**: every script in `scripts/kafka-*.sh` exits 0 or 77; bench-compare matches Go ±5%; EOS suite green.

---

## Phase 9 — Cutover (1 week)

- Tag `v0.2.0-preview` (first Rust release). Patch-bump from there per the existing release policy.
- Dual-publish images (`skafka:v0.1.N-preview` Go + `skafka-rs:v0.2.0-preview` Rust) for one release.
- Helm chart adds `image.flavor: go | rust` (default `go`).
- Bake-time test on the existing skafka-migration-parity project: run the matrix against both flavors for 72 h.
- Flip default to `rust`; Go binaries deprecated; new development on Rust only.
- After two clean releases, delete the Go tree.

---

## Cross-phase work (continuous)

- **Fixtures**: every wire-affecting change adds/updates a binary fixture. Reviewer checks the diff includes a fixture before approving.
- **Benchmarks**: `cargo bench` lane on the storage engine, codec, and the produce hot path; CI publishes deltas as PR comments.
- **Audit**: `cargo audit` + `cargo deny` in CI from Phase 0.
- **Docs**: each crate ships a `README.md` with one diagram + one example. `CLAUDE.md` updated per phase.

## Effort estimate

| Phase | Weeks | Cumulative |
|------:|------:|-----------:|
| 0     | 1     | 1          |
| 1     | 2     | 3          |
| 2     | 3     | 6          |
| 3     | 2     | 8          |
| 4     | 2     | 10         |
| 5     | 3     | 13         |
| 6     | 2     | 15         |
| 7     | 3     | 18         |
| 8     | 2     | 20         |
| 9     | 1     | 21         |

~5 months for a single engineer; ~3 months with two engineers (codec + storage parallelize; operator + observability parallelize).

## Risks & mitigations

- **NFS semantics divergence**: Rust `std::fs` on Linux maps to the same syscalls as Go, but `fsync` error reporting differs. Mitigation: a `Fs` trait in `sk-storage` with a single concrete impl; all error paths go through it so a future swap (e.g. `io_uring`) is mechanical.
- **`kube-rs` CRD-derive vs `controller-gen` shape**: schemars output may differ subtly from kubebuilder. Mitigation: `xtask gen-crds` writes both forms and `xtask check-crd-shape` diffs against a golden file captured from the Go output for one release.
- **librdkafka transactional client edge cases**: hardest part of Phase 6. Mitigation: keep franz-go EOS test wired up as a smoke during Phase 6 (run via Go bin against Rust broker).
- **Performance regression on initial Rust port**: Phase 8 gate is ±5% of Go; if Phase 2 benchmarks miss by > 30%, pause and profile before continuing.
