# Phase 5 — Coordinator & assignment

Detailed work plan for the sixth phase of the Rust rewrite. Companion to
[`rewrite.md`](./rewrite.md); the high-level summary lives there. Builds
on the codec scaffolding from [`phase-1.md`](./phase-1.md), the storage
engine from [`phase-2.md`](./phase-2.md), the single-broker server from
[`phase-3.md`](./phase-3.md), and the auth surface from
[`phase-4.md`](./phase-4.md).

**Goal.** Replace `LocalLeaseManager` with a real `Coordinator` driven
by `/data/__cluster/assignment.json`; light up consumer-group
coordination + offset commit/fetch; bring up the controller side
(election + heartbeat gRPC + balancer + assignment writer); wire
`kube` into the broker for the read-only paths (`BrokerRegistry`,
`TopicWatcher`, controller-`Lease` poll, readiness gate). A 3-broker
`kind` cluster running the Rust binary reassigns within 5 s of a kill
and survives `kafka-consumer-perf-test` against a 6-partition topic
with `kafka-console-consumer --group g`.

**Length.** ~3 weeks, single engineer. Workstream A (codec backfill)
is the biggest single chunk; B/C/D/E land in parallel after A; F/G
thread them through; H closes with the multi-broker smoke.

**Out of scope for Phase 5.**

- Transactional coordinator (`TxnStateStore`, `txn_*` handlers,
  `fence_log`) — Phase 6.
- Operator CR *writers* (`KafkaTopic.Spec.Partitions` patch,
  `KafkaCluster` reconciler, etc.) — Phase 7. Phase 5 reads
  `KafkaTopic` CRs only.
- `IncrementalAlterConfigs` (key 44) and `CreatePartitions` (key 37)
  — they patch `KafkaTopic.Spec`. Skeleton handlers return
  `UNSUPPORTED_VERSION` until Phase 7 has the CR writers.
- `DescribeConfigs` (key 32) — Phase 7 alongside admin shape.
- `AlterClientQuotas` (48) / `DescribeClientQuotas` (49) — accessors
  landed in Phase 4; admin handlers are thin wrappers, land in Phase 7
  with admin shape.
- `WriteTxnMarkers` (key 27, gh #114) — Phase 6.
- `notify`-driven inotify on `assignment.json` — Phase 5 ships the 1 s
  poll; inotify is a Phase 8 perf nicety.
- Sticky-rebalance via explicit `consumerGroups[]` overrides on
  `assignment.json` — Phase 5 ships hash-only `BalanceGroups`; the
  overrides path stays a forward-compat shim per the gh #92 note in
  `CLAUDE.md`.

**Scope boundary (what real clients exercise).**
`kafka-console-consumer --group g --from-beginning` and
`kafka-consumer-perf-test` complete one full join → sync → fetch →
commit cycle; a second consumer joining triggers a rebalance and both
members get non-overlapping partition sets;
`kafka-consumer-groups --list / --describe / --delete` all work
against any broker in the cluster (the broker routes to the
coordinator-of-G via `FindCoordinator` exactly like Apache). Killing
the controller broker triggers re-election and assignment refresh
within 5 s; killing a non-controller broker triggers partition
takeover by the controller's recompute.

---

## Workstreams

Eight workstreams. A blocks F. B/C/D/E land in parallel after A. F
threads through `sk-protocol`. G closes the K8s seam. H is the
multi-broker smoke.

- **A** — Codec backfill (11 new API modules: keys 8–16, 42, 47)
- **B** — `sk-coordinator`: `Manager`, `OffsetStore`, group state
  machine, group handlers
- **C** — `sk-broker`: `Coordinator` (assignment.json watcher),
  `group_hash`, takeover drivers, `controller_watch`, `self_fence`,
  `heartbeat_client`
- **D** — `sk-controller`: election (K8s Lease), heartbeat gRPC
  server, balancer, assignment writer, K8s mirror
- **E** — `sk-k8s`: `BrokerRegistry`, `BrokerIdentity`,
  `TopicWatcher`, `ReadinessUpdater`, common `kube::Client` plumbing
- **F** — `sk-protocol` / `sk-broker` wire-up: register the new
  handlers; thread the real `Coordinator` into Produce / Fetch
  ownership + Metadata leader lookup
- **G** — `bins/skafka` main: replace `LocalLeaseManager`; spawn
  controller-mode tasks under the election callback; wire
  `ReadinessUpdater` to the gate
- **H** — Tests: port `tests/controller-failover`,
  `tests/stale-controller-race`; multi-broker `kind` smoke against
  `kafka-consumer-perf-test`

Dependencies: A blocks F; B/C/D/E land in parallel; F blocked by
B+C+E; G blocked by F; H blocked by G.

---

## A — Codec backfill (11 new modules)

`crates/sk-codec/src/api/` grows 11 entries. Versions / flexibility
table (matches `archive/internal/broker/broker.go:555-891`):

| Key | API              | Versions | Flexible from | Notes                                                                                                    |
|----:|------------------|---------:|--------------:|----------------------------------------------------------------------------------------------------------|
|   8 | OffsetCommit     | 0–8      | 8             | metadata field per-partition; v6+ carries `committed_leader_epoch`                                       |
|   9 | OffsetFetch      | 0–8      | 6             | v7+ batch-fetch many groups in one request                                                               |
|  10 | FindCoordinator  | 0–4      | 3             | `key_type` field in v1+ (group / txn); v4+ batches many keys                                             |
|  11 | JoinGroup        | 0–9      | 6             | v5+ adds `group_instance_id`; protocol metadata is byte-opaque (`Bytes`)                                 |
|  12 | Heartbeat        | 0–4      | 4             | trivial; v3+ adds `group_instance_id`                                                                    |
|  13 | LeaveGroup       | 0–5      | 4             | v3+ multi-member batch                                                                                   |
|  14 | SyncGroup        | 0–5      | 4             | `assignments[]` byte-opaque blob per member (Kafka client-side partition-assignor output)                |
|  15 | DescribeGroups   | 0–5      | 5             | v5+ `include_authorized_operations` field                                                                |
|  16 | ListGroups       | 0–4      | 3             | v4+ `states_filter` field                                                                                |
|  42 | DeleteGroups     | 0–2      | 2             | (gh #89)                                                                                                 |
|  47 | OffsetDelete     | 0–0      | never         | non-flexible; one shot                                                                                   |

Each module follows the `sasl_authenticate.rs` shape: owning
`String` / `Bytes` types (these are control-path, not hot-path),
`Decode` / `Encode` helpers, `pub const SPEC: ApiSpec`,
`request_hdr` / `response_hdr` const fns, registry row added to
`api::registry::ALL`.

Fixtures: capture one request + response per (key, version) against
Apache 3.7 driven by `kafka-console-consumer --group g` +
`kafka-consumer-groups`. Use the `xtask fixture-capture` machinery
Phase 3 stood up. Roundtrip byte-equal + `proptest` round trip +
`record_decode_count()` tripwire at end-of-test.

**Exit:** `sk_codec::api::registry::ALL.len() == 19`; fixture
round-trip green for at least one version per new key.

---

## B — `sk-coordinator`: Manager, OffsetStore, group state machine

`crates/sk-coordinator/src/lib.rs` grows the module tree, mirroring
`archive/internal/coordinator/`:

```rust
pub mod errors;
pub mod manager;
pub mod offset_store;
pub mod group;
pub mod handlers;            // find_coordinator, join, sync, heartbeat,
                             // leave, offset_commit, offset_fetch,
                             // list_groups, describe_groups, delete_groups,
                             // offset_delete
```

### `offset_store.rs`

Port `archive/internal/coordinator/offsets.go` 1:1. Per-group file
under `<data_dir>/__cluster/__consumer_offsets/<groupID>.json`; same
JSON shape so a cutover Phase-9 broker reads the same files. APIs:

```rust
pub struct OffsetStore { /* … */ }

impl OffsetStore {
    pub fn commit(&self, group_id: &str, offsets: HashMap<String, i64>) -> io::Result<()>;
    pub fn commit_with_metadata(&self, group_id: &str, offsets: HashMap<String, i64>,
                                md: HashMap<String, String>) -> io::Result<()>;
    pub fn fetch(&self, group_id: &str, specs: &[FetchSpec]) -> HashMap<String, i64>;
    pub fn fetch_metadata(&self, group_id: &str, specs: &[FetchSpec]) -> HashMap<String, String>;
    pub fn has_group(&self, group_id: &str) -> bool;
    pub fn delete(&self, group_id: &str) -> io::Result<()>;
    pub fn delete_partitions(&self, group_id: &str, keys: &[String])
        -> io::Result<HashMap<String, bool>>;
    pub fn load(&self, group_id: &str) -> io::Result<()>;

    // Phase 6 surface — Phase 5 ships the API for the trait but the
    // commit/discard paths are no-ops behind a feature flag until txn
    // handlers wire up.
    pub fn store_pending(&self, group_id: &str, pid: i64, offsets: HashMap<String, i64>);
    pub fn commit_pending(&self, group_id: &str, pid: i64) -> io::Result<()>;
    pub fn discard_pending(&self, group_id: &str, pid: i64);
}
```

Atomic write: tempfile + `rename` via `sk-storage::atomic_write`
(re-exported, or duplicated as a free fn — the storage and coord
sides both want it). The fdsync after rename happens inside the
helper, matching Go's `_persistFsync`.

### `group.rs`

Port `archive/internal/coordinator/group.go`. State machine:

```rust
pub enum GroupState { Empty, PreparingRebalance, CompletingRebalance, Stable, Dead }

pub(crate) struct Group {
    pub id: String,
    pub state: GroupState,
    pub generation: i32,
    pub leader: Option<String>,
    pub protocol_type: String,
    pub protocol: Option<String>,
    pub members: HashMap<String, GroupMember>,
    pub pending_members: HashSet<String>,
    pub rebalance_timer: Option<tokio::task::JoinHandle<()>>,
    /* … */
}

pub(crate) struct GroupMember {
    pub id: String,
    pub group_instance_id: Option<String>,
    pub client_id: String,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub subscription: Bytes,        // byte-opaque protocol metadata
    pub assignment: Option<Bytes>,  // set by sync()
    pub last_heartbeat: Instant,
    pub join_waker: Option<JoinWaker>,
    pub sync_waker: Option<SyncWaker>,
}
```

The Go version uses `chan` for `joinWaiters[]` / `syncState`. In
tokio:

- `JoinWaker` / `SyncWaker` = `tokio::sync::oneshot::Sender<Response>`
  stored against the member; the handler future awaits its end.
- `rebalance_timer` uses `tokio::time::sleep_until(deadline)` spawned
  as a tracked task; cancel via `JoinHandle::abort`.
- The `initialRebalanceDelayMs = 3000` constant sits in `group.rs`.

Locking: one `Mutex<HashMap<String, Arc<RwLock<Group>>>>` on
`Manager`, one `RwLock<Group>` per group — matches the Go
`sync.Mutex` shape (groups are coarse-grained).

### `manager.rs`

Port `archive/internal/coordinator/coordinator.go`. Ownership comes
from a `GroupAssignmentSource` trait — Phase 5 wires it to
`sk_broker::Coordinator`; tests use `LocalGroupSource`:

```rust
pub trait GroupAssignmentSource: Send + Sync + 'static {
    fn owns_group(&self, group_id: &str) -> bool;
    fn group_coordinator(&self, group_id: &str) -> Option<BrokerId>;
}

pub struct Manager {
    pub broker_id: BrokerId,
    pub offsets: Arc<OffsetStore>,
    pub groups: DashMap<String, Arc<RwLock<Group>>>,
    pub group_source: ArcSwap<Arc<dyn GroupAssignmentSource>>,
    pub txn_source: ArcSwap<Arc<dyn TxnAssignmentSource>>,  // Phase 6
}
```

`set_group_assignment_source` does an `ArcSwap::store` (the gh #92
hot-swap; `bins/skafka/main.rs` calls it once after
`sk-broker::Coordinator` boots — the bootstrap source is a
`LocalGroupSource` stub, swapped at runtime). Behavior calls follow
the Go shape verbatim: `find_coordinator`, `join_group`,
`sync_group`, `heartbeat`, `leave_group`, `offset_commit`,
`offset_fetch`, `delete_groups`, `delete_offsets`, `describe_groups`,
`list_groups`, `relinquish_group`, `local_groups`.

### `handlers/`

One free `async fn` per API, each in its own file, registered via the
Phase 3 `Handler` trait. They route through `&Manager` (held on
`HandlerCtx`). Same shape as the Phase 3 produce / fetch handlers.

`OffsetCommit` calls
`authorizer.authorize(principal, Resource::group(id), Operation::Read)`
per-partition (matches Apache); `OffsetFetch` uses
`Operation::Describe`. The same lookup-then-authorize pattern Phase 4
wired through produce / fetch — no new auth surface, just additional
call sites.

**Exit:** `cargo test -p sk-coordinator` green; the consumer-group
state machine drives one round (join → sync → heartbeat → commit →
fetch); `OffsetStore` round-trips through tempdir.

---

## C — `sk-broker`: Coordinator + group hash + takeover + heartbeat client

### `assignment.rs` (new — Rust analogue of `kafkaapi.Assignment`)

`pkg/kafkaapi/assignment.go` doesn't have a target crate; the type
lives in Go's "shared types" package. In Rust we own it on `sk-broker`
(it's the broker's view) and re-export from `sk-controller`. Use
`serde_json` against the verbatim Go JSON shape:

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Assignment {
    pub controller_epoch: i64,
    pub assignment_version: i64,
    pub generated_at: String,          // RFC3339Nano; cutover requires byte-equal
    pub controller: String,
    pub brokers: Vec<BrokerAssignment>,
    pub partitions: Vec<PartitionAssignment>,
    #[serde(default)]
    pub consumer_groups: Vec<ConsumerGroupAssignment>,
}
```

Match Go's `time.RFC3339Nano` so a Rust-written file is byte-
comparable against a Go-written one (cutover requirement).

### `coordinator.rs`

Port `archive/internal/broker/coordinator.go`. Watcher behavior:

- `notify::RecommendedWatcher` on
  `<data_dir>/__cluster/assignment.json` (parent-dir watch + filename
  filter, matches Go's fsnotify usage).
- 1 s `tokio::time::interval` poll as safety net (Go uses
  `time.NewTicker(time.Second)`).
- On change: parse, validate epoch ≥ current, atomic-swap
  `ArcSwap<Arc<Assignment>>`, rebuild `Owns` / `LeaderFor` lookup
  maps, fire registered handlers via a
  `Vec<AssignmentChangeHandler>` (each handler is
  `Arc<dyn Fn(prev, next) + Send + Sync>` — async-shaped means the
  handler spawns its own task; Go does this too).
- `OwnsGroup` / `GroupCoordinator` / `OwnsTxn` / `TxnCoordinator`
  two-tier: explicit `consumer_groups[]` entry → that; else hash
  fallthrough via `group_hash.rs`. Implements
  `GroupAssignmentSource` so
  `Manager::set_group_assignment_source(c.clone())` works.
- `CurrentEpoch(topic, partition)` returns the partition's epoch
  field; the storage engine consumes this in
  `append(... epoch=...)` — replaces the `epoch=0` hardcode in
  Phase-3 produce.

### `group_hash.rs`

Port `archive/internal/broker/group_hash.go` line-for-line. The hash
is documented (FNV-1a 32-bit of `groupID || "/coord"` etc); read the
Go source for the exact formula. `coordinator_slot(key, n)` returns
`slot % n`; `pick_*_coordinator` falls forward in a deterministic-
rotation pattern when the preferred slot is down. The Go
`TestGroupCoordinatorSlot_Stable` table ports verbatim.

### `takeover.rs` and `group_takeover.rs`

Two `AssignmentChangeHandler` impls. `TakeoverDriver` walks `prev` vs
`next.partitions`, calls `engine.take_over(part_key)` for newly-owned
partitions and `engine.relinquish(part_key)` for newly-released ones.
`GroupTakeoverDriver` does the same for `consumer_groups[]` plus an
orphan-sweep over `mgr.local_groups()` (gh #89, fix for the stale
`--list` symptom).

### `controller_watch.rs`

Port `archive/internal/broker/controller_watch.go`. 1 s
`kube::Api::<Lease>::get("skafka-controller")` poll; tracks
`lease.spec.holder_identity` + `lease.spec.lease_transitions` as the
cluster epoch. Used by `self_fence.rs`: if observed epoch > our last-
applied epoch and our heartbeat is stale, broker self-terminates
(`std::process::exit(2)`). Matches the Go contract — fences out
partitioned ex-controllers from re-writing `assignment.json`.

### `heartbeat_client.rs`

Port `archive/internal/broker/heartbeat_client.go`. tonic-built
client (`crates/sk-broker/build.rs` already runs `tonic-build`
against `proto/heartbeat.proto`):

- Long-lived bidirectional `Stream` call.
- 1 s ticker pushes a `BrokerStatus` upstream (broker_id,
  timestamp_ms, last_seen_assignment_version, partitions[],
  active_groups[]).
- Receive loop forwards `ControllerCommand`s to a `CommandHandler`
  callback (PING → noop, LEAVING → bump reconnect,
  ASSIGNMENT_CHANGED → trigger `Coordinator::poll_now`).
- Target resolution via `WithTargetFunc(fn() -> String)` — same shape
  as Go; the broker passes a closure that reads
  `ControllerWatch::current_holder()` to resolve the controller's DNS
  name.
- Reconnect-with-backoff loop in `run`. `tonic` does the gRPC heavy
  lifting; `last_received` tracked as `AtomicI64` for `self_fence`.

`stubs.go` in Go is a test seam exporting `Coordinator{...}`
internals — we use `pub(crate)` + `#[cfg(test)]` exports instead.
`topics.go` is already covered by `topic_registry.rs`, just needs
`set_topic_id`, cleanup policy fields, and `TopicConfig` parity —
extend in place.

**Exit:** `coordinator_test.rs` reproduces the Go `coordinator_test.go`
table cases; `group_hash` proptest "swap broker order → same key →
same coord" green; `take_over` / `relinquish` correctness verified
against a fake `StorageEngine`.

---

## D — `sk-controller`: election + heartbeat server + balancer + writer + mirror

Port `archive/internal/controller/`. Five modules:

### `election.rs`

Port `election.go`. `kube::runtime::lease` doesn't quite expose what
we need (it manages a Lease but doesn't surface `lease_transitions`
cleanly). Pragmatic shape: a hand-rolled loop using
`kube::Api::<Lease>::patch` with server-side apply + a
`holder_identity` field-manager; renew at `renew_deadline`, retry at
`retry_period`. `Acquired(epoch)` callback fires once with the post-
acquire `lease_transitions`; `Lost` callback fires on lease loss.

Mirror the Go timings (15 s lease, 10 s renew, 2 s retry) under a
`with_timings` builder so tests can shrink them.

### `heartbeat_server.rs`

Port `heartbeat_server.go`. tonic server impl of
`ControllerHeartbeat.Stream`:

- `recv_loop`: drain `stream.next().await` for `BrokerStatus`; update
  `client_state` per broker (last_seen, last_applied_version,
  active_groups); fire `push_assignment_changed(version)` callback to
  outside.
- `send_loop`: 1 s PING ticker + a
  `mpsc::Receiver<ControllerCommand>` for `ASSIGNMENT_CHANGED` /
  `LEAVING` pushes.
- Public observers: `broker_last_seen(broker_id) -> Option<Instant>`,
  `connected_brokers() -> Vec<String>`,
  `active_groups() -> Vec<String>` (union over all broker states —
  feeds the `BalanceGroups` `GroupSource`).

### `balancer.rs`

Port `balancer.go`. Two free fns:

- `balance(topics, brokers) -> Vec<PartitionAssignment>` —
  rendezvous-hash pick per `(topic, partition)`; smoothing pass to
  even out load (Go's `smoothPartitions`).
- `balance_groups(groups, brokers) -> Vec<ConsumerGroupAssignment>` —
  Phase 5 ships pure rendezvous; the gh #92 sticky-rebalance lever
  stays a forward-compat shim.

Port the Go FNV-1a / rendezvous hash bit-for-bit so a Rust-written
assignment matches a Go-written one byte-for-byte against the same
inputs (cutover requirement).

### `assignment_writer.rs`

Port `assignment.go`. Loop:

- `update_assignment(change)` queues an `AssignmentChange` on a
  `mpsc`.
- Single consumer task drains and `recompute_and_write(change)`:
  - Pull `TopicSource::topics()`,
    `BrokerSource::alive_brokers()`,
    `GroupSource::active_groups()`.
  - Run `balance` + `balance_groups`; bump `assignment_version`.
  - Atomic-write `<data_dir>/__cluster/assignment.json` via tempfile
    + rename.
  - Fire `K8sMirror::mirror(...)` fire-and-forget; push
    `ASSIGNMENT_CHANGED` via the heartbeat server.

Source traits (`TopicSource`, `BrokerSource`, `GroupSource`,
`CRMirror`) mirror the Go shapes. In production: `TopicSource` is
fed by the `TopicWatcher` (E), `BrokerSource` by `BrokerRegistry`
(E), `GroupSource` by `HeartbeatServer::active_groups()`.

### `k8s_mirror.rs`

Port `k8s_mirror.go`. Fire-and-forget update of the
`KafkaClusterAssignments` CR's `Status` block; truncate to
`DefaultMaxCRPartitions = 8000` if `partitions.len()` exceeds it (the
CR-size guard). Phase 5 ships a `NoopMirror`; the real mirror slots
into Phase 7 alongside the rest of `sk-operator-api`.

**Exit:** controller can win the lease against a `kind` cluster,
write `assignment.json`, bump epoch on re-election;
`tests/controller-failover/` ported and green.

---

## E — `sk-k8s`: BrokerRegistry, TopicWatcher, ReadinessUpdater, identity

Port `archive/internal/k8s/`. Workspace deps `kube` and `k8s-openapi`
already exist from Phase 0.

### `identity.rs`

Port `broker.go`'s `BrokerIdentity`. Parses `MY_POD_NAME`
(`skafka-0`, `skafka-1`, …) → ordinal. Builds the FQDN against
`<headless-svc>.<namespace>.svc.cluster.local`. Pure code, no kube
deps.

### `endpoints.rs`

Port `endpoints.go`. Watches `EndpointSlice` objects matching
`kubernetes.io/service-name=<headless>` via `kube::runtime::watcher`.
Maintains an in-memory map of
`pod_name → BrokerEndpoint { id, host, listener_ports: HashMap<String, i32> }`.
Fires `on_change(&[BrokerEndpoint])` whenever the set or any port
changes.

The `listener_ports` map is the gh #125 fix: each pod's container
ports get mirrored into the endpoint so the metadata response can look
up the matching port per listener-name.

### `topic_watcher.rs`

Port `topic_watcher.go`. Watches `KafkaTopic` CRs via
`kube::runtime::watcher::<KafkaTopic>` (the `KafkaTopic` type comes
from `sk-operator-api` — see the decision below). Fires
`TopicEvent::{Added, Modified, Deleted}` to a callback. The Phase-5
broker uses this to drive the `TopicRegistry` updates that today come
from `SKAFKA_TOPICS` env JSON — env JSON stays as the fallback path
for dev mode.

The `processEvent` shape — fires `TopicDeleted` on
`metadata.deletionTimestamp` non-nil rather than on the final
`Deleted` event — ports verbatim. This is the gh #76 NFS silly-rename
guard.

`SetTopicID` propagation: `Status.TopicID` is read off the CR and
pushed to `TopicRegistry::set_topic_id(name, uuid)` so the v10+
Metadata response carries real UUIDs (gh #105).

### `readiness.rs`

Port `readiness.go`. Sets / clears the `skafka.io/PartitionsReady`
condition on the pod's `ReadinessGate`. Drives the chart's pod
readiness flow — pod isn't ready until partition directories exist
on the PVC.

`watch_and_set_ready(changes: mpsc::Receiver<()>)` matches the Go
signature; `bins/skafka/main.rs` sends one tick after the initial
`assignment.json` apply.

**`sk-operator-api` slice in Phase 5.** Phase 5 wants the
`KafkaTopic` CR type for the `TopicWatcher`. Ship just the
`KafkaTopic` (read-only) Rust struct in `sk-operator-api` in Phase 5
— no `schemars`, no CRD-yaml gen. The full Phase 7 fills in the
other three CRDs + the yaml gen. Document the partial scope in the
crate README.

**Exit:** integration test in `tests/k8s/` — spin up a fake `kube`
mock via `wiremock`; assert `BrokerRegistry::on_change` fires when
an EndpointSlice changes; assert `TopicWatcher` fires `TopicDeleted`
on `deletionTimestamp` non-nil before the kube event arrives.

---

## F — `sk-protocol` / `sk-broker` wire-up

Three changes:

### 1. Register the new handlers

Same shape as Phase 4's SASL handlers. In `bins/skafka/main.rs`'s
`build_dispatcher` (move to `sk-broker::dispatcher::register_default`
once the list gets long):

```rust
d.register( 8, 0, 8, Arc::new(OffsetCommitHandler::new(broker.clone())));
d.register( 9, 0, 8, Arc::new(OffsetFetchHandler::new(broker.clone())));
d.register(10, 0, 4, Arc::new(FindCoordinatorHandler::new(broker.clone())));
d.register(11, 0, 9, Arc::new(JoinGroupHandler::new(broker.clone())));
d.register(12, 0, 4, Arc::new(HeartbeatHandler::new(broker.clone())));
d.register(13, 0, 5, Arc::new(LeaveGroupHandler::new(broker.clone())));
d.register(14, 0, 5, Arc::new(SyncGroupHandler::new(broker.clone())));
d.register(15, 0, 5, Arc::new(DescribeGroupsHandler::new(broker.clone())));
d.register(16, 0, 4, Arc::new(ListGroupsHandler::new(broker.clone())));
d.register(42, 0, 2, Arc::new(DeleteGroupsHandler::new(broker.clone())));
d.register(47, 0, 0, Arc::new(OffsetDeleteHandler::new(broker.clone())));
```

Each handler lives in `sk-broker/src/handlers/` (one file per API),
pulls `Arc<Manager>` from the `Broker`, decodes via
`sk-codec::api::<key>::decode_request`, calls the corresponding
`manager.method(req)`, encodes the response.

### 2. Thread the real `Coordinator` into Produce / Fetch / Metadata

Three call sites change in `sk-broker/src/handlers/`:

- `produce.rs`: ownership check is no longer "always true" —
  `if !broker.coordinator.owns(topic, partition) { error_code = 6 /* NOT_LEADER_OR_FOLLOWER */ }`.
  Pass `broker.coordinator.current_epoch(topic, partition)` into
  `engine.append(epoch, ...)` (replaces the Phase-3 hardcoded `0`).
- `fetch.rs`: same ownership check (Fetch returns
  `NOT_LEADER_OR_FOLLOWER` for off-broker partitions; Apache returns
  the same).
- `metadata.rs`: per-partition leader is
  `coordinator.leader_for(topic, partition)`; `controller_id` is
  `coordinator.controller_id()` (was -1 in Phase 3); brokers block
  enumerates `broker_registry.all()` rather than the single-broker
  self.

`Broker` grows new fields:

```rust
pub coordinator: Arc<Coordinator>,           // C
pub broker_registry: Arc<BrokerRegistry>,    // E
pub coord_manager: Arc<Manager>,             // B
```

`Broker::new` stays for dev mode (constructs a `Coordinator` over a
`LocalAssignment` stub that says "self owns everything" — replaces
`LocalLeaseManager`). `Broker::with_cluster(...)` is the new
constructor that takes the real ones.

### 3. `Manager::set_group_assignment_source` swap at boot

Same shape as Go: bootstrap source is a `LocalGroupSource` (always
returns `owns=true, coord=self`); after `Coordinator` boots,
`bins/skafka/main.rs` calls
`manager.set_group_assignment_source(coordinator.clone() as Arc<dyn GroupAssignmentSource>)`.
The `ArcSwap` keeps the swap lock-free.

**Exit:** dispatch unit tests cover happy and rejected paths for
each new handler against an in-memory `Manager`; produce / fetch
handlers return error 6 when `coordinator.owns(...)` is false.

---

## G — `bins/skafka` cluster bring-up

Changes the boot order around the auth setup. Pseudocode:

```rust
let cli = Cli::from_env()?;
init_tracing(&cli.log_level);

let engine = build_engine(...)?;
let topics = Arc::new(TopicRegistry::new());          // empty; filled by watcher

let kube_client = if cli.cluster_mode {
    Some(kube::Client::try_default().await?)
} else { None };

// k8s sources (sk-k8s)
let identity      = BrokerIdentity::from_env()?;
let broker_reg    = Arc::new(BrokerRegistry::new(identity.self_endpoint(), identity.dns()));
let topic_watcher = TopicWatcher::new(kube_client.clone(), &cli.namespace, topics.clone())?;
let readiness     = ReadinessUpdater::new(kube_client.clone(), &cli.pod_name, &cli.namespace);

// broker-side Coordinator (sk-broker)
let coord_watch   = ControllerWatch::new(kube_client.clone(), &cli.namespace);
let coordinator   = Arc::new(Coordinator::new(identity.id(), engine.clone(),
                                              cli.data_dir.clone())?);

// drivers — handler-time event consumers
let takeover_drv  = Arc::new(TakeoverDriver::new(engine.clone(), identity.id()));
coordinator.on_assignment_change(takeover_drv.clone());
let coord_mgr     = Arc::new(coordinator::Manager::new(identity.id(),
                                                       OffsetStore::new(&cli.data_dir)));
let group_take    = Arc::new(GroupTakeoverDriver::new(coord_mgr.clone(), identity.id()));
coordinator.on_assignment_change(group_take.clone());
coord_mgr.set_group_assignment_source(coordinator.clone());

// auth (Phase 4, unchanged)
let auth = build_auth(&cli)?;
let broker = Arc::new(Broker::with_cluster(...));

// dispatcher (Phase 4 + Phase 5 handlers)
let dispatcher = register_default(broker.clone(), &cli.listeners, auth.engines.clone());

// run watchers + coordinator + heartbeat client + controller-side tasks
tokio::spawn(broker_reg.watch(kube_client.clone()));
tokio::spawn(topic_watcher.run());
tokio::spawn(coord_watch.run());
tokio::spawn(coordinator.run());
let hb_client = HeartbeatClient::new(identity.id())
    .with_target_func(/* read coord_watch holder */)
    .on_command(/* trigger coordinator.poll_now on ASSIGNMENT_CHANGED */);
tokio::spawn(hb_client.run());

// Controller-mode bring-up — fired by the Election callback.
let election = controller::Election::new(kube_client.clone(), &cli.namespace, identity.id())
    .with_callbacks(
        on_acquired = |epoch| { /* spawn controller side */ },
        on_lost     = || { /* cancel controller-side tasks */ },
    );
tokio::spawn(election.run());

// readiness gate flip — wait for assignment.json applied once
let (gate_tx, gate_rx) = mpsc::channel(1);
coordinator.on_first_apply(move || { let _ = gate_tx.try_send(()); });
tokio::spawn(readiness.watch_and_set_ready(gate_rx, /* … */));

// existing Phase 4 reload + server loop
spawn_reloader(...);
let server = Server::new(...);
server.serve(cancel).await?;

// existing Phase 3 drain
engine.drain().await?;
```

Dev mode (no `MY_POD_NAME`): skip all kube + heartbeat + election;
build `Coordinator` over the same `LocalAssignment` stub
`Broker::new` uses. `auth_smoke.rs` and `smoke.rs` keep running
unmodified.

`Cli` grows `namespace`, `pod_name`, `cluster_mode: bool` (auto-
derived from `MY_POD_NAME` presence).

**Exit:** `bins/skafka` binary boots against a `kind` cluster, joins
the headless-service endpoint set, applies the first
`assignment.json`, flips the readiness gate.

---

## H — Tests

Three integration test bodies:

1. **`tests/controller-failover/`** — ported from
   `archive/tests/controller-failover/`. Spin up 3 `bins/skafka`
   processes against a tempdir-NFS-loopback (or use the `kind`-based
   harness if the budget allows). Kill the controller broker, assert
   a new controller takes the lease within
   `lease_duration + jitter`, the assignment epoch bumps, the
   surviving brokers reload `assignment.json`, and produce / fetch
   resumes on a re-assigned partition.
2. **`tests/stale-controller-race/`** — ported. Two controllers race;
   the loser's write to `assignment.json` is rejected because its
   epoch is stale. Assert no torn `assignment.json` on disk.
3. **`bins/skafka/tests/cluster_smoke.rs`** — multi-broker rdkafka
   smoke. Spawn 3 brokers, produce 1k records to a 3-partition topic
   with `acks=all`, consume back via `kafka-consumer-group=g`, expect
   every record exactly once. Kill one non-controller broker
   mid-run; consumer rebalances; messages resume without duplicate
   delivery (within the consumer's at-least-once contract).

`tests/controller-failover/` and `tests/stale-controller-race/` are
the load-bearing port tests — they're what gh #148 will block on.

**Exit:** all three pass; `tests/` ports keep the original test
names so a `git log` cross-reference is mechanical.

---

## Phase 5 exit criteria (all must hold)

1. `cargo test --workspace --all-features` green, under 6 min warm
   cache.
2. `cargo clippy --workspace --all-targets -- -D warnings` and
   `cargo fmt --check` pass.
3. `sk_codec::api::registry::ALL.len() == 19` — 11 new keys (8, 9,
   10, 11, 12, 13, 14, 15, 16, 42, 47) with the version ranges in
   §A.
4. `bins/skafka` runs in cluster mode (with `MY_POD_NAME` set + a
   working `kube::Client`) and in dev mode (without) — both pass the
   Phase 3 + Phase 4 smoke suites unmodified.
5. `kafka-console-consumer --group g --from-beginning` against the
   Rust binary reads back records produced by a
   `kafka-console-producer`, the offset is committed to
   `<data_dir>/__cluster/__consumer_offsets/g.json` on the same disk
   shape as Go.
6. `kafka-consumer-groups --list / --describe / --delete` work
   against any broker in a 3-broker cluster (proves
   `FindCoordinator` + cross-broker routing).
7. A 3-broker `kind` cluster reassigns within 5 s of
   `kubectl delete pod skafka-0`; partition leadership migrates
   cleanly; no `.nfsXXXX` orphans appear in `data/` after a full
   restart cycle.
8. `tests/controller-failover/` and `tests/stale-controller-race/`
   ports both green.
9. `sk_codec::tripwires::record_decode_count()` and
   `batch_reencode_count()` both read 0 after the cluster smoke run
   — proves the consumer-group APIs stayed byte-opaque (Phase 1
   contract still holds).
10. Go tree under `archive/` unchanged; chart, CRDs, `scripts/`,
    and `proto/heartbeat.proto` bit-identical to their pre-Phase-5
    contents.

---

## Risks & mitigations

- **`kube-rs` Lease shape.** `kube::runtime::lease` doesn't surface
  `lease_transitions` cleanly. Mitigation: hand-roll the election
  loop with `kube::Api::<Lease>::patch` + server-side apply; ports
  the Go shape verbatim. Re-evaluate when `kube` v1.0 lands.
- **`assignment.json` byte-equality with Go.** Cutover (Phase 9)
  wants a Rust-written file to be readable by a Go reader without
  semantic drift. `time.RFC3339Nano` vs
  `chrono::Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true)`
  differ subtly in trailing-zero handling. Mitigation: golden-file
  test in `tests/fixtures/assignment.json` captured from a Go-side
  run; assert byte-equal on a known-input `recompute_and_write`.
- **tokio-vs-Go rebalance-timer semantics.** Go uses
  `time.NewTimer` with `Reset`; tokio prefers `tokio::time::sleep`
  per-call. The `evictNonRejoiningMembers` path in `group.rs` races
  between sleep firing and member rejoining. Mitigation: use
  `tokio::time::sleep_until(deadline)` + a cancellation `Notify` per
  member rather than `Reset`; deterministic via
  `tokio::time::pause()` in unit tests.
- **`OffsetStore` JSON shape vs Go.** Cutover requires bit-identical
  reads. Mitigation: capture two Go-written offset files in
  `tests/fixtures/offset_store/` and assert `serde_json` round-trip
  byte-equal; same trick the auth phase used for `credentials.json`.
- **Heartbeat backpressure on tonic stream.** Go's gRPC keeps the
  stream alive on slow consumers via per-direction goroutines.
  tonic uses `tokio::sync::mpsc` under the hood; a slow `recv_loop`
  can stall `send_loop`. Mitigation: bound the `mpsc` per-direction
  at the same size Go uses; document the back-pressure path; add a
  metric on send-buffer occupancy. This is the gh #77 reactive-
  rebalance regression vector — call it out in PR review.
- **Controller-mode tasks under election callback.**
  `kube::runtime::lease` doesn't model "stop these tasks on lease
  loss" cleanly. Mitigation: a single `CancellationToken` owned by
  the on-acquired callback; on-lost calls `cancel`. Same shape as
  the Phase 3 server-cancel.
- **`sk-operator-api` slice in Phase 5.** Phase 5 wants the
  `KafkaTopic` CR type for the `TopicWatcher`. Mitigation: ship just
  the `KafkaTopic` + `KafkaClusterAssignments` types in
  `sk-operator-api` (no schemars, no CRD yaml gen) in Phase 5; the
  rest follows in Phase 7. Document the partial scope in the crate
  README.
- **`assignment.json` write-vs-watch race.** A controller write that
  happens while a peer's watcher is reading mid-file → torn JSON.
  Mitigation: the Phase 5 writer uses tempfile + atomic rename
  inside `__cluster/`; readers tolerate `EOF` mid-parse and retry
  once. Matches Go.

---

## What this enables for Phase 6

After Phase 5 merges, Phase 6 (transactions, idempotence advanced
features) lands by:

1. `TxnStateStore` slot-sharded under
   `<data_dir>/__cluster/txn_state/`; `Coordinator::owns_txn`
   already returns the right answer.
2. Transactional handlers (24, 25, 26, 27, 28) plug into the
   existing `Manager::wire_txn_offset_hook` seam;
   `OffsetStore::store_pending` / `commit_pending` /
   `discard_pending` are already in place from B.
3. `txn_reaper` is a `tokio::time::interval` task wired in
   `bins/skafka/main.rs` alongside the Phase 5 watchers.
4. `fence_log.rs` cross-broker fence broadcast reuses the
   `notify` + poll shape from `Coordinator`'s assignment watcher.

No further Phase 5 changes — Phase 6 is pure addition on top of the
stable seams.
