# Phase 6 — Transactions, idempotence, fence broadcast

Detailed work plan for the seventh phase of the Rust rewrite. Companion to
[`rewrite.md`](./rewrite.md); the high-level summary lives there. Builds
on the codec scaffolding from [`phase-1.md`](./phase-1.md), the storage
engine + idempotence window from [`phase-2.md`](./phase-2.md), the
single-broker server from [`phase-3.md`](./phase-3.md), auth from
[`phase-4.md`](./phase-4.md), and the cluster + coordinator surface
from [`phase-5.md`](./phase-5.md).

**Goal.** Light up Kafka transactions end-to-end. `TxnStateStore` lands
as 50-slot JSON shards under `/data/__cluster/txn_state/`; the five
transactional handlers (`InitProducerId` upgrade + keys 24, 25, 26, 27,
28) plug into the `Manager::wire_txn_offset_hook` seam Phase 5 left
behind; the timeout reaper aborts stalled `Ongoing` transactions every
10 s; cross-broker producer-epoch fences propagate via a per-broker
log under `/data/__cluster/fence_log/`. A franz-rs / rdkafka client
running KIP-447 EOS (consume → process → produce → `sendOffsetsToTxn`
→ commit) against a single Rust broker passes round-trip, byte-equal.

**Length.** ~2 weeks, single engineer. Workstream A (codec) is small
(5 modules vs Phase 5's 11). B (`TxnStateStore`) is the load-bearing
chunk and runs in parallel with A. C/D/E/F land after B; G closes
with the EOS smoke.

**Out of scope for Phase 6.**

- **Cross-broker `WriteTxnMarkers` RPC (gh #114).** When the txn
  coordinator and the group coordinator are different brokers,
  `EndTxn` needs to dispatch marker writes to the partition leaders.
  Phase 6 implements the *same-broker* fast path (txn coord == group
  coord == partition leader) and returns `NOT_COORDINATOR` (16) for
  the cross-broker case. Multi-broker EOS lands behind a feature flag
  pending gh #114, mirroring the Go shape.
- **`TxnStateStore` slot migration.** The Go implementation has a
  5-phase `migrateLayout()` that re-shards slots when the broker
  count changes. Phase 6 ships the steady-state path; migration is
  punted to Phase 6.1 / Phase 7 — pin `num_slots = 50` for the whole
  cluster.
- **`PrepareCommit` / `PrepareAbort` markers on disk.** Apache writes
  these to `__transaction_state` so a failed coordinator can resume
  marker dispatch. skafka collapses Prepare → Complete atomically in
  the slot file (no separate marker phase), same as Go. Documented as
  a non-goal in [`CLAUDE.md`](../CLAUDE.md).
- **`DescribeTransactions` (key 65) / `ListTransactions` (key 66).**
  Admin observability; not required by EOS. Phase 8 candidates.

**Scope boundary (what real clients exercise).**
`franz-rs` and `rdkafka` configured with `enable.idempotence=true` +
`transactional.id=tx-1` complete `InitProducerId` → `AddPartitionsToTxn`
→ `Produce(transactional=1)` → `AddOffsetsToTxn` → `TxnOffsetCommit` →
`EndTxn(commit=true)`. A second consumer with `isolation.level=read_committed`
sees only the committed records, never the aborted ones. A producer
crash + restart against the same `transactional.id` returns the same
PID with `epoch += 1`, fences any in-flight zombie batches from the
prior session across every partition the PID touched.

---

## Workstreams

Seven workstreams. A blocks D/E/F. B is the load-bearing parallel
chunk and feeds C. C threads through `sk-broker`. G closes the EOS
smoke.

- **A** — Codec backfill (5 new API modules: keys 24, 25, 26, 27, 28)
- **B** — `sk-coordinator::txn_state`: `TxnStateStore`, slot files,
  state machine, timeout reaper, offset-commit hook
- **C** — `sk-broker` handlers: rewire `init_producer_id` to use
  `TxnStateStore`; add `add_partitions_to_txn`, `add_offsets_to_txn`,
  `end_txn`, `txn_offset_commit`, `write_txn_markers`
- **D** — `sk-broker` control-batch encoder + `WriteTxnMarkers`
  same-broker fast path
- **E** — `sk-coordinator::fence_log` + `sk-broker::fence_watcher`
  (gh #108 phase 2 cross-broker producer-epoch fence broadcast)
- **F** — `bins/skafka` main: register keys 24–28, wire
  `TxnStateStore` into the dispatcher, spawn the reaper task + fence
  watcher under the existing coordinator runtime
- **G** — Tests: port `archive/tests/kafka-compat/eos_v2_test.go` +
  `archive/tests/integration/txn_fence_broadcast_test.go`;
  `crates/sk-coordinator/tests/txn_state.rs` unit coverage matching
  the 1300-LOC Go test file

Dependencies: A blocks C+D; B blocks C+E+F; C blocks D+F; D blocks G;
E lands in parallel with C/D; F blocked by C+D+E; G blocked by F.

---

## A — Codec backfill (5 new modules)

`crates/sk-codec/src/api/` grows 5 entries. Versions / flexibility
table (matches `archive/internal/broker/broker.go`):

| Key | API                  | Versions | Flexible from | Notes                                                                                   |
|----:|----------------------|---------:|--------------:|-----------------------------------------------------------------------------------------|
|  24 | AddPartitionsToTxn   | 0–3      | 3             | v4 multi-txn batching skipped (rdkafka / franz-go don't emit it)                        |
|  25 | AddOffsetsToTxn      | 0–3      | 3             | trivial; one `(group_id, pid, epoch)` per call                                          |
|  26 | EndTxn               | 0–3      | 3             | top-level `error_code` only; per-partition errors land via WriteTxnMarkers              |
|  27 | WriteTxnMarkers      | 0–1      | 1             | per-broker marker dispatch; control-batch payload is byte-opaque to codec               |
|  28 | TxnOffsetCommit      | 0–3      | 3             | wraps `OffsetCommit` shape with `pid`, `epoch`, `generation_id`                         |

Each module follows the `sasl_authenticate.rs` shape established in
Phase 4: owning `String` / `Bytes` types (control-path, not hot-path),
`Decode` / `Encode` helpers, `pub const SPEC: ApiSpec`, `request_hdr`
/ `response_hdr` const fns, registry row added to
`api::registry::ALL`. After Phase 6 lands,
`sk_codec::api::registry::ALL.len() == 24`.

Fixtures: capture one request + response per (key, version) against
Apache 3.7 driven by a franz-go EOS round-trip script. Use the
`xtask fixture-capture` machinery from Phase 3. Roundtrip byte-equal
+ `proptest` round trip + `record_decode_count()` tripwire at
end-of-test (the `WriteTxnMarkers` control-batch payload stays
opaque — Phase 1 contract still holds).

---

## B — `sk-coordinator::txn_state`

The Go reference is `archive/internal/coordinator/txn_state.go` (1026
LOC). Port verbatim into `crates/sk-coordinator/src/txn_state.rs` with
the same on-disk shape so a Go-written slot file is readable by Rust
and vice versa (gh #169 hedge applies).

### B.1 — Data model

```rust
pub struct TxnEntry {
    pub pid: i64,
    pub epoch: i16,
    pub state: TxnState,                 // Empty/Ongoing/PrepareCommit/PrepareAbort/CompleteCommit/CompleteAbort
    pub partitions: Vec<TxnTopic>,       // (topic, partitions[]) tuples
    pub groups: Vec<String>,             // group IDs from AddOffsetsToTxn
    pub ongoing_since_ms: i64,           // 0 outside Ongoing; UnixMillis() when Ongoing entered
    pub transaction_timeout_ms: i32,
}

pub struct TxnTopic { pub topic: String, pub partitions: Vec<i32> }

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Copy)]
pub enum TxnState { Empty, Ongoing, PrepareCommit, PrepareAbort, CompleteCommit, CompleteAbort }
```

`serde(rename_all = "camelCase")` on the struct so JSON keys match Go
exactly. Empty / unknown state values deserialize to `Empty` (forward
compat).

### B.2 — Slot file shape

Per slot: `<data_dir>/__cluster/txn_state/slot-<n>.json` for
`n ∈ 0..50`. JSON: `{ "entries": { "<txnID>": TxnEntry, ... } }`.
Atomic write via tmp + fsync + rename (same primitive
`crates/sk-storage::atomic_write` uses).

`slot_for(txn_id) = fnv1a(txn_id.as_bytes()) % NUM_SLOTS` — match
the Go hash byte-for-byte. Capture a Go-written `slot-N.json` into
`tests/fixtures/txn_state/` and assert `serde_json` round-trip
byte-equal (same trick the Phase 4 / Phase 5 ports used).

### B.3 — State machine

Six terminal transitions, all gated by `(pid, epoch)` match:

| From               | API               | To              | Side effect                                           |
|--------------------|-------------------|-----------------|-------------------------------------------------------|
| Empty / Complete\* | AddPartitionsToTxn| Ongoing         | stamp `ongoing_since_ms = now_ms()`                   |
| Empty / Complete\* | AddOffsetsToTxn   | Ongoing         | stamp `ongoing_since_ms = now_ms()`                   |
| Ongoing            | AddPartitionsToTxn| Ongoing         | union (topic, partition)                              |
| Ongoing            | AddOffsetsToTxn   | Ongoing         | record group id                                       |
| Ongoing            | EndTxn(commit)    | CompleteCommit  | clear partitions; fire offset hook (commit)           |
| Ongoing            | EndTxn(abort)     | CompleteAbort   | clear partitions; fire offset hook (discard)          |
| Ongoing (overdue)  | reaper            | CompleteAbort   | epoch += 1; clear partitions; fire offset hook        |
| Prepare\*          | (any)             | —               | reject with CONCURRENT_TRANSACTIONS (51)              |

Sentinel errors map to the wire shape:

| Rust error                  | Wire code | Apache constant                        |
|-----------------------------|----------:|----------------------------------------|
| `ErrTxnEmptyId`             | 71        | INVALID_TRANSACTIONAL_ID               |
| `ErrTxnUnknownProducer`     | 49        | UNKNOWN_PRODUCER_ID                    |
| `ErrTxnEpochFenced`         | 47        | INVALID_PRODUCER_EPOCH                 |
| `ErrTxnConcurrent`          | 51        | CONCURRENT_TRANSACTIONS                |
| `ErrTxnInvalidState`        | 50        | INVALID_TXN_STATE                      |

Each transitional method (`add_partitions`, `add_offsets_to_txn`,
`end_txn`, `get_or_allocate_with_timeout`) is `&self` + interior
mutability — `tokio::sync::Mutex` per slot (50 mutexes total). Reads
are `loadSlot()`-style: read-fresh-on-every-call so a new coordinator
after failover sees the latest state via close-to-open NFS
consistency (the Go invariant at `txn_state.go:59-62`).

### B.4 — Public surface

```rust
impl TxnStateStore {
    pub async fn get_or_allocate<F>(&self, txn_id: &str, alloc: F) -> Result<(i64, i16)>
        where F: FnOnce() -> i64;                               // alloc() returns next free PID

    pub async fn get_or_allocate_with_timeout<F>(&self, txn_id: &str, timeout_ms: i32, alloc: F) -> Result<(i64, i16)>;

    pub async fn add_partitions(&self, txn_id: &str, pid: i64, epoch: i16, additions: &[TxnTopic]) -> Result<()>;
    pub async fn add_offsets_to_txn(&self, txn_id: &str, pid: i64, epoch: i16, group_id: &str) -> Result<()>;
    pub async fn end_txn(&self, txn_id: &str, pid: i64, epoch: i16, commit: bool) -> Result<EndTxnOutcome>;

    pub async fn abort_overdue_owned(&self, now_ms: i64, owns_txn: &dyn TxnOwnership) -> Result<Vec<AbortedTxn>>;

    pub fn wire_offset_hook(&self, hook: Arc<dyn TxnOffsetHook>);
}
```

`EndTxnOutcome` carries `{ partitions: Vec<TxnTopic>, groups: Vec<String> }`
so the handler can dispatch marker writes (Workstream D) and fire the
offset hook (Workstream C).

### B.5 — Timeout reaper

`abort_overdue_owned(now_ms, owns_txn)` walks each slot, transitions
overdue `Ongoing` entries (`now_ms - ongoing_since_ms > transaction_timeout_ms`)
to `CompleteAbort`, bumps `epoch += 1`, fires the offset hook with
`discard`, persists the slot. Gated by `owns_txn.owns_txn(&txn_id)`
so a multi-broker cluster doesn't N-way-race on the same overdue
txn (gh #91).

Driven from `bins/skafka/main.rs` (Workstream F) by a
`tokio::time::interval(Duration::from_secs(10))` task spawned
alongside the Phase 5 watchers — matches Apache's
`transaction.abort.timed.out.transaction.cleanup.interval.ms`
default.

### B.6 — Offset hook wiring

`TxnOffsetHook` is the seam Phase 5 already cut. The Phase 6 wiring
in `bins/skafka/main.rs`:

```rust
let txn_store = Arc::new(TxnStateStore::open(&data_dir).await?);
let hook = Arc::new(GroupCoordinatorOffsetHook { mgr: coord_manager.clone() });
txn_store.wire_offset_hook(hook);
coord_manager.set_txn_assignment_source(txn_owns_via_coordinator);  // gh #91 routing
```

`GroupCoordinatorOffsetHook` calls
`mgr.offset_store().commit_pending(group_id, pid)` on
`CompleteCommit` and `discard_pending(group_id, pid)` on
`CompleteAbort`. The methods are already in
`crates/sk-coordinator/src/offset_store.rs` per the Phase 5 survey —
no new code on the offset side.

---

## C — `sk-broker` handlers

Five new files in `crates/sk-broker/src/handlers/`; one rewrite.

### C.1 — `init_producer_id.rs` (rewrite)

The Phase 3 scaffold returns `TRANSACTIONAL_ID_NOT_FOUND` (74) when
the request carries a non-empty `transactional_id`. Phase 6 swaps in
the real path:

```rust
async fn handle(req, ctx) -> Response {
    let txn_id = req.transactional_id.unwrap_or_default();
    if txn_id.is_empty() {
        // non-transactional: monotonic PID counter, epoch=0
        return Response { producer_id: ctx.next_pid(), producer_epoch: 0, .. };
    }
    if !ctx.owns_txn(&txn_id) { return Response::err(NOT_COORDINATOR /* 16 */); }
    let (pid, epoch) = ctx.txn_store
        .get_or_allocate_with_timeout(&txn_id, req.transaction_timeout_ms, || ctx.next_pid())
        .await?;
    if epoch > 0 {
        // gh #30: broadcast fence so peer brokers expire zombie batches
        ctx.fencer.fence_producer_epoch(pid, epoch).await;
    }
    Response { producer_id: pid, producer_epoch: epoch, .. }
}
```

`ctx.fencer` is wired in Workstream E. Same-broker fence is in-process
via `DiskStorageEngine::fence_producer_epoch` (already in
`crates/sk-storage/src/engine.rs` per the Phase 2 survey).

### C.2 — `add_partitions_to_txn.rs` (key 24)

```rust
async fn handle(req, ctx) -> Response {
    if !ctx.owns_txn(&req.transactional_id) { return Response::err_top(NOT_COORDINATOR); }
    let additions = req.topics.into_iter().map(|t| TxnTopic { topic: t.name, partitions: t.partitions }).collect();
    match ctx.txn_store.add_partitions(&req.transactional_id, req.producer_id, req.producer_epoch, &additions).await {
        Ok(()) => Response::ok_per_partition(/* all partitions: 0 */),
        Err(e) => Response::ok_per_partition(/* all partitions: e.wire_code() */),
    }
}
```

Per-partition error codes mirror the top-level decision (Apache's v0–v3
shape; v4 multi-txn batching skipped per "out of scope" above).

### C.3 — `add_offsets_to_txn.rs` (key 25)

Trivial: `ctx.txn_store.add_offsets_to_txn(...)`, return top-level
`ErrorCode`.

### C.4 — `txn_offset_commit.rs` (key 28)

```rust
async fn handle(req, ctx) -> Response {
    if !ctx.is_group_coordinator(&req.group_id) { return Response::err_top(NOT_COORDINATOR); }
    ctx.coord_manager.txn_offset_commit(req).await  // stages into OffsetStore::store_pending
}
```

The staging side already exists in
`crates/sk-coordinator/src/offset_store.rs`; this handler just
routes.

### C.5 — `end_txn.rs` (key 26)

```rust
async fn handle(req, ctx) -> Response {
    if !ctx.owns_txn(&req.transactional_id) { return Response::err_top(NOT_COORDINATOR); }
    let outcome = ctx.txn_store.end_txn(&req.transactional_id, req.producer_id, req.producer_epoch, req.committed).await?;
    // Same-broker fast path: write markers locally for every partition we lead.
    ctx.write_txn_markers_locally(req.producer_id, req.producer_epoch, req.committed, &outcome.partitions).await?;
    // Cross-broker case (txn_coord != partition leader): NOT_COORDINATOR for now (gh #114).
    Response::ok()
}
```

The `write_txn_markers_locally` helper sits in Workstream D.

### C.6 — `write_txn_markers.rs` (key 27)

Receiver-side marker writer for the (future) cross-broker case. Phase
6 implements it (so an external coordinator could drive it) but
doesn't *invoke* it cross-broker — that's gh #114. The handler:

```rust
async fn handle(req, ctx) -> Response {
    for marker in req.markers {
        // Validate leadership for every partition listed.
        for topic in &marker.topics {
            for part in &topic.partitions {
                if !ctx.coordinator.owns(&topic.name, *part) {
                    /* per-partition error 16 NOT_LEADER_FOR_PARTITION */;
                    continue;
                }
                let batch = build_control_batch(marker.producer_id, marker.producer_epoch, marker.transaction_result, marker.coordinator_epoch);
                ctx.engine.append(&topic.name, *part, ctx.coordinator.current_epoch(&topic.name, *part), Acks::All, batch).await?;
            }
        }
    }
    Response::ok()
}
```

---

## D — Control-batch encoder + same-broker fast path

New file: `crates/sk-broker/src/control_batch.rs`.

`build_control_batch(pid, epoch, commit, coord_epoch) -> Bytes` writes
a v2 RecordBatch with:

- `attributes` bits: `0x20` (control) | `0x10` (transactional)
- `producerId = pid`, `producerEpoch = epoch`, `baseSequence = -1`
  (control batches don't consume sequence)
- One record with key = `version (i16=0) + type (i16: 0=ABORT, 1=COMMIT)`
  and value = `EndTxnMarker { version: i16, coordinator_epoch: i32 }`

Match `archive/internal/protocol/handlers/control_batch.go` byte-for-byte.
Capture a Go-written marker batch as a fixture under
`crates/sk-broker/tests/fixtures/control_batch.bin` and assert
byte-equal.

`Broker::write_txn_markers_locally(pid, epoch, commit, parts)` iterates
the `parts` list, filters to partitions this broker owns
(`coordinator.owns(topic, part)`), builds the control batch, and
appends through the existing `engine.append` path with `acks=All`.
Partitions not owned by this broker are silently dropped — the gh #114
cross-broker RPC will pick them up later.

---

## E — Cross-broker fence broadcast (gh #108 phase 2)

Two files: `crates/sk-coordinator/src/fence_log.rs` and
`crates/sk-broker/src/fence_watcher.rs`. Ports
`archive/internal/coordinator/fence_log.go` (138 LOC) and
`archive/internal/broker/fence_watcher.go` (157 LOC) verbatim.

### E.1 — `FenceLog` (writer)

```rust
pub struct FenceLog { path: PathBuf /* __cluster/fence_log/from-<broker_id>.json */ }

impl FenceLog {
    pub async fn append(&self, pid: i64, epoch: i16) -> Result<()>;  // idempotent: ignores epoch <= current
    pub async fn snapshot(&self) -> Result<HashMap<i64, i16>>;       // for tests
}
```

JSON map: `{ "<pid>": <epoch>, ... }`. Atomic tmp + rename. Each
broker writes only its own file (`from-skafka-0.json`,
`from-skafka-1.json`, ...) — no cross-broker writes, no locking.

Wired into `InitProducerIdHandler` (Workstream C.1): after the
in-process `FenceProducerEpoch`, append `(pid, epoch)` to the local
broker's `FenceLog`.

### E.2 — `FenceWatcher` (reader)

`tokio::time::interval(Duration::from_secs(2))` task that reads every
`from-*.json` *except* its own, dedupes by tracking the highest
epoch already applied per (peer, pid), and calls
`engine.fence_producer_epoch(pid, epoch)` for new entries. Skips
self-file to avoid feedback loops.

Same shape as the `Coordinator` assignment watcher — reuse the
`notify` + 1 s mtime poll plumbing from
`crates/sk-broker/src/coordinator.rs`. (The Phase 5 acknowledgement
that fsnotify doesn't cross NFS hosts applies here; the 2 s poll is
the load-bearing mechanism. See gh #166 for the doc gap.)

---

## F — `bins/skafka` wire-up

Five hooks in `bins/skafka/src/main.rs`:

1. Register keys 24, 25, 26, 27, 28 in `build_dispatcher`. Pattern
   identical to the Phase 5 registrations:
   `d.register(24, 0, 3, AddPartitionsToTxnHandler::new(broker.clone()))`.
2. Build `TxnStateStore::open(&data_dir).await?` after the
   `Coordinator` is up.
3. Build `FenceLog::open(&data_dir, &broker_id).await?` and pass
   into the (revised) `InitProducerIdHandler` constructor.
4. Spawn `FenceWatcher::run(cancel.clone())` alongside the other
   coordinator-mode tasks.
5. Spawn the txn reaper:
   ```rust
   let txn_store = txn_store.clone();
   let coord = coordinator.clone();
   tokio::spawn(async move {
       let mut t = tokio::time::interval(Duration::from_secs(10));
       loop {
           tokio::select! {
               _ = t.tick() => { txn_store.abort_overdue_owned(now_ms(), &*coord).await.ok(); }
               _ = cancel.cancelled() => break,
           }
       }
   });
   ```
6. Set the group-coordinator offset hook on `txn_store` so EndTxn →
   `OffsetStore::commit_pending` / `discard_pending` wires through.

Dev mode (no `MY_POD_NAME`): `TxnStateStore` runs against the local
data dir; `FenceLog` / `FenceWatcher` and the reaper still run (the
reaper's `owns_txn` check defaults to "yes" via `LocalTxnSource`).
EOS works against the single-broker dev mode by construction.

---

## G — Tests

Four test bodies:

1. **`crates/sk-coordinator/tests/txn_state.rs`** — port of
   `archive/internal/coordinator/txn_state_test.go` (1335 LOC).
   State machine, rejoin epoch bump, `AbortOverdue`, group tracking,
   epoch overflow rotation, slot file persistence + close-to-open
   semantics (simulated via flush + reopen). Should be the biggest
   single test file in the Rust tree.
2. **`crates/sk-broker/tests/fence_broadcast.rs`** — port of
   `archive/tests/integration/txn_fence_broadcast_test.go`. Two
   in-process brokers sharing a tempdir; broker A fences PID 42,
   broker B's `FenceWatcher` ticks, broker B's in-memory dedupe
   window for PID 42 is cleared.
3. **`bins/skafka/tests/eos_v2.rs`** — port of
   `archive/tests/kafka-compat/eos_v2_test.go`. franz-rs or rdkafka
   driving full KIP-447 round-trip against a single Rust broker.
   Seed → txn → produce → `sendOffsetsToTxn` → commit → verify output
   contains exactly the produced records and the committed offset is
   visible to a `read_committed` consumer. Abort path: same but
   `commit=false`, verify the produced records are *not* visible to
   `read_committed`.
4. **`crates/sk-storage/tests/idempotence_fence.rs`** — already
   covered by Phase 2, but re-check that
   `FenceProducerEpoch` clears the dedupe window across all open
   partitions on a single broker (the gh #30 invariant). One extra
   assertion: snapshot persisted after fence call lists the new
   epoch.

`tests/eos_v2.rs` is the load-bearing port — gh #149 blocks on it.

---

## Phase 6 exit criteria (all must hold)

1. `cargo test --workspace --all-features` green, under 8 min warm
   cache (Phase 5 budget + 2 min for EOS smoke).
2. `cargo clippy --workspace --all-targets -- -D warnings` and
   `cargo fmt --check` pass.
3. `sk_codec::api::registry::ALL.len() == 24` — 5 new keys (24, 25,
   26, 27, 28) with the version ranges in §A.
4. `bins/skafka/tests/eos_v2.rs` passes against a single Rust
   broker: franz-rs / rdkafka EOS round-trip is byte-equal to the
   Go reference output captured under
   `tests/fixtures/eos_v2/`.
5. `InitProducerId` against a `transactional.id` returns the same
   PID with `epoch += 1` on reconnect; verify via a unit test that
   constructs two clients with the same `transactional.id` against
   one `TxnStateStore` instance.
6. The reaper aborts an `Ongoing` txn that has exceeded its
   `transactionTimeoutMs` within 10 s of the deadline (deterministic
   via `tokio::time::pause()`).
7. `FenceWatcher` propagates a producer-epoch bump from broker A to
   broker B within 2 s (deterministic via `Tick()` in tests).
8. On-disk shape matches Go byte-for-byte: capture Go-written
   `slot-<n>.json` and `from-skafka-<id>.json` into
   `tests/fixtures/txn_state/` and `tests/fixtures/fence_log/`;
   assert `serde_json` round-trip byte-equal.
9. `sk_codec::tripwires::record_decode_count()` and
   `batch_reencode_count()` both read 0 after the EOS smoke — the
   control-batch payload stays opaque to the codec.
10. Go tree under `archive/` unchanged; chart, CRDs, `scripts/`, and
    `proto/heartbeat.proto` bit-identical to their pre-Phase-6
    contents.

---

## Risks & mitigations

- **Slot-file JSON byte-equality with Go.** `serde_json` emits keys
  in struct-declaration order; Go's `encoding/json` emits them
  sorted (or in struct order via tags). Mitigation: capture a
  Go-written slot file as the golden fixture; if `serde_json` diverges,
  switch to a manual `serialize_struct` with explicit field order
  (same approach Phase 5 used for `assignment.json`).
- **`PrepareCommit` / `PrepareAbort` re-introduction.** A future
  refactor might want explicit Prepare → Complete phases on disk so
  coordinator failover can resume mid-EndTxn. Mitigation: the
  `TxnState` enum already has the variants; the state machine just
  collapses them. Adding the persisted phase is a single point of
  change in `end_txn` — call this out in the crate README.
- **Cross-broker `EndTxn` (gh #114) regression vector.** Phase 6
  returns `NOT_COORDINATOR` (16) when the txn coordinator and a
  partition leader differ. A misconfigured client could read this as
  retriable and loop. Mitigation: add a unit test that
  asserts `EndTxn` against a cross-broker partition returns 16
  exactly once; document the gh #114 closure as the unblock.
- **Reaper `owns_txn` racing with controller reassignment.** The
  reaper checks ownership at tick start; a reassignment mid-tick
  could leave the new owner unaware of the abort. Mitigation: the
  abort fires the offset hook synchronously and writes the slot file
  before the tick returns; the new owner re-reads on first access
  (close-to-open). This is the same invariant Go relies on.
- **`FenceWatcher` poll missing a quickly-bumped epoch.** A peer
  writes PID 42 epoch 3, then immediately PID 42 epoch 4, both
  before our 2 s tick. Mitigation: the watcher reads the *current*
  contents of each peer file on each tick, so it always sees the
  highest epoch — no replay of intermediate values needed.
- **Control-batch attributes byte mismatch.** The v2 batch `attributes`
  bits for control + transactional are easy to get wrong (Apache's
  layout splits them across two bits in different bytes). Mitigation:
  the `control_batch.bin` fixture from a Go-written marker is the
  ground truth; the Rust encoder must round-trip byte-equal.
- **`TxnStateStore` num_slots drift.** If a future operator change
  bumps `num_slots`, the slot-hash for an existing `txn_id` changes
  and the new owner reads `Empty`. Mitigation: pin `NUM_SLOTS = 50`
  as a `pub const` in `txn_state.rs`; document that migration is
  Phase 6.1 (Go's `migrateLayout()` is the reference); add a
  compile-time `static_assertions::assert_eq` so a careless edit
  fails the build.
- **EOS bench against Go.** The franz-rs EOS path adds 4 extra
  round-trips per txn (`InitProducerId`, `AddPartitionsToTxn`,
  `AddOffsetsToTxn`, `EndTxn`). If the Rust implementation is
  noticeably slower than Go, the EOS smoke under load will diverge
  in throughput. Mitigation: capture a Go-side EOS throughput
  baseline before Phase 6 lands; assert the Rust path is within
  ±15 % in `bins/skafka/tests/eos_v2.rs` (looser than the Phase 2
  ±10 % storage bench because EOS is round-trip-bound, not
  throughput-bound).

---

## What this enables for Phase 7

After Phase 6 merges, Phase 7 (operator) inherits a working
transactional broker:

1. `KafkaUser` reconciler can mint principals that pass through
   `AddPartitionsToTxn` etc. without any code-side glue — the auth
   path is unchanged from Phase 4.
2. `CreatePartitions` / `IncrementalAlterConfigs` admin handlers
   land next to the existing dispatcher with no txn-side changes.
3. The cross-broker `WriteTxnMarkers` RPC (gh #114) is a Phase 7 or
   Phase 8 candidate; Phase 6 leaves the receiver-side handler
   ready, so closing gh #114 is "wire a same-cluster gRPC client
   into `EndTxn`" not "rewrite the marker path."

No further Phase 6 changes — Phase 7 is pure addition on top of the
transactional surface.
