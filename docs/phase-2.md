# Phase 2 — Storage engine

Detailed work plan for the third phase of the Rust rewrite. Companion to
[`rewrite.md`](./rewrite.md); the high-level summary lives there. Builds on
the codec scaffolding from [`phase-1.md`](./phase-1.md) — `sk-storage`
consumes the byte-opaque `bytes::Bytes` produced by `sk-codec`'s Produce
decoder and never inspects record contents.

**Goal.** Land a port of `archive/internal/storage/` into `crates/sk-storage`
that preserves the Go engine's wire- and disk-format invariants byte-for-
byte and matches its hot-path throughput within 10%. The Go side is 10k LoC
and carries a decade of NFS-specific lessons; the Rust port keeps every
lesson intact and earns nothing by being clever.

**Length.** ~3 weeks, single engineer. Workstream A (filesystem trait +
manifest) blocks everything; once A is in, B (segments), C (idempotence),
and D (partition core) can land in parallel; E (engine seam) merges them;
F (recovery) and G (cleaner/compactor) ride out independently; H closes
with tests + bench.

**Out of scope for Phase 2.** Server bring-up (Phase 3 — `sk-protocol`),
auth gates on Append/Read (Phase 4), per-broker fence broadcast (Phase 6,
gh #108 phase 2), Reaper job-queue tuning beyond the v3 default. Tiered
storage is a non-goal per the parity boundary in `CLAUDE.md`. No `Record`
struct lands in `sk-storage` for the same reason it doesn't in `sk-codec`
— record bytes flow as `bytes::Bytes` end-to-end, and the byte-opacity
tripwires fire on the integration tests too.

**Scope boundary.** Every method on the Go `StorageEngine` interface
(`archive/internal/storage/engine.go:94-148`) must be representable. That
is: `Append`, `Read`, `HighWatermark`, `LogStartOffset`,
`OffsetForTimestamp` (gh #5), `OffsetForLeaderEpoch` (gh #101),
`DeleteRecords` (gh #31), `CreatePartition`, `DeletePartition`,
`PartitionSize`, `DataDir`, `TakeOver`, `Relinquish`. Plus the disk
formats: `manifest.json`, `producer-state.snapshot`, the
epoch-prefixed segment filenames (`{epoch:08x}-{base_offset:020d}.{log,index}`),
and the per-topic `.config.json` file the operator drops in
`/data/<topic>/`.

---

## Workstreams

Eight workstreams. A blocks everything; once A lands, B/C/D can land in
parallel; E threads them together; F/G/H land in order.

- **A** — Filesystem trait + manifest + on-disk types
- **B** — Segments (open, append, fsync, roll)
- **C** — Idempotent producer state + snapshot
- **D** — Partition core (mutex + atomic snapshot + committer task)
- **E** — `DiskStorageEngine` + `StorageEngine` trait + Append/Read hot path
- **F** — Recovery + takeover/relinquish + FD lifecycle
- **G** — Cleaner + compactor + `DeleteRecords` + Reaper
- **H** — Tests (proptest, fault injection, cross-engine) + bench harness

Dependencies: A blocks everything else; B/C/D land in any order after A;
E blocked by B/C/D; F blocked by B+E; G blocked by E; H lands last and
gates the merge.

---

## A — Filesystem trait + manifest + on-disk types

Goal: a `trait Fs` that every disk operation routes through, so the
fault-injection wrapper from workstream H can intercept every syscall, and
so a future `io_uring` impl is mechanical. Keep the trait small — read,
write, fsync, rename, remove, readdir. Anything richer (atomic-rename
helper, `tmp + rename` patterns) lives one layer up as a free function
that calls into the trait.

`crates/sk-storage/src/fs.rs`:

```rust
pub trait Fs: Send + Sync + 'static {
    fn open_read (&self, p: &Path)              -> io::Result<Box<dyn FileRead>>;
    fn open_write(&self, p: &Path, append: bool) -> io::Result<Box<dyn FileWrite>>;
    fn create    (&self, p: &Path)              -> io::Result<Box<dyn FileWrite>>;
    fn fsync     (&self, f: &mut dyn FileWrite) -> io::Result<()>;
    fn rename    (&self, from: &Path, to: &Path) -> io::Result<()>;
    fn remove    (&self, p: &Path)              -> io::Result<()>;
    fn mkdir_all (&self, p: &Path)              -> io::Result<()>;
    fn readdir   (&self, p: &Path)              -> io::Result<Vec<PathBuf>>;
    fn stat      (&self, p: &Path)              -> io::Result<Metadata>;
}

pub trait FileRead:  Read  + Seek + Send + 'static {}
pub trait FileWrite: Write + Seek + Send + 'static { fn as_raw(&self) -> RawFd; }
```

`RealFs` is the production impl — a thin wrapper over `std::fs`. Tests use
`FailingFs` from workstream H, which wraps `RealFs` with a per-call
`Result<(), io::Error>` knob keyed by syscall name + path glob.

**Tmp + rename helper.** `archive/internal/storage/manifest.go:78-119`
writes `manifest.json.tmp`, fsyncs it, then renames. Port that as a free
function in `crates/sk-storage/src/atomic_write.rs`:

```rust
pub fn atomic_write_json<T: Serialize>(fs: &dyn Fs, dir: &Path, name: &str, payload: &T)
    -> io::Result<()>
```

Same NFSv4 same-directory rename semantics; the function is small enough
to land here once and be reused by `manifest.rs` and `producer_snapshot.rs`.

`manifest.rs` mirrors `archive/internal/storage/manifest.go` byte-for-
byte: serde struct `Manifest { epoch: i64, high_watermark: i64,
log_start_offset: i64 }` with `#[serde(rename_all = "camelCase")]` so the
on-disk JSON matches the Go output exactly. `read` returns
`io::ErrorKind::NotFound` when both `manifest.json` and the legacy
`.leader-epoch` 8-byte file are absent. The legacy-file migration path
ports verbatim: if the manifest is missing but `.leader-epoch` exists,
parse 8 BE bytes → `Manifest { epoch, hwm: 0, log_start_offset: 0 }` and
let the caller fill in HWM via a segment scan.

`topicconfig.rs` mirrors `archive/internal/storage/topicconfig.go`:
`.config.json` per `/data/<topic>/`, every field `Option<i64>` (or
`Option<String>` for `cleanup_policy`) so "unset" survives the round
trip. Same camelCase JSON keys. The operator owns writes; the broker
reads and watches via `notify` (workstream D wires the watcher).

**Exit:** `RealFs` round-trips a `Manifest` through `tmp + rename` and
the file lands at exactly the byte layout the Go side produces (verified
by a diff against a captured `.json` fixture). `FailingFs` mock from H
exists in stub form so B/C/D can target a trait, not `std::fs`.

---

## B — Segments (open, append, fsync, roll)

Port `archive/internal/storage/segment.go` (722 LoC). The segment file is
the only place where `sendfile(2)` is touched on the hot path, so this
module is performance-critical. Keep the API surface tight.

`crates/sk-storage/src/segment.rs`:

```rust
#[derive(Debug, Clone, Copy)]
pub struct SegmentMeta {
    pub base_offset: i64,
    pub epoch: i64,
    pub size: u64,
}

pub struct ActiveSegment {
    meta: SegmentMeta,
    log:  Option<Box<dyn FileWrite>>,
    index: Option<Box<dyn FileWrite>>,
    next_offset: i64,           // next record offset (= base + count so far)
    bytes_since_index: i64,     // when ≥ index_interval_bytes, write an index entry
    max_timestamp: i64,         // for retention.ms + offset-for-timestamp
}

impl ActiveSegment {
    pub fn create(fs: &dyn Fs, dir: &Path, base: i64, epoch: i64) -> io::Result<Self>;
    pub fn open  (fs: &dyn Fs, meta: SegmentMeta, dir: &Path)   -> io::Result<Self>;
    pub fn open_handles  (&mut self, fs: &dyn Fs) -> io::Result<()>;
    pub fn close_handles (&mut self) -> io::Result<()>;
    pub fn append_batch  (&mut self, raw: &[u8], index_interval_bytes: i64) -> io::Result<()>;
    pub fn roll_fast     (self, fs: &dyn Fs, dir: &Path, new_base: i64, epoch: i64)
                         -> io::Result<(ActiveSegment, RolledTail)>;
    pub fn search_index  (&self, target_offset: i64) -> i64;
}

pub struct RolledTail { pub finalize: Box<dyn FnOnce() -> io::Result<()> + Send> }
```

**Filenames.** `segment_log_path(dir, base, epoch) = dir/{epoch:08x}-{base:020d}.log`
— preserve the Go format exactly. A `parse_segment_stem` helper recovers
`(base, epoch)` from a filename and also accepts the legacy
`{base:020d}.log` form for migration. Tests against captured
filenames from the Go broker confirm the format byte-for-byte.

**`roll_fast` vs `finalize`.** Mirror the Go split:

- `roll_fast` runs under `Partition` mutex: `log.fsync()` (the durability
  boundary), create new active segment + open its handles, return the
  old active wrapped in a `RolledTail` closure.
- The closure (a `Box<dyn FnOnce>`) is spawned with `tokio::task::spawn_blocking`
  by `Partition` after the lock drops: index fsync, close old log + index,
  manifest persist. The blocking-pool spawn matches Go's
  `go ps.finalizeAfterRoll()` and keeps the appender hot path free of
  index I/O.

**Single FD ownership.** `open_handles` / `close_handles` are the
Phase 4 (Go-side) follow-up to gh #76 — only the partition's current
leader holds the active segment's file descriptors. `Partition::take_over`
calls `open_handles`; `Partition::relinquish` calls `close_handles`.
Followers' `ActiveSegment` exists but `log == None && index == None`. A
`grep -rn 'Box<dyn FileWrite>' crates/sk-storage/` should show every
write-side FD coming from this seam; no other code should call
`fs.open_write` for a log/index file.

**Index entries.** Same 8-byte format as Go: `{offset_delta:i32}{file_pos:i32}`,
written every `index_interval_bytes` of log data (default 4096). Index is
sparse, so `search_index` returns the closest entry ≤ target and the
caller scans forward from there. Mmap is feature-gated behind
`#[cfg(feature = "mmap")]` (`memmap2` crate) per the Phase 0 lint
exception in `clippy.toml` — the index is the one place `unsafe` is
allowed in the workspace, and the unsafe is bounded to a single function.

**`recover_segment` + `rebuild_index`** live in workstream F (recovery),
not here. The segment module exposes the structural primitives only.

**Exit:** `proptest` over `(N batches, batch_size)` → `append → fsync →
re-open → byte-equal log file`. `roll_fast` produces a finalised tail
that closes cleanly when its closure runs. Filename parser accepts both
formats and rejects malformed stems.

---

## C — Idempotent producer state + snapshot

Port `archive/internal/storage/idempotence.go` + `producer_snapshot.go`.
This is the gh #12 contract: producers see duplicate-detection across
in-flight retries, the dedupe window survives broker restart, and a
fenced producer cannot resurrect via a stale PID.

`crates/sk-storage/src/idempotence.rs`:

```rust
const RING_SIZE: usize = 5;   // Java's max.in.flight.requests.per.connection default

#[derive(Debug, Clone)]
pub struct ProducerEntry {
    pub pid: i64,
    pub epoch: i16,
    pub ring: [Option<BatchSlot>; RING_SIZE],
    pub last_seq: i32,            // last accepted base_sequence (for monotonicity)
    pub last_offset: i64,
}

#[derive(Debug, Clone, Copy)]
pub struct BatchSlot { pub base_seq: i32, pub last_seq: i32, pub base_offset: i64 }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Duplicate(i64),   // echo cached base_offset, no log write
    OutOfOrder,       // wire error 45 (OUT_OF_ORDER_SEQUENCE_NUMBER)
    InvalidEpoch,     // wire error 47 (INVALID_PRODUCER_EPOCH)
    Accept,
}

pub struct ProducerStates {
    inner: DashMap<i64, ProducerEntry>,   // pid → entry
}

impl ProducerStates {
    pub fn classify(&self, info: BatchProducerInfo) -> Outcome;
    pub fn record_accepted(&self, info: BatchProducerInfo, base_offset: i64);
    pub fn fence(&self, pid: i64, new_epoch: i16);   // gh #108 cross-broker fence
    pub fn snapshot(&self) -> Vec<ProducerEntry>;
    pub fn restore(&self, entries: Vec<ProducerEntry>);
}
```

`BatchProducerInfo` is a small struct populated by the codec-layer batch
header walker (the same path that drives the byte-opacity tripwire check
— `sk-codec::recordbatch_count` already reads everything up through
`producer_epoch + base_sequence + num_records`). The classifier reads only
PID, epoch, and base_sequence + num_records; records bytes are never
inspected.

**Concurrency.** `DashMap` is the right shape — sharded so concurrent
PIDs don't contend, and the per-shard lock window covers the
`classify → record_accepted` pair (called consecutively under
`Partition` mutex by the engine). No global lock around the producer
states.

**Snapshot format.** Port `producer_snapshot.go` 1:1: serde struct
`{version: u8, entries: Vec<ProducerSnapshotEntry>}` where each entry is
`{pid, epoch, last_seq, last_offset, ring: Vec<BatchSlot>}`. JSON not
protobuf — matches Go output for cross-port snapshot reuse.

**When written.** Mirror Go: on segment roll (`finalize_async`'s tail)
and on `Partition::relinquish`. Not on every append — the dedupe window
is reconstructable from the active segment if the snapshot is older, so
the snapshot is a fast-path optimisation for restart, not a correctness
requirement on every batch. Producer state is restored on
`Partition::take_over` before `recover_segment` runs.

**Exit:** `proptest` over `(produce sequence, retry interleavings) →
classifier outputs match Go reference for every batch`. Snapshot
round-trip: write → read → byte-equal. Cross-engine: feed the same
sequence to a `MemoryProducerStates` (HashMap-backed, no concurrency)
and `ProducerStates`, assert identical outputs.

---

## D — Partition core

Port `archive/internal/storage/engine.go:221-589` (the `partitionState`
struct + its committer goroutine). This is the concurrency hot path —
get it wrong and the broker either deadlocks under load or drops the
acks=all contract.

`crates/sk-storage/src/partition.rs`:

```rust
pub struct Partition {
    inner:    Mutex<PartitionInner>,         // write path
    snapshot: ArcSwap<ReadSnapshot>,         // lock-free read side
    flush:    FlushCoord,                    // committer signalling
    committer_handle: Option<JoinHandle<()>>,
}

struct PartitionInner {
    dir: PathBuf,
    active: ActiveSegment,
    closed: Vec<SegmentMeta>,
    log_start: i64,
    high_water: i64,
    epoch: i64,
    next_write_seq: u64,        // gh #132 sequence-numbered durability
    completed_write_seq: u64,
    pending_flush_records: i64,
    flush_barriers: BTreeMap<u64, u64>,  // flush_seq → writeSeq watermark
    requested_flush_seq: u64,
    completed_flush_seq: u64,
    flush_err: Option<StorageError>,     // sticky
    closing: bool,
    producer_states: ProducerStates,
    retention_bytes_override: i64,
    min_compaction_lag_ms_override: i64,
    delete_retention_ms_override: i64,
}

pub struct ReadSnapshot {
    pub high_water: i64,
    pub log_start: i64,
    pub epoch: i64,
    pub closed: Arc<Vec<SegmentMeta>>,
    pub active_meta: SegmentMeta,
}

struct FlushCoord {
    req_tx: mpsc::Sender<()>,
    cond:   Arc<Notify>,            // appenders wait here on acks=all
}
```

**Committer task.** One `tokio::task` per partition, spawned by
`Partition::open`. Loop:

1. `req_rx.recv().await` — wakeup on flush request.
2. Snapshot `(flush_seq, write_watermark)` under `inner.lock()`.
3. Wait for `completed_write_seq >= write_watermark` via a `Notify`.
4. Take the `inner.lock()` again, grab a clone of
   `inner.active.log.as_ref()?.try_clone()` so the next mutator can swap
   `inner.active` mid-fsync without invalidating the pointer.
5. Drop the lock; call `fs.fsync(log)` under
   `tokio::task::spawn_blocking` with a `tokio::time::timeout(FsyncMaxLatency)`
   wrapper.
6. On success: `inner.lock().completed_flush_seq = flush_seq;
   flush.cond.notify_waiters()`. On timeout (gh #95): same lock,
   `inner.flush_err = Some(StorageError::Stalled)`, broadcast, continue.

The `try_clone` is the gh #82 trick: the committer doesn't hold the
mutex during fsync, so concurrent Appends to the same partition share
one fsync round-trip instead of serialising one per record. Same
NFS-throughput multiplier the Go side gets.

**Atomic-mirror reads.** `Partition::high_watermark()` and
`Partition::log_start_offset()` read `snapshot.load()` — never take the
mutex. Mirrors the gh #134 fix where a stuck NAS fsync used to hold
`ps.mu` for the watchdog deadline and starve every OTel metric
callback. `ReadSnapshot` is rewritten via `ArcSwap::store(Arc::new(...))`
under the mutex alongside every HWM/log_start/segment-roll update.

**Topic-config watcher.** `Partition::watch_config(fs, &topic_dir)`
spawns a `notify`-driven task that re-reads `.config.json` on mtime
change and updates the per-partition override fields. Mirrors the Go
side's hot-reload — operator changes to retention.ms / segment.bytes
/ cleanup.policy take effect without a broker restart.

**`drain_and_exit`.** Mirror Go's `drainAndExit`: stop accepting new
flush requests, drain the in-flight ones, persist manifest +
producer snapshot one last time, close handles. Called by `Relinquish`.

**Exit:** `proptest` over `(N producers × M concurrent appends, flush
interval, acks=−1 vs acks=1)`: every acks=all append observes its data
durable after its `Append` future resolves; every acks=1 append's data
is durable within one committer cycle of its return. `tokio::test`
fault injection: fsync timeout → `flush_err` sticky → next Append
returns `StorageError::Stalled` immediately.

---

## E — DiskStorageEngine + StorageEngine trait + hot path

Port `archive/internal/storage/engine.go`'s top-level surface. This is
where the partition map lives, where the public `Append` / `Read`
methods route requests, and where the `StorageEngine` trait is defined
so tests can mock the engine without touching disk.

`crates/sk-storage/src/engine.rs`:

```rust
#[async_trait]
pub trait StorageEngine: Send + Sync + 'static {
    async fn append(&self, topic: &str, partition: i32, epoch: u32, acks: i16,
                    batch: Bytes) -> Result<i64, StorageError>;
    async fn read  (&self, topic: &str, partition: i32, start_offset: i64,
                    max_bytes: usize) -> Result<Bytes, StorageError>;
    fn high_watermark   (&self, topic: &str, partition: i32) -> Result<i64, StorageError>;
    fn log_start_offset (&self, topic: &str, partition: i32) -> Result<i64, StorageError>;
    fn offset_for_timestamp (&self, topic: &str, partition: i32, ts_ms: i64)
                             -> Result<(i64, i64), StorageError>;
    fn offset_for_leader_epoch (&self, topic: &str, partition: i32, leader_epoch: i32)
                                -> Result<(i32, i64), StorageError>;
    async fn delete_records (&self, topic: &str, partition: i32, target_offset: i64)
                             -> Result<i64, StorageError>;
    async fn create_partition (&self, topic: &str, partition: i32) -> Result<(), StorageError>;
    async fn delete_partition (&self, topic: &str, partition: i32) -> Result<(), StorageError>;
    fn partition_size  (&self, topic: &str, partition: i32) -> i64;
    fn data_dir        (&self) -> &Path;
    async fn take_over (&self, topic: &str, partition: i32, epoch: u32)
                        -> Result<i64, StorageError>;
    async fn relinquish (&self, topic: &str, partition: i32) -> Result<(), StorageError>;
}

pub struct DiskStorageEngine {
    fs: Arc<dyn Fs>,
    cfg: Config,
    partitions: DashMap<(String, i32), Arc<Partition>>,
    reaper: Option<Arc<PartitionReaper>>,
}
```

**`MemoryStorage`.** A second `StorageEngine` impl that keeps log bytes
in a `Vec<Bytes>` per partition. Used by the dev-mode path (no
`MY_POD_NAME`), and by unit tests that don't need disk. Ports
`archive/internal/broker/memory_storage.go` — same byte-opacity contract
(records bytes flow through unchanged).

**Read hot path.** Mirror Go's `ReadSegmentRef` for splice support: the
read path returns a `SegmentRef` (= file handle + offset + length +
cleanup closure) when the records live on disk and the caller can
`sendfile(2)` them straight to the wire. The `Bytes`-returning
`StorageEngine::read` is the fallback for memory storage and the
non-splice code paths. Phase 3's Fetch handler uses `SegmentRef`; Phase
2 lands the helper. The `Bytes`-returning variant just slurps the
segment into a buffer and returns it.

**Append hot path.** Same algorithm as Go:

1. Lock `inner`.
2. Classify against `producer_states` (`Outcome::Duplicate` echoes the
   cached base_offset, no log write).
3. Check epoch fence (`epoch < inner.epoch` → `ErrEpochMismatch`).
4. Maybe roll segment if `inner.active.size + batch.len() > SegmentBytes`
   (calls `Partition::roll_fast`; the finalize closure is spawned via
   `spawn_blocking` after the lock drops).
5. Append batch bytes to `inner.active.log` (the actual `pwrite`).
6. Update `inner.high_water`, `inner.next_write_seq`,
   `inner.pending_flush_records`.
7. Rewrite `ReadSnapshot` and `snapshot.store(...)`.
8. If `pending_flush_records >= FlushIntervalMessages`, send on
   `flush.req_tx` (non-blocking — the channel has capacity 1 with
   coalescing).
9. Drop the lock.
10. If `acks == -1`, wait on `flush.cond` until our `flush_seq` is
    `<= completed_flush_seq` (or `flush_err.is_some()`).

The Go version's `gh #132 v2` sequence-numbered durability is what
lets step 5 happen outside the mutex (via the pwrite to a fd we cloned
under the lock). Phase 2 keeps that — under-lock atomic pwrite is fine
on small batches, but a megabyte produce blocking the whole partition
on a slow NFS is exactly the workload group-commit fixes.

**Error type.** `StorageError` enum (thiserror): `Stalled`, `EpochMismatch`,
`OutOfOrderSequence`, `DuplicateSequence`, `InvalidProducerEpoch`,
`UnknownTopicOrPartition`, `OffsetOutOfRange`, `Closed`, `Io(io::Error)`.
Mirrors the sentinel set the Go side returns and that the protocol
handlers map to wire error codes.

**Exit:** `MemoryStorage` and `DiskStorageEngine` both pass the same
black-box integration test (append → fetch → byte-equal) for every
combination of `acks ∈ {-1, 1}`, `flush_interval_messages ∈ {1, 10, 0}`.
`StorageEngine` trait object is dynamically dispatched and lives behind
`Arc<dyn StorageEngine>` everywhere — no generic spread.

---

## F — Recovery + takeover/relinquish + FD lifecycle

Port `recoverSegment`, `rebuildIndex`, `TakeOver`, `Relinquish`, and the
phantom-HWM healer from `engine.go:1729-1893`. This is where the gh #76
"leader-only FDs" contract is enforced.

`crates/sk-storage/src/recover.rs`:

```rust
pub fn recover_segment(seg: &mut ActiveSegment, fs: &dyn Fs)
    -> Result<i64, StorageError>;   // returns recovered_hwm

pub fn rebuild_index(seg: &mut ActiveSegment, fs: &dyn Fs, interval: i64)
    -> Result<(), StorageError>;
```

`recover_segment` scans the active segment from `segment_base_offset`
forward, reading 12-byte batch prefixes (offset + length), validating
that each batch's wire length fits, and stopping at the first truncated
batch or invalid header. Same algorithm as
`archive/internal/storage/segment.go:627-678`. Reads only the 12-byte
prefix (with one 4-byte timestamp peek for `max_timestamp`); records
bytes are never decoded. Byte-opacity intact.

`rebuild_index` walks the active segment from start to end, writing a
sparse index entry every `interval` bytes. Used when the index file is
missing or stale on takeover.

`takeover.rs`:

```rust
impl DiskStorageEngine {
    pub async fn take_over(&self, topic: &str, partition: i32, epoch: u32)
        -> Result<i64, StorageError> {
        // 1. Get or create the Partition entry.
        // 2. Lock its inner.
        // 3. Persist new epoch into manifest.
        // 4. Open active segment handles (the gh #76 contract).
        // 5. Recover any partial writes via recover_segment.
        // 6. Rebuild the index if the index file is missing or shorter
        //    than the log warrants.
        // 7. Restore producer state from producer-state.snapshot.
        // 8. Persist manifest + producer snapshot one final time
        //    (write the new epoch + recovered HWM to disk).
        // 9. Return the recovered HWM.
    }

    pub async fn relinquish(&self, topic: &str, partition: i32)
        -> Result<(), StorageError> {
        // 1. drain_and_exit the partition's committer.
        // 2. Persist manifest + producer snapshot.
        // 3. close_handles on the active segment.
        // 4. Optionally enqueue into reaper for slow follow-up work.
    }
}
```

**`RelinquishAll`** iterates `self.partitions` and calls `relinquish` on
each. Used by `bins/skafka`'s SIGTERM path (gh #61 + gh #139). Port
`splitPartKey` for the `topic/partition` key encoding — parse from the
right to handle slash-bearing topic names.

**Phantom HWM healer.** Port `shouldHealPhantomHWM` /
`dropEmptyClosedSegmentsLocked`. A "phantom HWM" is a manifest carrying
an HWM that points past the actual log bytes — happens when the
operator deletes segments by hand or when a partial roll left the
manifest ahead of the index. The healer rewinds HWM to the largest
offset present in the segment files.

**Exit:** `proptest` over `(N batches, crash points)`: kill the engine
at random points during Append, restart, verify HWM points to the last
durable batch and no record beyond is fetchable. Cross-engine: feed the
same sequence to `MemoryStorage` and `DiskStorageEngine`, restart the
disk engine mid-sequence, assert the recovered byte stream is a prefix
of the memory engine's.

---

## G — Cleaner + compactor + DeleteRecords + Reaper

Port `cleaner.go`, `compactor.go`, `compactor_decoder.go`, `reaper.go`.

`crates/sk-storage/src/cleaner.rs` — size + age retention. Per
partition: walk closed segments oldest-first, drop while total size >
retention bytes OR `max_timestamp` is older than retention.ms. The
active segment is never reaped from here; `DeleteRecords` handles
active-segment reclaim. Cleaner runs on a tokio interval; gated on the
partition being owned by this broker (`Coordinator::owns(topic,
partition)` — wired in Phase 5; Phase 2 stubs this to "always true" in
dev mode).

`compactor.rs` — log-compaction. Port the Go side verbatim including
the gh #116 knobs:

- `min.compaction.lag.ms` (KIP-58): a segment is eligible only if its
  `max_timestamp` is older than `now - min.compaction.lag.ms`. Default
  0 = no gate.
- `delete.retention.ms` (KIP-354): a tombstone (record value = null) is
  retained until its batch's `base_timestamp` is older than `now -
  delete.retention.ms`, then dropped. Default 0 = tombstones live
  forever. Granularity is per-batch in skafka (Apache is per-record);
  that's an explicit divergence and documented in `CLAUDE.md` —
  preserve it in the Rust port.

**Byte opacity in the compactor.** Compaction needs record keys, so the
compactor IS the one place in the broker that reads record contents.
The Go side guards this with `BumpCodecRecordDecode("compactor")` so
the byte-opacity tripwire fires only for the compactor's site. Port
that contract: the Rust compactor calls
`sk_codec::tripwires::bump_codec_record_decode("compactor")` on every
batch it decodes, and `sk_codec::tripwires::bump_codec_batch_reencode("compactor")`
on every batch it re-emits. The byte-opacity integration test in
`sk-test-harness` runs against the **Produce/Fetch** path only, never
the compactor — so the tripwire-zero invariant holds where it should
and the compactor's per-batch counter increment is the expected
exception.

The compactor lives in `crates/sk-storage` (where its callers are), but
the record-level helpers it consumes live in `crates/sk-test-harness::recordbatch`
per the byte-opacity rule from Phase 1. Move them out of
`sk-test-harness` only if the integration tests' grep gate complains —
preferred path is to keep `sk-test-harness::recordbatch` as the
shared record helper and make the compactor depend on it as a
`[dependencies]`, not `[dev-dependencies]`.

`delete_records.rs`:

```rust
impl DiskStorageEngine {
    pub async fn delete_records(&self, topic: &str, partition: i32, target_offset: i64)
        -> Result<i64, StorageError>;
}
```

Same algorithm as Go: advance `log_start_offset`, drop closed segments
whose `last_offset < log_start_offset`, and when `log_start_offset >=
high_water` reclaim the active segment too (close handles, delete log +
index, create a fresh empty active at `log_start_offset`).
`target_offset == -1` is the "purge to HWM" sentinel from KIP-107.

`reaper.rs` — port `archive/internal/storage/reaper.go`. Async work
queue for the slow phase of `ClosePartition` (NFS unlink can take
seconds on a deep dir). Bounded `mpsc::Sender<reapJob>`, N worker
tasks, metrics on queue depth (Phase 8 wires the OTel meter). Phase 2
lands the structure with default-off (`reaper: None` everywhere except
when explicitly enabled) — the engine falls back to synchronous close.

**Exit:** cleaner test ports
`archive/internal/storage/cleaner_test.go` shape: produce N records,
wait past retention.ms, run cleaner, assert oldest segments gone.
Compactor lag test ports
`compactor_lag_test.go`: produce, advance clock past
min.compaction.lag.ms, compact, assert tombstones survive until
delete.retention.ms elapses. DeleteRecords test
covers the active-segment reclaim path
(`log_start >= high_water`).

---

## H — Tests (proptest + fault injection + cross-engine) + bench

The Go storage package has 7 KB of tests across 25 files — every
invariant the v3 broker depends on has a regression test. The Rust port
inherits all of them in shape, not as line-by-line ports.

`crates/sk-storage/tests/` layout:

```
tests/
  fault_injection.rs      # FailingFs wrapper: drop fsync, return EIO mid-write
  group_commit.rs         # gh #82: N concurrent appends share one fsync
  idempotence.rs          # gh #12: dedupe window survives restart
  cross_engine.rs         # MemoryStorage vs DiskStorageEngine byte-equal
  takeover.rs             # gh #76 + recovery
  cleaner.rs              # retention.ms + retention.bytes
  compactor.rs            # gh #116 lag + tombstone retention
  delete_records.rs       # gh #31 + active-segment reclaim
  byte_opacity.rs         # records pass through unchanged; tripwires == 0
  relinquish_all.rs       # SIGTERM drain (gh #61 + gh #139)
  phantom_hwm.rs          # gh #?, manifest > log → healer rewinds
```

**`FailingFs`.** Wraps a `RealFs` (or a memory-backed fake) with a
per-syscall `FnMut(&str, &Path) -> Result<(), io::Error>` knob. Tests
configure scenarios like "fail next fsync on path matching
*partition-0/*.log*" and verify the engine returns `StorageError::Stalled`
without corrupting state. Lives behind `#[cfg(test)]` so production
builds carry no overhead.

**`proptest` strategies.** One strategy per shape:
- `arb_batch_sequence`: `Vec<(producer_id, epoch, base_seq, num_records, batch_size)>`
  generating valid v2 RecordBatch frames via `sk-test-harness::recordbatch`.
- `arb_crash_point`: a `usize` selecting which batch in the sequence the
  engine crashes after.
- `arb_fsync_failure`: an `Option<usize>` selecting which fsync call
  returns EIO.

Three meta-properties: append-fetch byte equality; crash-recovery
prefix; idempotent-producer dedupe across retries.

**Cross-engine.** Drive the same sequence into `MemoryStorage` and
`DiskStorageEngine`; after every batch, assert
`memory.read(...) == disk.read(...)` byte-for-byte. Both engines see
the same byte-opacity tripwire baseline (`record_decode == 0`,
`batch_reencode == 0`) at the end of the run.

**Bench.** `crates/sk-storage/benches/append.rs` and `read.rs`,
criterion-driven. Two backends:

- tmpfs (in-process, baseline) — measures fsync-per-batch latency
  without the NFS round-trip noise.
- NFS loopback (`mount -t nfs localhost:/tmp /mnt`) — measures the
  group-commit benefit under the production substrate.

Targets per Phase 2 exit criterion: within 10% of Go's measured
throughput on the same hardware. The Go numbers live in
`archive/internal/storage/*_test.go`'s `BenchmarkXxx` outputs; capture
a baseline before the Rust port lands and check it into
`benches/baseline.md`.

CI runs the proptest suite at default budget (256 cases / strategy);
nightly cranks to 4096 against `main`.

**Exit:** every test file above is green; `cargo bench -p sk-storage`
on tmpfs reports per-fsync throughput within 10% of the Go number on
the same hardware; tripwire integration test confirms record_decode +
batch_reencode counts are 0 across the full storage test sweep.

---

## Phase 2 exit criteria (all must hold)

1. `cargo test -p sk-storage` runs in under 60 s on a warm cache and is
   green.
2. `cargo bench -p sk-storage` per-fsync throughput on tmpfs is within
   10% of the Go reference number captured at the start of Phase 2.
3. `cargo bench -p sk-storage` per-fsync throughput on NFS-loopback is
   within 10% of the Go reference number.
4. `cargo clippy -p sk-storage --all-targets -- -D warnings` passes.
   No `unwrap()` / `expect()` / `panic!()` / `as` casts outside
   `#[cfg(test)]` and the mmap-feature-gated module.
5. Byte-opacity invariant holds against the Produce/Fetch path: the
   `crates/sk-storage/tests/byte_opacity.rs` integration test asserts
   `sk_codec::tripwires::record_decode_count() == 0` and
   `batch_reencode_count() == 0` after a full produce-fetch round trip
   through `DiskStorageEngine`. The compactor's per-batch increments
   are visible only in `tests/compactor.rs`.
6. On-disk format is byte-identical to Go for the three persisted
   files: `manifest.json` (verified against a captured fixture),
   `producer-state.snapshot` (likewise), and segment filenames
   (`{epoch:08x}-{base:020d}.log`).
7. `bins/skafka` is wired against the trait — `Box<dyn StorageEngine>`
   in `Broker`; `DiskStorageEngine` in prod, `MemoryStorage` in
   dev-mode (selected by `MY_POD_NAME` env presence).
8. Recovery test: kill the engine at 100 random points during a
   10k-batch produce sequence, restart, assert the recovered byte
   stream is a prefix of the originally produced one for every run.
9. The Go tree under `archive/` is unchanged. Chart, CRDs, scripts,
   and `proto/heartbeat.proto` are bit-identical to their pre-Phase-2
   contents.

If any of these fail, do not merge — fix and re-run.

---

## Risks & mitigations

- **NFS fsync error reporting differs between Go and Rust.** Linux NFS
  returns `EIO` on `fsync` after a writeback failure, but the precise
  conditions (which writes are dropped, which are retried) depend on
  kernel version and NFS option set. Mitigation: every fsync error
  path in the engine is sticky (`flush_err` field, never cleared) and
  surfaces as `StorageError::Stalled` to the next Append. Phase 2 ports
  the gh #95 fsync-watchdog deadline verbatim. A divergence on a
  specific kernel/NFS option set is a real risk; mitigation is the
  bench-compare integration on the in-cluster NFS substrate before
  Phase 9 cutover.
- **`tokio::fs::File::sync_all` is not equivalent to Go's
  `os.File.Sync` under all conditions.** Tokio routes fsync through
  the blocking pool, which is fine, but the worker thread's panic
  recovery differs. Mitigation: every fsync runs inside
  `spawn_blocking` with a `tokio::time::timeout` wrapper, and the
  caller treats a `JoinError` the same as a timeout — engine in
  `Stalled` state.
- **`DashMap` lock fairness under contention.** `DashMap` uses
  parking-lot's RwLock; under heavy contention on a single shard, the
  per-shard write lock can starve readers. The producer-state map is
  the only path where this matters in practice (every Append touches
  it). Mitigation: keep the `classify → record_accepted` window
  small (no allocations, no syscalls), and benchmark the multi-PID
  contention scenario in `bench/append.rs`. Fall back to
  `parking_lot::RwLock<HashMap>` if `DashMap` shows up in flamegraphs.
- **`spawn_blocking` cost amortisation on small batches.** Routing
  every fsync through the blocking pool adds ~20 µs per call. On
  10-byte produce batches that's 5% of the total path. Mitigation:
  group-commit (which Phase 2 ports anyway) makes this a fixed cost
  per *cycle*, not per batch — N concurrent appends share the one
  `spawn_blocking` call.
- **`memmap2` portability.** mmap is the one unsafe boundary in the
  workspace. The `Fs` trait is also useful here: mmap-backed index
  reads sit behind `#[cfg(feature = "mmap")]` and the fallback is a
  plain pread loop. Mitigation: ship mmap as a feature-gated
  optimisation in Phase 2 and benchmark it off vs on before deciding
  whether to default-on in Phase 9.
- **`spawn_blocking` pool exhaustion under many partitions.** The
  default Tokio runtime caps the blocking pool at 512 threads. A broker
  with 1000 partitions all fsyncing concurrently saturates the pool
  and Appends start blocking on `tokio::task::spawn_blocking` itself.
  Mitigation: every committer is its own task that runs sequentially
  per partition, so the blocking pool concurrency is bounded by the
  number of partitions currently flushing, not the number of partitions
  total. If we see pool exhaustion in the in-cluster bench, bump the
  pool via `Runtime::Builder::max_blocking_threads(...)` in
  `bins/skafka/main.rs`.

---

## What this enables for Phase 3

After Phase 2 merges, Phase 3 (single-broker server) lands by:

1. Wiring `bins/skafka/main.rs` to construct a `DiskStorageEngine`
   (prod) or `MemoryStorage` (dev) behind `Arc<dyn StorageEngine>`,
   and handing it to the `Broker` struct.
2. The Produce handler in `crates/sk-protocol/src/handlers/produce.rs`
   pulling `records: Option<Bytes>` straight from
   `sk_codec::api::produce::Request` and calling
   `engine.append(...)` — no decode, no reencode.
3. The Fetch handler using `DiskStorageEngine::read_segment_ref(...)`
   for the splice fast path, falling back to
   `StorageEngine::read(...)` for memory storage and other cold paths.
4. The SIGTERM drain path calling `engine.relinquish_all()` before
   the K8s pod terminates.

No further storage changes — Phase 3 is pure server bring-up against a
stable storage API.
