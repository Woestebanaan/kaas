# Phase 3 Breakdown: Shared Storage Engine

## Current State (end of Phase 2)

Everything in Phase 1 and 2 is complete and all tests pass:

```
ok  github.com/woestebanaan/skafka/internal/protocol        (server, dispatcher, frame codec)
ok  github.com/woestebanaan/skafka/internal/protocol/codec  (primitives, RecordBatch, CRC32C)
ok  github.com/woestebanaan/skafka/internal/protocol/codec/api  (all 21 API codecs)
ok  github.com/woestebanaan/skafka/tests/kafka-compat       (franz-go + kafka-go e2e tests)
```

The broker binary (`cmd/skafka/main.go`) starts and accepts connections. Franz-go and
kafka-go can produce and consume records end-to-end against it. The storage layer is
currently a `MemoryStorage` stub in `internal/broker/stubs.go` — data is lost on restart.

### Key files to know before starting Phase 3

| File | Role |
|---|---|
| `internal/storage/engine.go` | `StorageEngine` interface (the target for Phase 3) |
| `internal/broker/stubs.go` | `MemoryStorage` stub — replace with real impl |
| `internal/lock/lock.go` | `PartitionLock` interface — implement `flock.go` and `nfs.go` |
| `internal/lease/manager.go` | `LeaseManager` interface — implement in Phase 4 |
| `internal/broker/broker.go` | Wires storage/lease/lock/auth into handlers |
| `internal/protocol/handlers/produce.go` | Calls `StorageEngine.Append` |
| `internal/protocol/handlers/fetch.go` | Calls `StorageEngine.Read` + `HighWatermark` |
| `internal/protocol/handlers/list_offsets.go` | Calls `HighWatermark` + `LogStartOffset` |
| `internal/protocol/codec/types.go` | `RecordBatch`, `Record`, `EncodeRecordBatch`, `DecodeRecordBatch` |

### Kubernetes cluster status (as of 2026-04-19)

Single-node k3s (NixOS 25.11, kernel 6.12.75, 20 CPU, 94GB RAM, 1.8TB ephemeral,
NVIDIA GPU). Only `local-path` storage class — NO ReadWriteMany support yet.
Options for Phase 3 testing:
- NixOS NFS server (`services.nfs.server`) + nfs-subdir-external-provisioner
- Rook-Ceph single-node (best for production fidelity, ~15 min install)
- hostPath volume for unit/integration tests without multi-pod RWX

---

## StorageEngine interface (what Phase 3 must implement)

```go
// internal/storage/engine.go
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
```

---

## Filesystem layout on PVC

```
/data/
  {topic}/
    {partition}/
      {base_offset:020d}.log        ← record data (RecordBatch format)
      {base_offset:020d}.index      ← sparse offset→file-position index
      {base_offset:020d}.timeindex  ← sparse timestamp→offset index
      .leader-epoch                 ← epoch of broker that wrote each segment
      .lock                         ← flock file (CephFS) or advisory sentinel (NFS)
  __cluster/
    acls.json                       ← operator writes; brokers inotify-watch
    credentials.json                ← SCRAM hashed credentials
```

Segment filename = zero-padded 20-digit base offset of the first record in that file.
Segment file format MUST match Apache Kafka exactly so `kafka-dump-log.sh` works out of
the box. This means storing raw RecordBatch bytes exactly as written by producers (no
re-encoding), and writing the offset index in Kafka's binary format.

---

## Step 3.1 — Segment file format

Files: `internal/storage/segment.go`

### .log file

The .log file is a sequence of RecordBatch entries concatenated together.
Each RecordBatch is:
```
[baseOffset:int64][batchLength:int32][...RecordBatch bytes as per Kafka spec...]
```

This is exactly the binary format produced by `codec.EncodeRecordBatch`. On produce,
we write the RecordBatch bytes verbatim — no re-encoding. On fetch, we read whole
RecordBatches and return them verbatim.

Key: we store the RAW bytes that arrived from the producer (after CRC validation),
not individual records. This means:
- `Append()` takes `[]storage.Record` but what we ACTUALLY write is the original
  RecordBatch bytes. We need to preserve the original bytes from the produce handler.

**Important design decision**: the current `StorageEngine.Append` takes `[]Record`
(decoded), but for disk storage we want the raw RecordBatch bytes. Two options:
a) Change `Append` to take `[]byte` (raw RecordBatch) — simpler, Kafka-compatible files
b) Keep `[]Record` and re-encode to RecordBatch on write — works but slightly more CPU

Recommendation: change `Append` to take `[]byte` (raw RecordBatch bytes). This also
means the produce handler should pass the raw bytes rather than decode→re-encode.
Update the interface and produce handler accordingly.

### .index file

Sparse offset index: one entry per `indexIntervalBytes` (default 4096) of log data.
Each entry is 8 bytes: relative_offset(int32) + position(int32).
- relative_offset = absolute_offset - base_offset_of_segment
- position = byte offset within the .log file where this batch starts

On fetch:
1. Binary search .index for the largest relative_offset ≤ requested_offset
2. Seek .log to that position
3. Scan forward until we find the exact offset or reach end of file

### Segment rolling

Roll to a new segment when active segment exceeds `segmentBytes` (default 1GB).
New segment filename = current high watermark (next offset to be written).

### Active segment tracking

```go
type activeSegment struct {
    baseOffset int64
    logFile    *os.File
    indexFile  *os.File
    logSize    int64      // bytes written so far
    lastOffset int64      // highest offset written
}
```

**Done when:** `go test ./internal/storage/...` passes with round-trip tests:
write RecordBatch → read it back → verify bytes identical.

---

## Step 3.2 — Two-lock write safety

Files: `internal/lock/flock.go`, `internal/lock/nfs.go`

### CephFS lock (flock.go)

```go
type FlockLock struct {
    dataDir string
    mu      sync.Mutex
    held    map[string]int  // key → fd
}

func (f *FlockLock) Lock(topic string, partition int32) error {
    path := filepath.Join(f.dataDir, topic, strconv.Itoa(int(partition)), ".lock")
    fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
    // syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB)
    // Store fd in f.held
}
```

`syscall.Flock` with `LOCK_EX|LOCK_NB` = exclusive non-blocking lock.
Returns EWOULDBLOCK if another process holds it (split-brain protection).

### NFS lock (nfs.go) — advisory only

For NFS, `flock()` is unreliable across nodes. Use a sentinel file approach:
- `.lock` contains `hostname:pid:timestamp`
- On Lock: write our identity, re-read, verify we won (simple advisory protocol)
- Warn prominently in logs: "NFS advisory lock — not safe for multi-pod production"

### Two-lock check in Append

```go
func (e *StorageEngine) Append(ctx context.Context, topic string, partition int32, raw []byte) (int64, error) {
    // BOTH locks required — defense in depth
    if !e.leases.IsLeader(topic, partition) {
        return -1, ErrNotLeader
    }
    if !e.locks.IsLocked(topic, partition) {
        return -1, ErrLockNotHeld
    }
    return e.writeToSegment(topic, partition, raw)
}
```

Write test: verify `Append()` returns `ErrNotLeader` when `IsLeader = false`, and
`ErrLockNotHeld` when lock is not held.

**Done when:** unit tests pass for both lock types and the two-lock rejection cases.

---

## Step 3.3 — StorageEngine implementation

File: `internal/storage/engine.go` (implementation, replacing the stub)

```go
type DiskStorageEngine struct {
    dataDir  string
    mu       sync.RWMutex
    segments map[string]*partitionState  // key: "topic/partition"
}

type partitionState struct {
    mu         sync.Mutex
    active     *activeSegment
    logStart   int64   // oldest available offset
    highWater  int64   // next offset to be written
}
```

### Append

1. Verify leader + lock held (Step 3.2)
2. Decode just the `baseOffset` and `numRecords` from the raw RecordBatch header
   (no full decode needed — just enough to know the offset range)
3. Write raw bytes to active .log file
4. Update .index if threshold crossed
5. Update `highWater = baseOffset + numRecords`
6. Roll segment if `logSize > segmentBytes`
7. Flush per flush policy (every N records, every M bytes, every T ms)

### Read

1. Find the segment whose base_offset ≤ startOffset (binary search segment list)
2. Seek to approximate position using .index binary search
3. Scan .log forward reading RecordBatches until:
   - The batch's baseOffset ≥ startOffset, start collecting
   - Accumulated bytes ≥ maxBytes, stop
4. Return raw RecordBatch bytes (the fetch handler sends them verbatim to the client)

Note: the `Fetch` handler in `internal/protocol/handlers/fetch.go` currently calls
`encodeRecords()` to build a new RecordBatch. Once the storage engine returns raw bytes,
update the fetch handler to pass them through unchanged.

### HighWatermark / LogStartOffset

Simple reads from `partitionState` — no disk I/O needed.

### CreatePartition / DeletePartition

```go
func (e *DiskStorageEngine) CreatePartition(topic string, partition int32) error {
    dir := filepath.Join(e.dataDir, topic, strconv.Itoa(int(partition)))
    return os.MkdirAll(dir, 0755)
}
```

**Done when:** integration test: produce 10,000 records via real skafka, restart the
broker, consume all 10,000 from disk, verify order and values.

---

## Step 3.4 — Leader epoch and takeover

File: `internal/storage/segment.go` (add to existing)

### .leader-epoch file

One file per partition directory: `/data/{topic}/{partition}/.leader-epoch`
Contains a single int64 (big-endian): current leader epoch.

### On leader takeover (called from LeaseManager.OnStartedLeading callback)

```
1. Acquire filesystem lock (Step 3.2)
2. Read .leader-epoch from disk
3. If disk_epoch < current_epoch:
   a. Open active .log file
   b. Scan backward from end, find last COMPLETE RecordBatch:
      - Read batchLength from each batch header
      - Validate CRC of the batch
      - Last batch with valid CRC = last complete write
   c. Truncate .log file at end of last valid batch
   d. Rebuild .index from scratch (scan forward from start of segment)
4. Write new epoch to .leader-epoch
5. Update highWatermark
6. Mark partition writable
```

The CRC validation reuses `codec.ValidateCRC` from `internal/protocol/codec/crc32c.go`.
The scanning uses our `codec.DecodeRecordBatch` reader.

**Done when:** test: simulate a crash mid-write (truncate .log at arbitrary byte), call
takeover, verify the truncation removes the partial batch and the offset index is rebuilt.

---

## Step 3.5 — inotify watch on cluster files

File: `internal/storage/watcher.go`

Uses the `fsnotify` library (add to go.mod: `go get github.com/fsnotify/fsnotify`).

```go
type ClusterFileWatcher struct {
    aclsPath        string
    credentialsPath string
    onACLReload     func(path string)
    onCredReload    func(path string)
}
```

- Watch `/data/__cluster/acls.json` and `/data/__cluster/credentials.json`
- Debounce 100ms after last event before calling reload callback
- The auth engine (Phase 7) registers the reload callbacks
- Log reload events at INFO

For Phase 3, just wire up the watcher infrastructure. The actual reload logic for ACLs
and credentials comes in Phase 7.

**Done when:** `go test ./internal/storage/...` includes a test that writes to a watched
file and verifies the callback fires.

---

## Step 3.6 — Retention cleaner

File: `internal/storage/cleaner.go`

Background goroutine, runs only on partition leader (checked via `LeaseManager.IsLeader`).
Runs every 5 minutes.

```go
func (c *RetentionCleaner) Run(ctx context.Context) {
    ticker := time.NewTicker(5 * time.Minute)
    defer ticker.Stop()
    for {
        select {
        case <-ticker.C:
            c.runOnce()
        case <-ctx.Done():
            return
        }
    }
}

func (c *RetentionCleaner) runOnce() {
    for _, p := range c.engine.allPartitions() {
        if !c.leases.IsLeader(p.topic, p.partition) {
            continue  // only leader runs cleaner
        }
        c.cleanPartition(p.topic, p.partition)
    }
}
```

Deletion policy:
- List all .log segments for the partition (sorted by base_offset, oldest first)
- For each segment (except the active one):
  - If segment's maxTimestamp < now - retentionMs: delete .log, .index, .timeindex
- Never delete the active segment

The `retentionMs` comes from the KafkaTopic CRD config (default 7 days = 604800000ms).
For Phase 3, read from a simple config struct; wiring to CRD watcher comes in Phase 6.

**Done when:** unit test: create 3 segments with old timestamps, run cleaner, verify
oldest 2 are deleted and active segment is untouched.

---

## Step 3.7 — Wire to broker and integration test

1. Replace `broker.NewMemoryStorage()` in `cmd/skafka/main.go` with `storage.NewDiskStorageEngine(dataDir)`
2. Replace `broker.NewLocalPartitionLock()` with `lock.NewFlockLock(dataDir)` (on CephFS)
   or `lock.NewNFSLock(dataDir)` (on NFS)
3. Update `internal/protocol/handlers/produce.go`:
   - Pass raw RecordBatch bytes to `Append` instead of decoded records
   - Remove the `decodeRecords()` call (no longer needed for storage)
4. Update `internal/protocol/handlers/fetch.go`:
   - Remove `encodeRecords()` — return raw bytes from storage directly
5. Add `CreatePartition` call in `CreateTopicsHandler` and `KafkaTopic` operator controller

Integration test (no Kubernetes needed — use hostPath):
```go
// tests/integration/disk_storage_test.go
func TestProduceConsumeRoundTrip(t *testing.T) {
    dir := t.TempDir()
    engine := storage.NewDiskStorageEngine(dir)
    engine.CreatePartition("test-topic", 0)
    
    // Produce 10,000 records
    // Restart engine (reopen files)
    // Consume all 10,000, verify order
}
```

**Done when:** the broker starts with real disk storage, and the e2e compatibility tests
(TestFranzGoProduceAndConsume, TestKafkaGoProduceAndConsume) still pass with data
surviving a broker restart.

---

## Dependencies to add before Phase 3

```bash
go get github.com/fsnotify/fsnotify@latest   # inotify watcher (Step 3.5)
```

No other new dependencies needed. All Kafka binary format work reuses the codec from
Phase 2 (`internal/protocol/codec/`).

---

## Interface change needed at the start of Phase 3

The current `StorageEngine.Append` takes `[]storage.Record` (decoded). For disk storage
we want raw RecordBatch bytes. At the start of Phase 3, change the interface to:

```go
// Before (current stub):
Append(ctx context.Context, topic string, partition int32,
       records []Record) (baseOffset int64, err error)

// After (Phase 3):
Append(ctx context.Context, topic string, partition int32,
       rawBatch []byte) (baseOffset int64, err error)
```

Update callers:
- `internal/protocol/handlers/produce.go`: pass `pd.Records` directly (raw bytes)
  instead of calling `decodeRecords(pd.Records)`
- `internal/broker/stubs.go` (MemoryStorage): decode the raw bytes internally for the
  stub, or keep the old interface for MemoryStorage only by having two implementations

Also update `StorageEngine.Read` to return raw RecordBatch bytes:

```go
// Before:
Read(ctx context.Context, topic string, partition int32,
     startOffset int64, maxBytes int) ([]Record, error)

// After:
Read(ctx context.Context, topic string, partition int32,
     startOffset int64, maxBytes int) ([]byte, error)
```

Update caller:
- `internal/protocol/handlers/fetch.go`: remove `encodeRecords()`, pass raw bytes
  directly to `FetchPartitionResponse.Records`

These interface changes are the FIRST thing to do at the start of Phase 3 before
implementing anything else. They touch: `engine.go`, `stubs.go`, `produce.go`,
`fetch.go`.

---

## Testing strategy for Phase 3 without CephFS

Since the cluster only has `local-path` (no RWX), Phase 3 tests can use:

1. **Unit tests** (`go test ./internal/storage/...`) — use `t.TempDir()`, no Kubernetes
2. **In-process integration** — start the broker pointing at a temp dir, run the
   compatibility tests (they already pass with MemoryStorage, must still pass with disk)
3. **Single-pod hostPath** — deploy to the k3s cluster with a hostPath volume (RWX not
   needed for single-pod dev). The storage engine code is identical; only the PVC
   provisioner changes for multi-pod production.

The multi-pod / multi-leader scenario (which requires RWX + flock) should be tested once
NFS or Ceph is available. For Phase 3, focus on single-broker correctness.

---

## Step order summary

| Step | File(s) | Depends on |
|---|---|---|
| 3.0 Interface change | `engine.go`, `stubs.go`, `produce.go`, `fetch.go` | nothing |
| 3.1 Segment read/write | `storage/segment.go` | 3.0 |
| 3.2 Filesystem locking | `lock/flock.go`, `lock/nfs.go` | 3.0 |
| 3.3 StorageEngine impl | `storage/engine.go` | 3.1, 3.2 |
| 3.4 Leader epoch/takeover | `storage/segment.go` | 3.3 |
| 3.5 inotify watcher | `storage/watcher.go` | 3.3 |
| 3.6 Retention cleaner | `storage/cleaner.go` | 3.3 |
| 3.7 Wire + integration test | `cmd/skafka/main.go`, handlers | 3.3–3.6 |

Start with 3.0, then 3.1 and 3.2 in parallel, then the rest in order.
