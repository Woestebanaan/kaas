# Storage engine hot path

Group-commit fsync, segment files, the manifest — and how a Produce and a Fetch request travel the broker end to end.

## A Produce request, end to end

```mermaid
sequenceDiagram
    participant C as Client
    participant L as Listener<br/>(kaas-protocol)
    participant H as Produce handler
    participant P as Partition
    participant K as Committer task

    C->>L: Produce (RecordBatch bytes)
    L->>L: read_frame → decode_request_header
    L->>H: dispatch by api_key<br/>(per-listener pre-auth gate)
    H->>H: topic exists? · Coordinator::owns?<br/>heartbeat fresh (self-fence)? · ACL Write?
    Note over H: any gate fails →<br/>NOT_LEADER_FOR_PARTITION (6) /<br/>TOPIC_AUTHORIZATION_FAILED (29)
    H->>P: engine.append(topic, partition, epoch, acks, bytes)
    activate P
    Note over P: under inner.lock()
    P->>P: sticky flush_err? · epoch fence
    P->>P: idempotence classify (PID, epoch, seq)<br/>duplicate → echo cached base_offset, no write
    P->>P: segment full? → roll_fast
    P->>P: rewrite baseOffset → HWM,<br/>append_batch (pwrite, bytes verbatim)
    P->>P: snapshot.store(ArcSwap) — lock-free readers
    P->>P: pending ≥ flush_interval_messages?<br/>→ requested_flush_seq += 1
    deactivate P
    P-->>K: flush.req_tx.try_send(()) — mpsc(1), coalesces
    opt acks = -1 and this append crossed the flush threshold
        Note over P: all concurrent appenders to this<br/>partition park on the same Notify —<br/>one fsync cycle serves them all
        K->>K: lock: target = requested_flush_seq
        K->>K: spawn_blocking → lock + log.sync_all()<br/>30 s watchdog → sticky Stalled on timeout
        K-->>P: completed_flush_seq = target,<br/>notify_waiters()
    end
    H-->>C: ProduceResponse(base_offset,<br/>throttle_time from one post-append quota check)
```

The RecordBatch bytes are never parsed on this path — only two fixed-size
header peeks (producer info for idempotence, offsets for assignment); the same
opaque bytes the client sent land verbatim on disk. The quota check runs once
per request over the summed byte count, after the appends, and feeds
`throttle_time_ms`.

## A Fetch request, end to end

```mermaid
sequenceDiagram
    participant C as Client
    participant H as Fetch handler
    participant E as Storage engine

    C->>H: Fetch (decode + dispatch, same front door as Produce)
    H->>H: topic exists? · Coordinator::owns? · ACL Read?
    Note over H: read cap = last stable offset (read_committed)<br/>or high watermark (read_uncommitted)
    H->>E: read(topic, partition, fetch_offset, max_bytes)
    E->>E: walk closed segments + active,<br/>copy batch bytes into memory
    E-->>H: Bytes (opaque, undecoded)
    H->>H: trim_to_offset(read_cap) — whole batches only
    H->>H: aborted_transactions_in_range<br/>→ AbortedTransaction list (read_committed)
    H-->>C: FetchResponse(session_id = 0,<br/>records bytes verbatim)
```

Two response-shape facts:

- **Stateless fetch sessions** (gh #4): `session_id = 0` on every response —
  Apache's documented contract for "broker doesn't support sessions", so clients
  send full Fetch data per request.
- **The response is materialized bytes**, copied from the segment files; a
  `sendfile`/splice zero-copy path is a future optimisation (the codec keeps
  records byte-opaque exactly so that a splice path stays possible).

## Concurrency model: inside one Partition

What runs under the partition mutex, what the per-partition committer task
does, and what segment roll defers to a background task:

```mermaid
flowchart TB
    subgraph appender["append() — any producer connection"]
        direction TB
        lock["inner.lock()"] --> classify["sticky-error check · epoch fence ·<br/>idempotence classify"]
        classify --> roll{"segment full?"}
        roll -- yes --> rollfast["roll_fast: fsync log ·<br/>create new active · pointer swap<br/>(old FDs move into deferred closure)"]
        roll -- no --> write
        rollfast --> write["append_batch → pwrite on active log"]
        write --> snap["snapshot.store (ArcSwap)"]
        snap --> trigger["pending ≥ flush_interval_messages?<br/>requested_flush_seq += 1"]
        trigger --> unlock["drop lock"]
        unlock --> wait["acks = -1 and triggered:<br/>park on Notify until<br/>completed_flush_seq ≥ mine"]
    end

    subgraph committer["committer task — one per partition"]
        direction TB
        recv["req_rx.recv()"] --> target["lock: target = requested_flush_seq<br/>(skip if already completed)"]
        target --> fsync["spawn_blocking { lock(); log.sync_all() }<br/>fsync runs holding the partition mutex"]
        fsync --> watchdog{"done within 30 s?"}
        watchdog -- yes --> ok["completed_flush_seq = target<br/>notify_waiters()"]
        watchdog -- "timeout / io error" --> dead["sticky flush_err = Stalled —<br/>partition fails fast on next append;<br/>orphaned fsync drains in background"]
    end

    subgraph deferred["deferred finalize — spawn_blocking after roll"]
        fin["fsync index · close old log + index FDs"]
    end

    trigger -- "try_send(()) on mpsc(1) — coalesces" --> recv
    ok -. "notify" .-> wait
    rollfast -.-> fin
    readers["lock-free readers: high_water() / log_start()<br/>load the ArcSwap snapshot — never touch the mutex"] -.-> snap
```

Three properties that make this work:

1. **Group commit**: N concurrent appenders share one fsync cycle — the
   capacity-1 flush channel coalesces requests, and every waiter with
   `flush_seq ≤ completed` wakes on the same `notify_waiters()`.
2. **Lock-free reads** (gh #134): HWM / log-start observation goes through the
   `ArcSwap` snapshot, so a stuck NFS fsync can't stall metrics callbacks.
3. **Fsync watchdog** (gh #95): a hung NFS server trips the 30 s timeout, sets a
   sticky `Stalled` error, and the partition fails fast instead of hanging
   appenders forever.

Note the current committer holds the partition mutex for the fsync window
(readers are unaffected via the ArcSwap; concurrent appenders queue on the
lock). Fsyncing a cloned FD outside the mutex is a known follow-up, not what
ships today.

## On-disk layout

```text
/data/__cluster/                  ── cluster-wide files
    assignment.json
    credentials.json
    acls.json
    txn_state/
        slot-00.json              ── 50 slots, hash(transactional_id) % 50
        ...
    producer_fences/
        from-kaas-0.json          ── one per broker; cross-broker producer
        ...                          epoch fence broadcast (gh #108 phase 2)
    marker_queue/
        to-kaas-1/                ── per-broker txn marker inbox (gh #175)
            <pid>-<epoch>.json
    __consumer_offsets/
        <group_id>.json           ── per-group offset file

/data/<topic>/
    .config.json                  ── operator-written; retention / segment
                                     bytes / compaction knobs

/data/<topic>/<partition>/
    manifest.json                 ── { epoch, highWatermark, logStartOffset }
    producer-state.snapshot       ── idempotent-producer dedupe window
    00000005-00000000000000000000.log   ── epoch=5, base_offset=0
    00000005-00000000000000000000.index ──   8-byte (rel_offset, file_pos)
    00000005-00000000000000001000.log   ── epoch=5, base_offset=1000
    ...
```

Each segment is a pair of files; the filename carries both the **leader epoch**
and the **base offset**, so a stale ex-leader's writes can never physically
collide with a new leader's segment:

```text
{epoch:08x}-{base_offset:020d}.log     ── append-only log of v2 RecordBatches
{epoch:08x}-{base_offset:020d}.index   ── sparse offset index, 8 bytes/entry
```

The manifest is written on partition open (takeover routes through open) and
on close/relinquish — **not** on segment roll and not per append — so its
`highWatermark` can lag in-memory state; recovery treats the log as
authoritative and reconciles on open by scanning the active segment to the
first malformed batch boundary.

The index is **sparse**: one `(rel_offset: i32, file_pos: i32)` entry every
`index.interval.bytes` of log data (4 KiB default). Lookup binary-searches to
the closest entry ≤ the target offset, then scans the log forward. The index
is *not* fsynced on the hot path — it's rebuildable from the log during
takeover recovery, so only the log's durability is on the `acks=all`
promise. Mmap of the index is feature-gated behind `mmap`, the one
unsafe-code carve-out in the workspace.

## Byte opacity: the broker never parses records

Exactly three places on the Produce path touch the RecordBatch bytes, and
none of them decode a record:

1. `kaas-codec` decodes the request with `records: Option<bytes::Bytes>` — a
   zero-copy slice into the frame buffer.
2. `kaas-storage/src/idempotence.rs` peeks the fixed-size batch header for
   PID / epoch / sequence — header only.
3. `kaas-storage/src/segment.rs` peeks the head for
   `(base_offset, last_offset_delta, max_timestamp)` — header only.

After that, the same opaque `&[u8]` the client sent lands verbatim on disk
(with the base offset rewritten in place): **the log file IS the wire
format**, which is what makes Fetch a byte copy rather than a re-encode.
Fetch is symmetric — batch bytes come back off disk undecoded. The invariant
is enforced by tripwire counters that must stay at zero (see
[Observability](./observability.md)) and the `bins/kaas/tests/byte_opacity.rs`
integration test.

## The durability dial

`KAAS_FLUSH_INTERVAL_MESSAGES` (default **1** = honest `acks=all`: every
batch waits for a group-commit fsync cycle) mirrors Apache Kafka's
`log.flush.interval.messages`. Raising it trades durability for throughput
by letting the committer skip cycles until N messages are pending — same
semantics, same trade, as Apache. On NFS substrates where the COMMIT
round-trip dominates, this and the group-commit coalescing are the two
levers that matter (see [Performance](../operations/performance.md)).

## Retention, DeleteRecords, and compaction — the honest state

Per-topic policy flows from `KafkaTopic.spec.config` through the
operator-written `.config.json` into the broker
(`crates/kaas-storage/src/topicconfig.rs`) — but as of today **no
background cleaner runs in production**:

- **`DeleteRecords`** (API key 21) is the one working reclamation path: it
  advances `logStartOffset` and unlinks closed segments the purge fully
  covers (the active segment is never reclaimed). Being a leader-side
  unlink, it actually frees disk per the
  [file-handle ownership rule](./file-handles.md). Topic deletion is the
  other.
- **Size-based retention** exists as code (`RetentionCleaner` in
  `crates/kaas-storage/src/cleaner.rs`, exercised by its unit tests) but
  is **not instantiated by the broker** — the interval loop its docstring
  promises is an open follow-up (gh #158), as are time-based retention
  and the compactor.
- **Compaction knobs** `min.compaction.lag.ms` (KIP-58) and
  `delete.retention.ms` (KIP-354) round-trip through CRs and
  DescribeConfigs but gate nothing yet. When the compactor lands,
  tombstone expiry will be **per-batch** (Apache is per-record) — a
  deliberate consequence of never opening batches. Status is tracked
  honestly on the [KIP-58](../compat/kip/kip-58.md) and
  [KIP-354](../compat/kip/kip-354.md) pages.
