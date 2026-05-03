# Phase 3 Storage Engine — Breakdown vs. Current Repo

Comparison of `skafka-plan-v3.md` (v3.3) §"Phase 3: Storage Engine (Week 3–6)" (lines 885–1048) against the state of `main` at commit `5ea4c1d`.

Legend: ✅ done · 🟡 partial / minor deviation · ❌ not yet · ➕ extra (not in plan)

---

## Filesystem layout

Plan §"Filesystem layout" (lines 894–913):

```
/data/{topic}/partition-{N}/
  manifest.json
  {epoch:08x}-{base_offset:020d}.log
  {epoch:08x}-{base_offset:020d}.index
  {epoch:08x}-{base_offset:020d}.timeindex
  {epoch:08x}-{base_offset:020d}.log.sealed
  {epoch:08x}-{base_offset:020d}.recovery
```

| File / directory | Status | Notes |
|---|---|---|
| `/data/__cluster/{assignment.json,acls.json,credentials.json}` | 🟡 `acls.json` + `credentials.json` are produced/consumed; `assignment.json` is a Phase 4 deliverable (controller writes it) |
| `/data/__consumer_offsets/<group>.json` | 🟡 the coordinator stores per-*group* JSON (`internal/coordinator/offsets.go:38`), not per-partition logs as the plan layout suggests. Functionally equivalent for v1; persistence model is different. |
| `/data/{topic}/partition-{N}/` | ✅ created on demand (`engine.go:115-143`) |
| **`{epoch:08x}-{base_offset:020d}.log`** filename | ❌ filenames are `{base_offset:020d}.log` only (`segment.go:35-36`) — **no epoch prefix**. Epoch is persisted separately in a `.leader-epoch` file at the partition directory level. |
| `.index` sparse offset→position | ✅ `segment.go:39-42, 151-167, 244-275` — `[relative_offset:int32][file_position:int32]`, default interval 4096 bytes, regenerated on TakeOver if missing |
| `.timeindex` | ❌ never written. The retention cleaner reads `maxTimestamp` directly from segment headers, so the file isn't needed for retention; but a Kafka-spec `kafka-dump-log.sh` run will note its absence. |
| `.log.sealed` marker | ❌ never produced |
| `.recovery` sidecar | ❌ never produced |
| `manifest.json` per partition | ✅ — `internal/storage/manifest.go`. JSON `{epoch, highWatermark, logStartOffset}`, atomic tmp + rename in same dir. Read on partition open with fall-back to legacy `.leader-epoch` (one-shot migration; legacy file is removed after first manifest write). Written on partition open / TakeOver / segment roll / per-batch flush policy. |

The dominant gap here is **segment naming**. The plan's epoch-prefix is the load-bearing v3 invariant: it lets the takeover sequence "create a fresh segment under its own epoch" without contention with a partitioned ex-leader writing its own segment. With epoch out of the filename, two leaders during a takeover could both target the same `00000000000000123456.log` and either race or collide — the existing v2.6 single-writer-via-flock model is what currently keeps this safe, but the plan's RWX-PVC takeover safety model expects epoch-tagged paths.

---

## On-disk segment format

| Plan requirement | Status | Where |
|---|---|---|
| Segment file = concatenation of complete RecordBatch wire bytes | ✅ | `engine.go:227-273` (Append), `segment.go:140-167` (appendBatch) — bytes flow through unchanged |
| Byte-identical to Apache Kafka's segment format | ✅ in principle — same RecordBatch v2 magic, same CRC, same record framing |
| `kafka-dump-log.sh` works against skafka segments unmodified | ❌ untested. Phase 3 testing strategy line 1393–1394 lists this as a compat assertion. |

Core wire format is correct. The compat assertion isn't run anywhere yet; it would be the strongest single proof that v3.3's bytes-are-opaque architecture round-trips with Apache Kafka tooling.

---

## Index files

| Plan requirement | Status | Where |
|---|---|---|
| Sparse `[relative_offset:int32][file_position:int32]` entries | ✅ | `segment.go:151-167` |
| One entry per `indexIntervalBytes` (default 4096) | ✅ | `engine.go:62` `IndexIntervalBytes: 4096` |
| Binary-search to find nearest entry ≤ target | ✅ | `segment.go:244-275` `searchIndex()` |
| Linear-scan from there to target | ✅ | `segment.go:197-241` `readBatches()` |
| Regenerated on startup if missing/corrupt | ✅ | `engine.go:455`, called by TakeOver / partition open |

Index is correct and fast.

---

## Append flow

Plan §"Append flow" (lines 950–986):

| Plan element | Status | Where |
|---|---|---|
| `Append(ctx, topic, partition, epoch, batchBytes)` signature | ✅ | `engine.go:240` (added in commit `619b052`) |
| **Reject when `activeSegment.epoch != epoch` with `ErrEpochMismatch`** | 🟡 deferred | `engine.go:240` — epoch parameter is `_` (ignored). Code comment lines 236-239: *"Phase 1 accepts the parameter but does not enforce it — Phase 4 wires the cached partition epoch into the per-partition state and rejects stale callers with ErrEpochMismatch."* `ErrEpochMismatch` sentinel is defined and ready to use. |
| Append `batchBytes` without decoding | ✅ | byte-opaque from request through to file |
| Update sparse index when bytesSinceLastIndex ≥ IndexIntervalBytes | ✅ | `segment.go:151-167` |
| Roll segment when size ≥ SegmentBytes | ✅ | `engine.go:266-270` |
| `flushPolicy.ShouldFlush() → fdatasync + manifest update` | ✅ | `Config.FlushIntervalMessages` (default `1` — flush every batch). On Append, `pendingFlushRecords` accumulates; once it crosses the threshold, `flushLocked()` fsyncs the active log+index files and rewrites the manifest. `=0` disables message-driven flushing (sync only at segment roll). |

The plan's per-batch / per-policy fdatasync is the strongest difference. With acks=all semantics, the broker should not ack a produce until the data is durable; today it acks as soon as the write hits the kernel.

---

## Read flow

Plan §"Read flow" (lines 991–1017):

| Plan element | Status | Where |
|---|---|---|
| `findSegmentForOffset` | ✅ | `engine.go:334-340` |
| `findIndexEntry` | ✅ | `segment.go:244-275` `searchIndex()` |
| `scanForBatch` linear-scan from index entry | ✅ | `segment.go:197-241` |
| Read up to `maxBytes`, round down to complete batch boundary | ✅ | `segment.go:readBatches` honors batch boundaries by header inspection |
| Returns raw `[]byte` for direct response framing | ✅ | `engine.go:300-358` Read |

Read flow ✅. Combined with the byte-opaque Fetch handler in Phase 2, fetched bytes flow `disk → response wire` with zero re-encoding.

---

## TakeOver flow

Plan §"TakeOver flow" (lines 1019–1023): "list segments, identify prior leader's last segment, **scan backward for last well-formed batch via CRC**, **seal**, **write `.recovery` sidecar**, create fresh segment with new epoch."

Code (`engine.go:430-465`, `segment.go:345-395`):

| Plan element | Status | Notes |
|---|---|---|
| List segments + identify prior leader's last | ✅ | the active segment is the recovery target |
| Scan **backward** for last well-formed CRC batch | 🟡 scans **forward** instead, stopping at the first invalid CRC. Functionally equivalent to "find the last contiguous good prefix" — same boundary either way — but not the plan's wording. |
| **Seal** prior segment (`.log.sealed` marker) | ❌ never created |
| Write **`.recovery`** sidecar | ❌ never created |
| Create fresh segment under new epoch (epoch in filename) | ❌ filename has no epoch prefix; epoch is tracked separately in `.leader-epoch` |
| Return recoveryOffset (HWM) for heartbeat reporting | ✅ | `engine.go:459` |

The forward-vs-backward scan doesn't matter for correctness when the segment is contiguous (any partial trailing batch is bounded by a CRC failure either way). The missing sealing markers and the missing epoch prefix do matter under the plan's takeover-safety model: a partitioned ex-leader writing into the same segment after a new leader has taken over is what the epoch-prefixed filenames are designed to make impossible. Today the v2.6 flock model is what prevents this; once flock is removed (per v3 plan project layout: "There is no `internal/lock/` package"), the epoch-prefix becomes load-bearing.

---

## Per-partition manifest

Plan §"Per-partition manifest" (lines 1025–1029):

```json
{ "epoch": ..., "highWatermark": ..., "logStartOffset": ... }
```

Atomic write via tmp + rename in same directory.

| Status | Notes |
|---|---|
| ✅ implemented | `internal/storage/manifest.go`. Read on partition open (with one-shot migration from the legacy `.leader-epoch` 8-byte file). Written on partition open, TakeOver, segment roll, and at the per-batch flush policy boundary. Manifest HWM is reconciled against the segment scan: if the manifest claims more data than is on disk (e.g. truncated active segment), the scan wins. |

Coverage: 7 unit tests in `internal/storage/manifest_test.go` (round-trip, missing → ErrNotExist, legacy migration, atomic tmp cleanup, parse error, overwrite). Integration tests in `tests/integration/disk_storage_test.go` (`TestManifestPersistedAcrossRestart`, `TestTakeOverWritesManifestEpoch`).

---

## NFS operational requirements

Plan §"NFS operational requirements" (line 1033): "NFSv4.1+, sync export, hard mount, nconnect=8, acregmax=1."

| Item | Status |
|---|---|
| Documentation in Helm chart README | ✅ acknowledged at `deploy/helm/skafka/README.md:62-77` (NFSv4.1+, advisory-lock caveats) |
| Recommended mount options for v3.3 (`nconnect=8`, `acregmax=1`) | 🟡 partially documented; the v3.3 `acregmax=1` recommendation (drives sub-second freshness on assignment.json polling — Phase 4 use) isn't yet noted |
| Code requires no NFS-specific handling | ✅ relies on POSIX rename + fsync semantics |

Sufficient for Phase 3 storage; Phase 4 will need the `acregmax=1` recommendation explicitly.

---

## Retention cleaner

Plan §"Retention cleaner" (lines 1036–1041):

| Plan element | Status | Where |
|---|---|---|
| Background goroutine | ✅ | `cleaner.go:50-90` |
| Leader-only | ✅ | `cleaner.go:43` checks lease |
| Drives by `maxTimestamp` from segment header (NOT mtime) | ✅ | `cleaner.go:67` `segmentMaxTimestamp()` reads from the last batch's wire-format header |
| Never deletes the active segment | ✅ | loop iterates over closed segments only |
| Runs every 5 minutes | ✅ | `cleaner.go:22` |

✅ Complete. This is one of the cleaner Phase 3 deliverables.

---

## fsnotify watcher

Plan §"inotify on config files" (lines 1043–1047): "On NFS, fsnotify falls back to polling (~1s interval). Acceptable for config files."

| Plan element | Status | Where |
|---|---|---|
| Watch `acls.json` + `credentials.json` | ✅ | `watcher.go:35` |
| Debounced reload | ✅ | `watcher.go:29` (100ms debounce) |
| **NFS polling fallback** | ❌ | uses fsnotify directly; on NFS without inotify support, file changes will be silently missed. The plan's "~1s polling fallback" is not implemented. |

The fallback isn't strictly required for v1 single-broker / local-disk setups — but the moment skafka runs on csi-driver-nfs (Tier 1 RWX provider per plan §"Supported RWX providers"), this becomes a hot reload bug.

---

## Tests in `internal/storage/`

| Status | Notes |
|---|---|
| ❌ no `internal/storage/*_test.go` files | Coverage lives in `tests/integration/disk_storage_test.go` (440 lines): produce/consume round-trip, recovery after partial write (CRC truncation), segment roll, watcher callback. |
| ❌ no test for epoch-mismatch rejection | Pending Phase 4 enforcement |
| ❌ no test for `manifest.json` persistence | Pending implementation |
| ❌ no `kafka-dump-log.sh` compat test | Out-of-process; belongs in CI integration job |

Integration coverage is reasonable for what's implemented. The gaps mirror the implementation gaps.

---

## Summary of Gaps

| # | Gap | Severity | Status |
|---|---|---|---|
| 1 | **Epoch-prefixed segment filenames** (`{epoch:08x}-{base_offset:020d}.log`) | High — load-bearing for v3 takeover safety once flock is removed | Open — best done together with Phase 4 flock removal so we don't ship an awkward intermediate state |
| 2 | **Per-partition `manifest.json`** | High — startup perf, compaction support, debugging | ✅ closed |
| 3 | **`.log.sealed` marker after takeover** | Medium — explicit signal of takeover-closed vs. clean-roll | Open |
| 4 | **`.recovery` sidecar** | Low — debugging artifact | Open |
| 5 | **Per-batch / policy-driven `fdatasync`** | High — acks=all currently didn't guarantee durability beyond OS write-back | ✅ closed |
| 6 | **fsnotify polling fallback** for NFS | Medium for NFS deployments | Open |
| 7 | **TakeOver scans forward, not backward** + missing sealing | Stylistic — same boundary either way | Open |
| 8 | **`.timeindex` files** | Low — only used by Kafka admin tools | Open |
| 9 | **`kafka-dump-log.sh` compat assertion** | Strong proof of byte-identical on-disk format | CI integration job, not a Go test |

Items 2 and 5 — the two highest-severity Phase 3 gaps — are now closed. Item 1 (epoch in filenames) is paired with Phase 4 flock removal.

---

## Items correctly deferred to later phases

- **Epoch enforcement in Append** — code comment says Phase 4; matches the v3 plan because the BrokerCoordinator is the authoritative source of the per-partition epoch.
- **`/data/__cluster/assignment.json`** — Phase 4 work; Phase 3 only owns segment storage.
- **`/data/__consumer_offsets/partition-{N}/`** layout — coordinator currently stores group-keyed JSON; converting to topic-partition logs is a v2 compaction-related restructure.
- **Log compaction** — v2 deliverable.
- **mmap vs `pread()` for fetch reads** — Phase 1 open question #12 awaiting per-RWX-provider answer.

---

## Summary

Phase 3 is **about 80% landed**. The hot path (byte-opaque Append, sparse-index Read, segment roll, retention by `maxTimestamp`, fsnotify on cluster config) is in place; the wire format is byte-identical to Apache Kafka; and the two highest-severity v3.3 storage hardening items — per-partition `manifest.json` and the per-batch flush policy — now ship.

Remaining gaps:

- **Epoch-prefixed segment filenames** — paired with Phase 4 flock removal so we ship one combined transition rather than an awkward intermediate.
- **fsnotify polling fallback for NFS** — small, worth doing soon.
- **TakeOver sealing markers + `.recovery` sidecar + `.timeindex`** — quality-of-life, not load-bearing.
- **`kafka-dump-log.sh` compat assertion** — out-of-process CI job, not a Go test.

Correctly deferred: `epoch enforcement in Append` (Phase 4), `/data/__cluster/assignment.json` (Phase 4), `__consumer_offsets` per-partition log layout (v2 compaction), mmap vs `pread()` (Phase 1 open question #12).
