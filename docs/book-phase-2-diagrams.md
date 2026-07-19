# Book Phase 2 — Architecture Diagrams (ASCII → mermaid)

Part of the [mdbook documentation plan](./book-plan.md) (§6, milestone 2).

- **Status**: **done** (2026-07-19). All 7 conversions + 4 new diagrams landed; every
  diagram parse-validated against the mermaid version the book ships (11.6.0, bundled by
  mdbook-mermaid 0.16.2). **Code-vs-ASCII divergences found during conversion** (diagrams
  drawn to code truth; ARCHITECTURE.md + CLAUDE.md still carry the stale claims — fix in
  the phase 3 prose sweep):
  1. The known task-6 item: EndTxn markers go through the shared-PVC marker queue, not a
     WriteTxnMarkers RPC. Fixed as planned.
  2. The committer fsyncs **holding the partition mutex** (re-locks inside
     `spawn_blocking`) — the "clone the log FD, sync outside the lock" description in
     ARCHITECTURE.md/CLAUDE.md is not what `crates/kaas-storage/src/partition.rs` ships.
  3. Fetch has **no sendfile/splice** — `engine.read` returns materialized `Bytes`;
     `read_segment_ref` doesn't exist. (Also: `fetch.rs`'s file docstring still claims
     read-uncommitted-only, but the code implements the KIP-98 LSO clamp + aborted list.)
  4. The manifest is **not** written on segment roll (only partition open and
     close/relinquish — `persist_state_locked` has exactly one caller).
  5. The txn timeout reaper as wired calls the **ungated** `abort_overdue` (ownership gate
     exists in the store API, production passes `None`) — CLAUDE.md claims it gates on
     gh #91 slot ownership. Possible multi-broker N-way race; worth a code fix or a doc fix.
  6. The deletionTimestamp-immediate topic-delete path lives in the **un-wired**
     `TopicWatcher`; production closes FDs via kube `Delete` event → assignment recompute →
     relinquish, and dirs are reclaimed by the operator **startup sweep** (the reconcile-time
     `handle_not_found` cleanup methods are not wired into the reconcile stream).
  7. `TxnState` carries `PrepareCommit`/`PrepareAbort` variants that are never visited
     (prepare→complete collapses into one atomic slot-file transition).
- **Depends on**: [Phase 1](./book-phase-1-scaffolding.md) (book builds green)
- **Delivers as**: one commit on `main`
- **Exit state**: every Part I chapter that needs a diagram has its mermaid version, verified
  against *current source*, rendering in both themes.

## Goal

Convert the hand-drawn ASCII diagrams in `docs/ARCHITECTURE.md` to mermaid inside the Part I
stub chapters. This is deliberately split from the prose port (phase 3): ASCII→mermaid
conversion is where subtle misrepresentation creeps in, so it gets its own review pass.

**Rule for every conversion: the source of truth is the code, not the ASCII.** Where the ASCII
diagram and the current code disagree, fix the content during conversion (and note it for the
phase 3 prose sweep). One known instance already found — see task 6 below.

## Inventory (fact-checked 2026-07-18)

`docs/ARCHITECTURE.md` has **9 fenced blocks**; 7 convert to mermaid, 2 stay as code blocks:

| # | Lines | Content | Target |
|---|---|---|---|
| 1 | 25–71 | "At a glance" — broker / operator / shared PVC / K8s API | `flowchart` component diagram |
| 2 | 125–196 | Produce request end-to-end (group-commit cycle) | `sequenceDiagram` |
| 3 | 221–247 | Fetch request end-to-end | `sequenceDiagram` |
| 4 | 271–296 | Controller election → `assignment.json` write → takeover | `sequenceDiagram` |
| 5 | 328–355 | On-disk storage layout tree | **keep as code block** (it's a file tree, not a diagram) |
| 6 | 363–366 | Segment filename format (`{epoch:08x}-{base_offset:020d}.log`) | **keep as code block** |
| 7 | 424–468 | Concurrency model inside one Partition (mutex, committer task, condvar) | `flowchart` |
| 8 | 511–534 | EndTxn commit flow (state transition + offset hook + markers) | `flowchart` (or sequence) — **content is stale, see task 6** |
| 9 | 543–611 | Crate dependency graph | `flowchart LR` |

## Tasks

1. Convert diagrams 1–4 (component + three sequences). For the produce sequence, make the
   group-commit cycle legible: concurrent appenders parking on the condvar while the
   per-partition committer runs one `sync_all()` per cycle.
2. Convert diagram 7 (partition concurrency): show what runs under the partition mutex vs the
   spawned deferred-finalize task on segment roll.
3. Convert diagram 9 (crate graph). Cross-check edges against `Cargo.toml` `[dependencies]` of
   each crate rather than transcribing the ASCII. (Optional: note in the chapter that the graph
   is hand-maintained; auto-generating from `cargo metadata` is a possible future
   `gen-api-matrix`-style xtask.)
4. Add the **new** diagrams the plan calls for (not in ARCHITECTURE.md today):
   - `stateDiagram-v2`: transaction state machine — Empty → Ongoing →
     CompleteCommit/CompleteAbort, including the timeout-reaper edge
     (Ongoing → CompleteAbort after `transactionTimeoutMs`, epoch bump).
   - `sequenceDiagram`: SCRAM pre-auth gate on an authed listener
     (`crates/kaas-protocol/src/dispatch.rs` — non-handshake APIs blocked until SASL completes).
   - `flowchart`: operator reconcile loops (per-CRD; reconcile-time cleanup, no finalizers).
   - `flowchart`: topic-delete handle-close path (`deletionTimestamp` → topic-watcher event →
     leader closes FDs → operator unlink; the NFS silly-rename story, gh #76).
5. Leave blocks 5 and 6 as plain code blocks in their chapters — file trees and filename
   formats read better as text.
6. **Fix the stale EndTxn diagram while converting** (block 8): the ASCII says
   "WriteTxnMarkers RPC to peer brokers (gh #114)", but since gh #175 cross-broker marker
   dispatch goes through the shared-PVC marker queue
   (`crates/kaas-coordinator/src/marker_queue.rs` → per-broker `to-<target>/` polling in
   `crates/kaas-broker/src/marker_watcher.rs`), and `EndTxn` returns as soon as the queue entry
   is written. Draw the current mechanism.

## Out of scope

- Porting the surrounding prose (phase 3). Chapters may temporarily read as
  "stub paragraph + finished diagram".
- Deleting or stubbing ARCHITECTURE.md (phase 3).

## Verification

- [ ] Each converted diagram reviewed **side-by-side against the code it describes**, not just
      against the ASCII original (list the source files checked in the commit message).
- [ ] Every diagram renders in light *and* dark theme via `cargo xtask docs --serve`.
- [ ] No mermaid parse warnings in `mdbook build` output.
- [ ] `mdbook-linkcheck` still green.
- [ ] The gh #114 → gh #175 correction from task 6 is reflected (grep the book for `#114`).
