# Book Phase 4 — Compatibility Core (matrix, KIP index, non-goals, verification story)

Part of the [mdbook documentation plan](./book-plan.md) (§6, milestone 4).

- **Status**: not started
- **Depends on**: [Phase 1](./book-phase-1-scaffolding.md) (can proceed in parallel with 2–3,
  but lands after them to keep the one-commit-per-milestone sequence clean)
- **Delivers as**: one commit on `main`
- **Exit state**: Part II exists minus the per-KIP/per-API deep pages; docs drift is a CI
  failure, not a hope.

## Goal

Build the "prove it" backbone: the auto-generated API matrix, the 29-KIP index, the explicit
non-goals page, and the verification-story page — plus the CI gate that keeps the generated
matrix honest forever.

## Deliverables

### 1. `cargo xtask gen-api-matrix`

New xtask match arm (same pattern as `gen-crds`). Dumps
`crates/sk-codec/src/api/registry.rs::ALL` (36 entries, count-asserted by the existing unit
test) into `docs/src/compat/api-matrix.md`:

- Columns: key · name · supported version range · flexible-from · KIP cross-refs · status.
- **Gap rows included**: keys 23, 33, 50/51, 60 (present in Apache 3.7's admin surface, not
  served by skafka — each an open follow-up), plus a link to the non-goals page for the
  deliberately-absent surfaces (KRaft/replication inter-broker keys, delegation tokens,
  tiered-storage-only fields).
- Implementation choice: either have the xtask *run* a small generator binary in `sk-codec`
  (`cargo run -p sk-codec --bin gen-api-matrix`-style, or a `#[test]`-adjacent example), or
  parse via a tiny `include!`-based helper — pick whatever keeps the ApiSpec table the single
  source of truth. Do **not** hand-maintain the table in markdown.

### 2. `cargo xtask check-docs-drift` + CI wiring

Mirrors `check-crd-drift` exactly: regenerate, then `git diff --exit-code` on the generated
file. Extended with the source-path scan from plan §3:

- Scan book markdown for `crates/...`/`bins/...` path citations and fail on paths that don't
  exist in the tree (a grep-xargs pass is fine; no need for anything clever).
- Add to the CI `rust` job (it needs the Rust toolchain anyway), alongside `check-crd-drift`.

### 3. KIP index page (`docs/src/compat/kip-index.md`)

The source-verified split (fact-checked 2026-07-18 — see plan §4 for the tables):

- **16 implemented**: 482, 516, 107, 195, 339, 546, 290, 800, 13, 371, 58, 354, 32, 98, 360, 447.
- **5 partial** (each with a "what's missing" cell): 101 (leader-epoch cache/lookup stubbed —
  `(-1,-1)` sentinel in `sk-storage/src/disk.rs`), 219 (throttle_time advertised, channel
  never muted), 345 (static members survive eviction; no `FENCED_INSTANCE_ID`), 394 (error
  code only; legacy assign-inline join path), 554 (operator rotation path only; keys 50/51
  unserved).
- **8 deliberate non-goals**: 227 (SessionID=0 by design), 405 (deferred), 48, 664
  (follow-up), 714, 848, 1071, 932.

Index rows link to the per-KIP pages (phase 5) — stub anchors until then.

### 4. Non-goals page (`docs/src/compat/non-goals.md`)

Port the rationale from CLAUDE.md §"Parity target & non-goals" + ARCHITECTURE.md's non-goals
section: KRaft (K8s Leases instead), replication/ISR (single-writer-per-partition), literal
`__transaction_state` topic (slot files on the PVC), tiered storage (deferred, not refused),
fetch sessions (stateless by contract). Each entry: *what Apache does* → *what skafka does
instead* → *why* → *what would change our mind* (where honest).

### 5. Verification-story page (`docs/src/compat/verification.md`)

- The `scripts/kafka-*.sh` matrix: 41 scripts, how `_common.sh` skip/exit-77 works, and the
  recorded baseline (`scripts/.parity-baseline.txt`: 21 PASS / 20 SKIP / 0 FAIL on
  `v0.2.0-preview`; note SKIPs map to documented non-goals).
- The integration suites: `bins/skafka/tests/` (smoke, auth, byte-opacity tripwire, cluster
  bringup, EOS v2 round trip), `crates/sk-controller/tests/` (failover, stale-controller race).
- The [skafka-migration-parity](https://github.com/users/Woestebanaan/projects/2) project board
  as the tracking surface.
- Bench methodology summary + link to `docs/perf-results/` (multi-run averaging, outlier
  exclusion — one paragraph, details live in Part IV's performance chapter, phase 6).

## Out of scope

- Per-KIP pages and per-API anchor content (phase 5) — this phase creates the *index and
  matrix*, phase 5 fills the deep links.
- Extending the matrix generator to also emit the KIP index (keep the KIP index hand-written;
  KIP status is a judgment call, not something the registry encodes).

## Verification

- [ ] `cargo xtask gen-api-matrix && git diff --exit-code` idempotent (run twice, no diff).
- [ ] Deliberately break it (comment out a registry entry locally) → `check-docs-drift` fails;
      restore → passes.
- [ ] Source-path scan: cite a bogus path in a scratch page → CI step fails; remove → green.
- [ ] Matrix row count = 36 + 5 gap rows; KIP index rows = 29.
- [ ] `mdbook-linkcheck` green (index → stub anchors resolve).
