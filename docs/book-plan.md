# Documentation Book Plan — mdbook + mdbook-mermaid

Status: **proposed** (not started).

## Context

skafka needs documentation that *proves* Kafka API compatibility and reliability — for users
evaluating it against Apache Kafka / Strimzi, and for future maintainers. Today the repo has:

- `docs/ARCHITECTURE.md` (~650 lines, 9 hand-drawn ASCII diagrams) — the behavioural spec.
- Crate-level `//!` doc comments — good seeds, not navigable documentation.
- **No root README, no docs site, no mdbook, no mermaid anywhere.**

The goal is an mdbook site (with mdbook-mermaid for diagrams) that explains all the code and
carries a dedicated compatibility section: one page per implemented Kafka API key and one page
per implemented KIP, plus an honest list of deliberate non-goals.

### Inventory the book must cover (as of `v0.2.3-preview`)

- **36 Kafka API keys** registered by the broker (`bins/skafka/src/main.rs` dispatch;
  `crates/sk-codec/src/api/registry.rs` ApiSpec table, with a test asserting the count).
  Known gaps vs the Apache 3.7 admin surface, each an open follow-up: key 23
  (OffsetForLeaderEpoch), 50/51 (Describe/AlterUserScramCredentials), 60 (DescribeCluster).
- **~30 distinct KIPs** referenced across the codebase: ~22 implemented, 8 deliberately not
  (see §4 below for the full split).
- 12 crates + 2 bins (~48k LoC — only `sk-test-harness` is still a stub).
- 41 `scripts/kafka-*.sh` integration scripts (current parity baseline: 21 PASS / 20 SKIP / 0 FAIL).

## 1. Book scaffolding

- **Book root at `docs/`**: `docs/book.toml` with `src = "src"`, build output `docs/book/`
  (gitignored). Existing `docs/*.md` stay at the `docs/` top level; the book links to them
  until their content is ported into chapters.
- **mdbook-mermaid**: run `mdbook-mermaid install docs/` once — commits `mermaid.min.js` +
  `mermaid-init.js` and adds the preprocessor block to `book.toml`. Pin versions
  (mdbook 0.4.x, mdbook-mermaid 0.15.x) in the CI install step.
- **xtask integration** (one new match arm in `xtask/src/main.rs`, same pattern as `gen-crds`):
  - `cargo xtask docs` → `mdbook build docs`
  - `cargo xtask docs --serve` → `mdbook serve docs` for local preview
- `book.toml` sets `git-repository-url` / `edit-url-template` (per-page "edit on GitHub"
  links) and enables `output.html.search`.

## 2. Book structure (`docs/src/SUMMARY.md`)

```text
Introduction (what skafka is, Kafka 3.7 parity target, design pillars)
Getting Started (Helm deploy, local dev mode)

Part I — Architecture              ← port of ARCHITECTURE.md, ASCII → mermaid
  System overview
  Broker/operator runtime independence
  Controller, leases & assignment.json
  Storage engine hot path (group commit, segments, manifest)
  File-handle ownership & takeover
  Consumer-group coordination (hash routing)
  Transactions & idempotence
  Listeners, authentication, authorization
  Kubernetes integration (CRDs, reconcilers, RBAC)
  Observability

Part II — Kafka Compatibility      ← the "prove it" section
  Wire protocol & framing (KIP-482 flexible versions)
  API support matrix               ← auto-generated, see §3
  Per-API reference: one page per API key (36), fixed template:
    versions · semantics · deviations from Apache · source paths · test coverage
  KIP index: matrix of all ~30 KIPs (implemented / partial / deliberate non-goal)
  Per-KIP pages (~22 implemented): what the KIP does, how skafka implements it,
    source refs, how it's verified
  Explicit non-goals with rationale (see §4)
  Verification story: scripts/kafka-*.sh matrix, integration suites,
    parity project board, bench methodology

Part III — Code Tour
  Workspace layout & crate dependency graph
  One chapter per crate (12) + both bins, seeded from existing //! docs

Part IV — Operations
  Helm chart & listener configuration
  Storage substrate requirements (NFS semantics)
  Releasing
  Performance vs Strimzi
```

**Mermaid targets** (replacing the 9 ASCII diagrams plus new ones):

- Component diagram: broker / operator / shared PVC / K8s API.
- Sequence diagrams: controller election → `assignment.json` write → takeover;
  produce group-commit cycle; SCRAM pre-auth gate on an authed listener.
- `stateDiagram-v2`: transaction state machine (Empty → Ongoing → CompleteCommit/Abort,
  timeout reaper edge).
- Flowcharts: operator reconcile loops, topic-delete handle-close path.

## 3. Auto-generated API matrix (the honesty lever)

`crates/sk-codec/src/api/registry.rs` already carries the `ApiSpec` table (36 keys, with a
test asserting the count). Add:

- `cargo xtask gen-api-matrix` — dumps that table into `docs/src/compat/api-matrix.md`
  (key, name, version range, status), merged with KIP cross-references.
- A `check-docs-drift` CI step mirroring the existing `check-crd-drift` pattern.

The compatibility page then *cannot* silently rot — the strongest evidence available that the
docs reflect the actual wire surface. The matrix also honestly lists the 4 admin keys the
broker doesn't serve yet.

## 4. KIP coverage (the section the book exists for)

Implemented (~22, each gets a page):

| Area | KIPs |
|---|---|
| Wire protocol / codec | 482 (flexible versions), 516 (topic IDs), 101 (OffsetForLeaderEpoch semantics in storage), 107 (DeleteRecords), 195 (CreatePartitions), 339 (IncrementalAlterConfigs), 546 (client quotas API), 290 (ACL pattern types), 554 (SCRAM API — codec/operator support landed; dispatcher wiring pending), 345 (static membership), 800 (leave/join reason) |
| Auth / quotas / storage | 13 (per-broker quotas), 371 (mTLS principal mapping), 219 (throttle ordering), 58 (min.compaction.lag.ms), 354 (delete.retention.ms), 32 (timestamp types, byte-opaque) |
| Transactions / idempotence | 98 (EOS foundation), 360 (PID re-init / epoch bump), 447 (EOS v2 group offsets), 394 (MEMBER_ID_REQUIRED) |

Deliberately not implemented (each gets a rationale entry, not silence):
**227** (fetch sessions — `SessionID=0` by design), **405** (tiered storage — deferred),
**48** (delegation tokens), **664** (Describe/ListTransactions — follow-up), **714** (client
metrics), **848** / **1071** (next-gen rebalance — post-3.7), **932** (share groups — 4.0+),
plus the architectural non-goals: KRaft, replication/ISR, literal `__transaction_state` topic.

Per-KIP page template: *what the KIP changes in Apache Kafka* → *how skafka implements it*
(source paths) → *how it's verified* (unit/integration test, `scripts/kafka-*.sh`
scenario, parity-board entry).

Per-API page template: purpose · supported versions · request/response handling ·
skafka-specific semantics & deviations from Apache 3.7 · source paths · test coverage.

## 5. CI & publishing

- New `docs` job in `.github/workflows/ci.yml` (ARC runner, same minimal-image caveats as the
  `rust` job): download pinned mdbook + mdbook-mermaid release binaries (faster than
  cargo-install on a cold runner), then `mdbook build docs`. Optionally add `mdbook-linkcheck`.
- **Publishing (decision)**: recommended — `docs-publish.yml` on push to `main` using
  `actions/upload-pages-artifact` + `actions/deploy-pages` (works from self-hosted runners;
  requires enabling GitHub Pages on the repo). Fallback: build-only CI gate, read locally via
  `cargo xtask docs --serve`.
- Optional: publish `cargo doc --workspace --no-deps` alongside at `/rustdoc/` — the book
  carries the narrative, rustdoc the per-item API reference.
- Optional but recommended (none exists today): a root `README.md` pointing at the book.

## 6. Implementation order (one commit per milestone, on main)

1. **Scaffolding** — `book.toml`, mermaid install, SUMMARY skeleton with stub pages,
   xtask `docs`, `.gitignore` entry, CI `docs` job. Book builds green end-to-end.
2. **Architecture** — port ARCHITECTURE.md into Part I; convert all 9 ASCII diagrams to
   mermaid; shrink ARCHITECTURE.md to a pointer stub (update the CLAUDE.md reference).
3. **Compatibility core** — API matrix (generated) + KIP index + non-goals page +
   verification story.
4. **Per-KIP pages** (~22) and **per-API pages** (36, template-driven; expect 2–3 commits).
5. **Code tour + operations + Pages deploy.**

## 7. Verification (every milestone)

- `cargo xtask docs` builds clean, zero mdbook warnings.
- `mdbook serve docs` spot-check: mermaid renders in both light and dark themes.
- CI `docs` job green; `check-docs-drift` passes (from milestone 3 on).
- Sample the per-API pages against the source: every cited path exists, every version range
  matches the dispatcher registration.

## 8. Open decisions (defaults in bold)

1. Publishing: **GitHub Pages** vs build-only CI artifact.
2. Per-API granularity: **one page per API key** (deep-linkable) vs grouped-by-domain pages.
3. ARCHITECTURE.md after the port: **reduce to a stub pointing at the book** vs keep duplicated.
