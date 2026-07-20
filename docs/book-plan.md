# Documentation Book Plan — mdbook + mdbook-mermaid

Status: **done** (all six phases landed 2026-07-19/20; published at
<https://woestebanaan.github.io/kaas/>, rebuilt on every push to `main`).
Per-phase execution breakdowns and their outcome notes live in
`docs/book-phase-{1..6}-*.md`. Two standing deviations from the plan as
written: the KIP index ships **12 implemented / 9 partial / 8 non-goals**
(not 16/5/8 — KIP-32/58/354/516 were demoted to partial by source
verification), and `check-docs-drift` additionally validates per-API anchors
because mdbook-linkcheck 0.7.7 does not check fragments.

> **Rename executed** (see [rename plan](./rename-plan.md)): the project renamed before
> phase 1 so the book (published URL, README, every chapter) is born under the final name.
> Name references throughout this plan and the phase files are swept in rename step R1.7.

## Context

kaas needs documentation that *proves* Kafka API compatibility and reliability — for users
evaluating it against Apache Kafka / Strimzi, and for future maintainers. Today the repo has:

- `docs/ARCHITECTURE.md` (672 lines, 9 fenced ASCII blocks — 7 are diagrams worth converting
  to mermaid, 2 are layout/format listings that should stay code blocks) — the behavioural spec.
- Crate-level `//!` doc comments — good seeds, not navigable documentation.
- **No root README, no docs site, no mdbook, no mermaid anywhere.**

The goal is an mdbook site (with mdbook-mermaid for diagrams) that explains all the code and
carries a dedicated compatibility section: one page per implemented Kafka API key and one page
per implemented KIP, plus an honest list of deliberate non-goals.

### Inventory the book must cover (as of `v0.2.3-preview`)

- **36 Kafka API keys** registered by the broker (`bins/kaas/src/main.rs` dispatch;
  `crates/kaas-codec/src/api/registry.rs` ApiSpec table, with a test asserting
  `ALL.len() == 36`). Known gaps vs the Apache 3.7 admin surface, each an open follow-up:
  key 23 (OffsetForLeaderEpoch — storage-side lookup also stubbed, see KIP-101 in §4),
  33 (legacy AlterConfigs — superseded by key 44 but still served by Apache 3.7),
  50/51 (Describe/AlterUserScramCredentials), 60 (DescribeCluster).
- **29 distinct KIPs** referenced across the codebase: 16 implemented, 5 partial,
  8 deliberately not (see §4 below for the source-verified split).
- 12 crates + 2 bins (~54k LoC of Rust incl. tests — only `kaas-test-harness` is still a stub).
- 41 `scripts/kafka-*.sh` integration scripts (parity baseline recorded in
  `scripts/.parity-baseline.txt`: 21 PASS / 20 SKIP / 0 FAIL on `v0.2.0-preview`).

## 1. Book scaffolding

- **Book root at `docs/`**: `docs/book.toml` with `src = "src"`, build output `docs/book/`
  (gitignored). Existing `docs/*.md` stay at the `docs/` top level; the book links to them
  until their content is ported into chapters.
- **mdbook-mermaid**: run `mdbook-mermaid install docs/` once — commits `mermaid.min.js` +
  `mermaid-init.js` and adds the preprocessor block to `book.toml`. Pin versions (mdbook 0.4.x,
  mdbook-mermaid latest stable; verify compatibility before locking — note: 0.17.x is current)
  in the CI install step.
- **xtask integration** (one new match arm in `xtask/src/main.rs`, same pattern as `gen-crds`):
  - `cargo xtask docs` → `mdbook build docs`
  - `cargo xtask docs --serve` → `mdbook serve docs` for local preview
- **Root `README.md`**: minimal stub pointing at the book (see §5). Promoted to milestone 1 so
  evaluators landing on the repo have a starting point.
- `book.toml` sets `git-repository-url` / `edit-url-template` (per-page "edit on GitHub"
  links), enables `output.html.search`, and sets `output.html.fold.enable = true` for nav
  experience across 4 parts.

## 2. Book structure (`docs/src/SUMMARY.md`)

```text
Introduction (what kaas is, Kafka 3.7 parity target, design pillars)
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
  Per-API reference: grouped-by-domain with anchors (`#produce`, `#fetch`, etc.),
    fixed template per anchor:
      versions · semantics · deviations from Apache · source paths · test coverage
  KIP index: matrix of all 29 KIPs (implemented / partial / deliberate non-goal)
  Per-KIP pages (16 implemented + 5 partial): what the KIP does, how kaas
    implements it, source refs, how it's verified — partial pages carry an
    explicit "what's missing" section
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

`crates/kaas-codec/src/api/registry.rs` already carries the `ApiSpec` table (36 keys, with a
test asserting the count). Add:

- `cargo xtask gen-api-matrix` — dumps that table into `docs/src/compat/api-matrix.md`
  (key, name, version range, status), merged with KIP cross-references.
- A `check-docs-drift` CI step mirroring the existing `check-crd-drift` pattern, extended to:
  - Validate that the generated matrix matches current dispatcher registrations.
  - Verify every source path cited in per-API/per-KIP pages resolves to a real file (simple
    grep-xargs scan for `src/` references across book markdown).

The compatibility page then *cannot* silently rot — the strongest evidence available that the
docs reflect the actual wire surface. The matrix also honestly lists the admin keys the
broker doesn't serve yet (23, 33, 50/51, 60).

## 4. KIP coverage (the section the book exists for)

Implemented (16, each gets a page):

| Area | KIPs |
|---|---|
| Wire protocol / codec | 482 (flexible versions), 516 (topic IDs), 107 (DeleteRecords), 195 (CreatePartitions), 339 (IncrementalAlterConfigs), 546 (client quotas API), 290 (ACL prefixed pattern types), 800 (join/leave reason) |
| Auth / quotas / storage | 13 (per-broker quotas, debt-carry), 371 (mTLS principal mapping), 58 (min.compaction.lag.ms), 354 (delete.retention.ms), 32 (timestamp types, byte-opaque) |
| Transactions / idempotence | 98 (EOS foundation), 360 (PID re-init / epoch bump), 447 (EOS v2 group offsets) |

Partial (5 — each gets a page with an explicit "what's missing" section; a previous draft of
this plan listed all five as implemented, corrected against source 2026-07-18):

| KIP | Landed | Missing |
|---|---|---|
| 101 (leader-epoch truncation) | segment filenames carry the leader epoch | leader-epoch cache + lookup — `DiskStorageEngine::offset_for_leader_epoch` returns the `(-1,-1)` sentinel (`crates/kaas-storage/src/disk.rs`); wire key 23 unregistered |
| 219 (throttle ordering) | `throttle_time_ms` computed (quota debt-carry) and returned in responses | broker never mutes the channel after responding — throttle enforcement relies on client cooperation |
| 345 (static membership) | `group.instance.id` plumbed through join/sync; static members survive the rebalance-eviction sweep (`crates/kaas-coordinator/src/group.rs`) | `FENCED_INSTANCE_ID` fencing of duplicate static members |
| 394 (MEMBER_ID_REQUIRED) | error code defined | the v4+ two-step handshake — `join()` still takes the legacy assign-inline path (explicit follow-up comment in `group.rs`) |
| 554 (SCRAM admin API) | operator-side rotation path (gh #104: KafkaUser pre-derived credential passthrough to `credentials.json`) | wire keys 50/51 entirely — no codec modules, no dispatch |

Deliberately not implemented (each gets a rationale entry, not silence):
**227** (fetch sessions — `SessionID=0` by design), **405** (tiered storage — deferred),
**48** (delegation tokens), **664** (Describe/ListTransactions — follow-up), **714** (client
metrics), **848** / **1071** (next-gen rebalance — post-3.7), **932** (share groups — 4.0+),
plus the architectural non-goals: KRaft, replication/ISR, literal `__transaction_state` topic.

Per-KIP page template: *what the KIP changes in Apache Kafka* → *how kaas implements it*
(source paths) → *how it's verified* (unit/integration test, `scripts/kafka-*.sh`
scenario, parity-board entry).

Per-API anchor template: purpose · supported versions · request/response handling ·
kaas-specific semantics & deviations from Apache 3.7 · source paths · test coverage.

**Note**: 36 individual API pages plus 21 KIP pages would be ~57 files of repetitive content
(~2.9k lines at 50 lines each). Favor grouped-by-domain pages with anchors over
one-file-per-API to reduce navigation overhead and maintenance burden. Individual deep-links
still work via anchor hrefs.

## 5. CI & publishing

- New `docs` job in `.github/workflows/ci.yml` (ARC runner, same minimal-image caveats as the
  `rust` job): download pinned mdbook + mdbook-mermaid release binaries (faster than
  cargo-install on a cold runner), then `mdbook build docs`.
- **Include `mdbook-linkcheck`** by default (not optional) — given the emphasis on honesty and
  no-silent-rot, link validation is core CI behaviour.
- **Publishing (decision)**: recommended — `docs-publish.yml` on push to `main` using
  `actions/upload-pages-artifact` + `actions/deploy-pages` (works from self-hosted runners;
  requires enabling GitHub Pages on the repo). Fallback: build-only CI gate, read locally via
  `cargo xtask docs --serve`.
- Optional: publish `cargo doc --workspace --no-deps` alongside at `/rustdoc/` — the book
  carries the narrative, rustdoc the per-item API reference.

## 6. Implementation order (one commit per milestone, on main)

Each milestone has a detailed execution breakdown in its own file:

1. **Scaffolding + README** ([`book-phase-1-scaffolding.md`](./book-phase-1-scaffolding.md)) —
   `book.toml`, mermaid install, SUMMARY skeleton with stub pages, xtask `docs`, `.gitignore`
   entry, CI `docs` job, root `README.md`. Book builds green end-to-end.
2. **Architecture diagrams** ([`book-phase-2-diagrams.md`](./book-phase-2-diagrams.md)) —
   convert the 7 ASCII diagrams from ARCHITECTURE.md to mermaid in Part I chapters. (Split
   from prose port because ASCII→mermaid conversions carry risk of subtle misrepresentation.)
3. **Architecture prose** ([`book-phase-3-architecture-prose.md`](./book-phase-3-architecture-prose.md)) —
   port remaining ARCHITECTURE.md content into Part I; shrink original to a pointer stub
   (update the CLAUDE.md reference).
4. **Compatibility core** ([`book-phase-4-compat-core.md`](./book-phase-4-compat-core.md)) —
   API matrix (generated) + KIP index + non-goals page + verification story.
5. **Per-KIP pages** (21) and **per-API anchors** ([`book-phase-5-kip-api-pages.md`](./book-phase-5-kip-api-pages.md);
   grouped-by-domain, template-driven; expect 2–3 commits).
6. **Code tour + operations + Pages deploy**
   ([`book-phase-6-code-tour-ops-publishing.md`](./book-phase-6-code-tour-ops-publishing.md)).

## 7. Verification (every milestone)

- `cargo xtask docs` builds clean, zero mdbook warnings.
- `mdbook serve docs` spot-check: mermaid renders in both light and dark themes.
- CI `docs` job green; `check-docs-drift` passes (from milestone 4 on), including source path
  validation for every file reference cited in the book.
- `mdbook-linkcheck` finds zero broken links (internal anchor hrefs, sibling pages, external).
- Sample the per-API anchors against the source: every cited path exists, every version range
  matches the dispatcher registration.

## 8. Open decisions (defaults in bold)

1. Publishing: **GitHub Pages** vs build-only CI artifact.
2. Per-API granularity: **grouped-by-domain with anchors** vs one page per API key.
3. ARCHITECTURE.md after the port: **reduce to a stub pointing at the book** vs keep duplicated.
