# Book Phase 5 — Per-KIP Pages + Per-API Anchors

Part of the [mdbook documentation plan](./book-plan.md) (§6, milestone 5).

- **Status**: **done** (2026-07-20, commits `4e15000` / `dc7f558` / 5c). Deviations
  from plan: the KIP split landed as **12 implemented / 9 partial** — source
  verification moved KIP-32 (no LogAppendTime, timestamp lookup stubbed), KIP-58 +
  KIP-354 (no cleaner/compactor wired at all, gh #158), and KIP-516 (all-zero topic
  IDs on the wire) to partial. `check-docs-drift` grew an API-anchor check
  (mdbook-linkcheck 0.7.7 doesn't validate fragments). Wire-correctness bugs found
  while verifying were filed as issues rather than fixed (per the out-of-scope rule):
  INVALID_TXN_STATE mapped to wire code 50 instead of 48; no authorization on the
  txn + several admin surfaces; acks=0 Produce still answered on the wire;
  InitProducerId v3+ ignoring the claimed producer id/epoch.
- **Depends on**: [Phase 4](./book-phase-4-compat-core.md) (matrix + KIP index provide the
  link targets and the drift gate)
- **Delivers as**: **2–3 commits** on `main` (the one exception to one-commit-per-milestone;
  suggested split: API domain pages → implemented-KIP pages → partial-KIP + wrap-up)
- **Exit state**: every matrix row and KIP index row links to real content; no stub anchors
  left in Part II.

## Goal

Fill in the compatibility deep pages: grouped-by-domain API reference covering all 36
registered keys (per plan §8's decided default: **anchors, not one-file-per-API**), and 21
per-KIP pages (16 implemented + 5 partial).

## API domain grouping (36 keys → 7 pages)

| Page | Keys |
|---|---|
| `compat/api/produce-fetch.md` | 0 Produce · 1 Fetch · 2 ListOffsets · 3 Metadata |
| `compat/api/consumer-groups.md` | 8 OffsetCommit · 9 OffsetFetch · 10 FindCoordinator · 11 JoinGroup · 12 Heartbeat · 13 LeaveGroup · 14 SyncGroup · 15 DescribeGroups · 16 ListGroups · 42 DeleteGroups · 47 OffsetDelete |
| `compat/api/transactions.md` | 22 InitProducerId · 24 AddPartitionsToTxn · 25 AddOffsetsToTxn · 26 EndTxn · 27 WriteTxnMarkers · 28 TxnOffsetCommit |
| `compat/api/topics-configs.md` | 19 CreateTopics · 20 DeleteTopics · 21 DeleteRecords · 32 DescribeConfigs · 37 CreatePartitions · 44 IncrementalAlterConfigs |
| `compat/api/acls-quotas.md` | 29 DescribeAcls · 30 CreateAcls · 31 DeleteAcls · 48 DescribeClientQuotas · 49 AlterClientQuotas |
| `compat/api/auth.md` | 17 SaslHandshake · 36 SaslAuthenticate |
| `compat/api/cluster-misc.md` | 18 ApiVersions · 35 DescribeLogDirs |

Every key gets a stable anchor (`#produce`, `#fetch`, …) matching what the generated matrix
links to. Grouping keeps related deviations in one place (e.g. the stateless-fetch-session
story spans Fetch + Metadata; the txn handlers share the coordinator-routing preamble).

### Per-API anchor template (from plan §4)

purpose · supported versions (must match the registry `SPEC`) · request/response handling ·
kaas-specific semantics & deviations from Apache 3.7 · source paths
(`crates/kaas-broker/src/handlers/<x>.rs`, codec module) · test coverage (unit / integration /
`scripts/kafka-*.sh` scenario).

Deviations worth first-class treatment (don't bury them): Fetch `SessionID=0` (gh #4),
read-committed isolation (gh #31), Metadata per-listener port advertisement (gh #125) +
TopicID propagation (gh #105), IncrementalAlterConfigs TOPIC-only scope, CreatePartitions /
IncrementalAlterConfigs writing through `KafkaTopic` CRs, InitProducerId same-PID/epoch+1
rejoin contract (gh #22), EndTxn returning once the marker-queue entry is written (gh #175).

## Per-KIP pages (21)

Template (plan §4): *what the KIP changes in Apache Kafka* → *how kaas implements it*
(source paths) → *how it's verified* (test, script scenario, parity-board entry).

- **16 implemented** (list in [phase 4](./book-phase-4-compat-core.md) / plan §4).
- **5 partial** — 101, 219, 345, 394, 554 — each page leads with the honest "what's missing"
  block from the plan's partial table, then covers what *is* there. These pages are the
  book's credibility test; do not soften them.

Batching guidance: KIP pages cluster naturally with the API domain pages (KIP-98/360/447 ↔
transactions page; KIP-290/546 ↔ acls-quotas; KIP-345/394/800 ↔ consumer-groups). Writing
each cluster together keeps cross-links coherent and is the natural commit boundary.

## Out of scope

- Changing any broker behaviour the pages document. If writing a page exposes a real gap
  worth fixing (e.g. registering key 33), file a gh issue and document current behaviour —
  don't fix-and-document in the same milestone.
- Auto-generating page skeletons from the registry (nice-to-have; hand-written against the
  template is fine at this volume).

## Verification

- [ ] Every registry key appears exactly once across the 7 domain pages; every anchor the
      generated matrix emits resolves (`mdbook-linkcheck` enforces this).
- [ ] Version ranges on every anchor cross-checked against `registry.rs` `SPEC` constants —
      sample-audit at minimum, ideally the `check-docs-drift` scan grows a version assertion.
- [ ] Every cited source path exists (`check-docs-drift` path scan from phase 4).
- [ ] KIP index has zero remaining stub links.
- [ ] Partial-KIP pages state what's missing above the fold.
