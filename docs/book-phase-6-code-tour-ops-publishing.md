# Book Phase 6 — Code Tour, Operations, Publishing

Part of the [mdbook documentation plan](./book-plan.md) (§6, milestone 6).

- **Status**: not started
- **Depends on**: [Phase 5](./book-phase-5-kip-api-pages.md) (publishing an incomplete
  compatibility section would undercut the book's whole pitch)
- **Delivers as**: one commit on `main` (publishing workflow may be a small follow-up commit
  if Pages repo settings need iteration)
- **Exit state**: the full book is live on GitHub Pages, rebuilt on every push to `main`.

## Goal

Finish Parts III and IV, then turn on publishing.

## Part III — Code Tour

- **Workspace layout chapter**: the 12 crates + 2 bins + xtask, with the phase 2 crate
  dependency graph embedded.
- **One chapter per crate + bin (14)**, seeded from the existing `//!` crate-level doc
  comments — port the prose, then *add what rustdoc can't say*: why the crate boundary is
  where it is, which invariants callers must hold (e.g. `kaas-storage`'s "read the hot-path
  section first" warning, `kaas-coordinator`'s assignment-source indirection).
- `kaas-test-harness` gets an honest three-line chapter: still a stub, populated as integration
  tests need it.
- Keep chapters short; deep architecture stays in Part I with cross-links rather than being
  repeated per-crate.

## Part IV — Operations

| Chapter | Content / sources |
|---|---|
| Helm chart & listener configuration | `deploy/helm/kaas/`: the Strimzi-shape `listeners[]` array (gh #126), per-listener auth axes, `authorization.{type,superUsers}`, CRD-upgrade caveat from the chart README, the legacy single-listener KafkaCluster synthesis note |
| Storage substrate requirements | RWO/local-path single-broker vs RWX multi-broker; the NFSv4 semantics contract (same-dir rename atomicity, fsync durability, close-to-open consistency); provider matrix from NOTES.txt; `KAAS_FLUSH_INTERVAL_MESSAGES` durability dial |
| Releasing | Port or link `docs/RELEASING.md` (tag-driven, patch-bump-never-recut); decide during the phase whether RELEASING.md becomes a stub like ARCHITECTURE.md or stays canonical with the book linking out — either is fine, don't duplicate |
| Performance vs Strimzi | Summarize `docs/perf-results/`: current standing (~75% of Strimzi single-consumer throughput; producer p50 gap is architectural — group-commit fsync vs page-cache ack), bench methodology (multi-run, outlier exclusion), and the known dead-ends so they aren't re-litigated |

## Publishing (plan §5 + §8 decision: GitHub Pages)

1. New `.github/workflows/docs-publish.yml`, triggered on push to `main`:
   build (same pinned-binary steps as the CI `docs` job) →
   `actions/upload-pages-artifact` → `actions/deploy-pages`. Both work from self-hosted/ARC
   runners.
2. One-time repo settings: enable Pages with "GitHub Actions" as the source; add the
   `pages: write` / `id-token: write` permissions to the workflow.
3. Point the root `README.md` and the ARCHITECTURE.md stub at the published URL.
4. **Optional** (skippable without guilt): publish `cargo doc --workspace --no-deps` under
   `/rustdoc/` in the same artifact — book carries the narrative, rustdoc the per-item API
   reference. Weigh artifact size on the ARC runner before committing to it.

## Out of scope

- Versioned docs (publishing per release tag) — single rolling `main` build is enough for the
  preview line; revisit at a stable release.
- Custom domain.

## Verification

- [ ] `cargo xtask docs` zero warnings; `mdbook-linkcheck` green across the now-complete book.
- [ ] Every crate chapter's claims spot-checked against the crate's current `//!` docs (they
      drift too).
- [ ] Pages deploy green; published site spot-checked: mermaid renders, search works, fold
      nav behaves, edit-on-GitHub links land on the right files.
- [ ] README links to the live site.
- [ ] Final sweep: plan §7's book-wide checklist run once end-to-end; `docs/book-plan.md`
      status flipped from **proposed** to **done** (or the plan file retired into the book's
      own contributing page).
