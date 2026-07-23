# docs/

The kaas documentation book and its supporting files. The book is published
at **<https://kaas.rs/book/>** (landing page at <https://kaas.rs/>), rebuilt
from `main` on every push.

## Layout

| Path | What it is |
|---|---|
| `src/` | the book's chapters — Parts I–IV, plus `SUMMARY.md` (the table of contents) |
| `book.toml` | mdbook config: `rust` default theme, search, fold nav, mermaid + linkcheck backends |
| `landing/index.html` | the custom landing page that owns the site root |
| `mermaid.min.js`, `mermaid-init.js` | committed by `mdbook-mermaid install`; required by the build |
| `perf-results/` | recorded benchmark reports, cited by Part IV's performance chapter |
| `ARCHITECTURE.md` | pointer stub — the architecture content lives in Part I |
| `RELEASING.md` | canonical release procedure (Part IV links out to it) |
| `book/` | build output, gitignored |

## Building

```bash
cargo xtask docs           # mdbook build (html + linkcheck)
cargo xtask docs --serve   # live-reloading local preview
```

Needs `mdbook`, `mdbook-mermaid`, and `mdbook-linkcheck` on `PATH`. CI pins
them in the `docs` job of `.github/workflows/ci.yml`: **mdbook 0.4.52,
mdbook-mermaid 0.16.2, mdbook-linkcheck 0.7.7**. Keep to the 0.4.x line —
mdbook-mermaid ≥ 0.17 targets mdbook 0.5's preprocessor protocol and fails
against 0.4. Bump all three together.

## The drift gates

`cargo xtask check-docs-drift` runs in the CI `rust` job and is what stops
the compatibility claims from rotting. Three checks:

1. **Generated API matrix** — `cargo xtask gen-api-matrix` renders
   `src/compat/api-matrix.md` from the `ApiSpec` registry in
   `crates/kaas-codec/src/api/registry.rs` (the same table that builds the
   ApiVersions response), then `git diff --exit-code`. *Fix a failure by
   running `cargo xtask gen-api-matrix` and committing the result.* Adding
   or removing an API key also needs its row in `API_DOCS`
   (`xtask/src/api_matrix.rs`) — the join is exhaustiveness-checked both
   ways.
2. **API anchors** — every registered key must have exactly one
   `## <ApiName>` heading on the domain page the matrix links to.
   mdbook-linkcheck 0.7.7 does *not* validate fragments, so without this a
   renamed heading would silently break every deep link into it.
3. **Source-path scan** — every `crates/…` / `bins/…` / `scripts/…` path
   cited anywhere in `src/` must exist in the tree, so a refactor that moves
   a file fails CI instead of leaving a stale citation.

## Publishing

`.github/workflows/docs-publish.yml` builds the book, assembles `_site/`
(landing page at the root + book under `book/`), and deploys to GitHub
Pages. The custom domain `kaas.rs` is configured in the repo's Pages
settings — it persists across deploys, so no `CNAME` file is needed in the
artifact. The CI `docs` job mirrors the assembly and verifies the landing
page's `book/…` links resolve, so link breakage fails before deploy.

## Writing conventions

- **Write for a reader who knows Kafka but not kaas.** Open pages by
  locating kaas in the Kafka mental model ("in Apache Kafka, X…; kaas
  instead…"), use Kafka ≤ 4.3 vocabulary for concepts Kafka already
  names, and tie subsystems back to the book's through-line: the three
  substitutions (quorum → Lease/CRs, replication → single writer on RWX,
  internal topics → JSON files). `src/architecture/volume-pool.md` is
  the exemplar.
- **No `gh #NN` or `crates/…` paths in narrative prose.** Contributor
  material goes in a closing `## Implementation notes (for contributors)`
  section (Part I/IV) or the `**Source**:` / `**Verified by**:` trailers
  (Part II). Open follow-ups may keep a trailing "(tracked as gh #NN)"
  parenthetical. Part III (code tour) is contributor-facing and exempt.
- **Code is the source of truth.** Where a doc and the source disagree, the
  source wins and the doc gets fixed — including when that means
  documenting a gap. Part II's partial-KIP pages lead with what's *missing*;
  don't soften them.
- **Cite real paths.** The scan enforces existence; keep citations specific
  enough to be useful (module, not just crate).
- **Cross-link instead of duplicating.** Deep architecture lives in Part I;
  per-API behaviour in Part II; crate chapters stay short and point at both.

The book was built out in six phases during 2026-07-19/20; those plan
documents were retired once complete and live in git history (see the
`docs(book): phase N` commits).
