# Book Phase 1 — Scaffolding + README

Part of the [mdbook documentation plan](./book-plan.md) (§6, milestone 1).

- **Status**: **done** (2026-07-19). Pins landed as mdbook v0.4.52 + mdbook-mermaid
  **v0.16.2** + mdbook-linkcheck v0.7.7 — the 0.17.0 mermaid pin below turned out to be
  built against the mdbook 0.5 preprocessor protocol (fails against 0.4.x with "Unable to
  parse the input"); 0.16.2 is the newest 0.4-compatible release.
- **Depends on**: nothing
- **Delivers as**: one commit on `main`
- **Exit state**: the book builds green end-to-end, locally and in CI, with stub content.

## Goal

Stand up the entire build/CI/publishing *skeleton* so every later phase is content-only work.
After this phase, adding a chapter is "write markdown, add a SUMMARY line" — no toolchain
decisions left.

## Deliverables

| File | Content |
|---|---|
| `docs/book.toml` | `[book]` with `src = "src"`, title, authors; `[output.html]` with `git-repository-url`, `edit-url-template`, `search` enabled, `fold.enable = true`; mermaid preprocessor block |
| `docs/src/SUMMARY.md` | Full 4-part skeleton from plan §2, every chapter as a stub page |
| `docs/src/**/*.md` | One stub file per SUMMARY entry (a heading + one-line abstract each, so `mdbook build` has zero missing-file warnings) |
| `docs/mermaid.min.js`, `docs/mermaid-init.js` | Committed by `mdbook-mermaid install docs/` |
| `xtask/src/main.rs` | Two new match arms: `docs` → `mdbook build docs`; `docs --serve` → `mdbook serve docs`. Update the usage string (`try: gen-proto \| gen-crds \| ...`) |
| `.gitignore` | Add `docs/book/` (build output) |
| `.github/workflows/ci.yml` | New `docs` job (see below) |
| `README.md` (repo root) | Minimal stub: what kaas is, parity target, link to the book + `docs/ARCHITECTURE.md`, quickstart pointer to the Helm chart. Currently **no root README exists** — evaluators land on a bare file listing |

## Tasks

1. Install pinned tooling locally: mdbook 0.4.x + mdbook-mermaid **0.17.0** (verified current
   on crates.io as of 2026-07-18; re-check compatibility with the chosen mdbook 0.4.x before
   locking the CI pin).
2. `mdbook init docs --force` equivalent by hand (don't clobber existing `docs/*.md` — they
   stay at the top level; the book lives under `docs/src/`).
3. `mdbook-mermaid install docs/` — commits the two JS assets and patches `book.toml`.
4. Author `SUMMARY.md` from plan §2 verbatim; generate stub pages.
5. Wire the two xtask match arms (same plain-match pattern as `gen-crds`).
6. Add the CI `docs` job:
   - ARC runner, same minimal-image caveats as the `rust` job (the runner image is bare —
     `curl` pinned release binaries from GitHub releases rather than `cargo install`; that's
     minutes vs seconds on a cold runner).
   - Steps: fetch mdbook + mdbook-mermaid + mdbook-linkcheck binaries → `mdbook build docs`.
   - `mdbook-linkcheck` is **on from day one** (plan §5 makes it core, not optional).
7. Write the root `README.md` stub.

## Out of scope

- Any real chapter content (phases 2–6).
- `gen-api-matrix` / `check-docs-drift` (phase 4).
- GitHub Pages publishing (phase 6) — this phase makes CI *build* the book, not deploy it.

## Verification

- [ ] `cargo xtask docs` builds clean with **zero mdbook warnings** (stub pages mean no
      missing-file warnings).
- [ ] `cargo xtask docs --serve` renders; a throwaway mermaid block renders in both light and
      dark themes (then remove the throwaway).
- [ ] `mdbook-linkcheck` passes (stubs contain no dead links).
- [ ] CI `docs` job green alongside `rust` / `docker` / `helm`.
- [ ] `git status` clean after a build (i.e. `docs/book/` correctly gitignored).
