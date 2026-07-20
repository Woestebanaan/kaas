# Releasing

Tag-driven releases: broker + operator images and the Helm chart, published to GHCR on every `v0.2.N-preview` tag.

The canonical, step-by-step procedure lives in `docs/RELEASING.md` in the
repository — this chapter is the orientation summary.

## The model

Pushing a semver tag to `main` triggers the release workflow
(`.github/workflows/docker-publish.yml`), which builds and publishes three
artifacts to GHCR:

- the broker image — `ghcr.io/woestebanaan/kaas[-preview]`
- the operator image — `ghcr.io/woestebanaan/kaas-operator[-preview]`
- the Helm chart — `oci://ghcr.io/woestebanaan/charts/kaas`

Pre-release tags (anything containing `-`, like `v0.2.4-preview`) get the
`-preview` image-name suffix automatically; the chart's image helpers
derive the same suffix from the tag, so the chart default always points at
images that exist.

## The two rules

1. **Tags are immutable.** Never re-cut or force-move a tag. A bad release
   is fixed by the next patch number, not by rewriting the old one.
2. **Always bump the patch**: `v0.2.N-preview` → `v0.2.N+1-preview`. (The
   one historical exception: the `v0.1.190-preview` → `v0.2.0-preview`
   jump at the Go→Rust cutover.)

## Before tagging

`cargo xtask ci` green locally, CRDs regenerated if the operator API
changed (`cargo xtask gen-crds` — CI fails on drift), and the book building
cleanly (`cargo xtask docs`). If CRDs changed, remember the chart
[does not upgrade them automatically](./helm.md) — release notes should
say so.
