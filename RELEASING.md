# Releasing skafka

Releases are tag-driven. Pushing a semver tag to `main` triggers the
`release` workflow (`.github/workflows/docker-publish.yml`), which builds and
publishes the broker image, the operator image, and the Helm chart to GHCR.

## Versioning

- Semver: `vMAJOR.MINOR.PATCH[-PRERELEASE]`.
- While the project is pre-1.0, every release uses the `-preview` suffix
  (e.g. `v0.1.3-preview`). Pre-release tags publish to separate image and
  chart names so they can't be mistaken for stable artifacts.
- **Bump the patch for every release. Never re-cut an existing tag.** Tags
  are immutable; downstream consumers (Helm, ArgoCD, image pulls) cache by
  digest, and re-pointing a tag silently breaks them.

History so far:

| Tag                | Headline change                                                  |
| ------------------ | ---------------------------------------------------------------- |
| `v0.1.0-preview`   | First preview cut — broker registers discovered topics on start. |
| `v0.1.1-preview`   | Helm chart honours `auth.enabled=false` via env.                 |
| `v0.1.2-preview`   | Broker shares one `FlockLock` between engine and produce path.   |
| `v0.1.3-preview`   | Coordinator caps initial rebalance delay for new groups.         |

## What gets published

The workflow strips the leading `v` and uses the remainder as both the image
tag and the Helm chart version (so `v0.1.3-preview` → `0.1.3-preview`).

Pre-release tags (`v*-*`) publish to:

- `ghcr.io/woestebanaan/skafka-preview:<version>`
- `ghcr.io/woestebanaan/skafka-operator-preview:<version>`
- `oci://ghcr.io/woestebanaan/charts/skafka:<version>`

Stable tags (no pre-release suffix) publish to the un-suffixed names
(`skafka`, `skafka-operator`). No stable tag has been cut yet.

Images are built `linux/amd64` only.

## Cutting a release

1. Make sure `main` is green and contains the changes you want to ship.
2. Pick the next patch version. Look at the latest tag:
   ```bash
   git tag -l 'v*' | sort -V | tail -n1
   ```
   Bump the patch (`v0.1.3-preview` → `v0.1.4-preview`).
3. Tag the tip of `main` and push the tag:
   ```bash
   git tag v0.1.4-preview
   git push origin v0.1.4-preview
   ```
4. Approve the run. The workflow uses the `release` GitHub Environment, so
   the job pauses for manual approval before pushing artifacts.
5. After the run completes, verify the artifacts exist:
   ```bash
   # chart
   helm pull oci://ghcr.io/woestebanaan/charts/skafka --version 0.1.4-preview
   # images (any registry-aware tool works)
   docker buildx imagetools inspect \
     ghcr.io/woestebanaan/skafka-preview:0.1.4-preview
   ```

The `Chart.yaml` `version`/`appVersion` fields are overridden by the CI
packaging step (`helm package --version --app-version`); you do **not** need
to bump them in-tree before tagging.

## Hotfixes

Same flow — there is no separate hotfix branch. Land the fix on `main`,
bump the patch, tag, push. If a bad release ships, cut a new patch with the
fix; do not delete or move the broken tag.

## Infrastructure notes

- The release job runs on the in-cluster `arc-runner-set` (ARC runners
  defined in the `k3s-cluster` repo under `apps/arc-runners`). It needs the
  DinD sidecar for `docker buildx`.
- Image and chart pushes use the workflow's `GITHUB_TOKEN` with
  `packages: write`; no PATs required.
- Cosign signing was removed (commits `54bcf9e`, `60e1810`); released
  artifacts are not signed today.
