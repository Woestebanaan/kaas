# Rename Plan — skafka → kaas

Status: **executed** (R1 swept 2026-07-19; this doc keeps the old name as a record and is grep-gate allowlisted). Executes **before** [book phase 1](./book-phase-1-scaffolding.md) — the
book must be born under the final name (published Pages URL, README, every chapter title).

Baseline (grep-verified 2026-07-18): the name appears in **1,143 places across 173 files**.
The tree is at `v0.2.3-preview`; the first post-rename tag continues the line as
`v0.2.4-preview` (patch bump, never re-cut).

## Name mapping

| Surface | Today | Becomes | Notes |
|---|---|---|---|
| Product / GitHub repo | `Woestebanaan/skafka` | `Woestebanaan/kaas` | GitHub redirects old URLs, remotes, and issue links |
| Broker binary | `bins/skafka` | `bins/kaas` | Dockerfile entrypoint path changes with it |
| Operator binary | `bins/skafka-operator` | `bins/kaas-operator` | |
| Lib crates | `crates/sk-*` (12) | `crates/kaas-*` | Decision D2 below — recommended in-scope |
| Env vars | `SKAFKA_*` (29, grep-verified) | `KAAS_*` | Parsed in `cli.rs` + observability; emitted by chart templates |
| CRD API group | `skafka.io` | **`kaas.rs`** (decided) | `group = "kaas.rs"` in the 4 `kube-derive` types in `crates/sk-operator-api/src/` |
| CRD kinds | `KafkaTopic`, `KafkaUser`, `KafkaCluster`, `KafkaClusterAssignments` | **unchanged** | They name Kafka concepts the CR manages (Strimzi-parallel), not the product |
| Generated CRD files | `deploy/crds/skafka.io_*.yaml` (+ chart mirror) | `deploy/crds/kaas.rs_*.yaml` | `cargo xtask gen-crds` derives filenames from the group; delete the old files in the same commit or `check-crd-drift` still passes with strays |
| Readiness gate | `skafka.io/PartitionsReady` | `kaas.rs/PartitionsReady` | `crates/sk-k8s/src/readiness.rs` + chart pod spec |
| Controller Lease | `skafka-controller` | `kaas-controller` | `coordinator.rs`, `kube_watchers.rs`, chart RBAC |
| Helm chart | `deploy/helm/skafka`, `name: skafka` | `deploy/helm/kaas`, `name: kaas` | Also every `skafka.*` helper in `_helpers.tpl`, labels, Service/StatefulSet names |
| In-cluster bootstrap DNS | `skafka.skafka.svc.cluster.local:9092` | `kaas.<ns>.svc.cluster.local:9092` | Default in `scripts/_common.sh`; also baked into the user-level bench/scripts skills (external follow-up) |
| Images | `ghcr.io/woestebanaan/skafka[-preview]`, `skafka-operator[-preview]` | `…/kaas[-preview]`, `kaas-operator[-preview]` | GHCR packages do **not** follow repo renames — new packages appear on first push; archive the old ones |
| Helm OCI chart | `oci://ghcr.io/woestebanaan/charts/skafka` | `…/charts/kaas` | |
| Proto package | `skafka.heartbeat.v1` | `kaas.heartbeat.v1` | Breaks broker↔broker gRPC compat mid-rolling-upgrade — irrelevant with the fresh-deploy cutover (R3). Also **delete the stale `option go_package`** (Go-era leftover) |
| ClusterId default | `skafka-dev` (dev mode) | `kaas-dev` | Prod value comes from `SKAFKA_CLUSTER_ID` → `KAAS_CLUSTER_ID`; wire-visible in Metadata but purely cosmetic to clients |
| OTLP service names | `skafka` / `skafka-operator` (`job=skafka`) | `kaas` / `kaas-operator` | `sk-observability/src/bootstrap.rs`; Grafana dashboards/queries keyed on `job=` are an external follow-up |
| GH project board | skafka-migration-parity | rename in place | Board ID/links keep working |

**Explicitly unchanged**: `scripts/kafka-*.sh` filenames (named for the Apache tools they run),
the Kafka 3.7 parity target, the `v0.2.N-preview` release line, and historical records —
`docs/perf-results/` and `scripts/.parity-baseline-go.txt` are records of runs against the old
name; rewriting them falsifies history. `scripts/.parity-baseline.txt` is *regenerated* (not
edited) in R4.

## Decisions

- **D1 — repo name**: `kaas` (per discussion; `kaas-broker` was the earlier candidate — final
  call before R2, since the repo rename is the one step other people's links depend on).
- **D2 — crate prefix**: rename `sk-*` → `kaas-*` (recommended). It's the largest mechanical
  churn (every `Cargo.toml`, every `use sk_…::` import) but it's internal-only, `cargo fix`-
  assisted, and this is the only cheap moment. Fallback if deferred: keep `sk-*` and accept a
  permanently half-renamed codebase.
- **D3 — PVC data**: **keep it** (see R3). Per-partition data (`/data/<topic>/<partition>/`)
  contains no broker names; the only broker-named artifacts are transient `__cluster/` files
  that are safely regenerated or deleted at cutover.
- **D4 — `kaas.rs` domain registration**: optional. A K8s API group doesn't require DNS
  ownership, but owning `kaas.rs` (a favorite TLD for Rust projects) secures the docs-site
  option before the book publishes. Check availability during R0; not a blocker.

## R0 — Preflight

- [ ] Freeze decisions D1–D4.
- [ ] `main` green (`cargo xtask ci`), working tree clean, `v0.2.3-preview` confirmed as the
      last skafka-named tag.
- [ ] Optionally register `kaas.rs`.

## R1 — Repo-internal rename (commit series on `main`)

Ordered so `cargo xtask ci` is green after every commit — no broken intermediate states:

1. **Crates + bins**: rename directories, `Cargo.toml` package names + workspace members,
   all imports (D2). Includes `xtask` usage strings and `sk-broker/build.rs` paths.
2. **Proto**: `package kaas.heartbeat.v1`, drop `option go_package`, `cargo xtask gen-proto`.
3. **Env vars**: `SKAFKA_*` → `KAAS_*` in `cli.rs`, observability, tests — and the chart
   templates that emit them, in the same commit (the chart is config source of truth; a split
   here ships a broker that ignores its own chart).
4. **CRD group + K8s identifiers**: `group = "kaas.rs"` in the 4 API types →
   `cargo xtask gen-crds` (new `kaas.rs_*.yaml`, delete `skafka.io_*.yaml` in both
   `deploy/crds/` and the chart mirror) → RBAC `apiGroups`, readiness-gate string, Lease name.
5. **Helm chart**: directory, `Chart.yaml`, `_helpers.tpl` helper names, labels, Service /
   StatefulSet names, NOTES.txt, `scripts/_common.sh` BOOTSTRAP default.
6. **CI + Dockerfiles**: `ci.yml`, `docker-publish.yml` image names, Dockerfile paths/binary
   names, the ARC-runner comment.
7. **Docs sweep**: `CLAUDE.md`, `ARCHITECTURE.md`, `RELEASING.md`, `book-plan.md` + the six
   `book-phase-*.md` files, this file's status line.

Gate after the series: `grep -ri skafka` returns zero hits outside the allowlist
(`docs/perf-results/`, `scripts/.parity-baseline-go.txt`). Run `cargo xtask ci` one final time.

## R2 — GitHub surface

- [ ] Rename the repo (`skafka` → `kaas`); redirects cover remotes/issues/PRs.
- [ ] Rename the parity project board; spot-check CLAUDE.md's board link still resolves.
- [ ] Tag `v0.2.4-preview` — first kaas-named release; `docker-publish.yml` creates the new
      GHCR packages. Set visibility to match the old ones.
- [ ] Archive (don't delete) the old `skafka*` GHCR packages and chart entry — running
      clusters may still reference pinned digests.
- [ ] External repos/config: `k3s-cluster` ArgoCD app + ARC runner set references; the
      user-level `bench-skafka` / `bench-compare` / `skafka-scripts` skills (bootstrap DNS +
      names); Grafana dashboards keyed on `job=skafka`.

## R3 — Cluster cutover (home k3s, preview line)

Topic data survives the rename; only broker-*named* transients need care:

1. **Drain**: stop producers/consumers cleanly — no in-flight transactions, so
   `__cluster/marker_queue/to-skafka-*/` and `fence_log/from-skafka-*.json` are empty or
   ignorable.
2. `kubectl get kafkatopics.skafka.io,kafkausers.skafka.io -A -o yaml` → export; rewrite
   `apiVersion: skafka.io/…` → `kaas.rs/…` (sed/yq); strip `status` + managed fields.
   `Status.TopicID` note: topic IDs regenerate on first reconcile under the new group —
   clients treat that as a topic re-creation, acceptable on the preview line (KIP-516
   contract: re-created topics get distinct IDs).
3. Uninstall the old release; delete the old `skafka.io` CRDs; **keep the PVC**.
4. On the PVC, delete the stale transients: `__cluster/assignment.json` (controller rewrites
   on first election), `fence_log/`, `marker_queue/` (per-broker names change with the
   StatefulSet). `credentials.json` / `acls.json` are rewritten by the operator on reconcile.
5. Install the `kaas` chart (`v0.2.4-preview` images), apply the converted CRs.

## R4 — Verify, then unblock the book

- [ ] `cargo xtask ci` green; CI green on the renamed repo (ARC runners picked up the new name).
- [ ] Full `scripts/kafka-*.sh` sweep against the live broker → regenerate
      `scripts/.parity-baseline.txt`; expect 21 PASS / 20 SKIP / 0 FAIL, now on kaas.
- [ ] One producer-bench sanity run (throughput within the established band — this is a
      rename, not a perf change).
- [ ] EOS smoke (`eos_v2.rs` live-broker mode) — exercises PID/txn state on the *retained* PVC.
- [ ] Final `grep -ri skafka` allowlist check.
- [ ] Start [book phase 1](./book-phase-1-scaffolding.md) under the kaas name.

## Rollback

R1 is plain commits on `main` — revert the series. R2's repo rename is reversible in settings
(redirects then point the other way). R3 keeps the PVC and the exported CR yamls, so the old
chart + old CRDs restore the previous cluster; the archived GHCR packages remain pullable.

## Execution log (2026-07-19)

**Done:** R0–R2 complete. R1 landed as 7 commits + one fix commit on `main` (fmt import
order + two file renames the content sweep missed: the fence-log test fixture and
`deploy/grafana/kaas-dashboard.json`). CI green on the renamed repo (rust/docker/helm).
Repo renamed to `Woestebanaan/kaas`, board renamed in place, `v0.2.4-preview` tagged;
`kaas-preview` / `kaas-operator-preview` / `charts/kaas` published to GHCR by the release
workflow on the repointed ARC runners. Deviations from plan: env-var count was 41 (not 29);
extra surfaces swept: OTLP metric names `skafka.*` → `kaas.*`, `deploy/rbac/`, chart
prometheusrule, `deploy/grafana/`. This file keeps the old name as the record of the
mapping and is allowlisted in the grep gate.

**R3 (in flight):** k3s-cluster cutover commit pushed (`apps/skafka` → `apps/kaas`,
CR apiVersions → `kaas.rs/v1alpha1`, appset conditional, kafbat-ui, Grafana dashboard).
`skafka-data` PV (`pvc-4086de79…`) patched to **Retain**; CR backups exported. The `kaas`
Application exists but is **not synced** — blocked on the two manual items below.

**Blocked / manual:**
1. **NAS outage** — 192.168.1.51 stopped serving NFS (all `skafka`/`strimzi` pods lost
   their mounts days before the rename). Bring the NAS back before syncing the kaas app.
2. **GHCR package visibility** — fresh packages default to private; ArgoCD's chart pull
   and kubelet image pulls 403 until `kaas-preview`, `kaas-operator-preview`, and
   `charts/kaas` are made public (github.com → profile → Packages → package settings →
   Danger Zone → change visibility), or a pull secret is configured.
3. **Archive old packages** — keep `skafka*` GHCR packages pullable (pinned digests) but
   mark them deprecated in their descriptions; GHCR has no archive API.
4. Optional: register `kaas.rs` (D4); rename the NAS-side directory
   `k8s-volumes/skafka/skafka-data` is *not* needed (the retained PV points at it).

**Remaining cutover sequence** (after 1+2): rebind PV into ns `kaas` (clear
`spec.claimRef`, pre-create PVC `kaas-data` with `volumeName` pinned), clean stale
transients on the PVC (`__cluster/assignment.json`, `fence_log/`, `marker_queue/`),
sync the `kaas` app, then run R4 verification.

## R3/R4 completion (2026-07-19, later)

NAS restored; cutover completed: PV rebound as `kaas-data` in ns `kaas` (volumeName
pinned in the kustomize overlay — gh k3s-cluster commit), old CRDs/namespace removed,
`kaas` app synced, 3/3 brokers Ready on the retained data.

**Post-cutover incident**: R3.4's "delete assignment.json" was wrong in a way this doc's
own model missed — per-partition assignment epochs are *move counters* (`prev+1`, reset
to 1 with no prev file), and retained manifests remembered counters up to 71, so every
append to a retained topic was fenced (`epoch mismatch` → client NotLeaderOrFollower).
Recovered by bumping the per-partition `"epoch"` fields to 200 in assignment.json with
brokers scaled to 0. Filed as gh#197 (with the empty assignments-CR-mirror observation).

**R4 results**:
- CI green on the renamed repo (ARC runners repointed, tag-driven release published).
- Parity sweep: **21 PASS / 20 SKIP / 0 FAIL** — totals identical to v0.2.0-preview;
  `scripts/.parity-baseline.txt` regenerated. One transient timeout
  (kafka-verifiable-consumer, fresh-topic assignment lag ~2 min after restart) passed
  on rerun.
- Producer bench: ~101 MB/s aggregate (5×~20 MB/s) vs the 46 MB/s band — well above;
  NAS was otherwise idle (strimzi still down post-outage).
- EOS smoke: **failed, pre-existing** — live multi-broker transactional produce dies
  with fatal OutOfOrderSequence at the 2nd–3rd txn boundary even on a fresh
  single-partition topic (gh#195); PID counter reset-on-boot also collides with
  retained producer snapshots (gh#196). Never rename-related: live EOS had never been
  exercised (streams jobs run at_least_once; eos_v2.rs is in-process and passes).
- Final grep gate clean (allowlist: perf-results, both parity baselines, this file).

The book (phase 1) is unblocked.
