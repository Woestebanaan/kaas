# Phase 9 — Cutover (Rust default, `archive/` deleted)

Detailed work plan for the tenth and final phase of the Rust rewrite.
Companion to [`rewrite.md`](./rewrite.md) §Phase 9; the high-level
summary lives there. Tracker: [gh #152], umbrella [gh #143]. Builds on
everything phases 0–8 landed — in particular Phase 8's dual-publish
workflow (both image flavors build on every `v*` tag since commit
`244d58f`) and Phase 8's parity artifacts (`scripts/.parity-baseline.txt`,
the `bench-compare` Strimzi-ratio methodology).

[gh #152]: https://github.com/Woestebanaan/skafka/issues/152
[gh #143]: https://github.com/Woestebanaan/skafka/issues/143

**Goal.** Make the Rust broker + operator the default production
artifact and retire the Go tree. Concretely: close the two parity
gates Phase 8 left open with follow-up, add the `image.flavor` Helm
knob, cut `v0.2.0-preview` (first Rust-numbered release), bake the
Rust flavor for 72 h against the parity matrix including a
rollback-and-back drill, flip the chart default to `rust`, and — after
two clean Rust-default releases — delete `archive/`, trim CI to
Rust-only, and rewrite the docs that still speak in "the Go tree is
the product" terms (`CLAUDE.md`, `RELEASING.md`, `rewrite.md`).

**Length.** ~1 week of active engineering, ~3–4 weeks of calendar time.
The gap is deliberate: 72 h of bake plus a two-release soak before
`archive/` deletion are elapsed-time gates, not effort. Workstreams A
and B parallelize; C–F are strictly sequential.

**Out of scope for Phase 9.**

- **Closing the Strimzi performance gap.** Per
  `memory/project_skafka_perf.md` the ~75 % single-consumer throughput
  and ~3.8× producer-p50 deltas are architectural (group-commit fsync
  vs page-cache ack; single-writer vs ISR). The Phase 9 gate is the
  same as Phase 8's: *don't regress the ratio*. Gap-closing is a
  post-cutover design conversation.
- **Full `kafka-compat` suite port** (Phase 8 workstream D residue —
  22 test bodies behind a `--features kafka-compat-rdkafka` gate
  needing librdkafka + kind). The bake matrix covers the same surface
  via `scripts/kafka-*.sh` + the EOS test; the port continues
  post-cutover as its own issue. If the bake surfaces a bug the suite
  would have caught, escalate the port into the phase.
- **Cross-broker `WriteTxnMarkers`** (gh #114) and the
  `DescribeTransactions`/`ListTransactions` admin surfaces — same
  status they had in Phases 7–8: follow-ups, not cutover blockers,
  because the Go broker doesn't have them either. Parity is against
  Go, not against Apache.
- **New admin surface beyond parity.** Workstream A triages the
  remaining parity-baseline FAILs against a Go control run; anything
  the Go broker *also* fails is reclassified as a documented skip, not
  implemented here (e.g. `DescribeLogDirs` if the control run confirms
  the Go broker never served it).
- **Per-handler `tracing` spans** — still the alert-free quality
  follow-up it was in Phase 8.

**Preconditions — the honest ledger.** Phase 8 closed "delivered with
follow-up" (`phase-8.md` §Landed vs pending). Phase 9 cannot flip a
default on top of unresolved follow-ups, so workstream A exists to
retire them:

1. `scripts/.parity-baseline.txt` was captured against
   `v0.1.179-preview`, **before** multi-broker landed
   (`v0.1.184`–`v0.1.190`, commits `28f5623`…`79202df`). 8 of its 12
   FAILs trace to the now-closed "multi-broker leader ownership"
   bucket. The baseline is stale in the good direction — re-capture.
2. The `bench-compare` Strimzi-ratio gate has **never produced
   numbers**: the Phase 8 run hit the bench Job's
   `activeDeadlineSeconds=1200` on both sides (workload arithmetic,
   not NAS health), and the Go *reference* capture
   (`docs/perf/go-reference-<sha>.md` per phase-8 §F.1) never landed
   either. Both captures happen fresh in A.3 — which is also required
   for a second reason: the NAS uplink ran at 100BaseT until
   2026-07-12 (`memory/user_nas_link_bandwidth.md`), so *any*
   pre-existing bench numbers are network-bound artifacts.
3. The stale Phase 8 bench report (`docs/perf/rust-phase-8-f2417d2.md`,
   all-N/A ratio column) has an uncommitted deletion in the working
   tree; fresh reports from A.3 supersede it. Commit the deletion
   together with the new captures.

---

## Workstreams

Six workstreams. A (parity-gate closure) and B (chart flavor knob)
run in parallel; C (cut `v0.2.0-preview`) needs both; D (72 h bake +
rollback drill) needs C; E (default flip) needs D; F (decommission)
needs two clean releases after E.

- **A** — Close the Phase 8 parity residue: re-baseline
  `scripts/kafka-*.sh` post-multi-broker, run a Go-flavor control
  pass, triage remaining FAILs into fix-before-flip vs documented
  divergence, and land the bench-compare ratio gate with real numbers.
- **B** — `image.flavor: go | rust` in the Helm chart: values shape,
  `_helpers.tpl` resolution (including the `-preview` name suffix the
  publish workflow uses), CI template coverage for both flavors.
- **C** — Tag `v0.2.0-preview`: the minor-bump exception, release
  notes with the deprecation timeline, re-pin the bench cluster
  through the new knob (dogfood).
- **D** — 72 h bake on the parity matrix + a rust→go→rust rollback
  drill that proves the on-disk contract in the direction that
  matters when things go wrong.
- **E** — Flip the chart default to `rust` (`v0.2.1-preview`), mark
  the Go flavor deprecated everywhere a consumer might read.
- **F** — Decommission after two clean Rust-default releases: delete
  `archive/`, trim CI + publish workflow, take over the plain image
  names, rewrite `CLAUDE.md` / `RELEASING.md` / `rewrite.md`, close
  the tracker.

---

## A — Close the Phase 8 parity residue

The flip decision in E is only as trustworthy as these gates. Order
inside A: A.1 and A.2 first (cheap, one day together), A.3 after
(bench runs are elapsed-time-heavy).

### A.1 — Re-capture the scripts baseline post-multi-broker

Deploy the current Rust pair (`v0.1.190-preview` images or newer) at
the production replica shape (`broker.replicaCount: 3` — the
`replicaCount: 1` note in the k3s-cluster values was a Phase 8
workaround for the then-missing multi-broker leader ownership; that
gap closed in `28f5623` + the `v0.1.185`–`190` hardening chain). Run
the full suite via the `skafka-scripts` skill.

Expected movement against the committed baseline:

- The 8 FAILs annotated `multi-broker leader ownership`
  (`kafka-console-{producer,consumer}.sh`, `kafka-e2e-latency.sh`,
  `kafka-get-offsets.sh`, `kafka-verifiable-consumer.sh`,
  `kafka-topics.sh`, `kafka-delete-records.sh`,
  `kafka-streams-application-reset.sh`) → PASS. Any that stay FAIL is
  a live multi-broker bug: file it, fix it, re-run. These are
  hard blockers — they're the produce/consume hot path.
- The 2 perf-test FAILs (`kafka-{producer,consumer}-perf-test.sh`)
  were 120 s-timeout arithmetic, same class as the bench Job problem.
  Fix the preset (record count), not the broker.
- The 20 SKIPs must stay SKIPs for the same documented reasons.

Commit the refreshed file over `scripts/.parity-baseline.txt` with an
updated header (broker version, capture date). This file is the D
bake's per-run comparison target, so it must be green-or-explained
*before* the bake starts.

### A.2 — Go control run: establish the true parity yardstick

The parity contract (rewrite.md §Phase 8, CLAUDE.md) is "every script
that exits 0 against the **Go** broker exits 0 against the Rust
broker" — but no Go-flavor result set was ever committed, so three of
the remaining FAILs can't be classified without one. Deploy the Go
flavor once (`--set image.repository=ghcr.io/woestebanaan/skafka-preview`
at the same chart version), run the suite, commit the result as
`scripts/.parity-baseline-go.txt` (control; not a regression gate,
a classification key). Then triage:

- **`kafka-acls.sh`** — annotated "ACL provisioning via KafkaUser CR;
  operator reconcile gap". If the Go operator materialises
  `acls.json` for the script's KafkaUser and the Rust operator
  doesn't, this is a **flip blocker**: existing KafkaUser CRs would
  silently lose enforcement on cutover. Fix in
  `crates/sk-operator-controllers` (the `kafkauser.rs` reconciler)
  inside this workstream. If the Go run also fails, reclassify with
  the evidence in the baseline comment.
- **`kafka-configs.sh`** — annotated "IncrementalAlterConfigs on
  cluster-level configs". The Go broker is TOPIC-only by design
  (BROKER/BROKER_LOGGER → `UNSUPPORTED_VERSION`, gh #9), so the Go
  control run likely fails the same step → documented divergence-
  from-Apache, not divergence-from-Go: downgrade to SKIP with a
  reason, mirroring how the suite treats other non-goals.
- **`kafka-log-dirs.sh`** — "DescribeLogDirs not implemented". Same
  test: if Go never served it, SKIP-with-reason; if Go serves it,
  it's a small read-only handler and lands here.

The rule A.2 enforces: **every FAIL row in the committed baseline
either becomes PASS or becomes a SKIP whose reason cites the Go
control run.** Zero unexplained FAILs enter the bake.

### A.3 — bench-compare ratio gate, with numbers this time

Two workload-arithmetic fixes to the bench preset before any capture
(the Phase 8 post-mortem: 100 M × 1 KB × 5 pods at ~7 MB/s sustained
cannot finish inside a 1200 s Job deadline — the NAS was healthy):

- Cut the record count in the producer Job manifest so a healthy run
  finishes in ~10 min (≈ 4 M × 1 KB per pod at the observed rates),
  **or** raise `activeDeadlineSeconds` to 3600. Prefer the smaller
  record count — shorter runs make the 5-run protocol tractable in
  one evening.
- Keep the NAS-liveness probe and the 120 s cooldown as-is.

Then, per `memory/feedback_bench_methodology.md` (5 runs, drop
fastest + slowest, average 3) and phase-8 §F:

1. **Go reference capture** — Go flavor deployed, 5 × `bench-compare`
   → commit `docs/perf/go-reference-<sha>.md`. This did not happen in
   Phase 8 and pre-cable-fix numbers are unusable, so it happens now,
   on the same post-2026-07-12 network the Rust runs will use.
2. **Rust capture** — Rust flavor, same protocol → commit
   `docs/perf/rust-phase-9-<sha>.md`.
3. **Gate** — Rust skafka/Strimzi ratio within ±5 % of Go
   skafka/Strimzi ratio per axis (producer p50/p95/p99, consumer
   throughput, e2e latency). CPU/RSS: report, don't gate (Rust is
   expected to win).

Failure handling unchanged from phase-8 §F.4: 5–15 % ratio widening →
profile (cargo-flamegraph via the ARC runners); > 30 % → stop the
phase per the rewrite risk register. **Do not proceed to C with a
known ratio regression > 5 %.**

**Exit A:** refreshed `scripts/.parity-baseline.txt` with zero
unexplained FAILs; `scripts/.parity-baseline-go.txt` control
committed; both bench reports committed and the ratio table has zero
red cells; the stale phase-8 perf report deletion is committed.

---

## B — `image.flavor` in the Helm chart

### B.1 — Values shape

```yaml
image:
  flavor: go            # go | rust — selects the default repositories
  repository: ""        # explicit value overrides flavor entirely
  tag: ""               # defaults to Chart.appVersion
  pullPolicy: IfNotPresent

operator:
  image:
    repository: ""      # explicit value overrides flavor entirely
    tag: ""
    pullPolicy: IfNotPresent
```

`repository` moves from a hardcoded default
(`ghcr.io/woestebanaan/skafka`) to empty-means-derived. One knob
(`image.flavor`) switches broker *and* operator — a mixed-flavor
deployment (Rust broker, Go operator) is not a supported shape, and
anyone who genuinely needs one can still pin both repositories
explicitly.

### B.2 — Helper resolution

Rewrite `skafka.brokerImage` / `skafka.operatorImage` in
`_helpers.tpl`:

```gotpl
{{- define "skafka.brokerImage" -}}
{{- $tag := .Values.image.tag | default .Chart.AppVersion -}}
{{- $repo := .Values.image.repository -}}
{{- if not $repo -}}
  {{- if not (has .Values.image.flavor (list "go" "rust")) -}}
    {{- fail (printf "image.flavor must be \"go\" or \"rust\", got %q" .Values.image.flavor) -}}
  {{- end -}}
  {{- $rs := eq .Values.image.flavor "rust" | ternary "-rs" "" -}}
  {{- $pre := contains "-" $tag | ternary "-preview" "" -}}
  {{- $repo = printf "ghcr.io/woestebanaan/skafka%s%s" $rs $pre -}}
{{- end -}}
{{ $repo }}:{{ $tag }}
{{- end -}}
```

Two deliberate properties:

- **Explicit `repository` always wins** — the k3s-cluster deployment
  (which pins `skafka-rs-preview` today) and any other pinner render
  byte-identically before and after this change.
- **The `-preview` suffix is derived from the tag**, mirroring the
  exact rule `docker-publish.yml`'s "Compute image names" step uses
  (`v*-*` → `-preview` names). Today's default
  (`ghcr.io/woestebanaan/skafka` + a `-preview` appVersion) points at
  an image name that has never existed for a prerelease tag — the
  default has always silently required an override. Deriving the
  suffix fixes that for both flavors at once, and is what lets D and
  E flip flavors with one `--set` instead of two repository pins.

Same shape for `skafka.operatorImage` over `.Values.operator.image`.
Add a resolved-images line to `NOTES.txt` so `helm install` output
shows exactly which pair a release runs.

### B.3 — CI + docs

- The `helm` CI job gains a second `helm template` invocation with
  `--set image.flavor=rust` (and keep one plain-default render), so
  both branches of the helper stay lint-green.
- `deploy/helm/skafka/README.md`: replace Phase 8's "Image overrides"
  note (two explicit repository pins) with the flavor knob as the
  primary interface; keep the explicit-pin paragraph for the
  mixed/airgapped cases.
- Chart-only change — no broker/operator code, no CRD drift. Helm
  chart version continues to ride the release tag as before.

**Exit B:** `helm template` renders the correct four
repository/flavor/tag combinations (go/rust × preview/stable);
explicit-pin values files render unchanged vs the previous chart;
CI covers both flavors.

---

## C — Cut `v0.2.0-preview`

### C.1 — The tag

First deviation from patch-bump-only since the policy was written,
and it's the one `rewrite.md` §Phase 9 pre-authorised: the version
line jumps `v0.1.190-preview` → `v0.2.0-preview` to mark the first
Rust-numbered release. Everything else about the release flow is the
standard `RELEASING.md` procedure. The publish workflow needs no
edits — it has dual-published since Phase 8, so this tag produces:

- `ghcr.io/woestebanaan/skafka-preview:0.2.0-preview` (Go, from `archive/`)
- `ghcr.io/woestebanaan/skafka-operator-preview:0.2.0-preview` (Go)
- `ghcr.io/woestebanaan/skafka-rs-preview:0.2.0-preview` (Rust)
- `ghcr.io/woestebanaan/skafka-operator-rs-preview:0.2.0-preview` (Rust)
- `oci://ghcr.io/woestebanaan/charts/skafka:0.2.0-preview` (chart with
  the flavor knob, default still `go`)

The Go images keep publishing under every `v0.2.x` tag until F — that
is the dual-publish window, and it's also the rollback supply for D's
drill (rolling back never requires reaching for an old tag).

### C.2 — Release notes carry the deprecation timeline

The `v0.2.0-preview` notes state the plan one release ahead, per the
gh #152 risk item:

- `image.flavor` exists, default `go` — this tag changes nothing for
  existing deployments.
- `v0.2.1-preview` (after the bake) flips the default to `rust`.
- Consumers pinning `image.repository` explicitly are **not** moved
  by the flip — if they want the Rust flavor they must switch to the
  knob or re-pin. (Today the only known consumer is the k3s-cluster
  repo, which C.3 moves onto the knob.)
- Go images continue to publish for now; removal comes after two
  clean Rust-default releases, announced again in `v0.2.1` notes.

`RELEASING.md` gets a short mid-cutover status update (the "first
Rust release will be tagged v0.2.0-preview" paragraph becomes past
tense; the full rewrite of that doc waits for F).

### C.3 — Dogfood the knob

Re-pin the k3s-cluster deployment (`apps/skafka` in the k3s-cluster
repo) to chart `0.2.0-preview`, replacing its two explicit
`*-rs-preview` repository pins with `image.flavor: rust`. This is
the bake deployment for D, running the knob path the flip will make
default rather than the override path nobody else uses.

**Exit C:** tag pushed; five artifacts verified on GHCR
(`docker buildx imagetools inspect` × 4 + `helm pull`); bench cluster
reconciled onto chart `0.2.0-preview` with `flavor: rust` and serving.

---

## D — 72 h bake + rollback drill

### D.1 — Bake protocol

Environment: the k3s bench cluster, NFS-RWX PVC, `replicaCount: 3`,
Rust flavor via the knob (C.3's deployment). Clock starts at the
first clean matrix run after C.3.

Matrix, executed at T0, T+24 h, T+48 h, T+72 h (each run ≈ 1 h; use
the existing skills — `skafka-scripts`, `bench-skafka` — the schedule
can be cron'd or driven manually):

1. `skafka-scripts` full suite → diff against the A.1 baseline. Any
   row downgrade fails the run.
2. `bench-skafka` (producer bench + Streams wordcount pipeline) →
   completes without DeadlineExceeded; throughput within the noise
   band of the A.3 Rust capture (±15 % single-run tolerance per the
   bench-methodology memory — single runs are noisy; investigate,
   don't auto-fail, inside the band).
3. EOS smoke: the franz-go EOS test from `archive/tests/kafka-compat/`
   run as a Go test against the live Rust broker (the Phase 6 pattern
   — the Go test binary is a client here, archive-freeze doesn't
   apply to running it).

Continuous between runs:

- Pod restart counters stay 0 across all brokers + operator
  (`kubectl get pods` — any restart is a bake failure regardless of
  matrix results).
- The 9 PrometheusRule alerts stay quiet (SkafkaByteOpacityViolated,
  SkafkaSelfFencing, SkafkaStaleControllerWriting,
  SkafkaNoCurrentController, SkafkaBrokerCountMismatch,
  SkafkaAssignmentFileWriteFailing,
  SkafkaAssignmentFileSizeApproachingCap,
  SkafkaCRMirrorErrorSustained, SkafkaHeartbeatRTTHigh).

**Why the Go flavor doesn't get a symmetric 72 h:** `rewrite.md` says
"run the matrix against both flavors"; on a single home cluster with
one NFS substrate, concurrent flavor runs would contend the NAS and
poison every number. The Go flavor's bake evidence is its months of
production history on this exact cluster (every `v0.1.x` release ran
here) plus the fresh A.2 control run. Sequential-and-asymmetric is
the honest reading of the intent.

**Clock rule:** any broker or operator code change (hotfix found
mid-bake) restarts the 72 h clock on the fixed build. Chart, docs,
and bench-manifest changes do not.

### D.2 — Rollback drill (the part that earns the flip)

The rewrite's core promise is "same on-disk layout, cutover is
image-only" — which means **rollback is image-only too**, and that
claim has never been exercised in the Rust→Go direction on real data.
After the T+24 h matrix run passes:

1. Note per-topic HWMs, consumer-group offsets, and an in-flight
   transactional producer's PID/epoch (`kafka-transactions.sh`
   describe output).
2. Flip the live deployment to `image.flavor: go` (one values change,
   ArgoCD sync). Go brokers take over the same PVC: same
   `assignment.json`, same epoch-prefixed segments, same
   `manifest.json` + `producer-state.snapshot` + `txn_state/slot-*.json`
   + `__consumer_offsets/*.json`.
3. Verify: consume from offset 0 on a Rust-written topic
   (CRC-validated by the client), committed offsets intact, a
   transactional producer resumes with a bumped epoch (not
   `OUT_OF_ORDER` / fence errors), no broker crash-loops.
4. Produce a marker batch under Go, flip back to `flavor: rust`,
   re-verify the same set including the Go-written marker.

Divergence anywhere here (a serde field-name mismatch in any of the
JSON files, a snapshot the other side can't parse, an epoch-prefix
interpretation difference) is a **cutover blocker** of the highest
order — it's the difference between "flip back" being a 2-minute
operation and a data-loss incident. Fix, re-tag (`v0.2.1` becomes the
bake build, flip moves to `v0.2.2`), restart the clock.

The drill runs at T+24 h rather than after T+72 h deliberately: if
the on-disk contract is broken, know it on day 2, not day 4.

**Exit D:** 4/4 matrix runs clean, zero restarts, alerts quiet for
72 h, rollback drill round-trips with byte-level verification.

---

## E — Flip the default

One values-line change plus its documentation shadow, shipped as
`v0.2.1-preview`:

- `deploy/helm/skafka/values.yaml`: `image.flavor: rust`.
- Chart `README.md` + `NOTES.txt`: Rust is the default; `flavor: go`
  documented as the deprecated escape hatch with its removal
  condition (two clean releases) stated.
- `RELEASING.md`: the "releases ship the Go broker from `archive/`"
  status paragraph inverts; patch line continues on `v0.2.x`.
- `CLAUDE.md` status header: default flipped, `archive/` now
  deprecated-pending-deletion rather than "frozen reference".
- Release notes: the flip announcement promised in C.2, the
  repository-pinner call-out repeated, and the Go-image end-of-life
  restated ("two clean releases from now").

Immediately after tagging, upgrade the bench cluster to chart
`0.2.1-preview` **with the flavor line removed from its values** —
from here on it runs the true default path every user gets.

**Exit E:** `v0.2.1-preview` published; a bare
`helm install skafka oci://…/charts/skafka` deploys the Rust pair;
bench cluster runs the default path; deprecation messaging live in
all four places listed.

---

## F — Decommission

Armed after **two clean Rust-default releases** (`v0.2.1-preview` and
`v0.2.2-preview`, or later if either needed a broker/operator hotfix).
"Clean" = release cut, post-release `skafka-scripts` run matches the
baseline, no regression hotfix required against that release's broker
or operator within its window as the deployed release.

### F.1 — Delete the Go tree

- `git rm -r archive/` — the tree stays fully recoverable at any
  `v0.1.x` tag; nothing is squashed or rewritten.
- `.github/workflows/ci.yml`: remove the `legacy-go` and `docker-go`
  jobs. Before removing `legacy-go`, confirm `xtask check-crd-drift`
  is the live CRD-drift gate (it replaced `controller-gen` in Phase
  7) — the drift check must not silently disappear with the job.
- `.github/workflows/docker-publish.yml`: remove the two Go
  build-push steps and the Phase 8 comment scaffolding.

### F.2 — Image naming end-state

With Go gone, the Rust images take over the **plain names** —
`skafka[-preview]`, `skafka-operator[-preview]` — rather than
carrying a `-rs` suffix forever:

- `docker-publish.yml` publishes each Rust image under both the plain
  and `-rs` names for **one transition release**, then drops the
  `-rs` aliases.
- The chart helper loses the flavor knob in the same release: the
  `image.flavor` value is removed from `values.yaml`, and the helper
  `fail`s with a migration message if a values file still sets it
  (silently ignoring a knob that used to select the *other binary*
  is worse than a loud template error).
- `RELEASING.md`'s "What gets published" section returns to exactly
  its pre-rewrite shape — plain names, one flavor.

### F.3 — Docs rewrite

The repo's documentation currently narrates the rewrite from the
middle of it. F lands the after-state:

- **`CLAUDE.md`** — the big one: drop the "Status: mid-rewrite"
  header, the `archive/` path-prefix convention, the entire "Go
  (inside `archive/`)" commands section, and the "scaffolded"
  language in the crate map (the map itself stays as the
  architecture index, now pointing only at `crates/`). The
  architecture/behaviour sections remain the spec — they were always
  language-neutral.
- **`rewrite.md`** — "Current state" section updated to "complete";
  the doc becomes historical record (link it from `phase-9.md`
  rather than deleting).
- **`RELEASING.md`** — final single-flavor shape; version-history
  table gains the `v0.2.0-preview` / flip / deletion milestones.
- **Issue hygiene** — close [gh #152] and the umbrella [gh #143];
  sweep the phase issues #144–#151, which are all still open despite
  their phases having shipped (close each with a pointer to its
  landing commits); re-home any still-live follow-ups (kafka-compat
  port, gh #114, per-handler spans, admin-surface gaps A.2
  documented) as standalone issues so nothing lives only in a closed
  phase issue.

**Exit F:** `archive/` gone; CI is `rust` + `docker-rust` + `helm`;
a tag push publishes plain-named Rust images + chart; docs read as a
Rust project; tracker closed.

---

## Release choreography

| Tag | Carries | Chart default | Go images |
|-----|---------|---------------|-----------|
| `v0.2.0-preview` | flavor knob (B), parity closure (A) | `go` | published |
| — 72 h bake + rollback drill on this build (D) — | | | |
| `v0.2.1-preview` | default flip (E) | `rust` | published, deprecated |
| `v0.2.2-preview` | normal development | `rust` | published, deprecated |
| — two-clean-release gate satisfied — | | | |
| `v0.2.3-preview` | decommission (F): archive deleted, plain names (+`-rs` aliases) | `rust` (knob removed) | **gone** |
| `v0.2.4-preview` | `-rs` aliases dropped | — | — |

(Ordinals shift right if any release needs a hotfix; the gates are
event-based, not tag-number-based.)

---

## Phase 9 exit criteria (all must hold)

1. `scripts/.parity-baseline.txt` re-captured at ≥ `v0.1.190` with
   multi-broker enabled: zero FAIL rows without a Go-control-run
   citation; the Go control result set committed alongside.
2. `bench-compare` ratio gate closed with committed evidence: 5-run
   Go reference + 5-run Rust capture, both post-2026-07-12 (NAS cable
   fix), every ratio axis within ±5 %.
3. `v0.2.0-preview` published all five artifacts; `helm template`
   CI covers both flavor branches; explicit-repository values render
   unchanged.
4. 72 h bake: 4/4 matrix runs clean against the refreshed baseline,
   zero pod restarts, all 9 alerts quiet throughout.
5. Rollback drill: rust→go→rust on live data with committed-offset,
   transactional-epoch, and cross-flavor-read verification — no data
   loss, no client-visible errors beyond the rebalance blips of a
   normal rollout.
6. Default flipped in `v0.2.1-preview` with the deprecation messaging
   in chart README, NOTES.txt, RELEASING.md, and release notes.
7. After two clean Rust-default releases: `archive/` deleted, CI and
   publish workflow Rust-only, plain image names taken over, flavor
   knob removed with a loud failure path, docs rewritten (CLAUDE.md,
   RELEASING.md, rewrite.md), #143/#152 closed and #144–#151 swept.

---

## Risks & mitigations

- **The kafka-acls FAIL is a real Rust-operator reconcile gap.**
  Then existing `KafkaUser` ACLs silently stop being enforced on
  flip — a security regression, the worst kind of silent. A.2 makes
  this determination *first*, before any tag is cut; if real, the fix
  is bounded (the `kafkauser.rs` reconciler materialising
  `acls.json`) and lands before C.
- **Rollback drill finds an on-disk format divergence** (serde
  field-name mismatch, snapshot the other flavor can't parse). This
  is exactly what the drill exists to find, at T+24 h with production
  still on Go-by-default. Mitigation beyond the drill: diff
  Go-written vs Rust-written instances of each JSON file
  (`manifest.json`, `producer-state.snapshot`, `txn_state/slot-*.json`,
  offsets) field-by-field as part of drill verification, not just
  behavioural checks.
- **Bench gate unmeasurable from environment noise.** The 5-run
  protocol + NAS-liveness probe + post-cable-fix captures are the
  mitigation; if ratios still swing > 5 % between identical runs,
  widen the sample per the methodology memory rather than shipping on
  a lucky run — and remember the node is still on WiFi
  (`enp129s0` down), so schedule captures off peak.
- **Repository pinners miss the flip.** They keep running Go silently
  until F deletes the images — then their pulls break at the *next*
  release, not at flip time. Mitigated by announcing in C.2 and E,
  and by the only known pinner (k3s-cluster) being moved onto the
  knob in C.3. GHCR keeps the last-published Go tags pullable
  indefinitely regardless.
- **Mid-bake hotfix churn resets the clock repeatedly.** If the clock
  restarts twice, stop treating it as bake noise: the flip criteria
  aren't met, and the honest move is to hold the `v0.2.1` flip until
  a build survives untouched — the choreography table shifts right by
  design.
- **`v0.2.0` minor bump surprises the tag-driven tooling.** The
  publish workflow keys only on `v*` and the `-` prerelease test, and
  the chart version is derived by stripping `v` — no ordinal
  assumptions anywhere. Verified in C.1's artifact check; the
  "never re-cut a tag" rule is unaffected.

---

## What this closes

Phase 9 ends the rewrite: [gh #143]'s checklist completes, the
`skafka-migration-parity` project stops being a migration matrix and
becomes the plain Kafka-parity tracker, and the repo's development
surface is a single-language Rust workspace. The post-cutover
follow-up queue (re-homed as standalone issues in F.3) inherits:
the full kafka-compat rdkafka port, cross-broker `WriteTxnMarkers`
(gh #114), per-handler tracing spans, the admin surfaces documented
as Go-parity skips in A.2, and the architectural
Strimzi-gap conversation that a memory-safe, GC-free broker is now
finally positioned to have.
