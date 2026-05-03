# Phase 8 Kubernetes Deployment — Breakdown vs. Current Repo

Comparison of `skafka-plan-v3.md` (v3.3) §"Phase 8: Kubernetes Deployment (Week 8–9)" (lines 1085–1091) against the state of `main` at commit `2023716`.

Legend: ✅ done · 🟡 partial / minor deviation · ❌ not yet · ➕ extra (not in plan)

---

## What Phase 8 actually has to deliver

The plan's Phase 8 body is one paragraph:

> Helm values include controller, partition, and storage tuning. NOTES.txt prints failover characteristics and storage requirements. Tier 1 RWX providers: csi-driver-nfs, AWS EFS, Azure Files Premium NFS, Azure NetApp Files, GCP Filestore, Longhorn-NFS, Rook-Ceph CephFS.

Three concrete deliverables:

| Item | Status |
|---|---|
| **A.** Helm values for controller / partition / storage tuning | 🟡 (storage ✅, controller ❌, partition ❌ — dead config) |
| **B.** NOTES.txt prints failover characteristics + storage requirements | ❌ (prints bootstrap servers + sample KafkaTopic, not the plan items) |
| **C.** Tier 1 RWX providers documented | ✅ (rewritten in README during Phase 7 follow-up) |

Plus a load-bearing real bug: the v3 `KafkaClusterAssignments` CRD is missing from the chart's `crds/` directory, so a fresh Helm install today doesn't ship the CRD that Phase 6 just wired up.

---

## What already exists

### Helm chart structure

| Resource | Where | Status |
|---|---|---|
| Broker StatefulSet | `templates/broker-statefulset.yaml` | ✅ — 3 replicas default, parallel pod management, readinessGate, init container, downward API, all v3 env vars |
| Headless service (`skafka-headless`) — per-pod DNS | `templates/broker-service.yaml` | ✅ — kafka, heartbeat, metrics ports |
| ClusterIP service (`skafka`) — load-balanced bootstrap | `templates/broker-service.yaml` | ✅ |
| Per-broker LoadBalancer Services for SNI passthrough | `templates/listener-services.yaml` | ✅ — Phase 9 substrate |
| TLSRoutes (Gateway API) | `templates/listener-tlsroutes.yaml` | ✅ — Phase 9 substrate |
| cert-manager Certificate for external listener | `templates/listener-certificate.yaml` | ✅ |
| PVC with `helm.sh/resource-policy: keep` | `templates/broker-pvc.yaml` | ✅ |
| PodDisruptionBudget | `templates/broker-pdb.yaml` | ✅ — defaults `maxUnavailable: 1` |
| Operator Deployment with shared PVC mount | `templates/operator-deployment.yaml` | ✅ — Phase 1 question #6 ("operator pod mounts the same shared PVC") |
| Broker + Operator RBAC | `templates/broker-rbac.yaml`, `templates/operator-rbac.yaml` | ✅ |
| ServiceMonitor (Prometheus Operator) | `templates/servicemonitor.yaml` | ✅ — opt-in |
| NOTES.txt | `templates/NOTES.txt` | 🟡 — prints bootstrap servers + sample KafkaTopic, but not failover/storage info |

### values.yaml surface

| Section | Coverage |
|---|---|
| `image`, `operator.image` — repo + tag + pullPolicy | ✅ |
| `broker.replicaCount`, `clusterID`, `ports.{kafka,tls,health,heartbeat}`, `resources` | ✅ — heartbeat port added in Phase 4+5 main.go wiring |
| `broker.config.{segmentBytes, retentionHours, numPartitions, rebalanceTimeoutMs}` | 🟡 — values exist in YAML, NOT plumbed to any env var → dead config |
| `broker.lease.{durationSeconds, renewDeadlineSeconds, retryPeriodSeconds}` | 🟡 — same: not plumbed; meant for v2.6 per-partition Lease, now obsolete since Phase 4 step 4 removed `CoordinatorLeaseManager` |
| `broker.readinessGate.enabled` | ✅ — wired |
| `storage.{className, size, accessMode, mountPath}` | ✅ — wired |
| `auth.{enabled, mechanisms, requireSasl, tls.*}` | ✅ — wired (Phase 7 follow-up) |
| `podDisruptionBudget.{enabled, maxUnavailable}` | ✅ |
| `serviceAccount.broker.*`, `operator.*` | ✅ |
| `autoscaling.*` (HPA stub) | 🟡 — declared but no HPA template yet |
| `observability.metrics.{enabled, port, serviceMonitor}` | ✅ |
| `observability.otlp.{enabled, endpoint, traces.samplerRatio}` | ✅ |
| `observability.logs.{level, format}` | ✅ |
| `listeners.internal.port` | ✅ |
| `listeners.external.{enabled, port, hostnamePattern, bootstrapHostname, tls.certManager.*, gateway.*}` | ✅ — Phase 9 surface area |

### CRDs

| CRD | In `deploy/helm/skafka/crds/` | In `deploy/crds/` (controller-gen) |
|---|---|---|
| KafkaCluster | ✅ | ✅ |
| KafkaTopic | ✅ | ✅ |
| KafkaUser | ✅ | ✅ |
| KafkaUserGroup | ✅ | ✅ |
| KafkaAcl | ✅ | ✅ |
| **KafkaClusterAssignments** | ❌ — missing | ✅ |

Helm installs only what's in `deploy/helm/skafka/crds/`. A fresh Helm install today does NOT install the `KafkaClusterAssignments` CRD that the Phase 6 K8sMirror writes to. The operator-side `reconcileAssignmentsCR` Create call would fail; the K8sMirror's Get would always return NotFound. **Hard blocker for Phase 6's CR mirror to work in a freshly installed cluster.**

---

## What Phase 8 must add

### A. Ship the `KafkaClusterAssignments` CRD in the Helm chart

```bash
cp deploy/crds/skafka.io_kafkaclusterassignments.yaml deploy/helm/skafka/crds/
```

…and verify CI's `make manifests` keeps the two copies in sync (currently the chart's `crds/` is hand-mirrored — a `cp` step in the Makefile is the cheapest way to prevent drift).

### B. Drop dead `broker.config.*` and `broker.lease.*` values

`broker.config.{segmentBytes, retentionHours, numPartitions, rebalanceTimeoutMs}` and `broker.lease.{durationSeconds, renewDeadlineSeconds, retryPeriodSeconds}` are documented in `values.yaml` but never reach the running broker. Either:

- **Drop them** if nobody depends on them (cleanest — dead config is worse than no config because it implies functionality that doesn't exist), OR
- **Plumb them** through env vars to actually configure the storage engine + the v3 controller Lease.

The plan says "Helm values include controller … tuning", so plumbing the controller knobs is on-spec. Specifically:

- `broker.controllerLease.durationSeconds → SKAFKA_CONTROLLER_LEASE_DURATION_SECONDS` → wires through to `cluster_runtime.go` → `controller.New(...).WithTimings(...)`.
- The legacy `broker.lease.*` block is renamed to `broker.controllerLease.*` so operators don't see two confusingly-similar names.
- `broker.config.segmentBytes` and `broker.config.retentionHours` plumb to `storage.Config` via `SKAFKA_SEGMENT_BYTES` / `SKAFKA_RETENTION_MS`. Currently `internal/storage/engine.go::DefaultConfig` returns hardcoded values; needs an env-aware constructor.
- `broker.config.numPartitions` is a default partition count for auto-created topics — that's a topic-creation concern, not broker config; better to drop.
- `broker.config.rebalanceTimeoutMs` is a coordinator/group setting — same: better to drop.

### C. NOTES.txt prints failover characteristics + storage requirements

Plan-shaped block to add at the bottom of `templates/NOTES.txt`:

```
Storage requirements:
  ReadWriteMany volume with NFSv4-class semantics:
    - same-directory rename atomicity
    - fsync durability
    - close-to-open consistency
  Recommended NFS mount options:
    nconnect=4-8 (parallel TCP connections — faster fsync)
    acregmax=1   (sub-second freshness on assignment.json polling)

Failover characteristics:
  Graceful broker shutdown:    ~150-400ms producer recovery
  Hard broker kill (SIGKILL):  ~4-5s recovery (CRC-truncate boundary)
  Hard controller failure:     <15s assignment refresh (Lease + push + poll)
  Storage backend total loss:  fatal — no replication; restore from backup
```

This is one templated block; under 30 lines.

### D. Surface the Phase 7 mTLS client-CA env var as a value

Phase 7 added `SKAFKA_TLS_CLIENT_CA_FILE` for opt-in client-cert enforcement. Helm chart should support it via `auth.tls.clientCAExistingSecret` or similar — load a CA bundle from a Kubernetes Secret, mount into the pod, set the env var.

### E. NFS-specific tuning knobs (optional)

For deployments on `csi-driver-nfs`, the Helm chart could let operators pass `mountOptions: ["nconnect=8", "acregmax=1"]` to the StorageClass-derived PVC. But: mountOptions live on the StorageClass itself, not the PVC, so this is more documentation than configuration. Mention in README under "csi-driver-nfs" section that the operator should configure `mountOptions` on the StorageClass.

---

## Suggested implementation order

1. **Ship the missing CRD** — `cp` + add a `make` target. ~15 min.
2. **Drop dead values** — remove `broker.config.*` and `broker.lease.*` from values.yaml, README. ~15 min.
3. **Plumb controller tuning** — new `broker.controllerLease.*` block, env vars in StatefulSet, `cluster_runtime.go` reads them. ~1.5h.
4. **NOTES.txt update** — append failover + storage block. ~30 min.
5. **mTLS client-CA Secret mount** — values knob, statefulset volume mount + env var. ~1h.
6. **Helm chart README pointer** to NFS mountOptions configuration. ~15 min.

Total: **3-4 hours of focused work**. All-mechanical; no architectural decisions.

---

## Items deliberately NOT in Phase 8

- **HPA implementation** — `autoscaling.*` values exist but no HPA template; Phase 8 plan doesn't mention HPA. Leave as future work.
- **Multi-cluster federation** — out of v1.
- **Operator HA** — operator deployment is `replicas: 1`. Multi-replica operator (lease-elected) is future.
- **In-tree CSI driver** — outside the project's scope; rely on the operator's StorageClass.

---

## Open questions for Phase 8 implementation

- **CRD lifecycle on Helm uninstall** — Helm doesn't delete CRDs on uninstall by default. Good for skafka because losing the CRDs would orphan KafkaCluster objects, but worth a one-line note in NOTES.txt or README.
- **`make manifests` keeping crds/ in sync** — the Makefile currently rebuilds `deploy/crds/` from operator/api annotations. The Helm chart's `crds/` directory is a hand-mirrored copy. Consider:
  - A `make helm-crds` target that copies `deploy/crds/*.yaml → deploy/helm/skafka/crds/`,
  - Or drop the chart-side `crds/` and have the chart reference `deploy/crds/` directly via Helm's CRD-from-template pattern (works but loses the chart-is-self-contained property).
- **values.yaml.template vs canonical values.yaml** — none of the existing chart templates use the dead `broker.config.*` block, so removing it is safe. Need to scan README for stale references.
- **Controller-only mode in v4** — Phase 1 question #11 reserves "dedicated controller mode" for v4. Today every broker can win the cluster Lease. The Helm chart shouldn't anticipate v4 (StatefulSet for both today is correct).

---

## Summary

Phase 8 is **mostly already done** — the Helm chart, RBAC, services, PVC, operator deployment, listeners, NOTES.txt, and CRDs (5 of 6) all exist from v2.6 + Phase 4/5/6 follow-ups. The plan's three explicit Phase 8 items map to:

- **(A) Tuning surface** — storage tuning ✅; partition + controller tuning are dead config. Plumb them through (or drop the dead ones).
- **(B) NOTES.txt failover/storage info** — currently absent; needs a one-paragraph addition.
- **(C) Tier 1 RWX providers** — already documented in chart README (Phase 7 follow-up rewrite).

Plus one real bug — **the `KafkaClusterAssignments` CRD is missing from the chart's `crds/`** — that quietly breaks Phase 6 in fresh deployments. That single `cp` is the most important Phase 8 work item; everything else is polish.

After Phase 8, the chart is "fresh-install ready" — you can `helm install` against an empty cluster (with a working RWX StorageClass) and end up with all six CRDs, the operator running, three brokers running, the controller elected, and `kubectl get kafkaclusterassignments` showing live state. That's the v1 production smoke test in one command.
