# Phase 6 Operator — Breakdown vs. Current Repo

Comparison of `skafka-plan-v3.md` (v3.3) §"Phase 6: Operator (Week 7–8)" (lines 1070–1075), §"What about the KafkaClusterAssignments CR?" (lines 378–395), and Phase 1 open question #6 (line 1461) against the state of `main` at commit `f6db6ec`.

Legend: ✅ done · 🟡 partial / minor deviation · ❌ not yet · ➕ extra (not in plan)

---

## What Phase 6 actually has to deliver

The plan's Phase 6 body fits in two sentences:

> controller-runtime / kubebuilder scaffold; KafkaTopic, KafkaUser, KafkaUserGroup, KafkaAcl, KafkaCluster controllers; **operator does NOT participate in heartbeat or assignment.**

That last clause is the whole architectural constraint: the v3 separation of concerns has the operator handling **declarative shape** (CRDs → desired state → cluster topology + files on the shared PVC), and the **elected cluster controller** (a broker holding the singleton Lease — see Phase 4) handling **runtime assignment** (who leads which partition right now). The operator never touches `assignment.json`, never elects, never heartbeats.

Combined with §"What about the KafkaClusterAssignments CR?" the operator-facing v3 deliverable is narrower than the rest of Phase 6 looks:

| v3 deliverable | Owner |
|---|---|
| **A.** `KafkaTopic`/`KafkaUser`/`KafkaUserGroup`/`KafkaAcl`/`KafkaCluster` reconcilers (declarative shape → PVC files + cluster topology) | operator |
| **B.** `KafkaClusterAssignments` CR mirror — best-effort, fire-and-forget, written by the controller after each `assignment.json` write | controller (lives in `internal/controller/`, not `operator/`) |
| **C.** Truncate the CR to the most-recently-changed partitions when the assignment exceeds Kubernetes' 1MB object size limit; mark `status.truncated: true` | controller |
| **D.** Operator does NOT watch `assignment.json` or write/read it | enforced by absence |

A and D are already true. B and C are the work.

---

## What already exists

| Item | Where | State |
|---|---|---|
| `KafkaCluster` reconciler — deploys StatefulSet, services, certs | `operator/controllers/kafkacluster_controller.go` | ✅ v2.6 |
| `KafkaTopic` reconciler — creates partition dirs on shared PVC | `operator/controllers/kafkatopic_controller.go` | ✅ v2.6 |
| `KafkaUser` reconciler — manages SCRAM credentials | `operator/controllers/kafkauser_controller.go` | ✅ v2.6 |
| `KafkaUserGroup` reconciler | `operator/controllers/kafkausergroup_controller.go` | ✅ v2.6 |
| `KafkaAcl` reconciler — writes ACLs JSON | `operator/controllers/kafkaacl_controller.go` | ✅ v2.6 |
| `KafkaClusterAssignments` CRD + types + deepcopy | `operator/api/v1alpha1/kafkaclusterassignments_types.go` | ✅ Phase 1 |
| Rendered CRD YAML | `deploy/crds/skafka.io_kafkaclusterassignments.yaml` | ✅ |
| RBAC: operator can `create`/`delete` `kafkaclusterassignments` | `deploy/rbac/operator-clusterrole.yaml:13-17` | ✅ |
| RBAC: controller can `get/list/watch/update` and `update/patch` `kafkaclusterassignments/status` | `deploy/rbac/broker-clusterrole.yaml:23-32` | ✅ Phase 1 |
| `controller.CRMirror` interface | `internal/controller/assignment.go:34-46` | ✅ Phase 1 (interface only) |
| `controller.NewNoopMirror()` placeholder | `internal/controller/assignment.go:53-55` | ✅ wired into the AssignmentLoop today |
| Operator `cmd/skafka-operator/main.go` registers all reconcilers + uses controller-runtime manager | `cmd/skafka-operator/main.go` | ✅ v2.6 |
| Existing reconciler tests | `operator/controllers/*_test.go` | ✅ v2.6 |

The five reconcilers are real, tested, and have been running in v2.6 deployments. The `KafkaClusterAssignments` CRD ships in Phase 1. Today's controller wires `NewNoopMirror()` so the CR never actually gets updated — that's the gap.

---

## What Phase 6 must add

### B. Real `KafkaClusterAssignments` CR mirror

New: `internal/controller/k8s_mirror.go` (or extend `internal/controller/mirror.go`).

```go
type K8sMirror struct {
    client    client.Client      // controller-runtime typed client
    namespace string
    name      string             // matches the cluster name; one CR per cluster
    maxBytes  int                // truncation threshold (default ~900KB to stay under 1MB)
}

func NewK8sMirror(c client.Client, namespace, name string) *K8sMirror

func (m *K8sMirror) Mirror(ctx context.Context, a *kafkaapi.Assignment) {
    // 1. Build the v1alpha1.KafkaClusterAssignmentsStatus from a.
    // 2. Truncate partitions[] when it would push the CR over 1MB.
    //    Sort by "recently changed" — partitions whose epoch was bumped
    //    in this version come first, then by topic+partition for stable order.
    // 3. Get-or-Create the CR; set ownerReference to KafkaCluster if discoverable.
    // 4. Update Status. Failures: logged, ignored — the file is the
    //    source of truth, the CR is debugging convenience.
}
```

Plan §"What about the KafkaClusterAssignments CR?" specifies:
- Single CR per cluster, sharing the KafkaCluster's name and namespace.
- Spec is empty; everything is in Status.
- Brokers do NOT watch this CR.
- Best-effort: failures are logged and ignored.
- 1MB Kubernetes object size cap → truncation with `status.truncated: true`.

### C. Truncation logic

Concretely: Kubernetes etcd has a hard 1MB cap on object size (`max-request-bytes` default 1.5MB but kube-apiserver enforces 1MB on writes). One PartitionAssignment serialised as the JSON shape used here is roughly 100 bytes. So 8000–10000 partitions is the rough limit before truncation kicks in.

Truncation strategy:
1. Estimate size by marshalling the full Status to JSON and measuring.
2. If > threshold (e.g. 900KB to leave headroom): keep the most-recently-changed N partitions. "Recently changed" = epoch differs from the previously-mirrored CR version, or absent from the previous version. Stable tiebreaker = `(topic, partition)` lexicographic.
3. Set `status.truncated = true`.

The "previously-mirrored CR version" needs persistence somewhere. Two cheap options:
- Read the existing CR before writing — the previous Status is right there.
- In-memory `lastMirroredVersion` map on the K8sMirror struct.

Either works. Reading the CR is more robust across controller restarts but doubles the API call count.

### D. Wire the real mirror into the cluster runtime

`cmd/skafka/cluster_runtime.go` currently passes `controller.NewNoopMirror()` into the AssignmentLoop. Replace with `controller.NewK8sMirror(client, namespace, clusterName)` when running in k8s mode. Plumb the controller-runtime / typed client + the cluster name from main.go.

### E. Operator startup creates an empty `KafkaClusterAssignments` CR

Plan §"What about the KafkaClusterAssignments CR?" line 388: the CR exists per cluster. The operator's `KafkaCluster` reconciler should ensure the matching `KafkaClusterAssignments` CR exists (status-only, all fields zero-valued) so:
- `kubectl get kafkaclusterassignments` works immediately, even before the controller has elected.
- Ownership is established (`ownerReferences` → KafkaCluster) so the CR is GC'd when the cluster is deleted.

This is a small extension to `kafkacluster_controller.go`'s reconcile path — analogous to how it currently creates services + certs.

### F. Tests

| Package | Target |
|---|---|
| `internal/controller/` | TestK8sMirrorWritesStatus: in-memory client → Mirror → assert Status fields match input. |
| `internal/controller/` | TestK8sMirrorTruncates: build an Assignment with > N partitions; expect truncated set + truncated=true flag. |
| `internal/controller/` | TestK8sMirrorMissingCR: CR doesn't exist (operator hasn't created it yet) → Mirror creates it (or logs+skips, depending on design). |
| `operator/controllers/` | TestKafkaClusterCreatesAssignmentsCR: Reconciling a fresh KafkaCluster creates the matching empty KafkaClusterAssignments CR. |

---

## Suggested implementation order

1. **K8sMirror skeleton** — implements `controller.CRMirror` over a controller-runtime `client.Client`. Get-or-Create, plain Status write, no truncation yet. ~1.5h.
2. **Truncation** — add the size-aware partition selection. ~1h.
3. **Wire into cluster_runtime.go** — replace `NewNoopMirror()` with `NewK8sMirror(...)` in k8s mode. Plumb the typed client + cluster name from main.go. ~0.5h.
4. **Operator extension** — `KafkaCluster` reconciler creates the empty `KafkaClusterAssignments` CR with ownerReference. ~0.5h.
5. **Tests** — covering the four scenarios in section F. ~2h.

Total: **5-6 hours of focused work**, fits in 1-2 sessions. Steps 1, 2 are pure-Go testable in isolation; step 4 reuses the operator's existing test harness.

---

## Items deliberately NOT in Phase 6

- **Operator participating in election or heartbeat** — explicitly excluded by the plan.
- **Operator watching `assignment.json`** — that file is broker-side; the operator doesn't read it.
- **Operator computing assignments** — that's the elected broker's job.
- **Adding new CRDs** — all six (KafkaCluster, KafkaTopic, KafkaUser, KafkaUserGroup, KafkaAcl, KafkaClusterAssignments) already exist.
- **Operator-driven topic creation** — the existing reconciler already creates partition dirs; the cluster controller picks them up via the broker's TopicRegistry → TopicSource adapter wired in cmd/skafka/cluster_runtime.go.
- **`__consumer_offsets` topic creation by operator** — Phase 5 open question #5 keeps v1 on JSON-per-group offsets; the operator doesn't need to ensure the offset topic exists.

---

## Open questions for Phase 6 implementation

- **Cluster name discovery** — the controller writes the CR with a `name` matching the KafkaCluster CR's name. How does the controller (an in-pod broker) know what its cluster name is? Options: env var injected by the StatefulSet (cleanest), label on the pod, or read from the operator-managed ConfigMap. Probably an env var from the Helm chart.
- **Truncation N (max partitions in the CR)** — pick a constant or compute dynamically? A fixed budget like "max 8000 partitions in the CR" is simpler; computing dynamically based on per-partition bytes is more robust to format changes. Start fixed.
- **CR Update vs. Patch** — Update is simpler (controller has the full Status). Patch is more conflict-tolerant if the operator ever races on it (today the operator only Creates the CR, doesn't write Status, so no race).
- **OwnerReference target** — the controller is in a broker pod; the broker can derive the KafkaCluster name from an env var. Setting `ownerReferences[0].kind=KafkaCluster, name=<envvar>` makes deletion cascade cleanly.

---

## Summary

Phase 6 is **almost done** because v2.6's operator already covers four-fifths of the plan: every reconciler (KafkaCluster, KafkaTopic, KafkaUser, KafkaUserGroup, KafkaAcl) exists, with tests, and runs against the shared PVC. The v3-specific deliverable is the `KafkaClusterAssignments` CR mirror — the controller's fire-and-forget write of the assignment summary for `kubectl get`-style debugging — and the operator-side bootstrap of the empty CR.

Five focused work items (~5-6h total). No architectural surprises — just plumbing the existing pieces (controller's `NewNoopMirror` → `NewK8sMirror`, KafkaCluster reconciler → also creates the assignments CR) into a coherent end-to-end flow.

After Phase 6, `kubectl get kafkaclusterassignments cluster-name -o yaml` shows the cluster's current partition assignment, refreshed within ~1s of every assignment change. That's the v3 architecture's main observability surface for cluster operators — without it, debugging a misbehaving cluster requires `kubectl exec` into a pod to read `/data/__cluster/assignment.json`.
