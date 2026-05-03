# Phase 4 Cluster Controller and Broker Coordinator — Breakdown vs. Current Repo

Comparison of `skafka-plan-v3.md` (v3.3) §"Cluster Coordination via Controller" (lines 131–161) and the v3.2-pseudocode reference at §"Phase 4: Cluster Controller and Broker Coordinator" (line 1051) against the state of `main` at commit `ac73ba1`.

Legend: ✅ done · 🟡 partial / minor deviation · ❌ not yet · ➕ extra (not in plan)

---

## What Phase 4 actually has to deliver

The architectural shift Phase 4 implements:

| From (v2.6, current `main`) | To (v3 target) |
|---|---|
| One Kubernetes Lease per partition (`skafka-{topic}-{partition}`) | One Lease for the whole cluster (`skafka-controller`) |
| Per-partition leader election via `k8s.io/client-go/tools/leaderelection` | Cluster-wide controller election; controller assigns partitions |
| Filesystem `flock` per partition (`internal/lock/`) | No flock; epoch-tagged segments are the safety boundary |
| Append checks `LeaseManager.IsLeader` + `flock` | Append checks `BrokerCoordinator.Owns` + heartbeat freshness + per-batch epoch fence |
| No central authoritative cluster state | `/data/__cluster/assignment.json`, atomically replaced, fenced by `leaseTransitions` |
| No broker-to-controller liveness signal | Bidi gRPC heartbeat (`BrokerStatus` ⇄ `ControllerCommand`) with `ASSIGNMENT_CHANGED` push |

Once Phase 4 ships, the v3 architecture is actually running. Until then, skafka is v2.6 with v3 plumbing in the data plane.

---

## What already exists (from Phase 1)

| Item | Where |
|---|---|
| `proto/heartbeat.proto` schema (BrokerStatus / ControllerCommand / ASSIGNMENT_CHANGED) | ✅ `proto/heartbeat.proto` |
| `pkg/heartbeatpb/doc.go` placeholder | ✅ — generated stubs not yet produced |
| `pkg/kafkaapi.Assignment` types (Assignment, BrokerAssignment, PartitionAssignment, ConsumerGroupAssignment) | ✅ |
| `pkg/kafkaapi.AssignmentStore` interface (Read, Write, Watch) | ✅ |
| `pkg/kafkaapi.Controller` interface (Start(epoch), Stop, UpdateAssignment, BrokerHealth) | ✅ |
| `pkg/kafkaapi.BrokerCoordinator` interface (Start, Stop, Owns, CurrentEpoch, OnAssignmentChange, LastHeartbeat) | ✅ |
| `pkg/kafkaapi.AssignmentChange{Reason, BrokerID, Topic}` + reasons | ✅ |
| `KafkaClusterAssignments` CRD (debug mirror) | ✅ |
| `tests/controller-failover/placeholder_test.go` (`t.Skip` stub) | ✅ |
| `tests/stale-controller-race/placeholder_test.go` (`t.Skip` stub) | ✅ |
| RBAC: `kafkaclusterassignments` + Lease `get/list/watch/create/update` | ✅ |
| `Makefile proto` target driven by `buf` | ✅ schema-ready, stubs not generated |
| Existing `leaderelection` wiring (per-partition) | 🟡 will become a reference for the new singleton path; `internal/lease/k8s_manager.go` |
| `internal/lock/` (flock-based partition lock) | ❌ to be deleted |
| `internal/lease/` per-partition LeaseManager | 🟡 stays (consumer-group coordinator leases keep using it for now); per-partition usage shrinks to nothing |

---

## What Phase 4 must add

### A. gRPC stub generation

`buf generate` must run and the generated `pkg/heartbeatpb/{heartbeat.pb.go, heartbeat_grpc.pb.go}` must be checked in (or generated in CI before build). Without this, every Phase 4 component is uncompilable.

### B. File-backed `AssignmentStore`

New: `internal/controller/assignment_store.go` (or `internal/storage/assignment_store.go`).

| Plan element | Notes |
|---|---|
| Read | `ReadFile("/data/__cluster/assignment.json")` + `json.Unmarshal` |
| Write | tmp + `f.Sync()` + `os.Rename` (NFSv4 atomicity); also clean up orphan `assignment.json.tmp` on Read or controller startup |
| Watch | `fsnotify` with 1s polling fallback (same pattern as Phase 3 §"inotify on config files"); fires on every change observed by either path |

Single-writer (the controller). Many readers (every broker).

### C. Controller (active only on the broker holding the Lease)

New: `internal/controller/`:

| File | Responsibility |
|---|---|
| `controller.go` | `Controller` lifecycle: Start(ctx, epoch) on lease acquisition, Stop on loss; serializes UpdateAssignment requests via a coalescing channel |
| `election.go` | Wraps `client-go/tools/leaderelection` for the **single** `skafka-controller` Lease. Reports `leaseTransitions` on every transition — that's the controller epoch |
| `assignment.go` | Owns the in-memory cluster state; drives recompute + write through AssignmentStore on broker churn / topic events |
| `balancer.go` | v1 placement: rendezvous-hash new partitions; reassign only when broker dies. No periodic smoothing in v1 (plan §Phase 1 open question #4) |
| `heartbeat_server.go` | gRPC server. Tracks each broker's `lastSeen` + `lastSeenAssignmentVersion`; pushes `PING`/`LEAVING`/`ASSIGNMENT_CHANGED` on the downstream stream |
| `mirror.go` | Fire-and-forget update of the `KafkaClusterAssignments` CR after each authoritative file write. CR write failures are logged and ignored |

### D. BrokerCoordinator (active on every broker)

New: `internal/broker/`:

| File | Responsibility |
|---|---|
| `controller_watch.go` | Lease informer for the `skafka-controller` Lease — provides current `leaseTransitions` to the epoch-fence check |
| `assignment_watch.go` | Reads `assignment.json`, validates `controllerEpoch >= leaseEpoch`, diffs against `lastAppliedVersion`, fires registered handlers |
| `assignment_poll.go` | 1s `os.Stat` mtime poll + 30s full-read fallback (NFS attribute-cache + mtime-resolution defense — plan §"The polling safety net") |
| `heartbeat_client.go` | Long-lived bidi gRPC stream to the controller. On disconnect: reconnect with backoff, re-discover controller via Lease informer |
| `self_fence.go` | Atomic `lastHeartbeat` timestamp; `IsHeartbeatFresh()` returns false when stale > heartbeatTimeout (default 3s) |
| `takeover.go` | On assignment change: for partitions newly owned by this broker, call `storage.TakeOver(epoch)`; for partitions lost, call `storage.Relinquish` |

### E. Wire BrokerCoordinator into the produce hot path

`internal/protocol/handlers/produce.go` currently checks `h.leases.IsLeader` + `h.locks.IsLocked` (v2.6 model, Phase 1 deferred-stub). The plan's pseudocode (§Phase 2 Produce handler, line 770–780) wants:

```go
if !coordinator.Owns(topic, partition)        { ... NOT_LEADER }
if !coordinator.IsHeartbeatFresh()            { ... NOT_LEADER }
epoch, _ := coordinator.CurrentEpoch(topic, partition)
storage.Append(ctx, topic, partition, epoch, batchBytes)
```

This closes:

- Phase 2 gap #5 — heartbeat-freshness check
- Phase 2 gap #6 — real epoch from BrokerCoordinator into Append
- Phase 1 deferred — `Append` epoch enforcement (storage side)

### F. Storage-side epoch enforcement

`DiskStorageEngine.Append` currently has the parameter `_ uint32` (Phase 1 deferred). With the BrokerCoordinator passing the real epoch, the storage engine adds:

```go
if epoch != 0 && epoch < ps.epoch {
    return -1, ErrEpochMismatch
}
```

…using the manifest-persisted `ps.epoch` (added in `ac73ba1`). This is the data-plane half of the epoch fence; the file-validation half lives in `assignment_watch.go`.

### G. Remove `internal/lock/`

Once the BrokerCoordinator is the source of truth for ownership, `flock` adds nothing — the v3 plan project layout (line 588) is explicit: *"There is no `internal/lock/` package."* All references in `cmd/skafka/main.go`, `internal/broker/`, `internal/protocol/handlers/produce.go`, `internal/storage/engine.go` need to drop the dependency.

### H. Switch segment filenames to epoch-prefixed format

Phase 3 deferred this. Once flock is gone, two leaders racing on the same `{base_offset:020d}.log` filename becomes a real (not flock-prevented) hazard — the epoch prefix is the v3 single-writer-by-construction story. Rename helpers in `internal/storage/segment.go`:

- `segmentLogPath(dir, baseOffset, epoch)` → `{epoch:08x}-{base_offset:020d}.log`
- `segmentIndexPath(...)` similarly
- `listSegments` parses both old and new formats during migration
- TakeOver writes `.log.sealed` (zero-byte marker) + `.recovery` sidecar containing the recovered offset

### I. Tests

| Package | Target tests |
|---|---|
| `tests/controller-failover/` | Election round-trip (broker A acquires, dies, broker B picks up); data-plane uninterrupted during transition; new controller's `leaseTransitions` is one greater |
| `tests/stale-controller-race/` | Partitioned ex-controller writes `assignment.json` with stale epoch; brokers observe via watcher and **ignore** the file; new controller's higher-epoch write replaces it |
| `internal/controller/` unit | Balancer placement determinism; assignment write atomicity; epoch monotonic across reconnects |
| `internal/broker/` unit | Self-fence transition (heartbeat ages out → IsHeartbeatFresh → false); push-vs-poll dedup via assignmentVersion; mtime-resolution defense via 30s full-read |

---

## Suggested implementation order

1. **gRPC stub generation** (`buf generate`) — small, mechanical, unblocks everything. ~30 min.
2. **File-backed `AssignmentStore`** — pure file I/O + fsnotify; no Kubernetes dependency; testable in isolation. ~1-2 hours.
3. **Controller election** (single Lease wrapper around `leaderelection`) — adapt the per-partition pattern in `internal/lease/k8s_manager.go`. ~2 hours.
4. **Heartbeat gRPC server + client (skeleton)** — bidi stream, reconnect, no business logic yet. ~2 hours.
5. **BrokerCoordinator file watch + epoch-fence + handlers** — wire `assignment_watch.go` and `assignment_poll.go` into the lease informer. ~2-3 hours.
6. **Self-fence + takeover.go** — atomic timestamp, IsHeartbeatFresh, drive `storage.TakeOver`/`Relinquish`. ~2 hours.
7. **Wire BrokerCoordinator into produce.go** + storage-side `ErrEpochMismatch` check. ~1 hour.
8. **Controller assignment computation + write loop + CR mirror** — coalesces broker churn / topic events into recomputes. ~3-4 hours.
9. **flock removal** — `git rm internal/lock/`, update all callers. Has to come after step 7 so we never have an "unguarded" interim. ~1 hour.
10. **Epoch-prefixed segment filenames** — Phase 3 paired item. ~2-3 hours.
11. **Real tests** in `tests/controller-failover/` and `tests/stale-controller-race/`. ~3-4 hours.

Total wall-clock estimate: **20-25 hours of focused work**, easily five to seven sessions. Each step ends in a commitable, testable state.

---

## Open questions for Phase 4 implementation

- **Controller hosting**: does the `skafka-controller` Lease run inside the broker process (every broker, only one wins) or in a dedicated controller pod? Plan §Phase 1 open question #11 says "Dedicated controller mode — deferred to v4". So in-process for v1: every broker tries to acquire the Lease; the winner runs the controller goroutines.
- **Topic events**: how does the controller learn about KafkaTopic CRD events? Either it watches `KafkaTopic` directly (operator pattern) or the operator updates a separate input that the controller polls. Existing `internal/k8s/topic_watcher.go` is already a watcher — likely we reuse it.
- **Heartbeat transport**: separate gRPC port (e.g. 9094) or multiplex onto the existing 9092/9093 Kafka port? Plan implies separate. ServiceAccount + headless Service for in-cluster broker-to-controller routing.
- **What about `.tmp` orphan cleanup?** The plan says: on controller startup, scan `/data/__cluster/` for `assignment.json.tmp` and delete. Add to controller bootstrap.
- **Existing `internal/lease/k8s_manager.go` after Phase 4?** Per-partition election is gone. But the consumer-group coordinator Lease (§Phase 5) still uses Kubernetes Leases for coordinator election. So the package survives, scoped to coordinator-leases only.

---

## Summary

Phase 4 is the architectural pivot from v2.6 to v3. It's not a single-session sprint — it's a multi-step transition that touches every layer except auth and pure protocol parsing. The Phase 1 work (interfaces, proto schema, CRD, RBAC, test placeholders) means the *contract* surface is already settled; Phase 4 is filling in implementations behind those contracts and migrating callers.

Recommended starting point: gRPC stub generation, then the file-backed AssignmentStore. Both are pure-Go work, no Kubernetes needed for testing, and they unblock everything that comes after.
