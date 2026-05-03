# Phase 5 Consumer Group Coordinator — Breakdown vs. Current Repo

Comparison of `skafka-plan-v3.md` (v3.3) §"Phase 5: Consumer Group Coordinator (Week 6–7)" (lines 1060–1066), §"Filesystem layout" (lines 901–904), and Phase 1 open question #5 (line 1458) against the state of `main` at commit `156818e`.

Legend: ✅ done · 🟡 partial / minor deviation · ❌ not yet · ➕ extra (not in plan)

---

## What Phase 5 actually has to deliver

The v3.3 plan body for Phase 5 is two sentences:

> Consumer groups assigned to brokers via the same controller assignment mechanism; assignment file holds them alongside topic partitions. `__consumer_offsets` partitions are stored on the shared PVC like any other topic and follow the byte-opaque storage model.

Combined with open question #5 (`__consumer_offsets in v1 — retention-based deletion + in-memory snapshot`) the v1 deliverable is narrower than it first looks:

| v3 architectural shift | Scope |
|---|---|
| **A.** Coordinator selection moves from per-group Kubernetes Lease (`skafka-coord-{groupID}`) to a row in the cluster controller's `assignment.json` (`consumerGroups[].broker`). | Phase 5 v1 |
| **B.** `FindCoordinator` resolves via the assignment file rather than per-group Lease informers. | Phase 5 v1 |
| **C.** When the controller moves group G from broker A to B, B loads G's state on takeover; A relinquishes. | Phase 5 v1 |
| **D.** `__consumer_offsets` becomes a real topic with partitions and segments (byte-opaque). | v2 (per open question #5; v1 keeps the per-group JSON snapshot the existing `OffsetStore` already does) |
| **E.** Log compaction for `__consumer_offsets` (Kafka's retention model for offsets). | v2 |

So Phase 5 is essentially **A + B + C**: rewire coordinator-for-group from per-group Lease to controller assignment. D and E are correctly deferred to v2.

---

## What already exists (from v2.6 + Phase 1)

| Item | Where | State |
|---|---|---|
| `kafkaapi.ConsumerGroupAssignment` struct (`{GroupID, Broker, Epoch}`) | `pkg/kafkaapi/assignment.go:63-67` | ✅ Phase 1 |
| `Assignment.ConsumerGroups []ConsumerGroupAssignment` field | `pkg/kafkaapi/assignment.go:46` | ✅ Phase 1 |
| Group state machine (Empty → PreparingRebalance → CompletingRebalance → Stable) | `internal/coordinator/group.go` | ✅ v2.6 |
| `Manager.JoinGroup`, `SyncGroup`, `Heartbeat`, `LeaveGroup`, `OffsetCommit`, `OffsetFetch`, `DescribeGroups`, `ListGroups`, `FindCoordinator` | `internal/coordinator/coordinator.go` | ✅ v2.6 |
| Per-group offset persistence (JSON snapshot at `__consumer_offsets/{groupID}.json`) | `internal/coordinator/offsets.go` | ✅ v2.6, matches v1 plan answer |
| Consumer-group API codec + handlers | `internal/protocol/codec/api/`, `internal/protocol/handlers/consumer_group.go` | ✅ v2.6 |
| `internal/protocol/handlers/consumer_group.go` Go-API integration test | `tests/integration/consumer_group_test.go` | ✅ v2.6 |
| Skipped wire-level franz-go group test | `tests/kafka-compat/compat_test.go::TestFranzGoConsumerGroup` | 🟡 Skipped — see [project_franzgo_consumer_group memory note](../.claude/projects/-home-coder-repos-skafka/memory/project_franzgo_consumer_group.md) |

The Manager already implements every consumer-group API handler. The state machine and offset storage work end-to-end against `tests/integration/consumer_group_test.go`. The piece that's still v2.6-shaped is **how the broker decides "am I the coordinator for group G?"**.

---

## What's currently v2.6-shaped (needs the v3 rewrite)

| v2.6 mechanism | v3 target |
|---|---|
| `Manager.ensureAcquiring(groupID)` calls `lease.AcquireCoordinator` → starts a `client-go/leaderelection` goroutine for a per-group Lease (`skafka-coord-{groupID}`) | `Manager` consults the broker's `kafkaapi.BrokerCoordinator` (or directly the `AssignmentStore`) to find the assigned coordinator broker for the group. |
| `Manager.isCoordinator(groupID)` returns `lease.IsCoordinator(groupID)` (the per-group Lease informer's view) | `isCoordinator` reads `assignment.json.consumerGroups[groupID].broker == this-broker-id` |
| `FindCoordinator` calls `lease.CoordinatorFor(groupID)` and `lookupBroker(ordinal)` | `FindCoordinator` looks up the assignment, then `lookupBroker(brokerID)` for the address |
| Per-group Kubernetes Leases (`skafka-coord-*`) — one Lease object per consumer group | Cluster controller's single Lease + `consumerGroups[]` rows in `assignment.json` |
| Group state implicitly bound to whatever broker won the per-group Lease | Group state explicitly bound to whichever broker the controller assigned; `TakeoverDriver`-style hook on `OnAssignmentChange` to load/unload state |

The plan project layout (line 588) explicitly says: *"the consumer-group coordinator Lease (§Phase 5) still uses Kubernetes Leases for coordinator election. So the package survives, scoped to coordinator-leases only."* — but Phase 5 in v3.3 contradicts that older note: *"Consumer groups assigned to brokers via the same controller assignment mechanism."* The v3.3 reading wins; the per-group Lease path goes away.

`internal/lease/k8s_manager.go` still has `AcquireCoordinator`, `IsCoordinator`, `CoordinatorFor`, `WaitForCoordinator`, `coordLeaseName`. After Phase 5, those become dead code.

---

## What Phase 5 must add

### A. Plumb consumer groups through the controller's `Balance` and `AssignmentLoop`

`internal/controller/balancer.go` currently only assigns topic partitions. It needs to also assign consumer groups:

- New parameter `prevGroups []kafkaapi.ConsumerGroupAssignment` and `groups []GroupSpec` (analogous to `TopicSpec`).
- Strict-stability rule: if a group is currently assigned to a still-alive broker, keep it. Otherwise rendezvous-hash to a fresh broker.
- Bump the per-group epoch on reassignment, just like partitions.

`internal/controller/assignment.go` needs:
- A `GroupSource` interface (analogous to `TopicSource`) — provides the live list of active consumer groups.
- Wire it into `recomputeAndWrite`; emit `ConsumerGroups` rows in the `Assignment`.

The simplest `GroupSource` for v1: the controller broker's own `coordinator.Manager` enumerates groups it currently has state for. But that's circular — the controller runs on one broker; groups are coordinated by potentially every broker. The cleaner shape: brokers report their currently-coordinated group IDs in their `BrokerStatus` heartbeat upstream; the controller aggregates these into the `GroupSource`.

Alternative for v1 simplicity: groups become known to the controller when a `JoinGroup` request arrives at any broker → that broker tells the controller via a new heartbeat field "I have a request for group G that doesn't exist in the assignment yet" → controller adds G to the assignment and assigns a coordinator. There's a transient redirect: the broker that received the JoinGroup may not be the assigned coordinator, so it returns `NotCoordinator` and the client retries against the freshly-assigned one.

### B. Rewire `Manager` to consult the BrokerCoordinator instead of per-group Leases

`internal/coordinator/coordinator.go`:

- Drop `leases lease.CoordinatorLeaseManager` and `lookupBroker func(int32) (...)` from `Manager`.
- Add `coord kafkaapi.BrokerCoordinator` (or an interface that exposes `OwnsGroup(groupID) bool` + `GroupCoordinator(groupID) (brokerID, ok)` + a broker-id → host/port lookup).
- `ensureAcquiring`, `isCoordinator`, `FindCoordinator.lookupOne` all reroute through the BrokerCoordinator.
- The first time a `JoinGroup` arrives for a not-yet-known group, the Manager calls a hook (`UpdateAssignment(reason: GroupCreated, GroupID: g)`) into the controller (or, if this broker is the controller, directly into the AssignmentLoop) and returns `CoordinatorLoadInProgress` until the assignment file lists the group.

### C. Group state takeover on assignment change

When the assignment moves group G from broker A to broker B:

- B needs to load G's offsets from `__consumer_offsets/{G}.json` (already supported via `OffsetStore.Load`).
- B's group state machine starts at `Empty`; the next `JoinGroup` rebuilds membership organically. Acceptable for v1 because Kafka clients reconnect with the same `member.id` and `JoinGroup` re-establishes them — at the cost of one rebalance round-trip.
- A needs to stop accepting new requests for G (return `NotCoordinator`) and persist any uncommitted offsets before relinquishing.

A `GroupTakeoverDriver` analogous to `internal/broker/takeover.go::TakeoverDriver` registers as an `OnAssignmentChange` handler:

```go
type GroupTakeoverDriver struct{ mgr *coordinator.Manager; brokerID string }
func (d *GroupTakeoverDriver) OnAssignmentChange(ctx context.Context, prev, next *Assignment) {
    // diff prev.ConsumerGroups vs next.ConsumerGroups
    // for groups newly ours: mgr.LoadGroup(g)
    // for groups newly not-ours: mgr.RelinquishGroup(g)
}
```

### D. Heartbeat upstream extension: brokers report active groups

`proto/heartbeat.proto::BrokerStatus` needs an `active_groups` field — the list of consumer groups this broker is *currently* serving, used by the controller as the GroupSource. Today's BrokerStatus has only partition-level state.

Schema change is small (one repeated string field) but it requires re-running `make proto` and updating the broker's heartbeat client to populate it.

### E. Tests

| Package | Target |
|---|---|
| `internal/controller/` | Balancer extended for groups: stability + rendezvous-hash on broker death; epoch bump on reassignment |
| `internal/controller/` | AssignmentLoop emits `consumerGroups` rows; controller failover preserves group epochs |
| `internal/coordinator/` | `Manager.FindCoordinator` consults the BrokerCoordinator (mock) and returns the assigned broker |
| `internal/coordinator/` | `GroupTakeoverDriver` diff: prev=A, next=B → A relinquishes, B loads |
| `tests/integration/consumer_group_test.go` | Existing Go-API tests should keep passing through the rewire (the API surface doesn't change) |
| `tests/kafka-compat/` | The skipped `TestFranzGoConsumerGroup` may start working once coordinator selection is via assignment.json — worth re-running and unskipping if it does |

### F. Cleanup of v2.6 per-group Lease code

After A–E land:

- `internal/lease/k8s_manager.go::AcquireCoordinator`, `ReleaseCoordinator`, `IsCoordinator`, `CoordinatorFor`, `WaitForCoordinator`, `coordLeaseName` → delete.
- `internal/lease/manager.go::CoordinatorLeaseManager` interface → delete.
- `internal/broker/stubs.go::LocalLeaseManager` simplifies: drop the coordinator-lease half.
- RBAC for `skafka-coord-*` Leases narrows: only the cluster-controller Lease remains.

---

## Suggested implementation order

1. **Heartbeat schema bump** — add `active_groups` repeated string to `BrokerStatus`. Regenerate stubs. Wire HeartbeatServer aggregation. ~1h.
2. **Balancer extension** — add `GroupSpec` + group placement to `Balance`. Pure-function refactor; tests easy. ~2h.
3. **AssignmentLoop GroupSource** — `recomputeAndWrite` emits consumer groups in the file. ~1h.
4. **Coordinator rewire** — `Manager` takes a `BrokerCoordinator` instead of `CoordinatorLeaseManager`. Update `FindCoordinator`, `isCoordinator`, drop `ensureAcquiring`. ~3h.
5. **GroupTakeoverDriver** — analog of `TakeoverDriver`, registers as `OnAssignmentChange` handler, drives Manager.LoadGroup / RelinquishGroup. ~2h.
6. **Migrate `cmd/skafka/main.go` wiring** — Manager now takes the BrokerCoordinator; drop the per-group Lease setup. ~1h.
7. **Tests** — extend balancer tests, add `internal/coordinator/` integration tests against a stub BrokerCoordinator, retry `TestFranzGoConsumerGroup` with the new wiring. ~3h.
8. **Delete v2.6 per-group Lease code** — `AcquireCoordinator` family, `coordLeaseName`, `CoordinatorLeaseManager` interface. RBAC trim. ~1h.

Total wall-clock: **12–14 hours of focused work, 4–6 sessions.** Each step is committable on its own.

---

## Open questions for Phase 5 implementation

- **GroupSource model**: brokers report active groups in heartbeat, OR the controller tracks groups via a side-channel (e.g. it watches a CRD)? Heartbeat is simpler; CRD is more loosely-coupled but adds operator surface.
- **Eager vs lazy group registration**: when a `JoinGroup` arrives for an unknown group, does the broker block until the controller adds it to the assignment, or does it temporarily coordinate (then hand off when the assignment lands)? Eager handoff is simpler; lazy is faster.
- **Stale-controller race for group assignments**: same epoch-fence pattern as partition assignments — the `controllerEpoch` already covers it, no additional work.
- **Group epoch semantics**: bumped on reassignment (analogous to partition epoch). Used for what? Members need a stable `generation_id` (already in v2.6 group state machine) but the controller's group-epoch is a separate concept — useful for a future "session takeover" optimization, not load-bearing in v1.
- **`__consumer_offsets` migration**: the JSON-per-group format works for v1. v2 will rewrite as a real topic with log segments. The `OffsetStore` interface boundary is well-placed for that swap.
- **kafbat-ui / Java AdminClient interop with the new FindCoordinator**: the response shape doesn't change (we still return `{NodeID, Host, Port}`); only the lookup path is different. No client-visible change expected.

---

## Summary

Phase 5 is a **rewire**, not a green-field implementation. The consumer-group machinery (state machine, all handlers, offset storage) is already in place and works against `tests/integration/consumer_group_test.go`. What changes is *how the broker decides who's the coordinator for each group*:

- **From** per-group Kubernetes Lease + `client-go/leaderelection`-per-group goroutines.
- **To** a row in the cluster controller's `assignment.json`, validated against the same epoch fence that already protects partition assignments.

The piece that *isn't* getting done in v1 is the `__consumer_offsets`-as-real-topic story; the JSON snapshot stays. That's deferred to v2 per open question #5.

After Phase 5, `internal/lease/` shrinks dramatically — only the singleton cluster-controller Lease (Phase 4) survives. The per-group Lease infrastructure (`AcquireCoordinator`, `coordLeaseName`, `CoordinatorLeaseManager`) becomes dead code and gets deleted.

Phase 5 also opens the door to **unskipping `TestFranzGoConsumerGroup`** in `tests/kafka-compat/`. The current skip is documented in memory as "wire-level interop bug" — quite possibly an artifact of the per-group Lease cold-start path that goes away with this rewire. Worth a re-run as the last step of Phase 5.
