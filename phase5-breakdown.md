# Phase 5 Breakdown: Consumer Group Coordinator

## Current State (end of Phase 4)

All tests pass. The consumer group handlers in
`internal/protocol/handlers/consumer_group.go` are stubs that return
`COORDINATOR_NOT_AVAILABLE` for every request. Franz-go and kafka-go compatibility
tests pass because neither library requires working consumer groups for the
produce/consume round-trip tests.

### Key files before starting Phase 5

| File | Role |
|---|---|
| `internal/protocol/handlers/consumer_group.go` | 8 stub handlers — fully replaced in Phase 5 |
| `internal/protocol/codec/api/join_group.go` | JoinGroupRequest / JoinGroupResponse codec |
| `internal/protocol/codec/api/sync_group.go` | SyncGroupRequest / SyncGroupResponse codec |
| `internal/protocol/codec/api/offset_commit.go` | OffsetCommitRequest / Response codec |
| `internal/protocol/codec/api/offset_fetch.go` | OffsetFetchRequest / Response codec |
| `internal/protocol/codec/api/find_coordinator.go` | FindCoordinatorRequest / Response codec |
| `internal/lease/k8s_manager.go` | KubernetesLeaseManager — extended with coordinator methods |
| `internal/k8s/endpoints.go` | BrokerRegistry — used by FindCoordinator to return broker address |
| `internal/broker/broker.go` | Wires handlers — gains coordinator wiring |

### What the Kafka consumer group protocol actually does

```
Client                          Coordinator broker
  │                                      │
  │──── FindCoordinator(group-id) ──────▶│  "who is coordinator for my-group?"
  │◀─── NodeID + Host + Port ────────────│  coordinator returns self or other broker
  │                                      │
  │──── JoinGroup(group-id, protocols) ─▶│  "I want to join"
  │     [BLOCKS until all members join]  │
  │◀─── generationID + leader + members ─│  leader gets full member list
  │                                      │
  │──── SyncGroup(assignments) ──────────▶  leader sends partition assignments
  │     [BLOCKS until leader syncs]      │  followers send empty assignments
  │◀─── my assignment ───────────────────│
  │                                      │
  │──── Heartbeat(generationID) ─────────▶  every session_timeout/3
  │◀─── OK ──────────────────────────────│
  │                                      │
  │──── OffsetCommit(topic/p → offset) ─▶│  after processing records
  │◀─── OK ──────────────────────────────│
  │                                      │
  │──── OffsetFetch(topic/p list) ───────▶│  on startup, where did I leave off?
  │◀─── committed offsets ───────────────│
```

The critical subtlety: JoinGroup and SyncGroup **block** until all group members
have called them. The handler goroutine parks on a channel until the rebalance
completes, then returns the response. This is idiomatic in our per-connection goroutine
model.

---

## Step 5.0 — Coordinator lease methods

File: `internal/lease/k8s_manager.go` (extend existing)

Add coordinator-specific methods to `KubernetesLeaseManager` so it can manage
both partition leases and group coordinator leases. The coordinator lease for a
group uses the same Kubernetes Lease mechanism but a different naming prefix.

```go
// AcquireCoordinator starts a LeaderElector for group coordination.
// Lease name: "skafka-coord-{sanitized-group-id}" (no partition suffix).
func (m *KubernetesLeaseManager) AcquireCoordinator(ctx context.Context, groupID string) error

// ReleaseCoordinator cancels the coordinator elector for groupID.
func (m *KubernetesLeaseManager) ReleaseCoordinator(groupID string) error

// IsCoordinator reports whether this broker currently holds the coordinator lease.
func (m *KubernetesLeaseManager) IsCoordinator(groupID string) bool

// CoordinatorFor returns the node ordinal of the current coordinator, -1 if unknown.
func (m *KubernetesLeaseManager) CoordinatorFor(groupID string) int32
```

Internally, coordinator state is tracked in separate maps (`coordHeld`, `coordLeaders`,
`coordCancels`) to avoid collision with partition lease maps.

Lease name: `"skafka-coord-" + sanitize(groupID)` (max 253 chars, same sanitize logic).

Add a `CoordinatorLeaseManager` interface to `internal/lease/manager.go` so the
coordinator package can depend on the interface, not the concrete type:

```go
type CoordinatorLeaseManager interface {
    AcquireCoordinator(ctx context.Context, groupID string) error
    ReleaseCoordinator(groupID string) error
    IsCoordinator(groupID string) bool
    CoordinatorFor(groupID string) int32
}
```

`KubernetesLeaseManager` and `LocalLeaseManager` both implement this interface.
`LocalLeaseManager` always reports `IsCoordinator=true` and `CoordinatorFor=0`.

**Done when:** unit test (fake client): two managers compete for the coordinator lease
for the same group; only one wins `IsCoordinator=true`.

---

## Step 5.1 — __consumer_offsets storage

File: `internal/coordinator/offsets.go`

For Phase 5 (v1 MVP), committed offsets are stored as a JSON map file per group on the
shared PVC. Full log compaction (Kafka's native `__consumer_offsets` format) is a v2
feature.

```
/data/__consumer_offsets/
  {group-id}.json    ← {"payments-topic/0": 12345, "payments-topic/1": 6789}
```

```go
type OffsetStore struct {
    dataDir string
    mu      sync.RWMutex
    cache   map[string]map[string]int64 // groupID → "topic/partition" → offset
}

func NewOffsetStore(dataDir string) *OffsetStore

// Commit writes committed offsets for a group atomically (write tmp, os.Rename).
func (s *OffsetStore) Commit(groupID string, offsets map[string]int64) error

// Fetch returns the committed offsets for a group+topic+partitions.
// Returns -1 for partitions with no committed offset.
func (s *OffsetStore) Fetch(groupID string, topics []FetchSpec) map[string]int64

// Load reads a group's offsets from disk into the in-memory cache.
func (s *OffsetStore) Load(groupID string) error
```

Where `FetchSpec` is:
```go
type FetchSpec struct {
    Topic      string
    Partitions []int32
}
```

Key encoding: `topic + "/" + strconv.Itoa(int(partition))`

Atomic write:
```go
tmp := filepath.Join(s.dataDir, "__consumer_offsets", groupID+".tmp")
final := filepath.Join(s.dataDir, "__consumer_offsets", groupID+".json")
os.WriteFile(tmp, jsonBytes, 0644)
os.Rename(tmp, final)
```

Only the coordinator broker writes; all reads come from the in-memory cache (loaded
on coordinator takeover).

**Done when:** unit test: Commit 3 offsets, Fetch them back (from cache), then
reopen a new OffsetStore (disk load) and Fetch again — same values.

---

## Step 5.2 — Group state machine

File: `internal/coordinator/group.go`

```go
type GroupState int

const (
    GroupEmpty               GroupState = iota
    GroupPreparingRebalance
    GroupCompletingRebalance
    GroupStable
    GroupDead
)

type Member struct {
    ID                 string
    ClientID           string
    GroupInstanceID    string
    SessionTimeoutMs   int32
    RebalanceTimeoutMs int32
    Protocols          []api.JoinGroupProtocol
    Assignment         []byte
    heartbeatTimer     *time.Timer
}

type joinWaiter struct {
    memberID string
    result   chan joinResult
}

type joinResult struct {
    resp *api.JoinGroupResponse
}

type Group struct {
    ID           string
    State        GroupState
    GenerationID int32
    ProtocolType string
    ProtocolName string
    LeaderID     string // MemberID of the group leader

    mu          sync.Mutex
    members     map[string]*Member
    joinWaiters []joinWaiter        // JoinGroup calls waiting for rebalance to complete
    syncResults map[string]chan []byte // memberID → assignment channel
    rebalanceTicker *time.Timer     // fires when RebalanceTimeoutMs elapses
}
```

### JoinGroup logic (called with group.mu held)

```
func (g *Group) handleJoin(req *JoinGroupRequest) <-chan joinResult:
    1. Generate MemberID if req.MemberID == ""  (UUID: "clientID-{uuid}")
    2. Upsert member into g.members
    3. Reset the member's heartbeat timer
    4. If state == Stable or state == CompletingRebalance:
           transition to PreparingRebalance
           notify all existing members of rebalance (send REBALANCE_IN_PROGRESS on
           their pending heartbeat / fetch channels — for Phase 5 we wait for them
           to naturally re-call JoinGroup via their own heartbeat timeout)
    5. If state == Empty:
           set state = PreparingRebalance
           start rebalanceTicker(max RebalanceTimeoutMs across members)
    6. Create result channel, append joinWaiter{memberID, ch}
    7. If all known members are now in joinWaiters: completeRebalance()
    8. Return the result channel (caller blocks on it)

func (g *Group) completeRebalance():
    1. state = CompletingRebalance
    2. Select protocol: find the protocol name supported by ALL members
       (intersection of members' protocol name lists; first wins)
    3. Elect leader: g.LeaderID = first member in joinWaiters (stable ordering)
    4. GenerationID++
    5. Send JoinGroupResponse to all joinWaiters:
         - Leader gets Members list populated (with metadata for assignment)
         - Non-leaders get empty Members list
    6. Clear joinWaiters
    7. Start syncWaiters map (per-member channels for SyncGroup)
```

### SyncGroup logic

```
func (g *Group) handleSync(req *SyncGroupRequest) <-chan []byte:
    1. Verify GenerationID matches (else ILLEGAL_GENERATION)
    2. Create per-member assignment channel, store in g.syncResults[memberID]
    3. If req.MemberID == g.LeaderID (leader is syncing):
         Store each member's assignment from req.Assignments
         Deliver to each member's channel in g.syncResults
         state = Stable
    4. Return g.syncResults[memberID] (caller blocks until leader delivers)
```

### Heartbeat logic

```
func (g *Group) handleHeartbeat(memberID string, generationID int32) int16:
    1. Verify state != Dead → UNKNOWN_MEMBER_ID
    2. Verify generationID matches → ILLEGAL_GENERATION
    3. Verify member exists → UNKNOWN_MEMBER_ID
    4. If state == PreparingRebalance → REBALANCE_IN_PROGRESS
    5. Reset member's heartbeat timer
    6. Return NONE
```

### LeaveGroup logic

```
func (g *Group) handleLeave(memberID string) int16:
    1. Remove member from g.members
    2. Cancel their heartbeat timer
    3. If state == Stable and len(members) > 0: transition to PreparingRebalance
    4. If len(members) == 0: transition to Empty
    5. Return NONE
```

### Session timeout

Each member's `heartbeatTimer` fires after `SessionTimeoutMs` if no heartbeat arrives.
On expiry: call `group.handleLeave(memberID)` under the group lock.

**Done when:** unit tests covering all state transitions:
- Empty → PreparingRebalance on first JoinGroup
- PreparingRebalance → CompletingRebalance when all members joined
- CompletingRebalance → Stable after SyncGroup
- Stable → PreparingRebalance on LeaveGroup
- Session timeout triggers LeaveGroup

---

## Step 5.3 — CoordinatorManager

File: `internal/coordinator/coordinator.go`

```go
type CoordinatorManager struct {
    leases      lease.CoordinatorLeaseManager
    brokers     handlers.BrokerSource // for FindCoordinator responses
    offsets     *OffsetStore
    ctx         context.Context

    mu     sync.Mutex
    groups map[string]*Group
}

func NewCoordinatorManager(
    leases lease.CoordinatorLeaseManager,
    brokers handlers.BrokerSource,
    offsets *OffsetStore,
) *CoordinatorManager
```

### FindCoordinator

```go
func (c *CoordinatorManager) FindCoordinator(groupID string) (nodeID int32, host string, port int32, errCode int16) {
    // Ensure a coordinator lease acquisition is in flight for this group.
    c.ensureAcquiring(groupID)

    ordinal := c.leases.CoordinatorFor(groupID)
    if ordinal < 0 {
        return 0, "", 0, int16(codec.ErrCoordinatorNotAvailable)
    }
    for _, b := range c.brokers.All() {
        if b.NodeID == ordinal {
            return b.NodeID, b.Host, b.Port, 0
        }
    }
    return 0, "", 0, int16(codec.ErrCoordinatorNotAvailable)
}

func (c *CoordinatorManager) ensureAcquiring(groupID string) {
    c.mu.Lock()
    _, known := c.groups[groupID]
    c.mu.Unlock()
    if !known {
        _ = c.leases.AcquireCoordinator(c.ctx, groupID)
    }
}
```

### Guard: only coordinator handles group requests

```go
func (c *CoordinatorManager) IsCoordinator(groupID string) bool {
    return c.leases.IsCoordinator(groupID)
}

func (c *CoordinatorManager) getOrCreateGroup(groupID string) *Group {
    c.mu.Lock()
    defer c.mu.Unlock()
    g, ok := c.groups[groupID]
    if !ok {
        g = &Group{ID: groupID, State: GroupEmpty, members: make(map[string]*Member)}
        c.groups[groupID] = g
    }
    return g
}
```

### Exported methods called by handlers

```go
func (c *CoordinatorManager) JoinGroup(req *api.JoinGroupRequest) *api.JoinGroupResponse
func (c *CoordinatorManager) SyncGroup(req *api.SyncGroupRequest) *api.SyncGroupResponse
func (c *CoordinatorManager) Heartbeat(req *api.HeartbeatRequest) *api.HeartbeatResponse
func (c *CoordinatorManager) LeaveGroup(req *api.LeaveGroupRequest) *api.LeaveGroupResponse
func (c *CoordinatorManager) OffsetCommit(req *api.OffsetCommitRequest) *api.OffsetCommitResponse
func (c *CoordinatorManager) OffsetFetch(req *api.OffsetFetchRequest) *api.OffsetFetchResponse
```

Each of these:
1. Calls `ensureAcquiring(groupID)` so lease acquisition is in flight
2. Checks `IsCoordinator(groupID)` — returns `NOT_COORDINATOR` (16) if false
3. Delegates to the appropriate `Group` method

**Done when:** integration test: two concurrent clients (goroutines) call JoinGroup
for the same group, both block, rebalance completes, both receive valid responses
with consistent generationID and leader election.

---

## Step 5.4 — Rewrite consumer group handlers

File: `internal/protocol/handlers/consumer_group.go` (full rewrite)

Replace all 8 stub handlers with real implementations that delegate to
`*coordinator.CoordinatorManager`. Each handler follows the same pattern:

```go
type JoinGroupHandler struct {
    coord *coordinator.CoordinatorManager
}

func NewJoinGroupHandler(coord *coordinator.CoordinatorManager) *JoinGroupHandler {
    return &JoinGroupHandler{coord: coord}
}

func (h *JoinGroupHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
    r := codec.NewReader(body)
    req, err := api.DecodeJoinGroupRequest(r, version)
    if err != nil {
        return nil, fmt.Errorf("join group decode: %w", err)
    }
    resp := h.coord.JoinGroup(req) // may block for rebalance duration
    w := codec.NewWriter()
    api.EncodeJoinGroupResponse(w, resp, version)
    return w.Bytes(), nil
}
```

Apply the same pattern for: `SyncGroupHandler`, `HeartbeatHandler`,
`LeaveGroupHandler`, `OffsetCommitHandler`, `OffsetFetchHandler`,
`FindCoordinatorHandler`, `DescribeGroupsHandler`, `ListGroupsHandler`.

`DescribeGroups` and `ListGroups` read from the `CoordinatorManager.groups` map and
return group state summaries.

**Done when:** all handlers compile with `coordinator.CoordinatorManager` injected.

---

## Step 5.5 — Wire to broker and integration test

### broker.go changes

`broker.Broker` gains a `coord *coordinator.CoordinatorManager` field.
`NewWithBrokerSource` (and `New`) accept the coordinator (can be `nil` for local-dev
— stubs remain valid when no coordinator is passed).

```go
func (b *Broker) RegisterHandlers(d *protocol.Dispatcher) {
    // ...existing handlers...
    if b.coord != nil {
        d.Register(10, 0, 4, handlers.NewFindCoordinatorHandler(b.coord))
        d.Register(11, 2, 9, handlers.NewJoinGroupHandler(b.coord))
        // ...etc
    } else {
        // Keep stubs for local-dev without a k8s cluster
        d.Register(10, 0, 4, handlers.NewFindCoordinatorHandlerStub())
        // ...etc
    }
}
```

### main.go changes

In Kubernetes mode:
```go
offsetStore := coordinator.NewOffsetStore(dataDir)
coordMgr := coordinator.NewCoordinatorManager(leaseManager, brokerSource, offsetStore)
b = broker.NewWithBrokerSource(..., coordMgr)  // pass coordinator
```

In local-dev mode: pass `nil` coordinator (stubs stay active).

### Integration test

File: `tests/integration/consumer_group_test.go`

```go
func TestConsumerGroupJoinSync(t *testing.T) {
    // Two goroutines simulate two consumers joining the same group.
    // Both call JoinGroup, block, then SyncGroup.
    // Verify: both get the same generationID, leader is deterministic,
    // assignments from SyncGroup are delivered correctly.
}

func TestHeartbeatSessionTimeout(t *testing.T) {
    // Member joins, stops heartbeating, verify it is removed after SessionTimeoutMs
    // and a rebalance is triggered.
}

func TestOffsetCommitFetch(t *testing.T) {
    // Commit offsets for several partitions, fetch them back.
    // Reopen OffsetStore (simulating restart), fetch again — same values.
}
```

Also update the kafka-compat tests to exercise consumer groups via franz-go and
kafka-go's consumer group APIs against a running broker.

---

## Rebalance timeout implementation note

The trickiest part is the rebalance timer. When a JoinGroup arrives:
- If this is the first member: start a timer for `RebalanceTimeoutMs`
- On timer fire: call `completeRebalance()` even if not all members have rejoined
  (stragglers are excluded from this generation)
- If all members join before the timer: call `completeRebalance()` immediately and
  stop the timer

This is important for clients that leave ungracefully (crash). Without the timer,
`completeRebalance` would never fire if a crashed member never rejoins.

Use `time.AfterFunc` for the timer; cancel it with `timer.Stop()`.

---

## Step order summary

| Step | File(s) | Depends on |
|---|---|---|
| 5.0 Coordinator lease methods | `lease/manager.go`, `lease/k8s_manager.go`, `broker/stubs.go` | nothing |
| 5.1 __consumer_offsets storage | `coordinator/offsets.go` | nothing |
| 5.2 Group state machine | `coordinator/group.go` | 5.0 (api types) |
| 5.3 CoordinatorManager | `coordinator/coordinator.go` | 5.0, 5.1, 5.2 |
| 5.4 Rewrite handlers | `handlers/consumer_group.go` | 5.3 |
| 5.5 Wire + tests | `broker/broker.go`, `cmd/skafka/main.go`, tests | 5.3, 5.4 |

Steps 5.1 and 5.2 can be done in parallel. Everything else is sequential.
