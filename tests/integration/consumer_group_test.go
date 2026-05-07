package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/woestebanaan/skafka/internal/coordinator"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

// alwaysGroupSource satisfies coordinator.GroupAssignmentSource by saying
// "this broker is coordinator for every group". The Phase 5 rewire moved
// coordinator selection from per-group Lease informers (the old
// alwaysCoordinator stub here) onto this GroupAssignmentSource interface.
type alwaysGroupSource struct{ brokerID string }

func (a *alwaysGroupSource) OwnsGroup(_ string) bool { return true }
func (a *alwaysGroupSource) GroupCoordinator(_ string) (string, bool) {
	return a.brokerID, true
}

var _ coordinator.GroupAssignmentSource = (*alwaysGroupSource)(nil)

// newTestCoordinator creates a Manager wired to an alwaysGroupSource —
// the test focuses on group state and offset storage, not on coordinator
// selection. fake.NewSimpleClientset is no longer needed because the
// per-group Lease path is gone.
func newTestCoordinator(t *testing.T, podName string) *coordinator.Manager {
	t.Helper()
	src := &alwaysGroupSource{brokerID: podName}
	lookup := func(_ string) (int32, string, int32, bool) { return 0, "localhost", 9092, true }
	return coordinator.NewManager(context.Background(), src, lookup, coordinator.NewOffsetStore(t.TempDir()))
}

// fastJoin builds a JoinGroupRequest with a short rebalance timeout so timer-based
// completion fires quickly in tests.
func fastJoin(groupID, protocolType string) *api.JoinGroupRequest {
	return &api.JoinGroupRequest{
		GroupID:            groupID,
		SessionTimeoutMs:   30_000,
		RebalanceTimeoutMs: 100, // 100ms — fast for tests
		ProtocolType:       protocolType,
		Protocols:          []api.JoinGroupProtocol{{Name: "range", Metadata: []byte("meta")}},
	}
}

// syncBoth calls SyncGroup for both members concurrently (required because the
// non-leader blocks until the leader provides assignments).
func syncBoth(t *testing.T, mgr *coordinator.Manager, groupID string, r1, r2 *api.JoinGroupResponse) {
	t.Helper()
	var wg sync.WaitGroup
	for _, r := range []*api.JoinGroupResponse{r1, r2} {
		r := r
		wg.Add(1)
		go func() {
			defer wg.Done()
			mgr.SyncGroup(&api.SyncGroupRequest{
				GroupID:      groupID,
				GenerationID: r.GenerationID,
				MemberID:     r.MemberID,
				Assignments: []api.SyncAssignment{
					{MemberID: r.MemberID, Assignment: []byte("partition-" + r.MemberID)},
				},
			})
		}()
	}
	wg.Wait()
}

func TestJoinGroupSingleMember(t *testing.T) {
	mgr := newTestCoordinator(t, "broker-0")

	resp := mgr.JoinGroup(fastJoin("test-group", "consumer"), "test-client")
	if resp.ErrorCode != 0 {
		t.Fatalf("JoinGroup errCode=%d", resp.ErrorCode)
	}
	if resp.MemberID == "" {
		t.Error("expected non-empty MemberID")
	}
	if resp.GenerationID != 1 {
		t.Errorf("GenerationID=%d, want 1", resp.GenerationID)
	}
	if resp.Leader != resp.MemberID {
		t.Errorf("single member should be leader")
	}
	if len(resp.Members) != 1 {
		t.Errorf("leader should receive 1-member list, got %d", len(resp.Members))
	}
}

// TestJoinGroupNewGroupCapsInitialDelay verifies that a single consumer joining a brand-new
// group with a production-realistic RebalanceTimeoutMs (Java client default = max.poll.interval.ms
// = 5 min) does not have to wait the full timeout. The initial-rebalance delay caps it to ~3s.
func TestJoinGroupNewGroupCapsInitialDelay(t *testing.T) {
	mgr := newTestCoordinator(t, "broker-0")

	req := &api.JoinGroupRequest{
		GroupID:            "new-group",
		SessionTimeoutMs:   30_000,
		RebalanceTimeoutMs: 300_000, // 5 min — what the Java consumer sends
		ProtocolType:       "consumer",
		Protocols:          []api.JoinGroupProtocol{{Name: "range"}},
	}
	start := time.Now()
	resp := mgr.JoinGroup(req, "client-1")
	elapsed := time.Since(start)

	if resp.ErrorCode != 0 {
		t.Fatalf("JoinGroup errCode=%d", resp.ErrorCode)
	}
	// Must complete within the initial rebalance delay (3s) plus generous slack.
	if elapsed > 5*time.Second {
		t.Errorf("JoinGroup blocked for %v, want <=5s (initial-rebalance delay should cap this)", elapsed)
	}
}

func TestJoinGroupTwoMembersConcurrent(t *testing.T) {
	mgr := newTestCoordinator(t, "broker-0")
	const groupID = "two-member-group"

	var r1, r2 *api.JoinGroupResponse
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); r1 = mgr.JoinGroup(fastJoin(groupID, "consumer"), "client-1") }()
	go func() { defer wg.Done(); r2 = mgr.JoinGroup(fastJoin(groupID, "consumer"), "client-2") }()
	wg.Wait()

	if r1.ErrorCode != 0 {
		t.Errorf("r1 errCode=%d", r1.ErrorCode)
	}
	if r2.ErrorCode != 0 {
		t.Errorf("r2 errCode=%d", r2.ErrorCode)
	}
	if r1.GenerationID != r2.GenerationID {
		t.Errorf("generationIDs differ: %d vs %d", r1.GenerationID, r2.GenerationID)
	}
	if r1.Leader != r2.Leader {
		t.Errorf("leader differs between responses: %q vs %q", r1.Leader, r2.Leader)
	}
	// Leader gets the full member list; follower gets empty.
	leaderResp, followerResp := r1, r2
	if r2.MemberID == r2.Leader {
		leaderResp, followerResp = r2, r1
	}
	if len(leaderResp.Members) != 2 {
		t.Errorf("leader should see 2 members, got %d", len(leaderResp.Members))
	}
	if len(followerResp.Members) != 0 {
		t.Errorf("follower should see 0 members, got %d", len(followerResp.Members))
	}
}

func TestSyncGroupRoundTrip(t *testing.T) {
	mgr := newTestCoordinator(t, "broker-0")
	const groupID = "sync-group"

	joinResp := mgr.JoinGroup(fastJoin(groupID, "consumer"), "client-1")
	if joinResp.ErrorCode != 0 {
		t.Fatalf("JoinGroup errCode=%d", joinResp.ErrorCode)
	}

	assignment := []byte("partition-0")
	syncResp := mgr.SyncGroup(&api.SyncGroupRequest{
		GroupID:      groupID,
		GenerationID: joinResp.GenerationID,
		MemberID:     joinResp.MemberID,
		Assignments:  []api.SyncAssignment{{MemberID: joinResp.MemberID, Assignment: assignment}},
	})
	if syncResp.ErrorCode != 0 {
		t.Fatalf("SyncGroup errCode=%d", syncResp.ErrorCode)
	}
	if string(syncResp.Assignment) != string(assignment) {
		t.Errorf("assignment=%q, want %q", syncResp.Assignment, assignment)
	}
}

// TestHeartbeatKeepsMemberAlivePastTimeout pins the gh #19 happy
// path: a member that heartbeats faster than its session timeout
// stays in the group indefinitely. Existing TestHeartbeatAndSession
// Timeout below covers the eviction case; this one covers the
// "consumer is healthy" case the rebalance state machine depends
// on. Without it, a regression where heartbeats fail to reset the
// timer would silently evict every consumer in <1s.
func TestHeartbeatKeepsMemberAlivePastTimeout(t *testing.T) {
	mgr := newTestCoordinator(t, "broker-0")
	const groupID = "alive-group"

	join := mgr.JoinGroup(&api.JoinGroupRequest{
		GroupID:            groupID,
		SessionTimeoutMs:   200,
		RebalanceTimeoutMs: 100,
		ProtocolType:       "consumer",
		Protocols:          []api.JoinGroupProtocol{{Name: "range"}},
	}, "alive-client")
	if join.ErrorCode != 0 {
		t.Fatalf("Join: %d", join.ErrorCode)
	}
	mgr.SyncGroup(&api.SyncGroupRequest{
		GroupID: groupID, GenerationID: join.GenerationID, MemberID: join.MemberID,
		Assignments: []api.SyncAssignment{{MemberID: join.MemberID, Assignment: []byte("p0")}},
	})

	// Heartbeat every 50ms (a quarter of the 200ms timeout) for 600ms total
	// — well past 3× session timeout. No errCode should ever go non-zero.
	hbReq := &api.HeartbeatRequest{GroupID: groupID, GenerationID: join.GenerationID, MemberID: join.MemberID}
	deadline := time.Now().Add(600 * time.Millisecond)
	for time.Now().Before(deadline) {
		if resp := mgr.Heartbeat(hbReq); resp.ErrorCode != 0 {
			t.Errorf("heartbeat errCode=%d, want 0 (timer not resetting)", resp.ErrorCode)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestSessionTimeoutEvictsSilentMemberOnly pins the gh #19
// multi-member contract: in a 2-member group, the member that
// stops heartbeating is evicted while the other one stays. Any
// regression that evicts both (e.g. a shared timer bug) or
// neither (timer not arming) corrupts every multi-consumer group.
func TestSessionTimeoutEvictsSilentMemberOnly(t *testing.T) {
	mgr := newTestCoordinator(t, "broker-0")
	const groupID = "two-member-group"

	// Both members join concurrently — the JoinGroup state machine
	// blocks the first arriving until the second arrives or the
	// rebalance timer fires.
	var r1, r2 *api.JoinGroupResponse
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		r1 = mgr.JoinGroup(&api.JoinGroupRequest{
			GroupID: groupID, SessionTimeoutMs: 200, RebalanceTimeoutMs: 100,
			ProtocolType: "consumer", Protocols: []api.JoinGroupProtocol{{Name: "range"}},
		}, "alive")
	}()
	go func() {
		defer wg.Done()
		r2 = mgr.JoinGroup(&api.JoinGroupRequest{
			GroupID: groupID, SessionTimeoutMs: 200, RebalanceTimeoutMs: 100,
			ProtocolType: "consumer", Protocols: []api.JoinGroupProtocol{{Name: "range"}},
		}, "silent")
	}()
	wg.Wait()
	if r1.ErrorCode != 0 || r2.ErrorCode != 0 {
		t.Fatalf("Join: r1=%d r2=%d", r1.ErrorCode, r2.ErrorCode)
	}
	syncBoth(t, mgr, groupID, r1, r2)

	// r1 heartbeats; r2 stays silent. After 400ms (2× timeout)
	// only r2 should be evicted.
	for i := 0; i < 8; i++ {
		mgr.Heartbeat(&api.HeartbeatRequest{
			GroupID: groupID, GenerationID: r1.GenerationID, MemberID: r1.MemberID,
		})
		time.Sleep(50 * time.Millisecond)
	}

	// r1 should still be alive — but the eviction of r2 triggers a
	// rebalance, so r1's heartbeat returns RebalanceInProgress (27)
	// rather than 0. That's the correct transition signalling the
	// client to re-join. The wrong behaviours we're guarding against
	// are UnknownMemberId (25 — r1 was evicted too) and 0 (no
	// rebalance triggered, both still in Stable).
	r1Hb := mgr.Heartbeat(&api.HeartbeatRequest{
		GroupID: groupID, GenerationID: r1.GenerationID, MemberID: r1.MemberID,
	})
	if r1Hb.ErrorCode == 25 {
		t.Errorf("r1 (alive member) got UnknownMemberId — was evicted alongside r2")
	}
	if r1Hb.ErrorCode == 0 {
		t.Errorf("r1's heartbeat returned 0 — eviction of r2 did not trigger rebalance")
	}

	// r2 (silent) should be UnknownMemberId.
	r2Hb := mgr.Heartbeat(&api.HeartbeatRequest{
		GroupID: groupID, GenerationID: r2.GenerationID, MemberID: r2.MemberID,
	})
	if r2Hb.ErrorCode != 25 {
		t.Errorf("r2 (silent) errCode=%d, want 25 (UnknownMemberId)", r2Hb.ErrorCode)
	}
}

// TestSessionTimeoutLastMemberTransitionsToEmpty pins the
// single-member edge case: when the last member of a group times
// out, the group state machine drops to Empty rather than firing
// a rebalance for nobody. Catches a regression where the timer
// fires startRebalanceTimer on len(members)==0 — which would
// hang the group's state machine forever in PreparingRebalance.
func TestSessionTimeoutLastMemberTransitionsToEmpty(t *testing.T) {
	mgr := newTestCoordinator(t, "broker-0")
	const groupID = "lonely-group"

	join := mgr.JoinGroup(&api.JoinGroupRequest{
		GroupID: groupID, SessionTimeoutMs: 150, RebalanceTimeoutMs: 100,
		ProtocolType: "consumer", Protocols: []api.JoinGroupProtocol{{Name: "range"}},
	}, "alone")
	if join.ErrorCode != 0 {
		t.Fatalf("Join: %d", join.ErrorCode)
	}
	mgr.SyncGroup(&api.SyncGroupRequest{
		GroupID: groupID, GenerationID: join.GenerationID, MemberID: join.MemberID,
		Assignments: []api.SyncAssignment{{MemberID: join.MemberID, Assignment: []byte("p0")}},
	})

	// Wait past the timeout; member gets evicted.
	time.Sleep(300 * time.Millisecond)

	// Heartbeat post-eviction returns UnknownMemberId.
	hb := mgr.Heartbeat(&api.HeartbeatRequest{
		GroupID: groupID, GenerationID: join.GenerationID, MemberID: join.MemberID,
	})
	if hb.ErrorCode != 25 {
		t.Errorf("post-eviction heartbeat errCode=%d, want 25", hb.ErrorCode)
	}

	// A fresh JoinGroup on the same groupID succeeds at generation 1
	// (the group reset to Empty, not Dead). If the group state
	// machine had hung in PreparingRebalance, this Join would block
	// until the rebalance timer fires (~100ms from now) — the test
	// caps that with the timer-based completion.
	rejoin := mgr.JoinGroup(&api.JoinGroupRequest{
		GroupID: groupID, SessionTimeoutMs: 150, RebalanceTimeoutMs: 100,
		ProtocolType: "consumer", Protocols: []api.JoinGroupProtocol{{Name: "range"}},
	}, "rejoiner")
	if rejoin.ErrorCode != 0 {
		t.Errorf("rejoin after sole-member eviction: errCode=%d, want 0 (group should be Empty + acceptable)", rejoin.ErrorCode)
	}
	if rejoin.GenerationID < 1 {
		t.Errorf("rejoin GenerationID=%d, want >=1", rejoin.GenerationID)
	}
}

// TestSessionTimeoutTimerResetsOnEveryHeartbeat is a defense-in-
// depth check on the timer-reset logic. Issues a heartbeat at
// 0ms, 100ms, 200ms, 300ms with sessionTimeout=150ms — every
// heartbeat must reset the timer so the member survives the
// full 400ms window. Without proper timer reset, the member
// would get evicted at the 150ms mark.
func TestSessionTimeoutTimerResetsOnEveryHeartbeat(t *testing.T) {
	mgr := newTestCoordinator(t, "broker-0")
	const groupID = "reset-group"

	join := mgr.JoinGroup(&api.JoinGroupRequest{
		GroupID: groupID, SessionTimeoutMs: 150, RebalanceTimeoutMs: 100,
		ProtocolType: "consumer", Protocols: []api.JoinGroupProtocol{{Name: "range"}},
	}, "ticker")
	if join.ErrorCode != 0 {
		t.Fatalf("Join: %d", join.ErrorCode)
	}
	mgr.SyncGroup(&api.SyncGroupRequest{
		GroupID: groupID, GenerationID: join.GenerationID, MemberID: join.MemberID,
		Assignments: []api.SyncAssignment{{MemberID: join.MemberID, Assignment: []byte("p0")}},
	})

	hb := &api.HeartbeatRequest{GroupID: groupID, GenerationID: join.GenerationID, MemberID: join.MemberID}

	// Heartbeats at 0, 100, 200, 300ms (intervals of 100 < 150 timeout).
	// Each one must reset; final heartbeat at 300ms must still see 0
	// errCode despite total elapsed exceeding 2× sessionTimeout.
	for i := 0; i < 4; i++ {
		if resp := mgr.Heartbeat(hb); resp.ErrorCode != 0 {
			t.Errorf("heartbeat at %dms: errCode=%d, want 0 (timer not reset)", i*100, resp.ErrorCode)
		}
		if i < 3 {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func TestHeartbeatAndSessionTimeout(t *testing.T) {
	mgr := newTestCoordinator(t, "broker-0")
	const groupID = "timeout-group"

	req := &api.JoinGroupRequest{
		GroupID:            groupID,
		SessionTimeoutMs:   200, // 200ms for test speed
		RebalanceTimeoutMs: 100,
		ProtocolType:       "consumer",
		Protocols:          []api.JoinGroupProtocol{{Name: "range"}},
	}
	joinResp := mgr.JoinGroup(req, "client-1")
	if joinResp.ErrorCode != 0 {
		t.Fatalf("JoinGroup errCode=%d", joinResp.ErrorCode)
	}

	// Sync to reach Stable state.
	syncResp := mgr.SyncGroup(&api.SyncGroupRequest{
		GroupID:      groupID,
		GenerationID: joinResp.GenerationID,
		MemberID:     joinResp.MemberID,
		Assignments:  []api.SyncAssignment{{MemberID: joinResp.MemberID, Assignment: []byte("p0")}},
	})
	if syncResp.ErrorCode != 0 {
		t.Fatalf("SyncGroup errCode=%d", syncResp.ErrorCode)
	}

	hbReq := &api.HeartbeatRequest{
		GroupID:      groupID,
		GenerationID: joinResp.GenerationID,
		MemberID:     joinResp.MemberID,
	}
	if hbResp := mgr.Heartbeat(hbReq); hbResp.ErrorCode != 0 {
		t.Errorf("immediate Heartbeat errCode=%d, want 0", hbResp.ErrorCode)
	}

	// Wait for session timeout (200ms) + buffer.
	time.Sleep(400 * time.Millisecond)

	hbResp := mgr.Heartbeat(hbReq)
	if hbResp.ErrorCode == 0 {
		t.Error("expected non-zero errCode after session timeout")
	}
}

func TestOffsetCommitFetch(t *testing.T) {
	dir := t.TempDir()
	offsets := coordinator.NewOffsetStore(dir)
	mgr := coordinator.NewManager(context.Background(), &alwaysGroupSource{brokerID: "test"},
		func(_ string) (int32, string, int32, bool) { return 0, "localhost", 9092, true },
		offsets)

	commitReq := &api.OffsetCommitRequest{
		GroupID: "commit-group",
		Topics: []api.OffsetCommitTopic{
			{
				Name: "payments",
				Partitions: []api.OffsetCommitPartition{
					{PartitionIndex: 0, CommittedOffset: 100},
					{PartitionIndex: 1, CommittedOffset: 200},
				},
			},
		},
	}
	commitResp := mgr.OffsetCommit(commitReq)
	for _, topic := range commitResp.Topics {
		for _, p := range topic.Partitions {
			if p.ErrorCode != 0 {
				t.Errorf("OffsetCommit partition %d errCode=%d", p.PartitionIndex, p.ErrorCode)
			}
		}
	}

	fetchReq := &api.OffsetFetchRequest{
		GroupID: "commit-group",
		Topics:  []api.OffsetFetchTopic{{Name: "payments", PartitionIndexes: []int32{0, 1, 2}}},
	}
	fetchResp := mgr.OffsetFetch(fetchReq)
	if len(fetchResp.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(fetchResp.Topics))
	}
	expected := map[int32]int64{0: 100, 1: 200, 2: -1}
	for _, p := range fetchResp.Topics[0].Partitions {
		if p.CommittedOffset != expected[p.PartitionIndex] {
			t.Errorf("partition %d: got %d, want %d", p.PartitionIndex, p.CommittedOffset, expected[p.PartitionIndex])
		}
	}

	// Reload from disk and verify persistence.
	offsets2 := coordinator.NewOffsetStore(dir)
	if err := offsets2.Load("commit-group"); err != nil {
		t.Fatalf("Load: %v", err)
	}
	mgr2 := coordinator.NewManager(context.Background(), &alwaysGroupSource{brokerID: "test"},
		func(_ string) (int32, string, int32, bool) { return 0, "localhost", 9092, true }, offsets2)
	fetchResp2 := mgr2.OffsetFetch(fetchReq)
	for _, p := range fetchResp2.Topics[0].Partitions {
		if p.CommittedOffset != expected[p.PartitionIndex] {
			t.Errorf("after reload: partition %d got %d, want %d", p.PartitionIndex, p.CommittedOffset, expected[p.PartitionIndex])
		}
	}
}

// TestManagerSetGroupAssignmentSourceHotSwap pins gh #92's load-
// bearing wiring: the manager starts with one source (the bootstrap
// LocalGroupSource in production; alwaysGroupSource in tests) and
// the cluster_runtime swaps in broker.Coordinator after it boots.
// The setter is the channel that flip happens through; without it
// production would be stuck on always-true and the v0.1.51 read-
// side filter would be a no-op.
func TestManagerSetGroupAssignmentSourceHotSwap(t *testing.T) {
	mgr := coordinator.NewManager(context.Background(), &alwaysGroupSource{brokerID: "test"},
		func(_ string) (int32, string, int32, bool) { return 0, "localhost", 9092, true },
		coordinator.NewOffsetStore(t.TempDir()))

	// alwaysGroupSource: every group is owned by this broker.
	r := mgr.JoinGroup(fastJoin("hotswap", "consumer"), "c")
	if r.ErrorCode != 0 {
		t.Fatalf("first Join (alwaysGroupSource): errCode=%d", r.ErrorCode)
	}

	// Swap in a never-source: this broker now owns nothing.
	mgr.SetGroupAssignmentSource(&neverGroupSource{})

	// Subsequent JoinGroup must be rejected with NOT_COORDINATOR
	// — proves the swap took effect on the live hot path.
	r2 := mgr.JoinGroup(fastJoin("post-swap", "consumer"), "c2")
	if r2.ErrorCode != 16 {
		t.Errorf("post-swap Join errCode=%d, want 16 (NOT_COORDINATOR) — swap did not take effect on JoinGroup", r2.ErrorCode)
	}

	// And FindCoordinator on the now-not-coordinator broker
	// returns CoordinatorNotAvailable so the client retries.
	fc := mgr.FindCoordinator(&api.FindCoordinatorRequest{Key: "any-group"})
	if fc.ErrorCode != 15 {
		t.Errorf("post-swap FindCoordinator errCode=%d, want 15 (CoordinatorNotAvailable)", fc.ErrorCode)
	}
}

// hashRoutingSource is a coordinator.GroupAssignmentSource that
// hash-routes groups to a fixed broker set. Mirrors what
// broker.Coordinator does in production via assignment.json's
// brokers list. Used to drive the manager's isCoordinator gate
// through Apache-Kafka-style deterministic routing without
// spinning up a full Coordinator + assignment file.
type hashRoutingSource struct {
	selfBrokerID string
	brokers      []string
}

func (h *hashRoutingSource) OwnsGroup(groupID string) bool {
	owner := h.coord(groupID)
	return owner == h.selfBrokerID
}
func (h *hashRoutingSource) GroupCoordinator(groupID string) (string, bool) {
	owner := h.coord(groupID)
	return owner, owner != ""
}
func (h *hashRoutingSource) coord(groupID string) string {
	if len(h.brokers) == 0 {
		return ""
	}
	// Use the same FNV-1a hash + modulo as production
	// (internal/broker/group_hash.go). Re-implemented locally so
	// this test doesn't depend on the broker package — keeps
	// integration tests self-contained.
	const offset32 = 2166136261
	const prime32 = 16777619
	hash := uint32(offset32)
	for i := 0; i < len(groupID); i++ {
		hash ^= uint32(groupID[i])
		hash *= prime32
	}
	return h.brokers[int(hash%uint32(len(h.brokers)))]
}

// TestJoinGroupHashRoutingExactlyOneOwner pins the gh #92 chain:
// with broker.Coordinator-shaped GroupAssignmentSource (deterministic
// hash), exactly ONE of three Manager instances accepts a JoinGroup
// for any given group. The other two return NOT_COORDINATOR.
// Without hash routing, either zero (chicken-and-egg deadlock) or
// all three (LocalGroupSource leak) accept — the live cluster's
// behaviour is determined by THIS contract.
func TestJoinGroupHashRoutingExactlyOneOwner(t *testing.T) {
	const groupID = "hash-routed-group"
	brokers := []string{"skafka-0", "skafka-1", "skafka-2"}

	// Build one Manager per broker, each with its own self-aware
	// hashRoutingSource. This is the test analogue of three
	// brokers each running their own coordinator.Manager wired
	// to their own broker.Coordinator.
	managers := make(map[string]*coordinator.Manager, 3)
	for _, b := range brokers {
		src := &hashRoutingSource{selfBrokerID: b, brokers: brokers}
		mgr := coordinator.NewManager(context.Background(), src,
			func(_ string) (int32, string, int32, bool) { return 0, "localhost", 9092, true },
			coordinator.NewOffsetStore(t.TempDir()))
		managers[b] = mgr
	}

	owners := 0
	var ownerID string
	for id, mgr := range managers {
		r := mgr.JoinGroup(fastJoin(groupID, "consumer"), "c")
		if r.ErrorCode == 0 {
			owners++
			ownerID = id
		} else if r.ErrorCode != 16 {
			t.Errorf("%s: unexpected errCode=%d (want 0=accept or 16=NOT_COORDINATOR)", id, r.ErrorCode)
		}
	}
	if owners != 1 {
		t.Errorf("JoinGroup accepted on %d brokers, want 1", owners)
	}
	// Bonus: the broker that accepted matches the standalone hash
	// — proves the manager's gate uses the source's answer
	// faithfully.
	expected := (&hashRoutingSource{brokers: brokers}).coord(groupID)
	if ownerID != expected {
		t.Errorf("acceptor=%s, but hash predicts %s", ownerID, expected)
	}
}

// TestFindCoordinatorHashFallthroughEndToEnd pins the
// FindCoordinator → groupSrc.GroupCoordinator chain. Production
// wires broker.Coordinator as the source; this test uses
// hashRoutingSource (the same answer shape) to drive the Manager
// without a full broker.Coordinator. The contract: FindCoordinator
// returns the hashed broker for an unknown group, with errCode=0.
// Without this end-to-end coverage, a refactor that breaks the
// FindCoordinator handler's lookupOne path could ship without the
// unit-level Coordinator tests catching it.
func TestFindCoordinatorHashFallthroughEndToEnd(t *testing.T) {
	brokers := []string{"skafka-0", "skafka-1", "skafka-2"}
	src := &hashRoutingSource{selfBrokerID: "skafka-1", brokers: brokers}
	mgr := coordinator.NewManager(context.Background(), src,
		// lookupBroker translates broker IDs to advertised host:port.
		func(id string) (int32, string, int32, bool) {
			switch id {
			case "skafka-0":
				return 0, "skafka-0.svc", 9092, true
			case "skafka-1":
				return 1, "skafka-1.svc", 9092, true
			case "skafka-2":
				return 2, "skafka-2.svc", 9092, true
			}
			return 0, "", 0, false
		},
		coordinator.NewOffsetStore(t.TempDir()))

	resp := mgr.FindCoordinator(&api.FindCoordinatorRequest{Key: "fresh-group"})
	if resp.ErrorCode != 0 {
		t.Fatalf("FindCoordinator errCode=%d, want 0 (hash fallthrough should resolve unknown groups)", resp.ErrorCode)
	}
	expected := src.coord("fresh-group")
	wantHost := expected + ".svc"
	if resp.Host != wantHost {
		t.Errorf("FindCoordinator host=%q, want %q (hash routes to %q)", resp.Host, wantHost, expected)
	}
}

// TestListGroupsHidesNonCoordinatorEntries pins the read-side
// filter that fixes the v0.1.49 verification bug: a broker that
// has a stale m.groups entry (e.g. left over from a previous
// "we own this" window) must NOT advertise it via ListGroups
// once the cluster assignment has moved the group elsewhere. The
// orphan sweep eventually drops the entry on the next assignment
// change, but ListGroups is queried independently of changes —
// the filter keeps the union of broker responses correct
// even before the sweep fires.
func TestListGroupsHidesNonCoordinatorEntries(t *testing.T) {
	mgr := newTestCoordinator(t, "broker-A")

	// Bring the group to Empty by joining + leaving.
	r := mgr.JoinGroup(fastJoin("vis-test", "consumer"), "c1")
	if r.ErrorCode != 0 {
		t.Fatalf("Join: %d", r.ErrorCode)
	}
	mgr.SyncGroup(&api.SyncGroupRequest{
		GroupID: "vis-test", GenerationID: r.GenerationID, MemberID: r.MemberID,
		Assignments: []api.SyncAssignment{{MemberID: r.MemberID, Assignment: []byte("p0")}},
	})
	mgr.LeaveGroup(&api.LeaveGroupRequest{GroupID: "vis-test", MemberID: r.MemberID})

	// alwaysGroupSource (the test fixture) returns true for every
	// OwnsGroup call, so the group lists.
	listed := mgr.ListGroups(&api.ListGroupsRequest{})
	found := false
	for _, g := range listed.Groups {
		if g.GroupID == "vis-test" {
			found = true
		}
	}
	if !found {
		t.Errorf("ListGroups should include owned group; got %+v", listed.Groups)
	}

	// Now flip ownership: the broker no longer owns the group.
	// Subsequent ListGroups must NOT advertise it even though
	// m.groups still has the entry until the orphan sweep runs.
	flip := &flippableGroupSource{owns: false, brokerID: "broker-A"}
	mgr2 := coordinator.NewManager(context.Background(), flip,
		func(_ string) (int32, string, int32, bool) { return 0, "localhost", 9092, true },
		coordinator.NewOffsetStore(t.TempDir()))

	// Manually populate m.groups via a JoinGroup that succeeds (flip
	// is currently true → coordinator) then flip false to simulate
	// the cluster reassigning the group elsewhere. Use the
	// recordingMgr-style pattern.
	flip.SetOwns(true)
	r2 := mgr2.JoinGroup(fastJoin("flipped", "consumer"), "c2")
	if r2.ErrorCode != 0 {
		t.Fatalf("setup Join: %d", r2.ErrorCode)
	}
	flip.SetOwns(false)

	listed2 := mgr2.ListGroups(&api.ListGroupsRequest{})
	for _, g := range listed2.Groups {
		if g.GroupID == "flipped" {
			t.Errorf("ListGroups leaked stale entry for non-owned group; got %+v", listed2.Groups)
		}
	}
}

// TestDescribeGroupsReportsDeadForNonOwnedGroup pins the symmetric
// behaviour for the AdminClient.describeConsumerGroups path: a
// query for a group on a broker that doesn't own it returns
// state="Dead" instead of leaking the broker's stale view.
func TestDescribeGroupsReportsDeadForNonOwnedGroup(t *testing.T) {
	flip := &flippableGroupSource{owns: true, brokerID: "broker-A"}
	mgr := coordinator.NewManager(context.Background(), flip,
		func(_ string) (int32, string, int32, bool) { return 0, "localhost", 9092, true },
		coordinator.NewOffsetStore(t.TempDir()))

	r := mgr.JoinGroup(fastJoin("desc-test", "consumer"), "c1")
	if r.ErrorCode != 0 {
		t.Fatalf("Join: %d", r.ErrorCode)
	}

	flip.SetOwns(false)
	resp := mgr.DescribeGroups(&api.DescribeGroupsRequest{Groups: []string{"desc-test"}})
	if len(resp.Groups) != 1 {
		t.Fatalf("expected 1 group result, got %+v", resp.Groups)
	}
	if resp.Groups[0].GroupState != "Dead" {
		t.Errorf("non-owned describe state=%q, want Dead", resp.Groups[0].GroupState)
	}
}

// flippableGroupSource is a coordinator.GroupAssignmentSource whose
// OwnsGroup return can be toggled at runtime — needed to simulate
// "broker used to own this, now doesn't" without spinning up the
// full assignment file plumbing.
type flippableGroupSource struct {
	mu       sync.Mutex
	owns     bool
	brokerID string
}

func (f *flippableGroupSource) OwnsGroup(_ string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.owns
}
func (f *flippableGroupSource) GroupCoordinator(_ string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.owns {
		return "", false
	}
	return f.brokerID, true
}
func (f *flippableGroupSource) SetOwns(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.owns = v
}

// TestDeleteGroupsEmptySucceeds: an empty group (no active members,
// just committed offsets from a prior session) is the AdminClient's
// happy path. The script audit's gh #89 reproducer follows this
// exact flow: consume into a new group, all members close, then
// admin-deletes it.
func TestDeleteGroupsEmptySucceeds(t *testing.T) {
	dir := t.TempDir()
	offsets := coordinator.NewOffsetStore(dir)
	mgr := coordinator.NewManager(context.Background(), &alwaysGroupSource{brokerID: "test"},
		func(_ string) (int32, string, int32, bool) { return 0, "localhost", 9092, true },
		offsets)
	const groupID = "del-empty"

	// Bring the group to Empty by joining + leaving cleanly.
	r := mgr.JoinGroup(fastJoin(groupID, "consumer"), "c1")
	if r.ErrorCode != 0 {
		t.Fatalf("Join: %d", r.ErrorCode)
	}
	mgr.SyncGroup(&api.SyncGroupRequest{
		GroupID: groupID, GenerationID: r.GenerationID, MemberID: r.MemberID,
		Assignments: []api.SyncAssignment{{MemberID: r.MemberID, Assignment: []byte("p0")}},
	})
	// Commit an offset so we can also assert it's deleted.
	mgr.OffsetCommit(&api.OffsetCommitRequest{
		GroupID: groupID, MemberID: r.MemberID, GenerationID: r.GenerationID,
		Topics: []api.OffsetCommitTopic{{Name: "t",
			Partitions: []api.OffsetCommitPartition{{PartitionIndex: 0, CommittedOffset: 100}},
		}},
	})
	if leave := mgr.LeaveGroup(&api.LeaveGroupRequest{GroupID: groupID, MemberID: r.MemberID}); leave.ErrorCode != 0 {
		t.Fatalf("Leave: %d", leave.ErrorCode)
	}

	resp := mgr.DeleteGroups(&api.DeleteGroupsRequest{GroupNames: []string{groupID}})
	if len(resp.Results) != 1 || resp.Results[0].ErrorCode != 0 {
		t.Errorf("Empty-group delete results=%+v, want errCode=0", resp.Results)
	}

	// Offset file must be gone — a fresh OffsetFetch returns -1.
	fetched := mgr.OffsetFetch(&api.OffsetFetchRequest{
		GroupID: groupID,
		Topics:  []api.OffsetFetchTopic{{Name: "t", PartitionIndexes: []int32{0}}},
	})
	if got := fetched.Topics[0].Partitions[0].CommittedOffset; got != -1 {
		t.Errorf("post-delete OffsetFetch=%d, want -1 (offset file should have been removed)", got)
	}
}

// TestDeleteGroupsRejectsNonEmpty: a group with active members
// must return NON_EMPTY_GROUP (67) — Kafka's strict semantics.
// Without this guard, an admin-delete during normal traffic would
// silently drop offsets out from under live consumers.
func TestDeleteGroupsRejectsNonEmpty(t *testing.T) {
	mgr := newTestCoordinator(t, "broker-0")
	const groupID = "del-busy"

	r := mgr.JoinGroup(fastJoin(groupID, "consumer"), "c1")
	if r.ErrorCode != 0 {
		t.Fatalf("Join: %d", r.ErrorCode)
	}
	mgr.SyncGroup(&api.SyncGroupRequest{
		GroupID: groupID, GenerationID: r.GenerationID, MemberID: r.MemberID,
		Assignments: []api.SyncAssignment{{MemberID: r.MemberID, Assignment: []byte("p0")}},
	})
	// State is Stable (not Empty/Dead) — delete must reject.

	resp := mgr.DeleteGroups(&api.DeleteGroupsRequest{GroupNames: []string{groupID}})
	if len(resp.Results) != 1 {
		t.Fatalf("results=%+v", resp.Results)
	}
	if resp.Results[0].ErrorCode != 67 {
		t.Errorf("active-group delete errCode=%d, want 67 (NON_EMPTY_GROUP)", resp.Results[0].ErrorCode)
	}
}

// TestDeleteGroupsUnknownGroupReturns69 covers the AdminClient's
// "delete a group that never existed" case — typical when a script
// retries cleanup. Returns 69 (GROUP_ID_NOT_FOUND); the AdminClient
// surfaces this as GroupIdNotFoundException.
func TestDeleteGroupsUnknownGroupReturns69(t *testing.T) {
	mgr := newTestCoordinator(t, "broker-0")
	resp := mgr.DeleteGroups(&api.DeleteGroupsRequest{GroupNames: []string{"never-existed"}})
	if len(resp.Results) != 1 || resp.Results[0].ErrorCode != 69 {
		t.Errorf("unknown group results=%+v, want errCode=69", resp.Results)
	}
}

// TestDeleteGroupsBatchHandlesPerGroup verifies one-bad-doesn't-
// poison-the-others: a batch delete with mixed Empty / Stable /
// Unknown groups returns per-group results with the right error
// for each, not a single all-or-nothing failure.
func TestDeleteGroupsBatchHandlesPerGroup(t *testing.T) {
	dir := t.TempDir()
	offsets := coordinator.NewOffsetStore(dir)
	mgr := coordinator.NewManager(context.Background(), &alwaysGroupSource{brokerID: "test"},
		func(_ string) (int32, string, int32, bool) { return 0, "localhost", 9092, true },
		offsets)

	// Empty group "a" — we just commit an offset and leave.
	r := mgr.JoinGroup(fastJoin("a", "consumer"), "c1")
	mgr.SyncGroup(&api.SyncGroupRequest{
		GroupID: "a", GenerationID: r.GenerationID, MemberID: r.MemberID,
		Assignments: []api.SyncAssignment{{MemberID: r.MemberID, Assignment: []byte("p0")}},
	})
	mgr.LeaveGroup(&api.LeaveGroupRequest{GroupID: "a", MemberID: r.MemberID})

	// Stable group "b" with an active member.
	r2 := mgr.JoinGroup(fastJoin("b", "consumer"), "c2")
	mgr.SyncGroup(&api.SyncGroupRequest{
		GroupID: "b", GenerationID: r2.GenerationID, MemberID: r2.MemberID,
		Assignments: []api.SyncAssignment{{MemberID: r2.MemberID, Assignment: []byte("p0")}},
	})

	resp := mgr.DeleteGroups(&api.DeleteGroupsRequest{GroupNames: []string{"a", "b", "c"}})
	if len(resp.Results) != 3 {
		t.Fatalf("batch result count=%d, want 3 (one per group)", len(resp.Results))
	}
	want := map[string]int16{"a": 0, "b": 67, "c": 69}
	for _, r := range resp.Results {
		if r.ErrorCode != want[r.GroupID] {
			t.Errorf("%s: errCode=%d, want %d", r.GroupID, r.ErrorCode, want[r.GroupID])
		}
	}
}

// TestDeleteGroupsNotCoordinatorReturns16 covers the multi-broker
// case the script audit will hit on a 3-broker cluster: the
// AdminClient sends DeleteGroups to whichever broker it thinks is
// coordinator; if that broker has been reassigned, return 16
// (NOT_COORDINATOR) so the client retries FindCoordinator.
func TestDeleteGroupsNotCoordinatorReturns16(t *testing.T) {
	// notCoordinator says "no" to OwnsGroup for everything.
	src := &neverGroupSource{}
	mgr := coordinator.NewManager(context.Background(), src,
		func(_ string) (int32, string, int32, bool) { return 0, "localhost", 9092, true },
		coordinator.NewOffsetStore(t.TempDir()))

	resp := mgr.DeleteGroups(&api.DeleteGroupsRequest{GroupNames: []string{"any"}})
	if len(resp.Results) != 1 || resp.Results[0].ErrorCode != 16 {
		t.Errorf("non-coordinator results=%+v, want errCode=16", resp.Results)
	}
}

// neverGroupSource is the inverse of alwaysGroupSource — a broker
// that owns NO groups, used to exercise the NOT_COORDINATOR path.
type neverGroupSource struct{}

func (*neverGroupSource) OwnsGroup(_ string) bool                      { return false }
func (*neverGroupSource) GroupCoordinator(_ string) (string, bool)     { return "", false }

// TestConsumerCleanShutdownPersistsOffsets is the broker-side analogue
// of what kafka-verifiable-consumer.sh exercises end-to-end (gh #88):
// after consuming N records the script's consumer commits offsets,
// sends LeaveGroup, prints "shutdown_complete" and exits 0. None of
// those steps individually is novel — TestOffsetCommitFetch and
// TestLeaveGroupTriggersRebalance already pin the per-call success
// codes — but the SEQUENCE matters: a regression where LeaveGroup
// silently flushes uncommitted state, or where OffsetCommit ignores
// commits sent during a generation that's about to rebalance, would
// make the next consumer in the same group re-read records from
// scratch (the verifiable-consumer's "consumed > expected" failure).
//
// Pattern: Join → Sync → OffsetCommit → LeaveGroup → fresh consumer
// joins same group → OffsetFetch returns committed offsets, NOT -1.
func TestConsumerCleanShutdownPersistsOffsets(t *testing.T) {
	dir := t.TempDir()
	offsets := coordinator.NewOffsetStore(dir)
	mgr := coordinator.NewManager(context.Background(), &alwaysGroupSource{brokerID: "test"},
		func(_ string) (int32, string, int32, bool) { return 0, "localhost", 9092, true },
		offsets)
	const groupID = "verifiable-cg"

	join := mgr.JoinGroup(fastJoin(groupID, "consumer"), "verif-1")
	if join.ErrorCode != 0 {
		t.Fatalf("JoinGroup errCode=%d", join.ErrorCode)
	}
	sync := mgr.SyncGroup(&api.SyncGroupRequest{
		GroupID: groupID, GenerationID: join.GenerationID, MemberID: join.MemberID,
		Assignments: []api.SyncAssignment{{MemberID: join.MemberID, Assignment: []byte("p0")}},
	})
	if sync.ErrorCode != 0 {
		t.Fatalf("SyncGroup errCode=%d", sync.ErrorCode)
	}

	// Consumer "consumed" 1000 records and is now committing the next
	// fetch position (offset 1000) right before shutdown.
	commit := mgr.OffsetCommit(&api.OffsetCommitRequest{
		GroupID:      groupID,
		MemberID:     join.MemberID,
		GenerationID: join.GenerationID,
		Topics: []api.OffsetCommitTopic{
			{Name: "verif-topic", Partitions: []api.OffsetCommitPartition{
				{PartitionIndex: 0, CommittedOffset: 1000},
			}},
		},
	})
	for _, top := range commit.Topics {
		for _, p := range top.Partitions {
			if p.ErrorCode != 0 {
				t.Fatalf("OffsetCommit p%d errCode=%d (clean-shutdown path must not fail mid-generation)",
					p.PartitionIndex, p.ErrorCode)
			}
		}
	}

	// Consumer's "shutdown_complete" trigger: LeaveGroup. This must not
	// asynchronously discard the just-committed offset.
	if leave := mgr.LeaveGroup(&api.LeaveGroupRequest{GroupID: groupID, MemberID: join.MemberID}); leave.ErrorCode != 0 {
		t.Fatalf("LeaveGroup errCode=%d", leave.ErrorCode)
	}

	// A fresh consumer joins the same group — the verifiable-consumer's
	// "second run reads only NEW records" expectation. OffsetFetch must
	// see 1000, not -1 (no committed offset).
	fetch := mgr.OffsetFetch(&api.OffsetFetchRequest{
		GroupID: groupID,
		Topics:  []api.OffsetFetchTopic{{Name: "verif-topic", PartitionIndexes: []int32{0}}},
	})
	if len(fetch.Topics) != 1 || len(fetch.Topics[0].Partitions) != 1 {
		t.Fatalf("OffsetFetch shape: %+v", fetch.Topics)
	}
	if got := fetch.Topics[0].Partitions[0].CommittedOffset; got != 1000 {
		t.Errorf("CommittedOffset after clean shutdown = %d, want 1000", got)
	}
}

func TestLeaveGroupTriggersRebalance(t *testing.T) {
	mgr := newTestCoordinator(t, "broker-0")
	const groupID = "leave-group"

	var r1, r2 *api.JoinGroupResponse
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); r1 = mgr.JoinGroup(fastJoin(groupID, "consumer"), "c1") }()
	go func() { defer wg.Done(); r2 = mgr.JoinGroup(fastJoin(groupID, "consumer"), "c2") }()
	wg.Wait()

	if r1.ErrorCode != 0 || r2.ErrorCode != 0 {
		t.Fatalf("JoinGroup: %d / %d", r1.ErrorCode, r2.ErrorCode)
	}

	// Both members must sync concurrently (non-leader blocks until leader provides assignments).
	syncBoth(t, mgr, groupID, r1, r2)

	// Member 1 leaves.
	leaveResp := mgr.LeaveGroup(&api.LeaveGroupRequest{GroupID: groupID, MemberID: r1.MemberID})
	if leaveResp.ErrorCode != 0 {
		t.Errorf("LeaveGroup errCode=%d", leaveResp.ErrorCode)
	}

	// Member 2 should get REBALANCE_IN_PROGRESS on heartbeat.
	hbResp := mgr.Heartbeat(&api.HeartbeatRequest{
		GroupID:      groupID,
		GenerationID: r2.GenerationID,
		MemberID:     r2.MemberID,
	})
	if hbResp.ErrorCode == 0 {
		t.Error("expected REBALANCE_IN_PROGRESS after member left, got 0")
	}
}
