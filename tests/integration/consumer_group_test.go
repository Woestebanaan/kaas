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

	resp := mgr.JoinGroup(fastJoin("test-group", "consumer"), 3, "test-client")
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
	resp := mgr.JoinGroup(req, 3, "client-1")
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
	go func() { defer wg.Done(); r1 = mgr.JoinGroup(fastJoin(groupID, "consumer"), 3, "client-1") }()
	go func() { defer wg.Done(); r2 = mgr.JoinGroup(fastJoin(groupID, "consumer"), 3, "client-2") }()
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

	joinResp := mgr.JoinGroup(fastJoin(groupID, "consumer"), 3, "client-1")
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
	}, 3, "alive-client")
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
		}, 3, "alive")
	}()
	go func() {
		defer wg.Done()
		r2 = mgr.JoinGroup(&api.JoinGroupRequest{
			GroupID: groupID, SessionTimeoutMs: 200, RebalanceTimeoutMs: 100,
			ProtocolType: "consumer", Protocols: []api.JoinGroupProtocol{{Name: "range"}},
		}, 3, "silent")
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
	}, 3, "alone")
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
	}, 3, "rejoiner")
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
	}, 3, "ticker")
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
	joinResp := mgr.JoinGroup(req, 3, "client-1")
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
	r := mgr.JoinGroup(fastJoin("hotswap", "consumer"), 3, "c")
	if r.ErrorCode != 0 {
		t.Fatalf("first Join (alwaysGroupSource): errCode=%d", r.ErrorCode)
	}

	// Swap in a never-source: this broker now owns nothing.
	mgr.SetGroupAssignmentSource(&neverGroupSource{})

	// Subsequent JoinGroup must be rejected with NOT_COORDINATOR
	// — proves the swap took effect on the live hot path.
	r2 := mgr.JoinGroup(fastJoin("post-swap", "consumer"), 3, "c2")
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
		r := mgr.JoinGroup(fastJoin(groupID, "consumer"), 3, "c")
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
	r := mgr.JoinGroup(fastJoin("vis-test", "consumer"), 3, "c1")
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
	r2 := mgr2.JoinGroup(fastJoin("flipped", "consumer"), 3, "c2")
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

	r := mgr.JoinGroup(fastJoin("desc-test", "consumer"), 3, "c1")
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
	r := mgr.JoinGroup(fastJoin(groupID, "consumer"), 3, "c1")
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

	r := mgr.JoinGroup(fastJoin(groupID, "consumer"), 3, "c1")
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
	r := mgr.JoinGroup(fastJoin("a", "consumer"), 3, "c1")
	mgr.SyncGroup(&api.SyncGroupRequest{
		GroupID: "a", GenerationID: r.GenerationID, MemberID: r.MemberID,
		Assignments: []api.SyncAssignment{{MemberID: r.MemberID, Assignment: []byte("p0")}},
	})
	mgr.LeaveGroup(&api.LeaveGroupRequest{GroupID: "a", MemberID: r.MemberID})

	// Stable group "b" with an active member.
	r2 := mgr.JoinGroup(fastJoin("b", "consumer"), 3, "c2")
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

	join := mgr.JoinGroup(fastJoin(groupID, "consumer"), 3, "verif-1")
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
	go func() { defer wg.Done(); r1 = mgr.JoinGroup(fastJoin(groupID, "consumer"), 3, "c1") }()
	go func() { defer wg.Done(); r2 = mgr.JoinGroup(fastJoin(groupID, "consumer"), 3, "c2") }()
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

// ---------------------------------------------------------------------------
// gh #98 regression tests
// ---------------------------------------------------------------------------

// TestGh98LeaderSessionExpiryDoesNotDeadlock pins divergence #1 +
// #3: when the pending leader (joinWaiters[0]) gets evicted by
// session timeout mid-rebalance, the next member's join goroutine
// must still receive a real response — not deadlock on <-ch.
//
// Pre-fix the heartbeat AfterFunc deleted from g.members but left
// g.joinWaiters intact, so:
//   - the dead leader's join() goroutine sat on <-ch forever (leak)
//   - selectProtocol's `members[waiters[0].memberID]` was nil so the
//     next completeRebalance returned protocolName=""
//
// This test:
//   1. Starts member A with a tiny session.timeout.ms (50ms) + slow
//      rebalance timeout (1s) so A registers, becomes leader, then
//      times out before completion.
//   2. Starts member B 200ms later (after A's session timer has
//      fired).
//   3. Asserts B gets a non-error JoinGroup response with itself as
//      leader and a non-empty protocolName.
func TestGh98LeaderSessionExpiryDoesNotDeadlock(t *testing.T) {
	mgr := newTestCoordinator(t, "broker-0")
	const groupID = "expiry-leader-group"

	// Member A: short session timer; if it doesn't heartbeat, the
	// AfterFunc will fire and remove from members + waiters.
	aDone := make(chan *api.JoinGroupResponse, 1)
	go func() {
		aDone <- mgr.JoinGroup(&api.JoinGroupRequest{
			GroupID:            groupID,
			SessionTimeoutMs:   50,
			RebalanceTimeoutMs: 1000,
			ProtocolType:       "consumer",
			Protocols:          []api.JoinGroupProtocol{{Name: "range", Metadata: []byte("a")}},
		}, 3, "client-a")
	}()

	// Wait long enough for A's heartbeat AfterFunc to fire (50ms +
	// scheduling slack). At this point removeMember has drained the
	// waiter slot and aDone has the synthetic UNKNOWN_MEMBER_ID resp.
	time.Sleep(150 * time.Millisecond)

	// A must have received its synthetic UNKNOWN_MEMBER_ID (NOT
	// blocked — that was the bug).
	select {
	case respA := <-aDone:
		if respA.ErrorCode != 25 { // UNKNOWN_MEMBER_ID
			t.Errorf("evicted member A: ErrorCode=%d, want 25 (UNKNOWN_MEMBER_ID)", respA.ErrorCode)
		}
	case <-time.After(time.Second):
		t.Fatal("evicted member A's join() goroutine deadlocked on <-ch")
	}

	// Member B: starts AFTER A has been evicted. B should successfully
	// rebalance with itself as leader.
	respB := mgr.JoinGroup(&api.JoinGroupRequest{
		GroupID:            groupID,
		SessionTimeoutMs:   30_000,
		RebalanceTimeoutMs: 200,
		ProtocolType:       "consumer",
		Protocols:          []api.JoinGroupProtocol{{Name: "range", Metadata: []byte("b")}},
	}, 3, "client-b")
	if respB.ErrorCode != 0 {
		t.Fatalf("member B Join: ErrorCode=%d, want 0", respB.ErrorCode)
	}
	if respB.ProtocolName != "range" {
		t.Errorf("member B ProtocolName=%q, want \"range\" (selectProtocol must not return empty after leader eviction)", respB.ProtocolName)
	}
	if respB.Leader != respB.MemberID {
		t.Errorf("member B Leader=%q, MemberID=%q (B should be the new leader)", respB.Leader, respB.MemberID)
	}
}

// TestGh98HeartbeatEmptyGroupReturnsUnknownMemberID pins divergence
// #7: a Heartbeat against a group with no members must return
// UNKNOWN_MEMBER_ID (25), not ErrNone (0). Pre-fix skafka's switch
// had no Empty case so the function fell through to ErrNone, letting
// disconnected clients silently keep heartbeating to a vanished group.
func TestGh98HeartbeatEmptyGroupReturnsUnknownMemberID(t *testing.T) {
	mgr := newTestCoordinator(t, "broker-0")
	const groupID = "empty-group"

	// Drive the group to state Empty: join + leave a single member.
	join := mgr.JoinGroup(fastJoin(groupID, "consumer"), 3, "tmp")
	if join.ErrorCode != 0 {
		t.Fatalf("setup join: %d", join.ErrorCode)
	}
	mgr.SyncGroup(&api.SyncGroupRequest{
		GroupID: groupID, GenerationID: join.GenerationID, MemberID: join.MemberID,
		Assignments: []api.SyncAssignment{{MemberID: join.MemberID, Assignment: []byte("p0")}},
	})
	mgr.LeaveGroup(&api.LeaveGroupRequest{GroupID: groupID, Members: []api.LeaveMember{{MemberID: join.MemberID}}})

	// Heartbeat from any memberID into the now-Empty group.
	hb := mgr.Heartbeat(&api.HeartbeatRequest{
		GroupID:      groupID,
		GenerationID: join.GenerationID,
		MemberID:     join.MemberID,
	})
	if hb.ErrorCode != 25 {
		t.Errorf("Heartbeat against Empty group: ErrorCode=%d, want 25 (UNKNOWN_MEMBER_ID)", hb.ErrorCode)
	}
}

// TestGh98Kip394FirstJoinReturnsMemberIDRequired pins divergence #2:
// at JoinGroup v4+, a dynamic member (no GroupInstanceID) joining
// with empty memberID gets a freshly-assigned ID back with
// ErrMemberIDRequired (79) and the broker does NOT trigger a
// rebalance. The client retries with the assigned ID; only the
// retry counts toward the rebalance.
//
// Without this, a network-blipped client that retries JoinGroup
// with empty memberID ends up registered TWICE in g.members on
// consecutive attempts — duplicate-member problem that amplifies
// gh #98 #1's leader-session-expiry race.
func TestGh98Kip394FirstJoinReturnsMemberIDRequired(t *testing.T) {
	mgr := newTestCoordinator(t, "broker-0")

	resp := mgr.JoinGroup(fastJoin("kip394-group", "consumer"), 4, "client-1")
	if resp.ErrorCode != 79 {
		t.Errorf("first join: ErrorCode=%d, want 79 (MEMBER_ID_REQUIRED)", resp.ErrorCode)
	}
	if resp.MemberID == "" {
		t.Error("first join: MemberID=\"\", expected an assigned ID for retry")
	}
	if resp.GenerationID != 0 {
		t.Errorf("first join: GenerationID=%d, want 0 (no rebalance triggered yet)", resp.GenerationID)
	}
}

// TestGh98Kip394SecondJoinTriggersRebalance covers the retry path:
// the client takes the assigned memberID from MEMBER_ID_REQUIRED
// and re-joins. That re-join goes through the normal rebalance
// path and returns a real generation + protocol.
func TestGh98Kip394SecondJoinTriggersRebalance(t *testing.T) {
	mgr := newTestCoordinator(t, "broker-0")
	groupID := "kip394-retry-group"

	// First join: MEMBER_ID_REQUIRED.
	first := mgr.JoinGroup(fastJoin(groupID, "consumer"), 4, "client-1")
	if first.ErrorCode != 79 {
		t.Fatalf("first join: ErrorCode=%d, want 79", first.ErrorCode)
	}
	assigned := first.MemberID

	// Second join with the assigned ID: should rebalance + succeed.
	req := fastJoin(groupID, "consumer")
	req.MemberID = assigned
	second := mgr.JoinGroup(req, 4, "client-1")
	if second.ErrorCode != 0 {
		t.Fatalf("second join: ErrorCode=%d, want 0", second.ErrorCode)
	}
	if second.MemberID != assigned {
		t.Errorf("second join: MemberID=%q, want assigned %q", second.MemberID, assigned)
	}
	if second.GenerationID < 1 {
		t.Errorf("second join: GenerationID=%d, want >=1 (rebalance must have completed)", second.GenerationID)
	}
	if second.ProtocolName != "range" {
		t.Errorf("second join: ProtocolName=%q, want \"range\"", second.ProtocolName)
	}
}

// TestGh98Kip394V3ClientUsesLegacyPath: a JoinGroup v3 (or any
// version below 4) keeps the pre-KIP-394 inline-memberID-assignment
// flow even when memberID is empty. Without this branch, every
// existing v0-v3 client (including older test fixtures) would
// suddenly start receiving MEMBER_ID_REQUIRED instead of completing
// the join.
func TestGh98Kip394V3ClientUsesLegacyPath(t *testing.T) {
	mgr := newTestCoordinator(t, "broker-0")

	resp := mgr.JoinGroup(fastJoin("kip394-legacy", "consumer"), 3, "client-old")
	if resp.ErrorCode != 0 {
		t.Errorf("v3 first join: ErrorCode=%d, want 0 (legacy inline-memberID path)", resp.ErrorCode)
	}
	if resp.MemberID == "" {
		t.Error("v3 first join: MemberID=\"\", expected inline assignment")
	}
}

// TestGh98SyncGroupResponseEchoesProtocolFields pins a contract
// the gh #98 #6 wire-encoder change surfaced as a real bug:
// skafka's (g *group).sync() never populated ProtocolType /
// ProtocolName in the SyncGroupResponse. Pre-#98-#6 the nullable
// encoder wrote those empty strings as null on the wire and the
// Java client treated null as "absent — trust my own state".
// Post-#98-#6 the non-nullable encoder writes them as empty
// strings, which the Java client validates and rejects with
// InconsistentGroupProtocolException ("received  but expected
// consumer").
//
// The fix is to actually echo the group's selected protocol back
// to the client — what Apache always did. This test asserts the
// SyncGroup response on the success path carries the
// ProtocolType/ProtocolName the JoinGroup that triggered the
// rebalance settled on.
func TestGh98SyncGroupResponseEchoesProtocolFields(t *testing.T) {
	mgr := newTestCoordinator(t, "broker-0")
	const groupID = "sync-protocol-echo"

	join := mgr.JoinGroup(fastJoin(groupID, "consumer"), 3, "client-1")
	if join.ErrorCode != 0 {
		t.Fatalf("Join: %d", join.ErrorCode)
	}

	syncResp := mgr.SyncGroup(&api.SyncGroupRequest{
		GroupID:      groupID,
		GenerationID: join.GenerationID,
		MemberID:     join.MemberID,
		Assignments: []api.SyncAssignment{
			{MemberID: join.MemberID, Assignment: []byte("p0")},
		},
	})
	if syncResp.ErrorCode != 0 {
		t.Fatalf("Sync: %d", syncResp.ErrorCode)
	}
	if syncResp.ProtocolType != "consumer" {
		t.Errorf("Sync ProtocolType=%q, want \"consumer\" (Java client validates this against its expected protocol)", syncResp.ProtocolType)
	}
	if syncResp.ProtocolName != "range" {
		t.Errorf("Sync ProtocolName=%q, want \"range\" (selected by completeRebalance)", syncResp.ProtocolName)
	}
}

// TestGh98Kip394StaticMemberSkipsMemberIDRequired: a member with
// GroupInstanceID set is "static" — it identifies itself by the
// instance ID across reconnects, so MEMBER_ID_REQUIRED would be
// pointless. Apache only requires-known-member-id for dynamic members.
func TestGh98Kip394StaticMemberSkipsMemberIDRequired(t *testing.T) {
	mgr := newTestCoordinator(t, "broker-0")

	req := fastJoin("kip394-static", "consumer")
	req.GroupInstanceID = "static-inst-1"
	resp := mgr.JoinGroup(req, 5, "client-static")
	if resp.ErrorCode != 0 {
		t.Errorf("static-member v5 join: ErrorCode=%d, want 0 (MEMBER_ID_REQUIRED is dynamic-only)", resp.ErrorCode)
	}
	if resp.MemberID == "" {
		t.Error("static-member join: MemberID=\"\", expected inline assignment")
	}
}

// ---------------------------------------------------------------------------
// Additional coordinator coverage (post-gh #98)
// ---------------------------------------------------------------------------

// TestGh98PendingMemberCleanupTimerFires pins KIP-394's cleanup
// behaviour: an assigned-but-never-retried memberID gets dropped
// from g.pendingMembers after initialRebalanceDelayMs. The map
// stays bounded; a future client that happens to receive the same
// generated ID can still join cleanly.
//
// Apache parity: GroupMetadata.scala's pendingMembers is purged on
// the same cadence so a network-blipped client doesn't permanently
// hold a member-ID slot.
func TestGh98PendingMemberCleanupTimerFires(t *testing.T) {
	mgr := newTestCoordinator(t, "broker-0")
	groupID := "kip394-cleanup-group"

	first := mgr.JoinGroup(fastJoin(groupID, "consumer"), 4, "client-1")
	if first.ErrorCode != 79 {
		t.Fatalf("first join: ErrorCode=%d, want 79 (MEMBER_ID_REQUIRED)", first.ErrorCode)
	}
	abandoned := first.MemberID

	// Wait past initialRebalanceDelayMs (3s) + a margin for scheduler
	// jitter. The cleanup timer must have fired and removed the
	// pending entry.
	time.Sleep(3500 * time.Millisecond)

	// A subsequent fresh join (empty memberID, v4+) should still get
	// a NEW MEMBER_ID_REQUIRED handshake — not blocked by the
	// abandoned slot.
	second := mgr.JoinGroup(fastJoin(groupID, "consumer"), 4, "client-2")
	if second.ErrorCode != 79 {
		t.Errorf("second join after cleanup: ErrorCode=%d, want 79", second.ErrorCode)
	}
	if second.MemberID == abandoned {
		t.Errorf("second join reused abandoned ID %q (cleanup should have freed it for fresh allocation)", abandoned)
	}
}

// TestGh98PendingMemberPromotionStopsCleanupTimer: when the client
// retries the JoinGroup with the assigned ID before the cleanup
// timer fires, the timer must be cancelled — otherwise it'd later
// "clean up" a now-fully-promoted active member, removing them
// from pendingMembers (harmless) but masking a real bug if anything
// else were keyed off pendingMembers.
//
// We can't directly observe the cancelled timer from outside the
// package, but we CAN observe its side-effect: a follow-up rebalance
// after the cleanup window should still treat the member as fully
// joined (not nudge it back into pendingMembers).
func TestGh98PendingMemberPromotionStopsCleanupTimer(t *testing.T) {
	mgr := newTestCoordinator(t, "broker-0")
	groupID := "kip394-promote-group"

	// Step 1: first join → MEMBER_ID_REQUIRED.
	first := mgr.JoinGroup(fastJoin(groupID, "consumer"), 4, "client-1")
	if first.ErrorCode != 79 {
		t.Fatalf("first: %d", first.ErrorCode)
	}

	// Step 2: retry within the cleanup window.
	req := fastJoin(groupID, "consumer")
	req.MemberID = first.MemberID
	second := mgr.JoinGroup(req, 4, "client-1")
	if second.ErrorCode != 0 {
		t.Fatalf("second: %d", second.ErrorCode)
	}

	// Step 3: drive the member through SyncGroup so state lands at
	// Stable (otherwise heartbeat returns REBALANCE_IN_PROGRESS
	// regardless of the cleanup-timer story).
	syncResp := mgr.SyncGroup(&api.SyncGroupRequest{
		GroupID: groupID, GenerationID: second.GenerationID, MemberID: second.MemberID,
		Assignments: []api.SyncAssignment{{MemberID: second.MemberID, Assignment: []byte("p0")}},
	})
	if syncResp.ErrorCode != 0 {
		t.Fatalf("Sync: %d", syncResp.ErrorCode)
	}

	// Step 4: wait past the cleanup window.
	time.Sleep(3500 * time.Millisecond)

	// Step 5: heartbeat must succeed — the cleanup timer firing
	// post-promotion must NOT evict the member or invalidate its
	// session. ErrUnknownMemberId (25) would mean the cleanup
	// removed the member; ErrNone (0) means the member is still
	// alive in g.members.
	hb := mgr.Heartbeat(&api.HeartbeatRequest{
		GroupID:      groupID,
		GenerationID: second.GenerationID,
		MemberID:     second.MemberID,
	})
	if hb.ErrorCode == 25 {
		t.Fatalf("heartbeat after cleanup-window: ErrorCode=25 (UNKNOWN_MEMBER_ID) — cleanup timer evicted the promoted member")
	}
	if hb.ErrorCode != 0 {
		t.Errorf("heartbeat after cleanup-window with promoted member: ErrorCode=%d, want 0", hb.ErrorCode)
	}
}

// TestRemoveMemberIdempotent drives the broker into a state where
// the same member is evicted twice — first by LeaveGroup, then by
// the heartbeat-timer's AfterFunc that started before LeaveGroup
// removed the timer. The second call must be a no-op (the helper
// short-circuits when the member is already gone) so no double-send
// on the (already-drained) waiter ch panics or leaks.
//
// Pre-fix this race didn't exist (no helper, just inline mutations)
// but the helper extracted in gh #98 #1+#3 needs explicit idempotency
// because both eviction paths call it.
func TestRemoveMemberIdempotent(t *testing.T) {
	mgr := newTestCoordinator(t, "broker-0")
	groupID := "remove-idempotent"

	// Member with very short session timeout — the heartbeat AfterFunc
	// will fire shortly.
	resp := mgr.JoinGroup(&api.JoinGroupRequest{
		GroupID:            groupID,
		SessionTimeoutMs:   100,
		RebalanceTimeoutMs: 100,
		ProtocolType:       "consumer",
		Protocols:          []api.JoinGroupProtocol{{Name: "range", Metadata: []byte{}}},
	}, 3, "client")
	if resp.ErrorCode != 0 {
		t.Fatalf("Join: %d", resp.ErrorCode)
	}
	mgr.SyncGroup(&api.SyncGroupRequest{
		GroupID: groupID, GenerationID: resp.GenerationID, MemberID: resp.MemberID,
		Assignments: []api.SyncAssignment{{MemberID: resp.MemberID, Assignment: []byte("p0")}},
	})

	// Race the two eviction paths: LeaveGroup vs session timeout.
	// LeaveGroup runs synchronously here; the heartbeat timer
	// AfterFunc may have already fired (memberID already removed)
	// or fire shortly after.
	leaveResps := mgr.LeaveGroup(&api.LeaveGroupRequest{
		GroupID: groupID,
		Members: []api.LeaveMember{{MemberID: resp.MemberID}},
	})
	if len(leaveResps.Members) != 1 {
		t.Fatalf("Leave: got %d responses, want 1", len(leaveResps.Members))
	}
	// Either ErrNone (LeaveGroup found the member alive) or
	// ErrUnknownMemberId (heartbeat-timer beat it to the punch) is
	// acceptable. What matters is no panic and no goroutine leak.
	if leaveResps.Members[0].ErrorCode != 0 && leaveResps.Members[0].ErrorCode != 25 {
		t.Errorf("Leave first member: ErrorCode=%d, want 0 or 25", leaveResps.Members[0].ErrorCode)
	}

	// Wait long enough that the heartbeat AfterFunc has fired (it
	// may already have, depending on timing). Heartbeat from the
	// dead memberID must surface UNKNOWN_MEMBER_ID — and the broker
	// must not have crashed.
	time.Sleep(200 * time.Millisecond)
	hb := mgr.Heartbeat(&api.HeartbeatRequest{
		GroupID:      groupID,
		GenerationID: resp.GenerationID,
		MemberID:     resp.MemberID,
	})
	if hb.ErrorCode != 25 {
		t.Errorf("post-eviction heartbeat: ErrorCode=%d, want 25 (UNKNOWN_MEMBER_ID)", hb.ErrorCode)
	}
}

// TestSelectProtocolNoSharedProtocolReturnsEmpty: two members with
// disjoint protocol lists (range vs roundrobin) end up at a
// completeRebalance whose selectProtocol returns "". The Java
// client surfaces InconsistentGroupProtocolException client-side
// when this happens — Apache's broker also returns the same
// (empty) field, just with the rebalance still completing so the
// client sees the raw mismatch instead of a hang.
func TestSelectProtocolNoSharedProtocolReturnsEmpty(t *testing.T) {
	mgr := newTestCoordinator(t, "broker-0")
	groupID := "no-shared-protocol"

	// Two concurrent joiners with completely disjoint protocols.
	// fastJoin uses "range"; we override the second to "roundrobin".
	mkReq := func(proto string) *api.JoinGroupRequest {
		return &api.JoinGroupRequest{
			GroupID:            groupID,
			SessionTimeoutMs:   30_000,
			RebalanceTimeoutMs: 100,
			ProtocolType:       "consumer",
			Protocols:          []api.JoinGroupProtocol{{Name: proto, Metadata: []byte("m")}},
		}
	}
	var wg sync.WaitGroup
	var r1, r2 *api.JoinGroupResponse
	wg.Add(2)
	go func() { defer wg.Done(); r1 = mgr.JoinGroup(mkReq("range"), 3, "c1") }()
	go func() { defer wg.Done(); r2 = mgr.JoinGroup(mkReq("roundrobin"), 3, "c2") }()
	wg.Wait()

	// Both members joined the SAME group, so they share generation.
	// One was leader (sent "range"), the other is a follower
	// ("roundrobin"). selectProtocol picks the first leader-protocol
	// every other waiter supports → none → "".
	if r1.GenerationID != r2.GenerationID {
		t.Errorf("members got different generations (%d vs %d)", r1.GenerationID, r2.GenerationID)
	}
	if r1.ProtocolName != "" || r2.ProtocolName != "" {
		t.Errorf("ProtocolName=%q/%q, want \"\"/\"\" (no shared protocol)", r1.ProtocolName, r2.ProtocolName)
	}
	// Both rebalanced cleanly — the broker did NOT hang. The Java
	// client would now raise InconsistentGroupProtocolException
	// client-side and surface a meaningful error to the consumer.
}

// TestGenerationMonotonicAcrossRebalances pins the contract that
// GenerationID increases on every rebalance and never decreases.
// A stale-generation Heartbeat from a previous round must surface
// ILLEGAL_GENERATION (22), not ErrNone.
func TestGenerationMonotonicAcrossRebalances(t *testing.T) {
	mgr := newTestCoordinator(t, "broker-0")
	groupID := "gen-monotonic"

	// Round 1: join + sync.
	r1 := mgr.JoinGroup(fastJoin(groupID, "consumer"), 3, "c1")
	if r1.ErrorCode != 0 {
		t.Fatalf("join 1: %d", r1.ErrorCode)
	}
	mgr.SyncGroup(&api.SyncGroupRequest{
		GroupID: groupID, GenerationID: r1.GenerationID, MemberID: r1.MemberID,
		Assignments: []api.SyncAssignment{{MemberID: r1.MemberID, Assignment: []byte("p0")}},
	})
	gen1 := r1.GenerationID

	// Round 2: leave + rejoin → triggers a new rebalance.
	mgr.LeaveGroup(&api.LeaveGroupRequest{
		GroupID: groupID, Members: []api.LeaveMember{{MemberID: r1.MemberID}},
	})
	r2 := mgr.JoinGroup(fastJoin(groupID, "consumer"), 3, "c1")
	if r2.ErrorCode != 0 {
		t.Fatalf("join 2: %d", r2.ErrorCode)
	}
	if r2.GenerationID <= gen1 {
		t.Errorf("gen2=%d, want > gen1=%d", r2.GenerationID, gen1)
	}

	// Round 3: same dance, expect another bump.
	mgr.SyncGroup(&api.SyncGroupRequest{
		GroupID: groupID, GenerationID: r2.GenerationID, MemberID: r2.MemberID,
		Assignments: []api.SyncAssignment{{MemberID: r2.MemberID, Assignment: []byte("p0")}},
	})
	mgr.LeaveGroup(&api.LeaveGroupRequest{
		GroupID: groupID, Members: []api.LeaveMember{{MemberID: r2.MemberID}},
	})
	r3 := mgr.JoinGroup(fastJoin(groupID, "consumer"), 3, "c1")
	if r3.GenerationID <= r2.GenerationID {
		t.Errorf("gen3=%d, want > gen2=%d", r3.GenerationID, r2.GenerationID)
	}

	// Stale heartbeat from gen1 against the now-current gen3 must
	// surface ILLEGAL_GENERATION.
	hb := mgr.Heartbeat(&api.HeartbeatRequest{
		GroupID:      groupID,
		GenerationID: gen1,
		MemberID:     r3.MemberID,
	})
	if hb.ErrorCode != 22 {
		t.Errorf("stale-gen Heartbeat: ErrorCode=%d, want 22 (ILLEGAL_GENERATION)", hb.ErrorCode)
	}
}

// TestSyncGroupResponseOmitsProtocolFieldsOnError: companion to
// TestGh98SyncGroupResponseEchoesProtocolFields. On error responses
// (REBALANCE_IN_PROGRESS, ILLEGAL_GENERATION, UNKNOWN_MEMBER_ID)
// the Java client doesn't validate ProtocolType/Name (it short-
// circuits on ErrorCode != 0). But the response should still be
// shaped correctly: empty fields, never the in-flight group's
// values.
func TestSyncGroupResponseOmitsProtocolFieldsOnError(t *testing.T) {
	mgr := newTestCoordinator(t, "broker-0")
	groupID := "sync-error-protocols"

	// Set up an active group so g.protocolType/protocolName are
	// non-empty (would surface in success-path responses).
	join := mgr.JoinGroup(fastJoin(groupID, "consumer"), 3, "c1")
	if join.ErrorCode != 0 {
		t.Fatalf("Join: %d", join.ErrorCode)
	}
	mgr.SyncGroup(&api.SyncGroupRequest{
		GroupID: groupID, GenerationID: join.GenerationID, MemberID: join.MemberID,
		Assignments: []api.SyncAssignment{{MemberID: join.MemberID, Assignment: []byte("p0")}},
	})

	// Stale-generation SyncGroup → ILLEGAL_GENERATION error path.
	// Response should NOT echo the group's protocol fields.
	stale := mgr.SyncGroup(&api.SyncGroupRequest{
		GroupID:      groupID,
		GenerationID: join.GenerationID - 1, // stale
		MemberID:     join.MemberID,
	})
	if stale.ErrorCode != 22 {
		t.Errorf("stale-gen Sync: ErrorCode=%d, want 22 (ILLEGAL_GENERATION)", stale.ErrorCode)
	}
	if stale.ProtocolType != "" || stale.ProtocolName != "" {
		t.Errorf("error-path Sync: ProtocolType=%q/Name=%q, want empty (Apache returns empty on error)",
			stale.ProtocolType, stale.ProtocolName)
	}

	// Unknown-member SyncGroup → UNKNOWN_MEMBER_ID. Same expectation.
	unknown := mgr.SyncGroup(&api.SyncGroupRequest{
		GroupID:      groupID,
		GenerationID: join.GenerationID,
		MemberID:     "ghost-member",
	})
	if unknown.ErrorCode != 25 {
		t.Errorf("unknown-member Sync: ErrorCode=%d, want 25 (UNKNOWN_MEMBER_ID)", unknown.ErrorCode)
	}
	if unknown.ProtocolType != "" || unknown.ProtocolName != "" {
		t.Errorf("unknown-member Sync: ProtocolType=%q/Name=%q, want empty",
			unknown.ProtocolType, unknown.ProtocolName)
	}
}
