package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/woestebanaan/skafka/internal/coordinator"
	"github.com/woestebanaan/skafka/internal/lease"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
	"k8s.io/client-go/kubernetes/fake"
)

// alwaysCoordinator is a minimal CoordinatorLeaseManager stub that always reports
// this broker as coordinator. Used for tests that focus on offset storage, not election.
type alwaysCoordinator struct{}

func (a *alwaysCoordinator) AcquireCoordinator(_ context.Context, _ string) error { return nil }
func (a *alwaysCoordinator) ReleaseCoordinator(_ string) error                    { return nil }
func (a *alwaysCoordinator) IsCoordinator(_ string) bool                          { return true }
func (a *alwaysCoordinator) CoordinatorFor(_ string) int32                        { return 0 }
func (a *alwaysCoordinator) WaitForCoordinator(_ context.Context, _ string) bool  { return true }

var _ lease.CoordinatorLeaseManager = (*alwaysCoordinator)(nil)

// newTestCoordinator creates a coordinator backed by a fake Kubernetes client.
func newTestCoordinator(t *testing.T, podName string) *coordinator.Manager {
	t.Helper()
	fakeClient := fake.NewSimpleClientset()
	lm := lease.NewKubernetesLeaseManager(fakeClient, "default", podName, nil, nil)
	lookup := func(_ int32) (string, int32, bool) { return "localhost", 9092, true }
	return coordinator.NewManager(context.Background(), lm, lookup, coordinator.NewOffsetStore(t.TempDir()))
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
	mgr := coordinator.NewManager(context.Background(), &alwaysCoordinator{},
		func(_ int32) (string, int32, bool) { return "localhost", 9092, true },
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
	mgr2 := coordinator.NewManager(context.Background(), &alwaysCoordinator{},
		func(_ int32) (string, int32, bool) { return "localhost", 9092, true }, offsets2)
	fetchResp2 := mgr2.OffsetFetch(fetchReq)
	for _, p := range fetchResp2.Topics[0].Partitions {
		if p.CommittedOffset != expected[p.PartitionIndex] {
			t.Errorf("after reload: partition %d got %d, want %d", p.PartitionIndex, p.CommittedOffset, expected[p.PartitionIndex])
		}
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
