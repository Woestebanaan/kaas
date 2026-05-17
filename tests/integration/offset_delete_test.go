package integration

import (
	"context"
	"testing"

	"github.com/woestebanaan/skafka/internal/coordinator"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

// commitOffsets is a test helper: brings a group to Stable, commits
// the named (topic, partition→offset) entries, then leaves so the
// group lands in Empty — the only state OffsetDelete accepts. Mirrors
// the setup shape used by TestDeleteGroupsEmptySucceeds.
func commitOffsets(t *testing.T, mgr *coordinator.Manager, groupID, topic string, partitions map[int32]int64) {
	t.Helper()
	r := mgr.JoinGroup(fastJoin(groupID, "consumer"), 3, "c1")
	if r.ErrorCode != 0 {
		t.Fatalf("Join: %d", r.ErrorCode)
	}
	mgr.SyncGroup(&api.SyncGroupRequest{
		GroupID: groupID, GenerationID: r.GenerationID, MemberID: r.MemberID,
		Assignments: []api.SyncAssignment{{MemberID: r.MemberID, Assignment: []byte("p0")}},
	})
	var parts []api.OffsetCommitPartition
	for p, off := range partitions {
		parts = append(parts, api.OffsetCommitPartition{PartitionIndex: p, CommittedOffset: off})
	}
	mgr.OffsetCommit(&api.OffsetCommitRequest{
		GroupID: groupID, MemberID: r.MemberID, GenerationID: r.GenerationID,
		Topics: []api.OffsetCommitTopic{{Name: topic, Partitions: parts}},
	})
	if leave := mgr.LeaveGroup(&api.LeaveGroupRequest{GroupID: groupID, MemberID: r.MemberID}); leave.ErrorCode != 0 {
		t.Fatalf("Leave: %d", leave.ErrorCode)
	}
}

// TestOffsetDeleteEmptyGroupRemovesRequestedPartitions is the
// happy-path acceptance test from gh #100: an Empty group with
// committed offsets on 3 partitions, delete 2 of them, the third
// survives. Also exercises the mixed-success-and-UNKNOWN response
// shape by including a partition that was never committed.
func TestOffsetDeleteEmptyGroupRemovesRequestedPartitions(t *testing.T) {
	dir := t.TempDir()
	offsets := coordinator.NewOffsetStore(dir)
	mgr := coordinator.NewManager(context.Background(), &alwaysGroupSource{brokerID: "test"},
		func(_ string) (int32, string, int32, bool) { return 0, "localhost", 9092, true },
		offsets)
	const groupID = "od-empty"

	commitOffsets(t, mgr, groupID, "t", map[int32]int64{0: 100, 1: 200, 2: 300})

	// Request delete on partitions [0, 1, 99]. 0 and 1 are committed,
	// 99 was never committed — that partition should surface
	// UNKNOWN_TOPIC_OR_PARTITION while 0 and 1 succeed.
	keys := []string{coordinator.OffsetKey("t", 0), coordinator.OffsetKey("t", 1), coordinator.OffsetKey("t", 99)}
	groupErr, removed := mgr.DeleteOffsets(groupID, keys)
	if groupErr != 0 {
		t.Fatalf("DeleteOffsets on Empty group: groupErr=%d, want 0", groupErr)
	}
	if !removed[coordinator.OffsetKey("t", 0)] || !removed[coordinator.OffsetKey("t", 1)] {
		t.Errorf("removed=%v, want t/0 and t/1 marked removed", removed)
	}
	if removed[coordinator.OffsetKey("t", 99)] {
		t.Errorf("removed[t/99]=true, want false (never committed)")
	}

	// Survivors: t/2 still has its offset, t/0 and t/1 are gone.
	fetched := mgr.OffsetFetch(&api.OffsetFetchRequest{
		GroupID: groupID,
		Topics:  []api.OffsetFetchTopic{{Name: "t", PartitionIndexes: []int32{0, 1, 2}}},
	})
	got := map[int32]int64{}
	for _, p := range fetched.Topics[0].Partitions {
		got[p.PartitionIndex] = p.CommittedOffset
	}
	if got[0] != -1 || got[1] != -1 {
		t.Errorf("post-delete fetch: t/0=%d t/1=%d, both want -1 (offsets cleared)", got[0], got[1])
	}
	if got[2] != 300 {
		t.Errorf("post-delete fetch: t/2=%d, want 300 (untouched)", got[2])
	}
}

// TestOffsetDeleteRejectsActiveGroup mirrors the DeleteGroups
// non-empty guard: a Stable group must reject OffsetDelete with
// NON_EMPTY_GROUP (67). Without this the per-partition reset could
// silently fence offsets out from under live consumers.
func TestOffsetDeleteRejectsActiveGroup(t *testing.T) {
	mgr := newTestCoordinator(t, "broker-0")
	const groupID = "od-busy"

	r := mgr.JoinGroup(fastJoin(groupID, "consumer"), 3, "c1")
	if r.ErrorCode != 0 {
		t.Fatalf("Join: %d", r.ErrorCode)
	}
	mgr.SyncGroup(&api.SyncGroupRequest{
		GroupID: groupID, GenerationID: r.GenerationID, MemberID: r.MemberID,
		Assignments: []api.SyncAssignment{{MemberID: r.MemberID, Assignment: []byte("p0")}},
	})

	groupErr, removed := mgr.DeleteOffsets(groupID, []string{coordinator.OffsetKey("t", 0)})
	if groupErr != 67 {
		t.Errorf("active-group DeleteOffsets groupErr=%d, want 67 (NON_EMPTY_GROUP)", groupErr)
	}
	if removed != nil {
		t.Errorf("removed=%v, want nil on group-level error", removed)
	}
}

// TestOffsetDeleteUnknownGroupReturns69 covers the AdminClient
// retry-after-cleanup case: delete on a group that was never
// joined and has no offsets on disk. Returns 69 (GROUP_ID_NOT_FOUND)
// so AdminClient surfaces GroupIdNotFoundException to operators.
func TestOffsetDeleteUnknownGroupReturns69(t *testing.T) {
	mgr := newTestCoordinator(t, "broker-0")
	groupErr, removed := mgr.DeleteOffsets("never-existed", []string{coordinator.OffsetKey("t", 0)})
	if groupErr != 69 {
		t.Errorf("unknown group groupErr=%d, want 69 (GROUP_ID_NOT_FOUND)", groupErr)
	}
	if removed != nil {
		t.Errorf("removed=%v, want nil on group-level error", removed)
	}
}

// TestOffsetDeleteNotCoordinatorReturns16: when assignment.json
// reports a different broker as coordinator for groupID, DeleteOffsets
// returns 16 (NOT_COORDINATOR) so the AdminClient retries
// FindCoordinator. Mirrors the cluster-runtime path where
// broker.Coordinator is the GroupAssignmentSource and assignment-hash
// landed the group on a sibling.
func TestOffsetDeleteNotCoordinatorReturns16(t *testing.T) {
	mgr := coordinator.NewManager(context.Background(), &neverGroupSource{},
		func(_ string) (int32, string, int32, bool) { return 1, "other-broker", 9092, true },
		coordinator.NewOffsetStore(t.TempDir()))

	groupErr, removed := mgr.DeleteOffsets("any-group", []string{coordinator.OffsetKey("t", 0)})
	if groupErr != 16 {
		t.Errorf("not-coordinator groupErr=%d, want 16 (NOT_COORDINATOR)", groupErr)
	}
	if removed != nil {
		t.Errorf("removed=%v, want nil on group-level error", removed)
	}
}
