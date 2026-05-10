package coordinator

import (
	"sync"
	"testing"
	"time"

	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

// TestSyncGroupFollowerInStableState guards skafka#111: a follower whose
// SyncGroupRequest arrives AFTER the leader's must still receive its
// cached assignment, not ErrRebalanceInProgress. Pre-fix skafka rejected
// any non-CompletingRebalance state in the SyncGroup handler, but the
// leader's path sets state=Stable BEFORE the followers' SyncGroups land.
// With concurrent multi-pod consumers (kafka-consumer-perf-test
// parallelism=4) this race fired often enough to wedge the rebalance.
func TestSyncGroupFollowerInStableState(t *testing.T) {
	g := newGroup("test-cg")
	g.members["leader"] = &groupMember{id: "leader"}
	g.members["follower"] = &groupMember{id: "follower"}
	g.leaderID = "leader"
	g.generationID = 1
	g.protocolType = "consumer"
	g.protocolName = "range"
	g.state = stateStable
	ss := &syncState{
		assignments: map[string][]byte{
			"leader":   []byte("leader-bytes"),
			"follower": []byte("follower-bytes"),
		},
		done: make(chan struct{}),
	}
	close(ss.done)
	g.currentSync = ss

	resp := g.sync(&api.SyncGroupRequest{
		MemberID:     "follower",
		GenerationID: 1,
	})

	if resp.ErrorCode != 0 {
		t.Fatalf("expected ErrNone, got %d", resp.ErrorCode)
	}
	if string(resp.Assignment) != "follower-bytes" {
		t.Fatalf("expected follower-bytes, got %q (len=%d)", resp.Assignment, len(resp.Assignment))
	}
	if resp.ProtocolType != "consumer" || resp.ProtocolName != "range" {
		t.Fatalf("expected protocol consumer/range, got %q/%q", resp.ProtocolType, resp.ProtocolName)
	}
}

// TestSyncGroupLeaderOmittedMemberFilledExplicitly guards the gh #111
// safety net Apache emits at GroupCoordinator.scala:605-609 — when the
// leader's req.Assignments is missing a group member, the broker fills
// that member's slot with explicit empty bytes (not nil) and emits a
// warn log. The wire result is still 0 bytes (Apache's choice), but
// having an explicit map entry makes the decision auditable in the
// broker logs and prevents future regressions where a nil-map lookup
// looks identical to "leader said empty".
func TestSyncGroupLeaderOmittedMemberFilledExplicitly(t *testing.T) {
	g := newGroup("test-cg")
	g.members["leader"] = &groupMember{id: "leader"}
	g.members["forgotten"] = &groupMember{id: "forgotten"}
	g.leaderID = "leader"
	g.generationID = 1
	g.protocolType = "consumer"
	g.protocolName = "range"
	g.state = stateCompletingRebalance
	g.currentSync = &syncState{
		assignments: map[string][]byte{},
		done:        make(chan struct{}),
	}

	resp := g.sync(&api.SyncGroupRequest{
		MemberID:     "leader",
		GenerationID: 1,
		Assignments: []api.SyncAssignment{
			{MemberID: "leader", Assignment: []byte("leader-bytes")},
			// "forgotten" deliberately omitted by the leader.
		},
	})

	if resp.ErrorCode != 0 {
		t.Fatalf("leader sync: expected ErrNone, got %d", resp.ErrorCode)
	}

	g.currentSync.mu.Lock()
	val, ok := g.currentSync.assignments["forgotten"]
	g.currentSync.mu.Unlock()
	if !ok {
		t.Fatalf("expected explicit fill for omitted member, ss.assignments[\"forgotten\"] not present")
	}
	if val == nil {
		t.Fatalf("expected explicit empty bytes (non-nil), got nil — defeats the audit-log purpose")
	}
	if len(val) != 0 {
		t.Fatalf("expected zero-length bytes, got %d", len(val))
	}
}

// TestSyncGroupConcurrentLeaderFirstFollowerSecond exercises the
// real race the gh #111 fix targets: leader and follower SyncGroup
// concurrently, leader wins the lock and transitions to Stable before
// the follower's request is processed. With the pre-fix strict state
// check the follower would have failed; with the fix it reads the
// cached assignment.
func TestSyncGroupConcurrentLeaderFirstFollowerSecond(t *testing.T) {
	g := newGroup("test-cg")
	g.members["leader"] = &groupMember{id: "leader"}
	g.members["follower"] = &groupMember{id: "follower"}
	g.leaderID = "leader"
	g.generationID = 1
	g.protocolType = "consumer"
	g.protocolName = "range"
	g.state = stateCompletingRebalance
	g.currentSync = &syncState{
		assignments: map[string][]byte{},
		done:        make(chan struct{}),
	}

	var leaderResp, followerResp *api.SyncGroupResponse
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		leaderResp = g.sync(&api.SyncGroupRequest{
			MemberID:     "leader",
			GenerationID: 1,
			Assignments: []api.SyncAssignment{
				{MemberID: "leader", Assignment: []byte("L")},
				{MemberID: "follower", Assignment: []byte("F")},
			},
		})
	}()

	// Slight delay so leader is more likely to acquire g.mu first and
	// transition to Stable before the follower's request enters the
	// handler. The fix has to make this case PASS rather than fail.
	time.Sleep(20 * time.Millisecond)

	go func() {
		defer wg.Done()
		followerResp = g.sync(&api.SyncGroupRequest{
			MemberID:     "follower",
			GenerationID: 1,
		})
	}()

	wg.Wait()

	if leaderResp.ErrorCode != 0 || string(leaderResp.Assignment) != "L" {
		t.Errorf("leader: errorCode=%d assignment=%q (want 0/L)",
			leaderResp.ErrorCode, leaderResp.Assignment)
	}
	if followerResp.ErrorCode != 0 || string(followerResp.Assignment) != "F" {
		t.Errorf("follower: errorCode=%d assignment=%q (want 0/F)",
			followerResp.ErrorCode, followerResp.Assignment)
	}
}

// TestSyncGroupCanceledByConcurrentJoinReturnsRebalanceInProgress
// guards the gh #111 cold-start race surfaced by the v0.1.89
// scripts/bench sweep on multi-cg-c. The follower is parked inside
// sync() on <-ss.done. While it's parked, a fresh JoinGroup arrives
// (a 5th consumer pod cold-starting), bumping the group from
// CompletingRebalance → PreparingRebalance and cancelling the
// in-flight syncState BEFORE the leader stored assignments. Pre-fix
// the follower woke up, read ss.assignments[its-id]=nil, and returned
// errorCode=NONE with a 0-byte assignment — Java raised
// IllegalStateException. Post-fix the canceled flag forces
// REBALANCE_IN_PROGRESS so the client cleanly re-joins the next round.
func TestSyncGroupCanceledByConcurrentJoinReturnsRebalanceInProgress(t *testing.T) {
	g := newGroup("test-cg")
	g.members["leader"] = &groupMember{id: "leader", protocols: []api.JoinGroupProtocol{{Name: "range"}}}
	g.members["follower"] = &groupMember{id: "follower", protocols: []api.JoinGroupProtocol{{Name: "range"}}}
	g.leaderID = "leader"
	g.generationID = 1
	g.protocolType = "consumer"
	g.protocolName = "range"
	g.state = stateCompletingRebalance
	g.currentSync = &syncState{
		assignments: map[string][]byte{},
		done:        make(chan struct{}),
	}

	// Park the follower inside sync(); it'll block on <-ss.done.
	respCh := make(chan *api.SyncGroupResponse, 1)
	go func() {
		respCh <- g.sync(&api.SyncGroupRequest{MemberID: "follower", GenerationID: 1})
	}()

	// Let the follower actually enter the wait.
	time.Sleep(20 * time.Millisecond)

	// Simulate a fresh joiner cancelling the round before the leader
	// stored anything. Mirrors the join() path at the
	// stateCompletingRebalance arm: bump state and cancel the sync.
	g.mu.Lock()
	g.state = statePreparingRebalance
	cancelSync(g.currentSync)
	g.currentSync = nil
	g.mu.Unlock()

	resp := <-respCh
	if resp.ErrorCode != int16(codec.ErrRebalanceInProgress) {
		t.Fatalf("expected REBALANCE_IN_PROGRESS (%d), got %d (assignment=%q len=%d)",
			codec.ErrRebalanceInProgress, resp.ErrorCode, resp.Assignment, len(resp.Assignment))
	}
	if len(resp.Assignment) != 0 {
		t.Fatalf("canceled response should have empty assignment, got %d bytes", len(resp.Assignment))
	}
}

// TestSyncGroupNotCanceledAfterLeaderStored guards the inverse: a
// normal leader-completes-first delivery must NOT be flipped to
// REBALANCE_IN_PROGRESS by a late cancel. cancelSync's already-closed
// branch should leave canceled=false.
func TestSyncGroupNotCanceledAfterLeaderStored(t *testing.T) {
	g := newGroup("test-cg")
	g.members["leader"] = &groupMember{id: "leader"}
	g.members["follower"] = &groupMember{id: "follower"}
	g.leaderID = "leader"
	g.generationID = 1
	g.protocolType = "consumer"
	g.protocolName = "range"
	g.state = stateStable
	ss := &syncState{
		assignments: map[string][]byte{
			"leader":   []byte("L"),
			"follower": []byte("F"),
		},
		done: make(chan struct{}),
	}
	close(ss.done) // simulate leader-delivered close
	g.currentSync = ss

	// Now a fresh joiner cancels — ss.done is already closed, so
	// cancelSync should see the closed channel and skip flipping
	// canceled. The follower's later sync() must return its assignment.
	cancelSync(ss)

	resp := g.sync(&api.SyncGroupRequest{MemberID: "follower", GenerationID: 1})
	if resp.ErrorCode != 0 {
		t.Fatalf("expected ErrNone, got %d", resp.ErrorCode)
	}
	if string(resp.Assignment) != "F" {
		t.Fatalf("expected F, got %q", resp.Assignment)
	}
}
