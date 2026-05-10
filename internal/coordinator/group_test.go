package coordinator

import (
	"bytes"
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

// TestSyncGroupLeaderOmittedMemberFilledWithValidEmpty guards the gh
// #111 layer-4 safety net: when the leader's req.Assignments is
// missing a group member, the broker fills that slot with a valid
// serialized ConsumerProtocolAssignment-v0 (10 bytes) instead of an
// empty byte slice. Apache stores Array.empty[Byte] here, but Java's
// ConsumerCoordinator then throws IllegalStateException — we exceed
// Apache parity by writing a wire-valid empty assignment so the
// client deserialises cleanly.
func TestSyncGroupLeaderOmittedMemberFilledWithValidEmpty(t *testing.T) {
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
	want := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff}
	if !bytes.Equal(val, want) {
		t.Fatalf("expected valid serialized empty assignment %x, got %x", want, val)
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

// TestInitialRebalanceWaitsForLateColdStartArrivals exercises gh #111
// layer 1: on a brand-new group, maybeCompleteRebalance must NOT
// short-circuit when the second joiner makes len(joinWaiters) catch
// up with len(members). Pre-fix the rebalance completed at 2/2 and
// the 3rd/4th cold-start members were forced into a second
// generation, racing the first generation's SyncGroup. Post-fix the
// rebalance waits the (extended) initial-delay timer; all 4 members
// land in a single generation.
func TestInitialRebalanceWaitsForLateColdStartArrivals(t *testing.T) {
	// Shrink the delay so the test doesn't sit for 3s. Each new
	// joiner extends the timer by this amount; after the last
	// joiner, the rebalance fires <delay> later.
	prev := initialRebalanceDelayMs
	initialRebalanceDelayMs = 80
	t.Cleanup(func() { initialRebalanceDelayMs = prev })

	g := newGroup("cold-start-cg")

	// Helper: drive a v9 JoinGroup synchronously through the
	// KIP-394 two-step flow, return the final response (gen + leader
	// + members list if leader).
	twoStep := func(name string) *api.JoinGroupResponse {
		first := g.join(&api.JoinGroupRequest{
			ProtocolType:       "consumer",
			SessionTimeoutMs:   30_000,
			RebalanceTimeoutMs: 30_000,
			Protocols:          []api.JoinGroupProtocol{{Name: "range", Metadata: []byte("m")}},
		}, 9, name)
		if first.ErrorCode != int16(codec.ErrMemberIDRequired) {
			t.Fatalf("first-step expected MemberIDRequired, got errorCode=%d", first.ErrorCode)
		}
		return g.join(&api.JoinGroupRequest{
			MemberID:           first.MemberID,
			ProtocolType:       "consumer",
			SessionTimeoutMs:   30_000,
			RebalanceTimeoutMs: 30_000,
			Protocols:          []api.JoinGroupProtocol{{Name: "range", Metadata: []byte("m")}},
		}, 9, name)
	}

	// 4 cold-start consumers fan out concurrently. Stagger them
	// inside the 80ms initial-delay window so the timer extension
	// must absorb late arrivals.
	results := make([]*api.JoinGroupResponse, 4)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			time.Sleep(time.Duration(idx*20) * time.Millisecond)
			results[idx] = twoStep("client-" + string(rune('A'+idx)))
		}(i)
	}
	wg.Wait()

	// Every member must land in the SAME generation — the cold-start
	// race shows up as different generationIDs for early vs late
	// arrivals. Membership in resp.Members for the leader's response
	// must include all 4.
	gen := results[0].GenerationID
	for i, r := range results {
		if r.ErrorCode != 0 {
			t.Fatalf("result[%d]: errorCode=%d", i, r.ErrorCode)
		}
		if r.GenerationID != gen {
			t.Fatalf("result[%d]: generation %d, want %d (all members must share one generation on cold start)",
				i, r.GenerationID, gen)
		}
	}

	// Find the leader's response and assert its Members list covers all 4.
	var leaderMembers []api.JoinGroupMember
	for _, r := range results {
		if r.MemberID == r.Leader {
			leaderMembers = r.Members
			break
		}
	}
	if len(leaderMembers) != 4 {
		t.Fatalf("leader's resp.Members has %d entries, want 4 — late cold-start members were lost",
			len(leaderMembers))
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
