package coordinator

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

// TestSyncGroupPartitionSharePerMemberAssignment exercises the
// gh skafka#134 reproducer: 4 members join one group, the leader
// delivers 4 distinct assignments via SyncGroup, and each member's
// own SyncGroup call must return ONLY its own assignment — not the
// union of all four.
//
// The bench reproducer (kafka-consumer-perf-test parallelism=4 on a
// 16-partition topic) shows every consumer reading the full topic
// (~10M records each = 4× the topic), suggesting one of:
//   - JoinGroup's leader-members list omits some members → leader
//     assigns everything to itself
//   - SyncGroup stores assignments but reads them back wrong
//   - Some other flow keeps consumers in a sole-member state
//
// This test isolates the SyncGroup leg of the flow. If it passes,
// the bug is upstream (JoinGroup metadata, Heartbeat rebalance
// signal, client-side eager-rebalance). If it fails, the broker
// itself is mis-routing assignments.
func TestSyncGroupPartitionSharePerMemberAssignment(t *testing.T) {
	prev := initialRebalanceDelayMs
	initialRebalanceDelayMs = 80
	t.Cleanup(func() { initialRebalanceDelayMs = prev })

	g := newGroup("partition-share-cg")

	// 4 members join via the cold-start KIP-394 two-step path.
	twoStep := func(name string) *api.JoinGroupResponse {
		first := g.join(&api.JoinGroupRequest{
			ProtocolType:       "consumer",
			SessionTimeoutMs:   30_000,
			RebalanceTimeoutMs: 30_000,
			Protocols:          []api.JoinGroupProtocol{{Name: "range", Metadata: []byte("m")}},
		}, 9, name)
		if first.ErrorCode != int16(codec.ErrMemberIDRequired) {
			t.Fatalf("%s first-step expected MemberIDRequired, got %d", name, first.ErrorCode)
		}
		return g.join(&api.JoinGroupRequest{
			MemberID:           first.MemberID,
			ProtocolType:       "consumer",
			SessionTimeoutMs:   30_000,
			RebalanceTimeoutMs: 30_000,
			Protocols:          []api.JoinGroupProtocol{{Name: "range", Metadata: []byte("m")}},
		}, 9, name)
	}

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

	// Locate the elected leader's response — that's the one with
	// resp.Members populated.
	var leaderResp *api.JoinGroupResponse
	for _, r := range results {
		if r.MemberID == r.Leader {
			leaderResp = r
			break
		}
	}
	if leaderResp == nil {
		t.Fatalf("no leader response found among %d results", len(results))
	}
	if len(leaderResp.Members) != 4 {
		t.Fatalf("leader's resp.Members has %d entries, want 4", len(leaderResp.Members))
	}

	// Now drive SyncGroup. The leader sends a fan-out assignment:
	// each member gets a distinct payload that names that member.
	// The follower-side SyncGroup must return EXACTLY that member's
	// payload, not the union of all four.
	leaderID := leaderResp.Leader
	gen := leaderResp.GenerationID

	per := map[string][]byte{}
	for _, m := range leaderResp.Members {
		per[m.MemberID] = []byte("assignment-for-" + m.MemberID)
	}

	syncResps := make(map[string]*api.SyncGroupResponse, 4)
	var smu sync.Mutex

	leaderAssignments := make([]api.SyncAssignment, 0, 4)
	for memberID, asgn := range per {
		leaderAssignments = append(leaderAssignments, api.SyncAssignment{
			MemberID:   memberID,
			Assignment: asgn,
		})
	}

	// All 4 members call sync concurrently. The leader's call carries
	// the fan-out assignments; the 3 followers call with no Assignments.
	var swg sync.WaitGroup
	for _, r := range results {
		swg.Add(1)
		go func(r *api.JoinGroupResponse) {
			defer swg.Done()
			req := &api.SyncGroupRequest{
				MemberID:     r.MemberID,
				GenerationID: gen,
			}
			if r.MemberID == leaderID {
				req.Assignments = leaderAssignments
			}
			resp := g.sync(req)
			smu.Lock()
			syncResps[r.MemberID] = resp
			smu.Unlock()
		}(r)
	}
	swg.Wait()

	// Every member must receive EXACTLY its own assignment.
	for memberID, want := range per {
		got := syncResps[memberID]
		if got == nil {
			t.Errorf("%s: no SyncGroupResponse received", memberID)
			continue
		}
		if got.ErrorCode != 0 {
			t.Errorf("%s: errorCode=%d", memberID, got.ErrorCode)
			continue
		}
		if !bytes.Equal(got.Assignment, want) {
			t.Errorf("%s: assignment mismatch\n  got:  %q\n  want: %q",
				memberID, got.Assignment, want)
		}
	}
}
