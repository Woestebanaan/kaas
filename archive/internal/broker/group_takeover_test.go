package broker

import (
	"context"
	"sync"
	"testing"

	"github.com/woestebanaan/skafka/pkg/kafkaapi"
)

// recordingMgr records RelinquishGroup calls for assertion. local
// is the in-memory groups list LocalGroups() returns — fixture for
// the orphan-sweep tests.
type recordingMgr struct {
	mu    sync.Mutex
	calls []string
	local []string
}

func (r *recordingMgr) RelinquishGroup(groupID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, groupID)
}

func (r *recordingMgr) LocalGroups() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.local))
	copy(out, r.local)
	return out
}

func (r *recordingMgr) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.calls))
	copy(out, r.calls)
	return out
}

func mkAssignmentWithGroups(parts []kafkaapi.PartitionAssignment, groups []kafkaapi.ConsumerGroupAssignment) *kafkaapi.Assignment {
	return &kafkaapi.Assignment{Partitions: parts, ConsumerGroups: groups}
}

func TestGroupTakeoverRelinquishesLostGroups(t *testing.T) {
	mgr := &recordingMgr{}
	d := NewGroupTakeoverDriver(mgr, "broker-A")

	prev := mkAssignmentWithGroups(nil, []kafkaapi.ConsumerGroupAssignment{
		{GroupID: "payments", Broker: "broker-A", Epoch: 1},
		{GroupID: "billing", Broker: "broker-A", Epoch: 1},
		{GroupID: "telemetry", Broker: "broker-B", Epoch: 1},
	})
	// payments reassigned away; billing stays; telemetry was never ours.
	next := mkAssignmentWithGroups(nil, []kafkaapi.ConsumerGroupAssignment{
		{GroupID: "payments", Broker: "broker-B", Epoch: 2},
		{GroupID: "billing", Broker: "broker-A", Epoch: 1},
		{GroupID: "telemetry", Broker: "broker-B", Epoch: 1},
	})

	d.OnAssignmentChange(context.Background(), prev, next)

	got := mgr.snapshot()
	if len(got) != 1 || got[0] != "payments" {
		t.Errorf("RelinquishGroup calls: got %v, want [payments]", got)
	}
}

func TestGroupTakeoverNoOpWhenNothingChanges(t *testing.T) {
	mgr := &recordingMgr{}
	d := NewGroupTakeoverDriver(mgr, "broker-A")

	a := mkAssignmentWithGroups(nil, []kafkaapi.ConsumerGroupAssignment{
		{GroupID: "payments", Broker: "broker-A", Epoch: 1},
	})
	d.OnAssignmentChange(context.Background(), a, a)

	if got := mgr.snapshot(); len(got) != 0 {
		t.Errorf("RelinquishGroup unexpectedly called: %v", got)
	}
}

func TestGroupTakeoverNilPrevTreatedAsEmpty(t *testing.T) {
	mgr := &recordingMgr{}
	d := NewGroupTakeoverDriver(mgr, "broker-A")

	next := mkAssignmentWithGroups(nil, []kafkaapi.ConsumerGroupAssignment{
		{GroupID: "payments", Broker: "broker-A", Epoch: 1},
	})
	d.OnAssignmentChange(context.Background(), nil, next)

	if got := mgr.snapshot(); len(got) != 0 {
		t.Errorf("nil prev should not relinquish anything; got %v", got)
	}
}

// TestGroupTakeoverSweepsOrphansNotInPrev guards the script-audit
// regression that surfaced on v0.1.49: a non-coordinator broker had
// "ghost-group" in its m.groups even though the group was never in
// its prev assignment view (so the original prev→next diff never
// fired RelinquishGroup). Fix is the orphan sweep — anything in
// LocalGroups not in nextOurs gets relinquished.
//
// Without the fix, this test would record zero RelinquishGroup
// calls (the broker thinks it has nothing to drop). With the fix,
// "ghost-group" is dropped on every assignment change until it's
// gone.
func TestGroupTakeoverSweepsOrphansNotInPrev(t *testing.T) {
	mgr := &recordingMgr{
		local: []string{"ghost-group", "owned-group"},
	}
	d := NewGroupTakeoverDriver(mgr, "broker-A")

	// prev: empty for this broker (the orphan was never in our view).
	prev := mkAssignmentWithGroups(nil, []kafkaapi.ConsumerGroupAssignment{
		{GroupID: "ghost-group", Broker: "broker-B", Epoch: 1},
	})
	// next: this broker owns "owned-group"; "ghost-group" still on B.
	next := mkAssignmentWithGroups(nil, []kafkaapi.ConsumerGroupAssignment{
		{GroupID: "owned-group", Broker: "broker-A", Epoch: 1},
		{GroupID: "ghost-group", Broker: "broker-B", Epoch: 1},
	})
	d.OnAssignmentChange(context.Background(), prev, next)

	got := mgr.snapshot()
	if len(got) != 1 || got[0] != "ghost-group" {
		t.Errorf("orphan sweep relinquished %v, want [ghost-group]", got)
	}
}

// TestGroupTakeoverSweepIdempotentWithDiff: a group that's BOTH in
// prev (ours) and in LocalGroups, and not in next, should be
// relinquished exactly once — the diff loop and the sweep loop
// must not double-fire. (Manager.RelinquishGroup is itself
// idempotent so this is a defensive assertion, but a regression
// that double-fires would still be a smell worth catching.)
func TestGroupTakeoverSweepIdempotentWithDiff(t *testing.T) {
	mgr := &recordingMgr{
		local: []string{"departing"},
	}
	d := NewGroupTakeoverDriver(mgr, "broker-A")

	prev := mkAssignmentWithGroups(nil, []kafkaapi.ConsumerGroupAssignment{
		{GroupID: "departing", Broker: "broker-A", Epoch: 1},
	})
	next := mkAssignmentWithGroups(nil, []kafkaapi.ConsumerGroupAssignment{
		{GroupID: "departing", Broker: "broker-B", Epoch: 2},
	})
	d.OnAssignmentChange(context.Background(), prev, next)

	got := mgr.snapshot()
	wantOnce := 0
	for _, g := range got {
		if g == "departing" {
			wantOnce++
		}
	}
	if wantOnce != 1 {
		t.Errorf("departing relinquished %d times, want 1 (sweep + diff double-fired)", wantOnce)
	}
}

func TestGroupTakeoverNewlyOwnedGroupsAreNoOp(t *testing.T) {
	mgr := &recordingMgr{}
	d := NewGroupTakeoverDriver(mgr, "broker-A")

	prev := mkAssignmentWithGroups(nil, nil)
	next := mkAssignmentWithGroups(nil, []kafkaapi.ConsumerGroupAssignment{
		{GroupID: "newgroup", Broker: "broker-A", Epoch: 1},
	})
	d.OnAssignmentChange(context.Background(), prev, next)

	// v1: lazy load on first JoinGroup, no proactive work for newly-owned.
	if got := mgr.snapshot(); len(got) != 0 {
		t.Errorf("newly-owned groups should not trigger Relinquish: %v", got)
	}
}
