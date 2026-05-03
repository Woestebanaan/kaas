package broker

import (
	"context"
	"sync"
	"testing"

	"github.com/woestebanaan/skafka/pkg/kafkaapi"
)

// recordingMgr records RelinquishGroup calls for assertion.
type recordingMgr struct {
	mu    sync.Mutex
	calls []string
}

func (r *recordingMgr) RelinquishGroup(groupID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, groupID)
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
