package broker

import (
	"context"

	"github.com/woestebanaan/skafka/pkg/kafkaapi"
)

// groupRelinquisher is the small contract GroupTakeoverDriver needs from
// coordinator.Manager — narrowed so this package doesn't need to import
// internal/coordinator (which would form a cycle: coordinator already
// imports... well it doesn't import broker, so no cycle. But narrowing
// is still cleaner — tests don't need to spin up a real Manager).
type groupRelinquisher interface {
	RelinquishGroup(groupID string)
}

// GroupTakeoverDriver is the consumer-group analogue of TakeoverDriver:
// it watches assignment changes and tells the coordinator.Manager to
// drop in-memory state for groups that are no longer assigned to this
// broker.
//
// v1 doesn't proactively load on takeover — the new coordinator's first
// JoinGroup creates the group via Manager.getOrCreate, which loads
// persisted offsets lazily. Acceptable cost: one rebalance round-trip
// per coordinator transition; v2 may add proactive state transfer.
type GroupTakeoverDriver struct {
	mgr      groupRelinquisher
	brokerID string
}

// NewGroupTakeoverDriver builds a driver. brokerID matches the value in
// kafkaapi.ConsumerGroupAssignment.Broker — typically the StatefulSet
// pod name.
func NewGroupTakeoverDriver(mgr groupRelinquisher, brokerID string) *GroupTakeoverDriver {
	return &GroupTakeoverDriver{mgr: mgr, brokerID: brokerID}
}

// OnAssignmentChange is the kafkaapi.AssignmentChangeHandler signature
// expected by Coordinator.OnAssignmentChange. It diffs prev vs next:
// every group that was ours but is no longer ours has its in-memory
// state dropped via Manager.RelinquishGroup. Newly-ours groups need
// no proactive work — the next JoinGroup builds state organically.
func (d *GroupTakeoverDriver) OnAssignmentChange(_ context.Context, prev, next *kafkaapi.Assignment) {
	prevOurs := groupsOwnedBy(prev, d.brokerID)
	nextOurs := groupsOwnedBy(next, d.brokerID)

	for groupID := range prevOurs {
		if _, stillOurs := nextOurs[groupID]; stillOurs {
			continue
		}
		d.mgr.RelinquishGroup(groupID)
	}
}

// groupsOwnedBy returns the set of group IDs assigned to brokerID under a.
func groupsOwnedBy(a *kafkaapi.Assignment, brokerID string) map[string]struct{} {
	out := make(map[string]struct{})
	if a == nil {
		return out
	}
	for _, g := range a.ConsumerGroups {
		if g.Broker == brokerID {
			out[g.GroupID] = struct{}{}
		}
	}
	return out
}
