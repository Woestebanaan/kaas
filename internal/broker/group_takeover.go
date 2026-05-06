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
	// LocalGroups returns every group ID this broker currently has
	// in-memory. Used by OnAssignmentChange's orphan sweep to drop
	// stale entries that the prev→next diff alone misses.
	LocalGroups() []string
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
// expected by Coordinator.OnAssignmentChange. It enforces:
//
//	post-condition: m.groups keys ⊆ groups assigned to this broker.
//
// The prev→next diff alone is not enough. A stray entry can land in
// m.groups when a JoinGroup arrives during the brief window the
// controller has assigned the group to this broker (and OwnsGroup
// returns true), and then later moves it elsewhere via a recompute.
// The diff at line `for groupID := range prevOurs` only relinquishes
// entries that were in the BROKER'S previous assignment view; an
// entry created when this broker had no prior assignment, or under
// an assignment that's been overwritten in memory before the next
// change handler fires, is never in `prevOurs` and therefore never
// gets cleaned up. The result is the script-audit symptom: stale
// `--list` rows on non-coordinator brokers (gh #89 follow-up,
// surfaced by the v0.1.49 verification).
//
// Two passes:
//  1. Diff prev→next for the common case (state-changes that did
//     touch this broker's known assignment view).
//  2. Orphan sweep: anything in `LocalGroups()` not in `nextOurs`
//     gets relinquished. This is the self-healing pass that closes
//     the leak.
func (d *GroupTakeoverDriver) OnAssignmentChange(_ context.Context, prev, next *kafkaapi.Assignment) {
	prevOurs := groupsOwnedBy(prev, d.brokerID)
	nextOurs := groupsOwnedBy(next, d.brokerID)

	// Single-fire: a group surfaced by both the diff and the sweep
	// passes still gets relinquished exactly once. Manager's
	// RelinquishGroup is itself idempotent, but emitting two
	// matching calls per change is wasteful and noisy in any
	// future logging.
	relinquished := make(map[string]struct{})
	relinquish := func(groupID string) {
		if _, done := relinquished[groupID]; done {
			return
		}
		relinquished[groupID] = struct{}{}
		d.mgr.RelinquishGroup(groupID)
	}

	for groupID := range prevOurs {
		if _, stillOurs := nextOurs[groupID]; stillOurs {
			continue
		}
		relinquish(groupID)
	}

	// Orphan sweep: drop any in-memory group not in the broker's
	// current assignment view. Closes the gap left by the prev→next
	// diff when a stray entry landed in m.groups under a transient
	// "I own this" window the broker had since-overwritten.
	for _, groupID := range d.mgr.LocalGroups() {
		if _, ours := nextOurs[groupID]; ours {
			continue
		}
		relinquish(groupID)
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
