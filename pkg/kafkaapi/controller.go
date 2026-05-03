package kafkaapi

import (
	"context"
	"time"
)

// ControllerLeaseName is the singleton cluster-wide controller Lease name.
// Lives here so both internal/controller (writer) and internal/broker
// (reader) can reference it without an import cycle.
const ControllerLeaseName = "skafka-controller"

// AssignmentChangeReason names the trigger that caused the controller to
// recompute and rewrite the assignment.
type AssignmentChangeReason string

const (
	AssignmentReasonBrokerJoined   AssignmentChangeReason = "broker_joined"
	AssignmentReasonBrokerLeaving  AssignmentChangeReason = "broker_leaving"
	AssignmentReasonBrokerDead     AssignmentChangeReason = "broker_dead"
	AssignmentReasonTopicCreated   AssignmentChangeReason = "topic_created"
	AssignmentReasonTopicDeleted   AssignmentChangeReason = "topic_deleted"
	AssignmentReasonTopicResized   AssignmentChangeReason = "topic_resized"
	AssignmentReasonAdminRebalance AssignmentChangeReason = "admin_rebalance"
)

// AssignmentChange is the input to Controller.UpdateAssignment. Phase 4
// fleshes out the variants; Phase 1 needs only the contract.
type AssignmentChange struct {
	Reason   AssignmentChangeReason
	BrokerID string // populated for broker_* reasons
	Topic    string // populated for topic_* reasons
}

// AssignmentChangeHandler is invoked on the broker side whenever the
// AssignmentStore signals a new authoritative assignment that has been
// validated against the controller Lease epoch.
type AssignmentChangeHandler func(ctx context.Context, prev, next *Assignment)

// Controller is the cluster controller role. Active only on the broker that
// holds the skafka-controller Lease; all brokers run the same binary, but
// only one runs the controller code path at a time.
//
// The controller owns the assignment file write path, the heartbeat gRPC
// server, and the best-effort KafkaClusterAssignments CR mirror. Phase 4
// implementation; Phase 1 defines only the contract.
type Controller interface {
	// Start activates the controller role. Must be called from the
	// OnStartedLeading callback of the controller Lease's leader elector
	// with the Lease's leaseTransitions value as the epoch.
	Start(ctx context.Context, epoch int64) error

	// Stop deactivates the controller role. Called from OnStoppedLeading.
	// Idempotent. Must release any held resources before returning.
	Stop() error

	// UpdateAssignment requests a recompute + write triggered by the given
	// change. Implementations may coalesce concurrent changes.
	UpdateAssignment(ctx context.Context, change AssignmentChange) error

	// BrokerHealth reports the controller's view of a broker's liveness,
	// derived from heartbeat freshness.
	BrokerHealth(brokerID string) BrokerHealth
}

// BrokerCoordinator is the broker-side coordination client. Active on every
// broker pod regardless of controller status. Tracks the local view of the
// assignment, drives storage TakeOver/Relinquish, and exposes ownership +
// heartbeat-freshness checks to the Append/Fetch hot path.
type BrokerCoordinator interface {
	// Start opens the heartbeat connection to the current controller and
	// begins watching the assignment file.
	Start(ctx context.Context) error

	// Stop closes the heartbeat stream and releases any owned partitions.
	Stop() error

	// Owns reports whether this broker is the assigned leader for the
	// given partition under the most recently applied assignment.
	Owns(topic string, partition int32) bool

	// CurrentEpoch returns the leadership epoch this broker holds for the
	// given partition. The boolean is false if this broker does not own
	// the partition.
	CurrentEpoch(topic string, partition int32) (epoch uint32, ok bool)

	// OnAssignmentChange registers a handler called after each successful
	// validation + apply of a new assignment. Handlers run synchronously
	// on the watcher goroutine; long work should be deferred.
	OnAssignmentChange(handler AssignmentChangeHandler)

	// LastHeartbeat reports the wall-clock time of the most recent PING
	// received from the controller. The Append path consults this for
	// self-fencing: stale heartbeat ⇒ refuse writes.
	LastHeartbeat() time.Time
}
