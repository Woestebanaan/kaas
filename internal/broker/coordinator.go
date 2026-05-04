package broker

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/woestebanaan/skafka/internal/assignment"
	"github.com/woestebanaan/skafka/internal/lease"
	"github.com/woestebanaan/skafka/internal/observability"
	"github.com/woestebanaan/skafka/pkg/kafkaapi"
)

// Coordinator is the broker-side concrete implementation of
// kafkaapi.BrokerCoordinator. It glues:
//
//   - AssignmentStore (file-backed, internal/assignment) — reads the file
//     and watches for changes via fsnotify + 1s mtime poll.
//   - ControllerWatch — provides the current Lease leaseTransitions value
//     used to fence stale assignment.json writes from a partitioned
//     ex-controller.
//   - HeartbeatClient — supplies LastHeartbeat() for self-fencing.
//
// The Coordinator does NOT yet drive storage TakeOver/Relinquish; that's
// step 6 (takeover.go). For now it observes the authoritative assignment,
// validates each new version against the current controller epoch, and
// dispatches registered handlers.
type Coordinator struct {
	brokerID string
	store    kafkaapi.AssignmentStore
	leases   *ControllerWatch
	heart    *HeartbeatClient

	mu                 sync.RWMutex
	current            *kafkaapi.Assignment
	lastAppliedVersion int64
	handlers           []kafkaapi.AssignmentChangeHandler

	// ownership maps "topic/partition" → epoch for fast Owns / CurrentEpoch lookups.
	ownership map[string]uint32
}

// NewCoordinator builds a Coordinator. Callers wire the dependencies:
// the file-backed AssignmentStore (typically rooted at /data), a
// ControllerWatch on the same namespace as the controller Lease, and a
// HeartbeatClient pointed at the current controller's gRPC endpoint.
func NewCoordinator(
	brokerID string,
	store kafkaapi.AssignmentStore,
	leases *ControllerWatch,
	heart *HeartbeatClient,
) *Coordinator {
	return &Coordinator{
		brokerID:  brokerID,
		store:     store,
		leases:    leases,
		heart:     heart,
		ownership: make(map[string]uint32),
	}
}

// Start subscribes to the AssignmentStore's Watch channel and dispatches
// changes through the validation pipeline. Run blocks until ctx is
// cancelled. If the store has no assignment yet, Start waits — joining a
// fresh cluster before the controller has written anything is normal.
func (c *Coordinator) Start(ctx context.Context) error {
	ch, err := c.store.Watch(ctx)
	if err != nil {
		return err
	}

	// Best-effort initial load: if assignment.json already exists, apply it
	// immediately so Owns/CurrentEpoch don't report empty state during the
	// gap between Start and the first Watch tick.
	c.applyIfNew(ctx)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ch:
			changed := c.applyIfNew(ctx)
			label := "false"
			if changed {
				label = "true"
			}
			observability.Global().AssignmentPolls.Add(ctx, 1,
				metric.WithAttributes(attribute.String("change_detected", label)))
		}
	}
}

// Stop is a no-op for the current Coordinator — Start blocks on ctx.Done so
// cancelling the context drives shutdown. Kept for interface conformance
// (kafkaapi.BrokerCoordinator).
func (c *Coordinator) Stop() error {
	return nil
}

// applyIfNew reads the current assignment file, validates it against the
// controller Lease epoch, dedups against lastAppliedVersion, and dispatches
// handlers when a fresh version is observed. Returns true if a new
// assignment was applied (used by AssignmentPolls's change_detected label).
func (c *Coordinator) applyIfNew(ctx context.Context) bool {
	a, err := c.store.Read(ctx)
	if err != nil {
		// fs.ErrNotExist on a fresh cluster is fine; other errors mean the
		// next Watch tick will retry.
		return false
	}

	// Epoch fence: reject files written by a controller older than the one
	// our cached Lease informer believes is current. Plan §"The
	// stale-controller race (and how the epoch fence resolves it)".
	leaseEpoch := c.leases.CurrentEpoch()
	if a.ControllerEpoch < leaseEpoch {
		observability.Global().StaleAssignmentsRejected.Add(ctx, 1)
		return false
	}

	// Version dedup: assignmentVersion is monotonic within a single
	// controller's tenure. Skip if we've already applied this version.
	c.mu.Lock()
	if a.AssignmentVersion <= c.lastAppliedVersion && c.current != nil &&
		c.current.ControllerEpoch == a.ControllerEpoch {
		c.mu.Unlock()
		return false
	}
	prev := c.current
	c.current = a
	c.lastAppliedVersion = a.AssignmentVersion
	c.rebuildOwnership()
	handlers := append([]kafkaapi.AssignmentChangeHandler(nil), c.handlers...)
	c.mu.Unlock()

	for _, h := range handlers {
		h(ctx, prev, a)
	}
	return true
}

// rebuildOwnership refreshes the topic/partition → epoch map from the
// current Assignment. Called with c.mu held.
func (c *Coordinator) rebuildOwnership() {
	m := make(map[string]uint32, len(c.current.Partitions))
	for _, p := range c.current.Partitions {
		if p.Broker != c.brokerID {
			continue
		}
		m[partitionKey(p.Topic, p.Partition)] = p.Epoch
	}
	c.ownership = m
}

// Owns reports whether this broker is the assigned leader for (topic,
// partition) under the most recently applied assignment.
func (c *Coordinator) Owns(topic string, partition int32) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.ownership[partitionKey(topic, partition)]
	return ok
}

// LeaderFor returns the broker ordinal that leads (topic, partition)
// under the most recently applied assignment. Returns -1 when the
// partition is not in the current assignment (unknown topic, or the
// controller hasn't recomputed since the topic was added).
//
// This is the assignment-driven replacement for the old per-partition
// Lease.LeaderFor lookup: the per-partition Lease holder and the CR's
// assigned broker could disagree (gh #75 split-brain), but assignment.json
// is the controller's single source of truth.
func (c *Coordinator) LeaderFor(topic string, partition int32) int32 {
	c.mu.RLock()
	a := c.current
	c.mu.RUnlock()
	if a == nil {
		return -1
	}
	for _, p := range a.Partitions {
		if p.Topic == topic && p.Partition == partition {
			ord := lease.ParseOrdinalFromIdentity(p.Broker)
			if ord < 0 {
				return -1
			}
			return ord
		}
	}
	return -1
}

// CurrentEpoch returns the leadership epoch this broker holds for (topic,
// partition). Second return is false when this broker doesn't own the
// partition.
func (c *Coordinator) CurrentEpoch(topic string, partition int32) (uint32, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.ownership[partitionKey(topic, partition)]
	return e, ok
}

// OnAssignmentChange registers a handler invoked after each successful
// validation + apply of a new assignment. Handlers run synchronously on
// the watcher goroutine; long work should be deferred.
func (c *Coordinator) OnAssignmentChange(h kafkaapi.AssignmentChangeHandler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.handlers = append(c.handlers, h)
}

// LastHeartbeat returns the wall-clock time of the most recent message
// received from the controller — delegated to the heartbeat client.
// Self-fence in step 6 will read this to decide IsHeartbeatFresh.
func (c *Coordinator) LastHeartbeat() time.Time {
	if c.heart == nil {
		return time.Time{}
	}
	return c.heart.LastReceived()
}

// OwnsGroup reports whether this broker is the assigned coordinator for
// groupID under the most recently applied assignment. Phase 5: replaces
// the v2.6 per-group Lease IsCoordinator check.
func (c *Coordinator) OwnsGroup(groupID string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.current == nil {
		return false
	}
	for _, g := range c.current.ConsumerGroups {
		if g.GroupID == groupID {
			return g.Broker == c.brokerID
		}
	}
	return false
}

// GroupCoordinator returns the broker ID assigned to coordinate
// groupID. Second return is false when the group has no row in the
// current assignment — the controller hasn't registered it yet.
// FindCoordinator surfaces that as CoordinatorNotAvailable so the
// client retries; the next BrokerStatus heartbeat reports the group
// in active_groups, the controller registers it, the client's next
// FindCoordinator succeeds.
func (c *Coordinator) GroupCoordinator(groupID string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.current == nil {
		return "", false
	}
	for _, g := range c.current.ConsumerGroups {
		if g.GroupID == groupID {
			return g.Broker, true
		}
	}
	return "", false
}

// Snapshot returns a defensive copy of the most recently applied
// assignment, useful for diagnostics. Returns nil before any assignment
// has been applied.
func (c *Coordinator) Snapshot() *kafkaapi.Assignment {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.current == nil {
		return nil
	}
	cp := *c.current
	return &cp
}

func partitionKey(topic string, partition int32) string {
	// Inline to avoid a fmt.Sprintf — Owns is on the produce hot path.
	// 32 bytes is enough for "topic-name/2147483647".
	buf := make([]byte, 0, len(topic)+12)
	buf = append(buf, topic...)
	buf = append(buf, '/')
	buf = appendInt32(buf, partition)
	return string(buf)
}

func appendInt32(dst []byte, v int32) []byte {
	if v == 0 {
		return append(dst, '0')
	}
	if v < 0 {
		dst = append(dst, '-')
		v = -v
	}
	var stack [10]byte
	n := 0
	for v > 0 {
		stack[n] = byte('0' + v%10)
		v /= 10
		n++
	}
	for i := n - 1; i >= 0; i-- {
		dst = append(dst, stack[i])
	}
	return dst
}

// Compile-time assertion: Coordinator satisfies the kafkaapi contract.
var _ kafkaapi.BrokerCoordinator = (*Coordinator)(nil)

// satisfy the assignment package import for documentation; the runtime
// dependency is via the AssignmentStore interface.
var _ = assignment.IsNotExist
