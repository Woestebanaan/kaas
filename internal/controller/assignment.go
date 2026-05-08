package controller

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/woestebanaan/skafka/internal/observability"
	"github.com/woestebanaan/skafka/pkg/kafkaapi"
)

// TopicSource exposes the live topic catalog the controller assigns
// partitions over. The KafkaTopic CRD watcher (operator side) is the
// canonical implementation; tests pass an in-memory stub.
type TopicSource interface {
	Topics() []TopicSpec
}

// BrokerSource reports which brokers the controller currently considers
// alive. The HeartbeatServer's ConnectedBrokers list is the canonical
// source; tests pass an in-memory stub. Drain / dead state is layered on
// top of "is this broker streaming heartbeats" by the controller's own
// liveness logic.
type BrokerSource interface {
	AliveBrokers() []string
}

// GroupSource reports the consumer groups currently active in the
// cluster. HeartbeatServer.ActiveGroups() (the union of every connected
// broker's BrokerStatus.active_groups) is the canonical source; tests
// pass an in-memory stub. AssignmentLoop calls this on every recompute
// to drive BalanceGroups.
type GroupSource interface {
	ActiveGroups() []string
}

// CRMirror is the best-effort write to the KafkaClusterAssignments CR for
// kubectl-style debugging. The plan is explicit that mirror failures are
// fire-and-forget — a successful AssignmentStore.Write is the
// authoritative record, and the CR is a convenience for cluster operators.
// A no-op stub satisfies the interface today; the real Kubernetes-backed
// implementation lands in a follow-up that wires the operator's typed
// client.
type CRMirror interface {
	Mirror(ctx context.Context, a *kafkaapi.Assignment)
}

// noopMirror is the zero-value mirror used when no real implementation is
// wired. Logs are surfaced through observability when that's hooked in.
type noopMirror struct{}

func (noopMirror) Mirror(_ context.Context, _ *kafkaapi.Assignment) {}

// NewNoopMirror returns a mirror that does nothing — placeholder until the
// operator-style CR client wiring lands.
func NewNoopMirror() CRMirror { return noopMirror{} }

// AssignmentLoop owns the controller's view of the cluster state and the
// recompute → write → notify pipeline. Each call to UpdateAssignment
// queues a change reason; the loop coalesces concurrent updates into a
// single write per tick.
//
// Lifecycle:
//   - Start(ctx, epoch, controllerID) — call once after winning the Lease.
//     Reads the existing assignment.json (if any) so a controller restart
//     keeps the same epoch monotonic series.
//   - Stop() — drains queued updates, performs one final write so the
//     observed state on disk matches the in-memory state at shutdown.
//
// The loop is intentionally single-goroutine — no per-update locking on
// the hot path beyond the channel push. Concurrent UpdateAssignment calls
// from broker churn + topic events serialize cleanly.
type AssignmentLoop struct {
	store        kafkaapi.AssignmentStore
	heart        *HeartbeatServer
	mirror       CRMirror
	topics       TopicSource
	brokers      BrokerSource
	groups       GroupSource // optional; nil means consumerGroups stays empty
	controllerID string

	// channelDepth is the queue size for pending change reasons. With
	// coalescing semantics, a few slots are plenty: the loop reads one,
	// performs the write, and re-checks the channel before sleeping.
	channelDepth int

	updates chan kafkaapi.AssignmentChange

	mu              sync.Mutex
	current         *kafkaapi.Assignment
	controllerEpoch int64
	versionCounter  int64
	started         bool
}

// NewAssignmentLoop builds a loop. heart and mirror may be nil in tests
// that don't exercise those paths.
func NewAssignmentLoop(
	store kafkaapi.AssignmentStore,
	heart *HeartbeatServer,
	mirror CRMirror,
	topics TopicSource,
	brokers BrokerSource,
	controllerID string,
) *AssignmentLoop {
	if mirror == nil {
		mirror = NewNoopMirror()
	}
	return &AssignmentLoop{
		store:        store,
		heart:        heart,
		mirror:       mirror,
		topics:       topics,
		brokers:      brokers,
		controllerID: controllerID,
		channelDepth: 32,
		updates:      make(chan kafkaapi.AssignmentChange, 32),
	}
}

// WithGroupSource attaches a GroupSource so each recompute also assigns
// consumer groups via BalanceGroups. Optional; nil leaves
// Assignment.ConsumerGroups empty (the v2.6 path that pre-dates this).
// In production, pass HeartbeatServer (which satisfies GroupSource via
// its ActiveGroups method).
func (l *AssignmentLoop) WithGroupSource(g GroupSource) *AssignmentLoop {
	l.groups = g
	return l
}

// Start runs the loop until ctx is cancelled. epoch is the
// leaseTransitions value from Election — it goes into every
// assignment.json this controller writes, so brokers can reject
// stale-controller writes. controllerID is the broker pod name that
// holds the Lease (for the Controller field of the JSON).
//
// Start performs an initial recompute + write so the controller's first
// version-bump is visible to brokers immediately after election, rather
// than waiting for the first explicit UpdateAssignment call.
func (l *AssignmentLoop) Start(ctx context.Context, epoch int64) error {
	l.mu.Lock()
	if l.started {
		l.mu.Unlock()
		return nil
	}
	l.started = true
	l.controllerEpoch = epoch
	l.mu.Unlock()

	// Bootstrap from existing on-disk state. Two reasons:
	//   1. Carry forward the assignmentVersion sequence — restarts shouldn't
	//      silently rewind the version counter, or brokers' lastAppliedVersion
	//      check would deduplicate fresh writes.
	//   2. Keep stable assignments stable: the balancer's strict-stability
	//      rule depends on knowing the prev assignment.
	if existing, err := l.store.Read(ctx); err == nil {
		l.mu.Lock()
		l.current = existing
		// Bump beyond whatever the previous controller wrote so our first
		// version is strictly greater.
		if existing.AssignmentVersion >= l.versionCounter {
			l.versionCounter = existing.AssignmentVersion
		}
		l.mu.Unlock()
	}

	// Initial recompute on Start. Time the first write end-to-end so
	// ControllerFailoverDuration captures the "won the lease → live"
	// gap; subsequent writes don't carry failover semantics.
	failoverStart := time.Now()
	_ = l.recomputeAndWrite(ctx, kafkaapi.AssignmentChange{
		Reason: kafkaapi.AssignmentReasonBrokerJoined, // most generic "we're starting up"
	})
	observability.Global().ControllerFailoverDuration.Record(ctx, time.Since(failoverStart).Seconds())

	for {
		select {
		case <-ctx.Done():
			return nil
		case change := <-l.updates:
			// Coalesce: drain any other queued changes; one write covers
			// all of them since recompute is idempotent over the latest
			// inputs.
			l.drainPending(change)
			_ = l.recomputeAndWrite(ctx, change)
		}
	}
}

// drainPending consumes any other queued change events without blocking.
// Used by the loop after picking up the first change to merge a burst of
// concurrent updates into a single recompute.
func (l *AssignmentLoop) drainPending(_ kafkaapi.AssignmentChange) {
	for {
		select {
		case <-l.updates:
		default:
			return
		}
	}
}

// UpdateAssignment queues a change. Non-blocking: if the channel is full
// the change is dropped (the next recompute will pick up the same inputs
// from BrokerSource / TopicSource anyway, so dropped reasons don't lose
// state). Returns nil so it satisfies the kafkaapi.Controller signature.
func (l *AssignmentLoop) UpdateAssignment(_ context.Context, change kafkaapi.AssignmentChange) error {
	select {
	case l.updates <- change:
	default:
		// Buffer full — coalesced into the next recompute.
	}
	return nil
}

// recomputeAndWrite is the core: snapshot inputs, run the balancer, write
// through the AssignmentStore, push notification, mirror to CR.
func (l *AssignmentLoop) recomputeAndWrite(ctx context.Context, change kafkaapi.AssignmentChange) error {
	ctx, span := observability.Tracer().Start(ctx, "controller.recompute",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String("change.reason", string(change.Reason)),
			attribute.String("change.topic", change.Topic),
			attribute.String("change.broker", change.BrokerID),
		),
	)
	defer span.End()

	brokers := l.brokers.AliveBrokers()
	topics := l.topics.Topics()

	// Pre-build the GroupSpec list outside the lock so the GroupSource's
	// own locking doesn't intersect with l.mu.
	var groupSpecs []GroupSpec
	if l.groups != nil {
		ids := l.groups.ActiveGroups()
		groupSpecs = make([]GroupSpec, 0, len(ids))
		for _, id := range ids {
			groupSpecs = append(groupSpecs, GroupSpec{GroupID: id})
		}
	}

	l.mu.Lock()
	prev := l.current
	parts := Balance(prev, brokers, topics)
	groups := BalanceGroups(prev, brokers, groupSpecs)
	l.versionCounter++
	version := l.versionCounter

	a := &kafkaapi.Assignment{
		ControllerEpoch:   l.controllerEpoch,
		AssignmentVersion: version,
		GeneratedAt:       time.Now().UTC(),
		Controller:        l.controllerID,
		Brokers:           buildBrokerEntries(brokers),
		Partitions:        parts,
		ConsumerGroups:    groups,
	}
	l.current = a
	l.mu.Unlock()

	observability.Global().AssignmentChanges.Add(ctx, 1)
	span.SetAttributes(
		attribute.Int64("assignment.version", version),
		attribute.Int64("assignment.epoch", l.controllerEpoch),
		attribute.Int("brokers.alive", len(brokers)),
		attribute.Int("topics.total", len(topics)),
		attribute.Int("partitions.total", len(parts)),
		attribute.Int("groups.total", len(groups)),
	)

	if err := l.store.Write(ctx, a); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "store.write")
		return err
	}

	if l.heart != nil {
		l.heart.PushAssignmentChanged(uint64(version))
		observability.Global().AssignmentPushes.Add(ctx, 1)
	}
	l.mirror.Mirror(ctx, a)
	return nil
}

// buildBrokerEntries renders the controller's view of broker liveness into
// the assignment.json broker list. v1 marks every alive broker as alive
// with the current wall-clock timestamp; broker drain / dead transitions
// will plug into this in a follow-up.
func buildBrokerEntries(brokers []string) []kafkaapi.BrokerAssignment {
	now := time.Now().UTC()
	out := make([]kafkaapi.BrokerAssignment, len(brokers))
	for i, b := range brokers {
		out[i] = kafkaapi.BrokerAssignment{
			ID:       b,
			Health:   kafkaapi.BrokerHealthAlive,
			LastSeen: now,
		}
	}
	return out
}

// Snapshot returns a defensive copy of the most recently written
// assignment, useful for diagnostics + tests.
func (l *AssignmentLoop) Snapshot() *kafkaapi.Assignment {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.current == nil {
		return nil
	}
	cp := *l.current
	return &cp
}
