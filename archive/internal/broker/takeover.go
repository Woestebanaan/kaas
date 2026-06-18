package broker

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/woestebanaan/skafka/internal/observability"
	"github.com/woestebanaan/skafka/internal/storage"
	"github.com/woestebanaan/skafka/pkg/kafkaapi"
)

// TakeoverDriver reacts to assignment changes by driving the underlying
// StorageEngine through TakeOver / Relinquish. It is registered as a
// handler on the Coordinator (via OnAssignmentChange) and runs
// synchronously on the watcher goroutine.
//
// The plan §"Takeover sequence (controller-driven)" defines the broker
// behaviour precisely: list segments, scan-and-seal the prior leader's
// last segment, write .recovery sidecar, create fresh segment under the
// new epoch. Most of that work lives inside storage.TakeOver; this driver
// is the dispatcher that decides which partitions need TakeOver vs.
// Relinquish and surfaces the recovered HWM for heartbeat reporting.
type TakeoverDriver struct {
	store    storage.StorageEngine
	brokerID string
}

// NewTakeoverDriver builds a driver bound to the given storage engine.
// brokerID is needed to filter assignment.Partitions down to "owned by us".
func NewTakeoverDriver(store storage.StorageEngine, brokerID string) *TakeoverDriver {
	return &TakeoverDriver{store: store, brokerID: brokerID}
}

// OnAssignmentChange is the handler signature expected by Coordinator.
// It diffs prev vs next and:
//   - Calls storage.TakeOver(ctx, topic, partition, epoch) for partitions
//     newly assigned to this broker (or assigned at a higher epoch than
//     before — leader-flapped during recovery).
//   - Calls storage.Relinquish(topic, partition) for partitions previously
//     ours but no longer ours.
//
// Errors from TakeOver/Relinquish are not retried here — the next heartbeat
// will report the partition as ERROR or RECOVERING and the controller can
// react. We do not want to block the assignment-watch goroutine on a
// per-partition recovery that may take seconds.
func (d *TakeoverDriver) OnAssignmentChange(ctx context.Context, prev, next *kafkaapi.Assignment) {
	prevOwned := ownedByBroker(prev, d.brokerID)
	nextOwned := ownedByBroker(next, d.brokerID)

	// Single root span per assignment-change wakeup, covering all
	// per-partition takeovers + relinquishes that this delta requires.
	// Assignment version goes on as an attribute so a Tempo search
	// "broker.assignment_changed where assignment.version=N" lines up
	// across all 3 brokers for the same controller write. ctx coming
	// in is the assignment-watcher's per-event context (no Lease
	// chain) so this becomes a fresh root trace.
	var nextVersion int64
	if next != nil {
		nextVersion = next.AssignmentVersion
	}
	ctx, span := observability.Tracer().Start(ctx, "broker.assignment_changed",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String("broker.id", d.brokerID),
			attribute.Int64("assignment.version", nextVersion),
			attribute.Int("partitions.prev", len(prevOwned)),
			attribute.Int("partitions.next", len(nextOwned)),
		),
	)
	defer span.End()

	// Newly-owned (or higher-epoch) partitions.
	takeovers := 0
	for k, ne := range nextOwned {
		pe, hadBefore := prevOwned[k]
		if hadBefore && pe.epoch == ne.epoch {
			continue
		}
		takeovers++
		ptCtx, ptSpan := observability.Tracer().Start(ctx, "broker.takeover_partition",
			trace.WithAttributes(
				attribute.String("topic", ne.topic),
				attribute.Int("partition", int(ne.partition)),
				attribute.Int64("epoch", int64(ne.epoch)),
			),
		)
		_, err := d.store.TakeOver(ptCtx, ne.topic, ne.partition, ne.epoch)
		if err != nil {
			ptSpan.RecordError(err)
			ptSpan.SetStatus(codes.Error, "TakeOver")
		}
		ptSpan.End()
	}

	// No-longer-owned partitions.
	relinquishes := 0
	for k, ref := range prevOwned {
		if _, stillOurs := nextOwned[k]; stillOurs {
			continue
		}
		relinquishes++
		_, ptSpan := observability.Tracer().Start(ctx, "broker.relinquish_partition",
			trace.WithAttributes(
				attribute.String("topic", ref.topic),
				attribute.Int("partition", int(ref.partition)),
			),
		)
		if err := d.store.Relinquish(ref.topic, ref.partition); err != nil {
			ptSpan.RecordError(err)
			ptSpan.SetStatus(codes.Error, "Relinquish")
		}
		ptSpan.End()
	}
	span.SetAttributes(
		attribute.Int("partitions.taken_over", takeovers),
		attribute.Int("partitions.relinquished", relinquishes),
	)
}

// ownedRef captures the topic/partition/epoch triple needed to call into
// the storage engine. Internal to the diff loop above.
type ownedRef struct {
	topic     string
	partition int32
	epoch     uint32
}

// ownedByBroker returns the subset of a.Partitions that are assigned to
// brokerID, keyed by partitionKey for set-difference math.
func ownedByBroker(a *kafkaapi.Assignment, brokerID string) map[string]ownedRef {
	out := make(map[string]ownedRef)
	if a == nil {
		return out
	}
	for _, p := range a.Partitions {
		if p.Broker != brokerID {
			continue
		}
		out[partitionKey(p.Topic, p.Partition)] = ownedRef{
			topic:     p.Topic,
			partition: p.Partition,
			epoch:     p.Epoch,
		}
	}
	return out
}
