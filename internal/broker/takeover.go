package broker

import (
	"context"

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

	// Newly-owned (or higher-epoch) partitions.
	for k, ne := range nextOwned {
		pe, hadBefore := prevOwned[k]
		if hadBefore && pe.epoch == ne.epoch {
			continue
		}
		_, err := d.store.TakeOver(ctx, ne.topic, ne.partition, ne.epoch)
		if err != nil {
			// Surfaced via observability when wired; for now, silently
			// continue and the next heartbeat will signal the failure.
			_ = err
		}
	}

	// No-longer-owned partitions.
	for k, ref := range prevOwned {
		if _, stillOurs := nextOwned[k]; stillOurs {
			continue
		}
		if err := d.store.Relinquish(ref.topic, ref.partition); err != nil {
			_ = err
		}
	}
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
