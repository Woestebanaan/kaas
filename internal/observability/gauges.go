package observability

import (
	"context"
	"fmt"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// PartitionGauge is one row in the per-partition gauge sample. The
// GaugeSource implementation enumerates these on every metrics scrape;
// the snapshot must be cheap to compute.
type PartitionGauge struct {
	Topic         string
	Partition     int32
	LeaderID      int64
	Epoch         int64
	HighWatermark int64
}

// GaugeSource snapshots the v3 runtime state that the Phase 10
// ObservableGauges sample. Implementations MUST be safe to call from a
// metrics callback (i.e. once per Prometheus scrape) and complete
// quickly — the SDK serialises callbacks per scrape, so a slow source
// stalls every other gauge.
//
// A nil GaugeSource (the default before Bootstrap wires one) makes
// every gauge report zero. That's the right answer in local-dev / test
// where there's no v3 runtime to snapshot.
type GaugeSource interface {
	// IsController returns 1 when this broker holds the cluster controller
	// lease, 0 otherwise. Sum across the fleet should always be 0 or 1
	// (during a failover briefly 0; in steady state 1).
	IsController() int64

	// AssignmentVersion is the most recent assignmentVersion this broker
	// has applied. Operators watch this for "stuck broker" detection
	// (a broker whose value stops climbing while peers advance).
	AssignmentVersion() int64

	// BrokerCountAlive is the live broker count from the broker source
	// (EndpointSlice in k8s mode). BrokerCountAssigned is the number of
	// distinct brokers in the current assignment.json. The two should
	// match in steady state; a divergence means the controller is
	// catching up.
	BrokerCountAlive() int64
	BrokerCountAssigned() int64

	// AssignmentFileSizeBytes is the on-disk size of
	// /data/__cluster/assignment.json. Crosses Kubernetes 1MB CR-status
	// cap territory at ~8000 partitions; the gauge is the
	// early-warning signal for "you're approaching the truncation bound".
	AssignmentFileSizeBytes() int64

	// Partitions returns one row per partition this broker is aware of.
	// Driven by the broker.Coordinator's snapshot — typically the union
	// of "this broker leads" and "this broker is in the assignment".
	Partitions() []PartitionGauge
}

var gaugeSource atomic.Pointer[GaugeSource]

// SetGaugeSource installs the snapshot source. Called by cmd/skafka
// after the v3 runtime is up. nil source resets to the no-op default
// (every gauge reports zero).
func SetGaugeSource(s GaugeSource) {
	if s == nil {
		gaugeSource.Store(nil)
		return
	}
	gaugeSource.Store(&s)
}

// loadGaugeSource returns the active source or nil. Used by the
// callback installed in installRuntimeGauges.
func loadGaugeSource() GaugeSource {
	p := gaugeSource.Load()
	if p == nil {
		return nil
	}
	return *p
}

// installRuntimeGauges registers the Phase 10 ObservableGauges on the
// given meter and installs a single callback that pulls every value
// from the active GaugeSource. Returns an error if any instrument
// creation fails.
//
// Called by Bootstrap once during process startup. Safe to call before
// SetGaugeSource — until a source is installed, every gauge reports 0.
func installRuntimeGauges(m metric.Meter) error {
	isController, err := m.Int64ObservableGauge("skafka.is.controller",
		metric.WithDescription("1 if this broker holds the cluster controller lease, 0 otherwise"))
	if err != nil {
		return fmt.Errorf("gauge is_controller: %w", err)
	}
	assignmentVersion, err := m.Int64ObservableGauge("skafka.assignment.version",
		metric.WithDescription("Most recent assignmentVersion applied by this broker"))
	if err != nil {
		return fmt.Errorf("gauge assignment_version: %w", err)
	}
	brokerCountAlive, err := m.Int64ObservableGauge("skafka.broker.count.alive",
		metric.WithDescription("Live brokers as observed by this broker"))
	if err != nil {
		return fmt.Errorf("gauge broker_count_alive: %w", err)
	}
	brokerCountAssigned, err := m.Int64ObservableGauge("skafka.broker.count.assigned",
		metric.WithDescription("Distinct brokers in the current assignment.json"))
	if err != nil {
		return fmt.Errorf("gauge broker_count_assigned: %w", err)
	}
	assignmentFileSize, err := m.Int64ObservableGauge("skafka.assignment.file.size",
		metric.WithDescription("Size of /data/__cluster/assignment.json"),
		metric.WithUnit("By"))
	if err != nil {
		return fmt.Errorf("gauge assignment_file_size: %w", err)
	}
	partitionLeader, err := m.Int64ObservableGauge("skafka.partition.leader",
		metric.WithDescription("Per-partition leader broker ordinal"))
	if err != nil {
		return fmt.Errorf("gauge partition_leader: %w", err)
	}
	partitionEpoch, err := m.Int64ObservableGauge("skafka.partition.epoch",
		metric.WithDescription("Per-partition leader epoch"))
	if err != nil {
		return fmt.Errorf("gauge partition_epoch: %w", err)
	}
	partitionHighWatermark, err := m.Int64ObservableGauge("skafka.partition.high.watermark",
		metric.WithDescription("Per-partition high watermark offset"))
	if err != nil {
		return fmt.Errorf("gauge partition_high_watermark: %w", err)
	}

	_, err = m.RegisterCallback(func(_ context.Context, obs metric.Observer) error {
		src := loadGaugeSource()
		if src == nil {
			// No source installed — emit zero so the gauges still appear in
			// Prometheus output (Grafana panels prefer present-but-zero over
			// missing series).
			obs.ObserveInt64(isController, 0)
			obs.ObserveInt64(assignmentVersion, 0)
			obs.ObserveInt64(brokerCountAlive, 0)
			obs.ObserveInt64(brokerCountAssigned, 0)
			obs.ObserveInt64(assignmentFileSize, 0)
			return nil
		}

		obs.ObserveInt64(isController, src.IsController())
		obs.ObserveInt64(assignmentVersion, src.AssignmentVersion())
		obs.ObserveInt64(brokerCountAlive, src.BrokerCountAlive())
		obs.ObserveInt64(brokerCountAssigned, src.BrokerCountAssigned())
		obs.ObserveInt64(assignmentFileSize, src.AssignmentFileSizeBytes())

		for _, p := range src.Partitions() {
			attrs := metric.WithAttributes(
				attribute.String("topic", p.Topic),
				attribute.Int("partition", int(p.Partition)),
			)
			obs.ObserveInt64(partitionLeader, p.LeaderID, attrs)
			obs.ObserveInt64(partitionEpoch, p.Epoch, attrs)
			obs.ObserveInt64(partitionHighWatermark, p.HighWatermark, attrs)
		}
		return nil
	},
		isController,
		assignmentVersion,
		brokerCountAlive,
		brokerCountAssigned,
		assignmentFileSize,
		partitionLeader,
		partitionEpoch,
		partitionHighWatermark,
	)
	if err != nil {
		return fmt.Errorf("register gauge callback: %w", err)
	}
	return nil
}
