package observability

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type stubGaugeSource struct {
	isController          int64
	assignmentVersion     int64
	brokerCountAlive      int64
	brokerCountAssigned   int64
	assignmentFileSize    int64
	partitions            []PartitionGauge
}

func (s *stubGaugeSource) IsController() int64            { return s.isController }
func (s *stubGaugeSource) AssignmentVersion() int64       { return s.assignmentVersion }
func (s *stubGaugeSource) BrokerCountAlive() int64        { return s.brokerCountAlive }
func (s *stubGaugeSource) BrokerCountAssigned() int64     { return s.brokerCountAssigned }
func (s *stubGaugeSource) AssignmentFileSizeBytes() int64 { return s.assignmentFileSize }
func (s *stubGaugeSource) Partitions() []PartitionGauge   { return s.partitions }

// TestRuntimeGaugesNoSource: Bootstrap registered the gauges before any
// SetGaugeSource call. The callback must still produce zero-valued
// samples — Prometheus prefers present-but-zero series over missing.
func TestRuntimeGaugesNoSource(t *testing.T) {
	defer SetGaugeSource(nil)
	SetGaugeSource(nil) // explicit reset

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	if err := installRuntimeGauges(mp.Meter("gauges-test")); err != nil {
		t.Fatalf("installRuntimeGauges: %v", err)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	// Find skafka.is.controller — should be a single sample with value 0.
	got := readGaugeValue(rm, "skafka.is.controller")
	if got != 0 {
		t.Errorf("is_controller without source = %d, want 0", got)
	}
}

// TestRuntimeGaugesWithSource: a populated source flows through the
// callback to the metrics reader.
func TestRuntimeGaugesWithSource(t *testing.T) {
	defer SetGaugeSource(nil)
	SetGaugeSource(&stubGaugeSource{
		isController:        1,
		assignmentVersion:   12847,
		brokerCountAlive:    3,
		brokerCountAssigned: 3,
		assignmentFileSize:  4096,
		partitions: []PartitionGauge{
			{Topic: "events", Partition: 0, LeaderID: 0, Epoch: 8, HighWatermark: 1000},
			{Topic: "events", Partition: 1, LeaderID: 1, Epoch: 8, HighWatermark: 950},
		},
	})

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	if err := installRuntimeGauges(mp.Meter("gauges-test")); err != nil {
		t.Fatalf("installRuntimeGauges: %v", err)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	checks := map[string]int64{
		"skafka.is.controller":         1,
		"skafka.assignment.version":    12847,
		"skafka.broker.count.alive":    3,
		"skafka.broker.count.assigned": 3,
		"skafka.assignment.file.size":  4096,
	}
	for name, want := range checks {
		if got := readGaugeValue(rm, name); got != want {
			t.Errorf("%s = %d, want %d", name, got, want)
		}
	}

	// Per-partition gauges: assert HWM is correctly disambiguated by
	// {topic, partition} attributes.
	hwms := readPartitionGauge(rm, "skafka.partition.high.watermark")
	if hwms["events/0"] != 1000 {
		t.Errorf("partition_high_watermark events/0 = %d, want 1000", hwms["events/0"])
	}
	if hwms["events/1"] != 950 {
		t.Errorf("partition_high_watermark events/1 = %d, want 950", hwms["events/1"])
	}
}

// readGaugeValue pulls the first data point of a non-attributed gauge.
func readGaugeValue(rm metricdata.ResourceMetrics, name string) int64 {
	for _, sm := range rm.ScopeMetrics {
		for _, inst := range sm.Metrics {
			if inst.Name != name {
				continue
			}
			g, ok := inst.Data.(metricdata.Gauge[int64])
			if !ok {
				continue
			}
			for _, dp := range g.DataPoints {
				if dp.Attributes.Len() == 0 {
					return dp.Value
				}
			}
		}
	}
	return -1 // sentinel to make missing gauges easy to spot in test failures
}

// readPartitionGauge buckets the data points of a per-partition gauge
// by "topic/partition" key.
func readPartitionGauge(rm metricdata.ResourceMetrics, name string) map[string]int64 {
	out := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, inst := range sm.Metrics {
			if inst.Name != name {
				continue
			}
			g, ok := inst.Data.(metricdata.Gauge[int64])
			if !ok {
				continue
			}
			for _, dp := range g.DataPoints {
				topic, _ := dp.Attributes.Value(attribute.Key("topic"))
				part, _ := dp.Attributes.Value(attribute.Key("partition"))
				out[topic.AsString()+"/"+part.Emit()] = dp.Value
			}
		}
	}
	return out
}
