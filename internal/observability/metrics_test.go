package observability

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// newTestMetrics installs a ManualReader-backed Metrics instance and returns a
// reader that the test uses to collect results synchronously.
func newTestMetrics(t *testing.T) (*Metrics, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m, err := newMetrics(mp.Meter("skafka-test"))
	if err != nil {
		t.Fatalf("newMetrics: %v", err)
	}
	return m, reader
}

func TestMetricsProduceBytes(t *testing.T) {
	m, reader := newTestMetrics(t)
	ctx := context.Background()

	m.ProduceBytes.Add(ctx, 100, metric.WithAttributes(attribute.String("topic", "t1")))
	m.ProduceBytes.Add(ctx, 50, metric.WithAttributes(attribute.String("topic", "t2")))
	m.ProduceBytes.Add(ctx, 25, metric.WithAttributes(attribute.String("topic", "t1")))

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}

	counts := collectSumCounts(t, rm, "skafka.produce.bytes")
	if counts["t1"] != 125 {
		t.Errorf("t1=%d, want 125", counts["t1"])
	}
	if counts["t2"] != 50 {
		t.Errorf("t2=%d, want 50", counts["t2"])
	}
}

func TestMetricsRequestLatencyHistogram(t *testing.T) {
	m, reader := newTestMetrics(t)
	ctx := context.Background()

	m.RequestLatency.Record(ctx, 0.001, metric.WithAttributes(attribute.Int("api_key", 0)))
	m.RequestLatency.Record(ctx, 0.5, metric.WithAttributes(attribute.Int("api_key", 0)))

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}

	found := false
	for _, sm := range rm.ScopeMetrics {
		for _, inst := range sm.Metrics {
			if inst.Name == "skafka.request.latency" {
				if h, ok := inst.Data.(metricdata.Histogram[float64]); ok && len(h.DataPoints) > 0 {
					if h.DataPoints[0].Count != 2 {
						t.Errorf("histogram count=%d, want 2", h.DataPoints[0].Count)
					}
					found = true
				}
			}
		}
	}
	if !found {
		t.Error("skafka.request.latency histogram not found")
	}
}

func TestGlobalReturnsNoopWhenUnset(t *testing.T) {
	// Global() must never return nil. Use the no-op path explicitly.
	globalMetrics.Store(nil)
	m := Global()
	if m == nil {
		t.Fatal("Global() returned nil — expected no-op singleton")
	}
	// Operations on no-op metrics must not panic.
	m.ProduceBytes.Add(context.Background(), 1)
	m.RequestLatency.Record(context.Background(), 0.1)
}

func TestSetGlobalReplacesDefault(t *testing.T) {
	m, _ := newTestMetrics(t)
	defer globalMetrics.Store(nil)
	SetGlobal(m)
	if Global() != m {
		t.Error("SetGlobal did not replace the active registry")
	}
}

// collectSumCounts pulls topic→value from the first sum instrument matching name.
func collectSumCounts(t *testing.T, rm metricdata.ResourceMetrics, name string) map[string]int64 {
	t.Helper()
	out := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, inst := range sm.Metrics {
			if inst.Name != name {
				continue
			}
			if s, ok := inst.Data.(metricdata.Sum[int64]); ok {
				for _, dp := range s.DataPoints {
					topic, _ := dp.Attributes.Value(attribute.Key("topic"))
					out[topic.AsString()] = dp.Value
				}
			}
		}
	}
	return out
}
