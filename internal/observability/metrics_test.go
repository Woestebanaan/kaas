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
	m, err := NewMetrics(mp.Meter("skafka-test"))
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}
	return m, reader
}

func TestMetricsProduceBytes(t *testing.T) {
	m, reader := newTestMetrics(t)
	ctx := context.Background()

	// gh #115 / gh #121 PR1: per-topic produce bytes is now an
	// ObservableCounter fed by an atomic accumulator. RecordProduce
	// is the hot-path API; the OTel callback emits cumulative.
	m.TopicTraffic.RecordProduce("t1", 0, 100)
	m.TopicTraffic.RecordProduce("t2", 0, 50)
	m.TopicTraffic.RecordProduce("t1", 0, 25)

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

// TestTopicTrafficIdleEmit pins the gh #115 contract: a topic that
// has been Touched but never received traffic STILL emits a
// cumulative observation (= 0) at every scrape. Pre-fix the
// timeseries was absent entirely; Grafana panels showed a gap
// indistinguishable from a crashed cluster.
func TestTopicTrafficIdleEmit(t *testing.T) {
	m, reader := newTestMetrics(t)
	ctx := context.Background()

	// Topic exists in the registry, but no traffic has flowed.
	m.TopicTraffic.Touch("idle-topic")

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}

	for _, suffix := range []string{"skafka.produce.records", "skafka.produce.bytes", "skafka.fetch.records", "skafka.fetch.bytes"} {
		counts := collectSumCounts(t, rm, suffix)
		v, ok := counts["idle-topic"]
		if !ok {
			t.Errorf("%s: idle topic absent from observation (gh #115 regression)", suffix)
			continue
		}
		if v != 0 {
			t.Errorf("%s: idle topic cumulative=%d, want 0", suffix, v)
		}
	}
}

// TestTopicTrafficAutoTouch verifies the hot-path API auto-creates
// the accumulator on first traffic, so callers never have to
// invoke Touch() defensively.
func TestTopicTrafficAutoTouch(t *testing.T) {
	m, reader := newTestMetrics(t)
	ctx := context.Background()

	m.TopicTraffic.RecordFetch("fresh-topic", 7, 1024)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}
	if got := collectSumCounts(t, rm, "skafka.fetch.records")["fresh-topic"]; got != 7 {
		t.Errorf("fetch.records['fresh-topic']=%d, want 7", got)
	}
	if got := collectSumCounts(t, rm, "skafka.fetch.bytes")["fresh-topic"]; got != 1024 {
		t.Errorf("fetch.bytes['fresh-topic']=%d, want 1024", got)
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

// TestLatencyHistogramsHaveExplicitBoundaries guards gh #79: every
// latency histogram (unit=s) must declare explicit bucket boundaries
// that span sub-millisecond → multi-second. Without this, OTel falls
// back to its default ms-scale boundaries (5, 10, 25 ... 10000) and
// every observation lands in [0, 5], collapsing percentiles to fixed
// 2.5 / 4.75 / 4.95 in Grafana regardless of actual load.
func TestLatencyHistogramsHaveExplicitBoundaries(t *testing.T) {
	m, reader := newTestMetrics(t)
	ctx := context.Background()
	// Drive every latency histogram once so the SDK emits a data point.
	m.RequestLatency.Record(ctx, 0.001)
	m.WriteLatency.Record(ctx, 0.001)
	m.ReadLatency.Record(ctx, 0.001)
	m.FsyncLatency.Record(ctx, 0.001)
	m.HeartbeatRTT.Record(ctx, 0.001)
	m.ControllerFailoverDuration.Record(ctx, 0.001)
	m.AssignmentFileWriteLatency.Record(ctx, 0.001)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}

	want := []string{
		"skafka.request.latency",
		"skafka.storage.write.latency",
		"skafka.storage.read.latency",
		"skafka.storage.fsync.latency",
		"skafka.heartbeat.rtt",
		"skafka.controller.failover.duration",
		"skafka.assignment.file.write.latency",
	}
	seen := map[string]bool{}
	for _, sm := range rm.ScopeMetrics {
		for _, inst := range sm.Metrics {
			h, ok := inst.Data.(metricdata.Histogram[float64])
			if !ok || len(h.DataPoints) == 0 {
				continue
			}
			bounds := h.DataPoints[0].Bounds
			// The OTel-default boundaries fan out from 5 with ms-scale
			// values. Real seconds-scale boundaries fall in [0.0001, 30].
			// A first-bucket upper edge ≤ 0.01 is a robust signal the
			// override is in effect; ≥ 1 means we're still on defaults.
			if len(bounds) == 0 {
				t.Errorf("%s: histogram has no bounds at all", inst.Name)
				continue
			}
			if bounds[0] > 0.01 {
				t.Errorf("%s: first bucket boundary = %v, want ≤ 0.01 — looks like the OTel default boundaries are still active (gh #79)", inst.Name, bounds[0])
			}
			seen[inst.Name] = true
		}
	}
	for _, name := range want {
		if !seen[name] {
			t.Errorf("%s not emitted; cannot verify boundaries", name)
		}
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
	m.TopicTraffic.RecordProduce("any", 0, 1)
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
