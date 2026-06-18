package observability

import (
	"context"
	"errors"
	"net"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// fakeExporter implements sdkmetric.Exporter and returns a configurable
// error from Export. The other methods are no-ops (we don't exercise
// them).
type fakeExporter struct {
	exportErr error
	calls     int
}

func (f *fakeExporter) Temporality(k sdkmetric.InstrumentKind) metricdata.Temporality {
	return metricdata.CumulativeTemporality
}
func (f *fakeExporter) Aggregation(k sdkmetric.InstrumentKind) sdkmetric.Aggregation {
	return sdkmetric.AggregationDefault{}
}
func (f *fakeExporter) Export(ctx context.Context, rm *metricdata.ResourceMetrics) error {
	f.calls++
	return f.exportErr
}
func (f *fakeExporter) ForceFlush(ctx context.Context) error { return nil }
func (f *fakeExporter) Shutdown(ctx context.Context) error   { return nil }

func TestObservedExporterRecordsSuccess(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m, err := NewMetrics(mp.Meter("skafka-test"))
	if err != nil {
		t.Fatal(err)
	}
	SetGlobal(m)
	defer SetGlobal(nil)

	wrapped := newObservedExporter(&fakeExporter{}).(*observedExporter)
	if err := wrapped.Export(context.Background(), &metricdata.ResourceMetrics{}); err != nil {
		t.Fatalf("Export: %v", err)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}

	if got := sumCounter(t, rm, "skafka.otlp.push.success"); got != 1 {
		t.Errorf("push.success=%d, want 1", got)
	}
	if got := sumCounter(t, rm, "skafka.otlp.push.failure"); got != 0 {
		t.Errorf("push.failure=%d, want 0", got)
	}
	if !histogramHasPoint(t, rm, "skafka.otlp.push.duration") {
		t.Error("push.duration: no data point")
	}
}

func TestObservedExporterRecordsFailure(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m, err := NewMetrics(mp.Meter("skafka-test"))
	if err != nil {
		t.Fatal(err)
	}
	SetGlobal(m)
	defer SetGlobal(nil)

	wrapped := newObservedExporter(&fakeExporter{exportErr: context.DeadlineExceeded}).(*observedExporter)
	if err := wrapped.Export(context.Background(), &metricdata.ResourceMetrics{}); err == nil {
		t.Fatal("expected error to propagate")
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}

	if got := sumCounter(t, rm, "skafka.otlp.push.failure"); got != 1 {
		t.Errorf("push.failure=%d, want 1", got)
	}
	if got := sumCounter(t, rm, "skafka.otlp.push.success"); got != 0 {
		t.Errorf("push.success=%d, want 0", got)
	}
}

func TestClassifyOTLPErr(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"timeout via DeadlineExceeded", context.DeadlineExceeded, "timeout"},
		{"timeout via net.Error", &timeoutNetErr{}, "timeout"},
		{"refused (string match)", errors.New("dial tcp: connection refused"), "refused"},
		{"refused via DNS", errors.New("lookup foo: no such host"), "refused"},
		{"other catch-all", errors.New("something unexpected"), "other"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyOTLPErr(tc.err); got != tc.want {
				t.Errorf("classifyOTLPErr(%v)=%q, want %q", tc.err, got, tc.want)
			}
		})
	}
}

// timeoutNetErr fakes a net.Error whose Timeout() returns true. The
// SDK's tcp errors produce these in the real world.
type timeoutNetErr struct{}

func (timeoutNetErr) Error() string   { return "i/o timeout" }
func (timeoutNetErr) Timeout() bool   { return true }
func (timeoutNetErr) Temporary() bool { return true }

var _ net.Error = timeoutNetErr{}

func sumCounter(t *testing.T, rm metricdata.ResourceMetrics, name string) int64 {
	t.Helper()
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, inst := range sm.Metrics {
			if inst.Name != name {
				continue
			}
			if s, ok := inst.Data.(metricdata.Sum[int64]); ok {
				for _, dp := range s.DataPoints {
					total += dp.Value
				}
			}
		}
	}
	return total
}

func histogramHasPoint(t *testing.T, rm metricdata.ResourceMetrics, name string) bool {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, inst := range sm.Metrics {
			if inst.Name != name {
				continue
			}
			if h, ok := inst.Data.(metricdata.Histogram[float64]); ok && len(h.DataPoints) > 0 {
				return true
			}
		}
	}
	return false
}
