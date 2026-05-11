package observability

import (
	"context"
	"errors"
	"net"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// observedExporter wraps a metric.Exporter so every Export call lands
// on the OTLPPush* instruments. Pre-gh #121 PR4 the SDK's PeriodicReader
// silently swallowed Export errors — the only symptom was that
// dashboards stopped getting new data. With this wrapper we get
// success/failure counters and a duration histogram, so "OTLP push is
// failing" becomes an alertable signal.
//
// The wrapper also records its OWN samples into the same MeterProvider
// it's exporting from. That's self-referential but well-defined: a
// failure here is observed on the NEXT push attempt (one-period lag).
// On the first failed cycle dashboards see nothing; on the second they
// see the previous failure. Acceptable trade-off vs running a separate
// out-of-band push channel.
//
// Wrapping requires implementing the full sdkmetric.Exporter interface
// (Temporality / Aggregation / ForceFlush / Shutdown / Export). All
// methods except Export are pure delegation.
type observedExporter struct {
	inner sdkmetric.Exporter
}

// newObservedExporter wraps inner. Returns inner unchanged when
// inner is nil (defensive — bootstrap only calls this with a real
// exporter).
func newObservedExporter(inner sdkmetric.Exporter) sdkmetric.Exporter {
	if inner == nil {
		return nil
	}
	return &observedExporter{inner: inner}
}

func (e *observedExporter) Temporality(k sdkmetric.InstrumentKind) metricdata.Temporality {
	return e.inner.Temporality(k)
}

func (e *observedExporter) Aggregation(k sdkmetric.InstrumentKind) sdkmetric.Aggregation {
	return e.inner.Aggregation(k)
}

func (e *observedExporter) Export(ctx context.Context, rm *metricdata.ResourceMetrics) error {
	start := time.Now()
	err := e.inner.Export(ctx, rm)
	dur := time.Since(start).Seconds()

	mx := Global()
	mx.OTLPPushDuration.Record(ctx, dur)
	if err == nil {
		mx.OTLPPushSuccess.Add(ctx, 1)
		return nil
	}
	mx.OTLPPushFailure.Add(ctx, 1, metric.WithAttributes(attribute.String("err_class", classifyOTLPErr(err))))
	return err
}

func (e *observedExporter) ForceFlush(ctx context.Context) error { return e.inner.ForceFlush(ctx) }
func (e *observedExporter) Shutdown(ctx context.Context) error   { return e.inner.Shutdown(ctx) }

// classifyOTLPErr buckets exporter errors into a small label space.
// Cardinality matters for the counter — we deliberately do NOT label
// by the raw error string, which would explode the timeseries count
// in the metrics backend.
//
// Categories:
//   - timeout:  context.DeadlineExceeded or i/o timeout
//   - refused:  TCP-level "connection refused" or DNS resolution failure
//   - other:    catch-all
func classifyOTLPErr(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	msg := err.Error()
	if strings.Contains(msg, "connection refused") || strings.Contains(msg, "no such host") {
		return "refused"
	}
	return "other"
}
