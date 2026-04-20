package observability

import (
	"sync/atomic"

	"go.opentelemetry.io/otel/metric"
	noopmetric "go.opentelemetry.io/otel/metric/noop"
)

// globalMetrics holds the active Metrics instance, installed by Bootstrap.
// Unset by default — Global() returns a no-op registry so tests and pre-bootstrap
// code can call it safely without nil checks.
var globalMetrics atomic.Pointer[Metrics]

// Global returns the active metrics registry, or a no-op registry when
// Bootstrap has not run. Call sites can always dereference fields directly.
func Global() *Metrics {
	if m := globalMetrics.Load(); m != nil {
		return m
	}
	return noopMetricsSingleton
}

// SetGlobal replaces the global metrics registry. Called by Bootstrap and by
// tests that install a reader-backed registry.
func SetGlobal(m *Metrics) {
	globalMetrics.Store(m)
}

// noopMetricsSingleton is built once against a no-op meter. All counters and
// histograms on it discard input; safe to use without bootstrap.
var noopMetricsSingleton = func() *Metrics {
	m, err := newMetrics(noopmetric.NewMeterProvider().Meter("noop"))
	if err != nil {
		panic("observability: noop metrics: " + err.Error())
	}
	return m
}()

// Metrics is the central registry of all OTel instruments emitted by skafka.
// Passed by pointer to every component that reports metrics. A nil *Metrics is
// safe to use — helper methods check for nil before recording.
type Metrics struct {
	// Throughput counters.
	ProduceRecords metric.Int64Counter
	ProduceBytes   metric.Int64Counter
	FetchRecords   metric.Int64Counter
	FetchBytes     metric.Int64Counter

	// Request-level latency. Topic label is intentionally omitted to cap cardinality;
	// topic lives in traces instead.
	RequestLatency metric.Float64Histogram

	// Storage.
	WriteLatency metric.Float64Histogram
	ReadLatency  metric.Float64Histogram
	FsyncLatency metric.Float64Histogram

	// Leadership.
	LeaseAcquired metric.Int64Counter
	LeaseLost     metric.Int64Counter

	// Consumer groups.
	GroupRebalances metric.Int64Counter

	// Auth.
	AuthSuccess   metric.Int64Counter
	AuthFailure   metric.Int64Counter
	ACLDeny       metric.Int64Counter
	QuotaThrottle metric.Int64Counter

	// TLS / external access.
	TLSHandshakes metric.Int64Counter
	CertReloads   metric.Int64Counter

	// Connection counters per listener.
	Connections metric.Int64Counter
}

// newMetrics creates all instruments on the given meter. Errors from individual
// instrument creation are joined and returned.
func newMetrics(m metric.Meter) (*Metrics, error) {
	mx := &Metrics{}
	var err error

	if mx.ProduceRecords, err = m.Int64Counter("skafka.produce.records",
		metric.WithDescription("Total records produced"),
		metric.WithUnit("{record}")); err != nil {
		return nil, err
	}
	if mx.ProduceBytes, err = m.Int64Counter("skafka.produce.bytes",
		metric.WithDescription("Total bytes produced"),
		metric.WithUnit("By")); err != nil {
		return nil, err
	}
	if mx.FetchRecords, err = m.Int64Counter("skafka.fetch.records",
		metric.WithDescription("Total records fetched"),
		metric.WithUnit("{record}")); err != nil {
		return nil, err
	}
	if mx.FetchBytes, err = m.Int64Counter("skafka.fetch.bytes",
		metric.WithDescription("Total bytes fetched"),
		metric.WithUnit("By")); err != nil {
		return nil, err
	}
	if mx.RequestLatency, err = m.Float64Histogram("skafka.request.latency",
		metric.WithDescription("Kafka request handler latency"),
		metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if mx.WriteLatency, err = m.Float64Histogram("skafka.storage.write.latency",
		metric.WithDescription("Partition append latency"),
		metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if mx.ReadLatency, err = m.Float64Histogram("skafka.storage.read.latency",
		metric.WithDescription("Partition read latency"),
		metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if mx.FsyncLatency, err = m.Float64Histogram("skafka.storage.fsync.latency",
		metric.WithDescription("Segment fsync latency"),
		metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if mx.LeaseAcquired, err = m.Int64Counter("skafka.lease.acquired",
		metric.WithDescription("Partition leader leases acquired by this broker")); err != nil {
		return nil, err
	}
	if mx.LeaseLost, err = m.Int64Counter("skafka.lease.lost",
		metric.WithDescription("Partition leader leases lost by this broker")); err != nil {
		return nil, err
	}
	if mx.GroupRebalances, err = m.Int64Counter("skafka.group.rebalances",
		metric.WithDescription("Consumer group rebalance completions")); err != nil {
		return nil, err
	}
	if mx.AuthSuccess, err = m.Int64Counter("skafka.auth.success",
		metric.WithDescription("Successful SASL / mTLS authentications")); err != nil {
		return nil, err
	}
	if mx.AuthFailure, err = m.Int64Counter("skafka.auth.failure",
		metric.WithDescription("Failed authentication attempts")); err != nil {
		return nil, err
	}
	if mx.ACLDeny, err = m.Int64Counter("skafka.acl.deny",
		metric.WithDescription("Authorization denials")); err != nil {
		return nil, err
	}
	if mx.QuotaThrottle, err = m.Int64Counter("skafka.quota.throttle",
		metric.WithDescription("Requests that hit a quota and were throttled")); err != nil {
		return nil, err
	}
	if mx.TLSHandshakes, err = m.Int64Counter("skafka.tls.handshakes",
		metric.WithDescription("TLS handshakes completed")); err != nil {
		return nil, err
	}
	if mx.CertReloads, err = m.Int64Counter("skafka.cert.reloads",
		metric.WithDescription("TLS certificate hot-reloads")); err != nil {
		return nil, err
	}
	if mx.Connections, err = m.Int64Counter("skafka.connections",
		metric.WithDescription("New client connections accepted")); err != nil {
		return nil, err
	}
	return mx, nil
}
