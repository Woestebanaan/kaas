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

	// Cluster controller leadership. ControllerFailovers fires once per
	// "this broker won the controller lease" event — broker_id is the
	// OTel resource attribute, so summing across the fleet gives total
	// failover count, while the per-broker decomposition tells you who
	// wins repeatedly. ControllerFailoverDuration is a histogram of
	// "won the lease → first AssignmentLoop write" — close to the
	// data-plane downtime during a failover, though not exact (the
	// previous controller's lease-lost event isn't visible from here).
	ControllerFailovers        metric.Int64Counter
	ControllerFailoverDuration metric.Float64Histogram

	// Assignment loop (controller-side).
	AssignmentChanges          metric.Int64Counter // recomputeAndWrite calls
	AssignmentFileWrites       metric.Int64Counter // FileStore.Write attempts (label: result=ok|error)
	AssignmentFileWriteLatency metric.Float64Histogram
	AssignmentPushes           metric.Int64Counter // PushAssignmentChanged broadcasts
	CRMirrorWrites             metric.Int64Counter // K8sMirror.Mirror attempts (label: result=ok|error)

	// Broker-side heartbeat + assignment polling.
	HeartbeatRTT             metric.Float64Histogram // round-trip time, computed via the proto's broker_status_timestamp_ms echo
	HeartbeatMisses          metric.Int64Counter     // count of "no PING received in N seconds" detections
	SelfFenceEvents          metric.Int64Counter     // count of self-fence triggers (Coordinator marks itself stale)
	AssignmentPolls          metric.Int64Counter     // every fsutil mtime poll iteration (label: change_detected=true|false)
	StaleAssignmentsRejected metric.Int64Counter     // assignment.json files dropped because controllerEpoch is behind

	// Consumer groups.
	GroupRebalances metric.Int64Counter

	// Auth.
	AuthSuccess metric.Int64Counter
	AuthFailure metric.Int64Counter
	ACLDeny     metric.Int64Counter

	// QuotaThrottle is declared for forward compatibility with the
	// per-principal quota engine planned post-v1; no v1 code path
	// emits it. Dashboards and alerts should treat it as flat-zero
	// and not be surprised when it stays that way.
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
	if mx.ControllerFailovers, err = m.Int64Counter("skafka.controller.failovers",
		metric.WithDescription("Times this broker won the cluster controller lease")); err != nil {
		return nil, err
	}
	if mx.ControllerFailoverDuration, err = m.Float64Histogram("skafka.controller.failover.duration",
		metric.WithDescription("Seconds from winning the lease to the first AssignmentLoop write"),
		metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if mx.AssignmentChanges, err = m.Int64Counter("skafka.assignment.changes",
		metric.WithDescription("AssignmentLoop recompute+write iterations")); err != nil {
		return nil, err
	}
	if mx.AssignmentFileWrites, err = m.Int64Counter("skafka.assignment.file.writes",
		metric.WithDescription("AssignmentStore.Write attempts (result=ok|error)")); err != nil {
		return nil, err
	}
	if mx.AssignmentFileWriteLatency, err = m.Float64Histogram("skafka.assignment.file.write.latency",
		metric.WithDescription("AssignmentStore.Write tmp+rename duration"),
		metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if mx.AssignmentPushes, err = m.Int64Counter("skafka.assignment.pushes",
		metric.WithDescription("ASSIGNMENT_CHANGED broadcasts via heartbeat server")); err != nil {
		return nil, err
	}
	if mx.CRMirrorWrites, err = m.Int64Counter("skafka.assignment.cr.mirror.writes",
		metric.WithDescription("KafkaClusterAssignments CR Status update attempts (result=ok|error)")); err != nil {
		return nil, err
	}
	if mx.HeartbeatRTT, err = m.Float64Histogram("skafka.heartbeat.rtt",
		metric.WithDescription("Broker→controller→broker heartbeat round-trip"),
		metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if mx.HeartbeatMisses, err = m.Int64Counter("skafka.heartbeat.misses",
		metric.WithDescription("Heartbeats not received within heartbeatTimeout")); err != nil {
		return nil, err
	}
	if mx.SelfFenceEvents, err = m.Int64Counter("skafka.self.fence.events",
		metric.WithDescription("Times this broker self-fenced due to stale heartbeat")); err != nil {
		return nil, err
	}
	if mx.AssignmentPolls, err = m.Int64Counter("skafka.assignment.polls",
		metric.WithDescription("assignment.json mtime poll iterations (change_detected=true|false)")); err != nil {
		return nil, err
	}
	if mx.StaleAssignmentsRejected, err = m.Int64Counter("skafka.stale.assignments.rejected",
		metric.WithDescription("assignment.json reads dropped because controllerEpoch was behind")); err != nil {
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
