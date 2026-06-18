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
	m, err := NewMetrics(noopmetric.NewMeterProvider().Meter("noop"))
	if err != nil {
		panic("observability: noop metrics: " + err.Error())
	}
	return m
}()

// latencySecondsBoundaries is the explicit histogram bucket boundary set
// for every Float64Histogram below that records latency in seconds.
// Without this, OTel falls back to its default boundaries
// (5, 10, 25 ... 10000) which are designed for HTTP latencies in
// MILLISECONDS — every observation we record (`time.Since(start).Seconds()`,
// always sub-second to mid-second) lands in the [0, 5] bucket and
// histogram_quantile interpolates p50/p95/p99 to fixed 2.5 / 4.75 / 4.95
// regardless of actual load (gh #79).
//
// Range: 100 µs (in-process hot path) to 30 s (failover / drain-scale
// events). 15 buckets is a reasonable resolution/cardinality trade-off.
var latencySecondsBoundaries = []float64{
	0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
	1, 2.5, 5, 10, 30,
}

// Metrics is the central registry of all OTel instruments emitted by skafka.
// Passed by pointer to every component that reports metrics. A nil *Metrics is
// safe to use — helper methods check for nil before recording.
type Metrics struct {
	// Throughput meter (gh #115 + gh #121 PR1). Hot-path Produce/Fetch
	// handlers call RecordProduce / RecordFetch to bump per-topic
	// atomic accumulators; the OTel SDK reads them at every scrape
	// via an ObservableCounter callback so idle topics always emit
	// a current-cumulative datapoint (no Grafana gaps).
	TopicTraffic *TopicTrafficMeter

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

	// Byte-opacity tripwires. The plan's load-bearing invariant is
	// "the broker is a byte mover, not a byte interpreter": no code
	// path should decode individual records or re-encode a
	// RecordBatch. These counters MUST stay at zero in steady state.
	// They have NO designated emit site — every increment is a bug.
	// BumpCodecRecordDecode / BumpCodecBatchReencode exist for future
	// violators to record themselves under so the alert fires loudly.
	CodecRecordDecode  metric.Int64Counter
	CodecBatchReencode metric.Int64Counter

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
	Connections     metric.Int64Counter
	ConnectionsOpen metric.Int64UpDownCounter

	// gh #132: per-topic produce/fetch ERROR counters. Pre-gh #132
	// the TopicTraffic success counters were the only signal — when
	// the NAS stalled and every Append() returned ErrStorageStalled,
	// produce_records_total went flat and the dashboard panel
	// effectively went silent. Failures still mean something; the
	// broker should *keep reporting* them so on-call sees the failure
	// rate rise instead of "no data".
	//
	// Cardinality: ~6 error_code values per topic (storage_stalled,
	// not_leader, corrupt_message, topic_auth_failed,
	// out_of_order_sequence, invalid_producer_epoch). Bounded.
	ProduceErrors metric.Int64Counter // (topic, error_code)
	FetchErrors   metric.Int64Counter // (topic, error_code)

	// gh #121 PR3: cleaner + compactor instrumentation. Pre-PR3 the
	// retention and log-compaction paths were completely unobserved —
	// the only way to know whether they ran was to grep slog output.
	// Cleaner = retention (delete-by-time, delete-by-size). Compactor
	// = log-compaction (keep-latest-per-key). They share NewMetrics
	// but live in distinct namespaces so dashboards can split them.
	CleanerRuns              metric.Int64Counter     // per-cycle (label: result=ok|error)
	CleanerDuration          metric.Float64Histogram // wall-clock per partition cleaned
	CleanerSegmentsDeleted   metric.Int64Counter     // (label: reason=time|size)
	CleanerBytesReclaimed    metric.Int64Counter     // sum of deleted-segment sizes (label: reason=time|size)
	CompactionRuns           metric.Int64Counter     // per partition compaction (label: result=ok|error|aborted)
	CompactionDuration       metric.Float64Histogram // wall-clock per partition compaction
	CompactionRecordsKept    metric.Int64Counter     // records surviving the compactor pass
	CompactionRecordsDropped metric.Int64Counter     // records superseded by later writes (deduplication win)
	CompactionBytesIn        metric.Int64Counter     // total source-segment bytes scanned
	CompactionBytesOut       metric.Int64Counter     // bytes written to the replacement segment

	// gh #121 PR4: OTLP push observability. The OTel SDK's
	// PeriodicReader silently drops samples when the exporter's
	// Export call fails (e.g. controller broker starves the push
	// goroutine during alive-set churn — the "context deadline
	// exceeded" symptom that triggered the broader gh #121 rewrite).
	// Pre-PR4 the failure was invisible from a dashboard; you'd notice
	// it only by an unexplained drop in series count. These counters
	// make the OTLP push itself observable.
	OTLPPushSuccess  metric.Int64Counter
	OTLPPushFailure  metric.Int64Counter // labelled by err class (timeout|refused|other)
	OTLPPushDuration metric.Float64Histogram

	// gh #121 PR5: operator-side reconciler observability. Each
	// reconciler is wrapped in an Observed() decorator at SetupWithManager
	// so dashboards can show "is the operator keeping up, per CR kind."
	// Pre-PR5 the operator inherited controller-runtime's built-in
	// Prometheus client_golang metrics — which don't flow through OTLP
	// and therefore never reach our Grafana, so visibility was zero.
	OperatorReconciles        metric.Int64Counter     // (kind=KafkaTopic|..., result=ok|requeue|error)
	OperatorReconcileDuration metric.Float64Histogram // (kind=...)

	// gh #121 PR4.5: broker-side K8s API call observability. The
	// broker hits the apiserver from a few cold-path goroutines —
	// TopicWatcher (List + Watch), election (Lease Get), readiness
	// updater (Pod Patch), endpoints watcher, CR mirror (Get + Update).
	// Pre-PR4.5 none of these were tracked, so 'apiserver is slow' had
	// no dashboard signal — the symptom was that watchers fell behind.
	// Wired via the RecordK8sCall helper below.
	K8sAPICalls   metric.Int64Counter     // (operation, resource, result=ok|error)
	K8sAPILatency metric.Float64Histogram // (operation, resource)
}

// latencyHist creates a seconds-unit Float64Histogram with the standard
// latency bucket boundaries. Used pervasively below; helper exists
// because the raw form was repeated ~12 times verbatim.
func latencyHist(m metric.Meter, name, desc string) (metric.Float64Histogram, error) {
	return m.Float64Histogram(name,
		metric.WithDescription(desc),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(latencySecondsBoundaries...))
}

// NewMetrics creates all instruments on the given meter. Returns the
// first error encountered during instrument construction.
func NewMetrics(m metric.Meter) (*Metrics, error) {
	mx := &Metrics{}

	// gh #115 + gh #121 PR1: per-topic produce/fetch are ObservableCounters
	// (always emit at scrape), not fire-and-forget Int64Counters.
	mx.TopicTraffic = NewTopicTrafficMeter()
	if err := registerTopicTrafficInstruments(m, mx.TopicTraffic); err != nil {
		return nil, err
	}

	registerers := []func(m metric.Meter, mx *Metrics) error{
		registerRequestLatencyMetrics,
		registerStorageLatencyMetrics,
		registerControllerMetrics,
		registerAssignmentMetrics,
		registerHeartbeatMetrics,
		registerCodecTripwireMetrics,
		registerGroupAndConnectionMetrics,
		registerAuthAndQuotaMetrics,
		registerPerAPIErrorMetrics,
		registerCleanerMetrics,
		registerCompactorMetrics,
		registerOTLPMetrics,
		registerOperatorMetrics,
		registerK8sAPIMetrics,
	}
	for _, r := range registerers {
		if err := r(m, mx); err != nil {
			return nil, err
		}
	}
	return mx, nil
}

// registerRequestLatencyMetrics — per-request handler latency histogram.
func registerRequestLatencyMetrics(m metric.Meter, mx *Metrics) (err error) {
	mx.RequestLatency, err = latencyHist(m, "skafka.request.latency", "Kafka request handler latency")
	return err
}

// registerStorageLatencyMetrics — Append / Read / Fsync histograms.
func registerStorageLatencyMetrics(m metric.Meter, mx *Metrics) error {
	var err error
	if mx.WriteLatency, err = latencyHist(m, "skafka.storage.write.latency", "Partition append latency"); err != nil {
		return err
	}
	if mx.ReadLatency, err = latencyHist(m, "skafka.storage.read.latency", "Partition read latency"); err != nil {
		return err
	}
	if mx.FsyncLatency, err = latencyHist(m, "skafka.storage.fsync.latency", "Segment fsync latency"); err != nil {
		return err
	}
	return nil
}

// registerControllerMetrics — cluster controller failover counters.
func registerControllerMetrics(m metric.Meter, mx *Metrics) error {
	var err error
	if mx.ControllerFailovers, err = m.Int64Counter("skafka.controller.failovers",
		metric.WithDescription("Times this broker won the cluster controller lease")); err != nil {
		return err
	}
	if mx.ControllerFailoverDuration, err = latencyHist(m, "skafka.controller.failover.duration",
		"Seconds from winning the lease to the first AssignmentLoop write"); err != nil {
		return err
	}
	return nil
}

// registerAssignmentMetrics — AssignmentLoop write + push observability.
func registerAssignmentMetrics(m metric.Meter, mx *Metrics) error {
	var err error
	if mx.AssignmentChanges, err = m.Int64Counter("skafka.assignment.changes",
		metric.WithDescription("AssignmentLoop recompute+write iterations")); err != nil {
		return err
	}
	if mx.AssignmentFileWrites, err = m.Int64Counter("skafka.assignment.file.writes",
		metric.WithDescription("AssignmentStore.Write attempts (result=ok|error)")); err != nil {
		return err
	}
	if mx.AssignmentFileWriteLatency, err = latencyHist(m, "skafka.assignment.file.write.latency",
		"AssignmentStore.Write tmp+rename duration"); err != nil {
		return err
	}
	if mx.AssignmentPushes, err = m.Int64Counter("skafka.assignment.pushes",
		metric.WithDescription("ASSIGNMENT_CHANGED broadcasts via heartbeat server")); err != nil {
		return err
	}
	if mx.CRMirrorWrites, err = m.Int64Counter("skafka.assignment.cr.mirror.writes",
		metric.WithDescription("KafkaClusterAssignments CR Status update attempts (result=ok|error)")); err != nil {
		return err
	}
	if mx.AssignmentPolls, err = m.Int64Counter("skafka.assignment.polls",
		metric.WithDescription("assignment.json mtime poll iterations (change_detected=true|false)")); err != nil {
		return err
	}
	if mx.StaleAssignmentsRejected, err = m.Int64Counter("skafka.stale.assignments.rejected",
		metric.WithDescription("assignment.json reads dropped because controllerEpoch was behind")); err != nil {
		return err
	}
	return nil
}

// registerHeartbeatMetrics — broker↔controller heartbeat RTT + misses.
func registerHeartbeatMetrics(m metric.Meter, mx *Metrics) error {
	var err error
	if mx.HeartbeatRTT, err = latencyHist(m, "skafka.heartbeat.rtt",
		"Broker→controller→broker heartbeat round-trip"); err != nil {
		return err
	}
	if mx.HeartbeatMisses, err = m.Int64Counter("skafka.heartbeat.misses",
		metric.WithDescription("Heartbeats not received within heartbeatTimeout")); err != nil {
		return err
	}
	if mx.SelfFenceEvents, err = m.Int64Counter("skafka.self.fence.events",
		metric.WithDescription("Times this broker self-fenced due to stale heartbeat")); err != nil {
		return err
	}
	return nil
}

// registerCodecTripwireMetrics — MUST-stay-zero counters for codec
// invariants. Bumped lines tell the alert: someone added record-level
// decoding to the broker.
func registerCodecTripwireMetrics(m metric.Meter, mx *Metrics) error {
	var err error
	if mx.CodecRecordDecode, err = m.Int64Counter("skafka.codec.record.decode",
		metric.WithDescription("Tripwire: code path decoded an individual record. MUST stay at zero — alert if non-zero")); err != nil {
		return err
	}
	if mx.CodecBatchReencode, err = m.Int64Counter("skafka.codec.batch.reencode",
		metric.WithDescription("Tripwire: code path re-encoded a RecordBatch. MUST stay at zero — alert if non-zero")); err != nil {
		return err
	}
	return nil
}

// registerGroupAndConnectionMetrics — rebalance + connection counters.
func registerGroupAndConnectionMetrics(m metric.Meter, mx *Metrics) error {
	var err error
	if mx.GroupRebalances, err = m.Int64Counter("skafka.group.rebalances",
		metric.WithDescription("Consumer group rebalance completions")); err != nil {
		return err
	}
	if mx.Connections, err = m.Int64Counter("skafka.connections",
		metric.WithDescription("New client connections accepted")); err != nil {
		return err
	}
	if mx.ConnectionsOpen, err = m.Int64UpDownCounter("skafka.connections.open",
		metric.WithDescription("Currently open client connections")); err != nil {
		return err
	}
	return nil
}

// registerAuthAndQuotaMetrics — auth success/failure, ACL denies, quota,
// TLS handshakes + cert reloads.
func registerAuthAndQuotaMetrics(m metric.Meter, mx *Metrics) error {
	var err error
	if mx.AuthSuccess, err = m.Int64Counter("skafka.auth.success",
		metric.WithDescription("Successful SASL / mTLS authentications")); err != nil {
		return err
	}
	if mx.AuthFailure, err = m.Int64Counter("skafka.auth.failure",
		metric.WithDescription("Failed authentication attempts")); err != nil {
		return err
	}
	if mx.ACLDeny, err = m.Int64Counter("skafka.acl.deny",
		metric.WithDescription("Authorization denials")); err != nil {
		return err
	}
	if mx.QuotaThrottle, err = m.Int64Counter("skafka.quota.throttle",
		metric.WithDescription("Requests that hit a quota and were throttled")); err != nil {
		return err
	}
	if mx.TLSHandshakes, err = m.Int64Counter("skafka.tls.handshakes",
		metric.WithDescription("TLS handshakes completed")); err != nil {
		return err
	}
	if mx.CertReloads, err = m.Int64Counter("skafka.cert.reloads",
		metric.WithDescription("TLS certificate hot-reloads (result=ok|error). gh #132: failures stay visible — cert-manager mid-rotation or stale Secret mounts surface as result=error and don't go silent.")); err != nil {
		return err
	}
	return nil
}

// registerPerAPIErrorMetrics — gh #132 per-partition Produce/Fetch
// error counters. Distinct from the TopicTraffic success counters so
// dashboards can show "X is failing" while success has gone flat.
func registerPerAPIErrorMetrics(m metric.Meter, mx *Metrics) error {
	var err error
	if mx.ProduceErrors, err = m.Int64Counter("skafka.produce.errors",
		metric.WithDescription("Per-partition Produce failures (labels: topic, error_code). Bumped on every error path — storage stalled, not leader, corrupt batch, auth denied, out-of-order sequence, fenced producer epoch."),
		metric.WithUnit("{error}")); err != nil {
		return err
	}
	if mx.FetchErrors, err = m.Int64Counter("skafka.fetch.errors",
		metric.WithDescription("Per-partition Fetch failures (labels: topic, error_code). Bumped on every error path — not leader, read failure, auth denied."),
		metric.WithUnit("{error}")); err != nil {
		return err
	}
	return nil
}

// registerCleanerMetrics — gh #121 PR3 retention cleaner observability.
func registerCleanerMetrics(m metric.Meter, mx *Metrics) error {
	var err error
	if mx.CleanerRuns, err = m.Int64Counter("skafka.cleaner.runs",
		metric.WithDescription("Retention cleaner partition-pass completions (result=ok|error)")); err != nil {
		return err
	}
	if mx.CleanerDuration, err = latencyHist(m, "skafka.cleaner.duration",
		"Wall-clock per retention cleaner partition pass"); err != nil {
		return err
	}
	if mx.CleanerSegmentsDeleted, err = m.Int64Counter("skafka.cleaner.segments.deleted",
		metric.WithDescription("Segments deleted by the retention cleaner (reason=time|size)"),
		metric.WithUnit("{segment}")); err != nil {
		return err
	}
	if mx.CleanerBytesReclaimed, err = m.Int64Counter("skafka.cleaner.bytes.reclaimed",
		metric.WithDescription("Bytes freed by retention deletes (reason=time|size). Approximates disk-pressure relief; on NFS the actual unlink may lag if another broker held the fd."),
		metric.WithUnit("By")); err != nil {
		return err
	}
	return nil
}

// registerCompactorMetrics — gh #121 PR3 log compactor observability.
func registerCompactorMetrics(m metric.Meter, mx *Metrics) error {
	var err error
	if mx.CompactionRuns, err = m.Int64Counter("skafka.compaction.runs",
		metric.WithDescription("Log compactor partition-pass completions (result=ok|error|aborted)")); err != nil {
		return err
	}
	if mx.CompactionDuration, err = latencyHist(m, "skafka.compaction.duration",
		"Wall-clock per log compactor partition pass"); err != nil {
		return err
	}
	if mx.CompactionRecordsKept, err = m.Int64Counter("skafka.compaction.records.kept",
		metric.WithDescription("Records surviving the compactor's keep-latest-per-key pass"),
		metric.WithUnit("{record}")); err != nil {
		return err
	}
	if mx.CompactionRecordsDropped, err = m.Int64Counter("skafka.compaction.records.dropped",
		metric.WithDescription("Records superseded by a later write for the same key — the dedup win"),
		metric.WithUnit("{record}")); err != nil {
		return err
	}
	if mx.CompactionBytesIn, err = m.Int64Counter("skafka.compaction.bytes.in",
		metric.WithDescription("Source-segment bytes scanned by the compactor (before dedup)"),
		metric.WithUnit("By")); err != nil {
		return err
	}
	if mx.CompactionBytesOut, err = m.Int64Counter("skafka.compaction.bytes.out",
		metric.WithDescription("Replacement-segment bytes written by the compactor (after dedup). bytes.in - bytes.out is the size savings."),
		metric.WithUnit("By")); err != nil {
		return err
	}
	return nil
}

// registerOTLPMetrics — gh #121 PR4 OTLP push self-observability. Wired
// via the exporter wrapper in bootstrap.go. Note the self-referential
// loop: a push failure increments OTLPPushFailure, which is itself
// exported on the next push — dashboards see failures on a one-period
// lag, which is the best you can do without an out-of-band channel.
func registerOTLPMetrics(m metric.Meter, mx *Metrics) error {
	var err error
	if mx.OTLPPushSuccess, err = m.Int64Counter("skafka.otlp.push.success",
		metric.WithDescription("OTLP metric exports that succeeded")); err != nil {
		return err
	}
	if mx.OTLPPushFailure, err = m.Int64Counter("skafka.otlp.push.failure",
		metric.WithDescription("OTLP metric exports that failed (err_class=timeout|refused|other)")); err != nil {
		return err
	}
	if mx.OTLPPushDuration, err = latencyHist(m, "skafka.otlp.push.duration",
		"Time spent in Exporter.Export — high values suggest backend pressure"); err != nil {
		return err
	}
	return nil
}

// registerOperatorMetrics — gh #121 PR5 operator reconciler observability.
// Lives on the shared Metrics struct so a broker-built test binary also
// has the instruments available (no-op when the broker doesn't wrap
// reconcilers).
func registerOperatorMetrics(m metric.Meter, mx *Metrics) error {
	var err error
	if mx.OperatorReconciles, err = m.Int64Counter("skafka.operator.reconciles",
		metric.WithDescription("Operator reconcile completions (kind=CR kind, result=ok|requeue|error)")); err != nil {
		return err
	}
	if mx.OperatorReconcileDuration, err = latencyHist(m, "skafka.operator.reconcile.duration",
		"Operator Reconcile() wall-clock per call (kind=...)"); err != nil {
		return err
	}
	return nil
}

// registerK8sAPIMetrics — gh #121 PR4.5 broker-side K8s API call observability.
func registerK8sAPIMetrics(m metric.Meter, mx *Metrics) error {
	var err error
	if mx.K8sAPICalls, err = m.Int64Counter("skafka.k8s.api.calls",
		metric.WithDescription("Apiserver calls from the broker (operation=Get|List|Watch|Patch|Update|Create, resource=KafkaTopic|EndpointSlice|Lease|Pod|KafkaClusterAssignments, result=ok|error)")); err != nil {
		return err
	}
	if mx.K8sAPILatency, err = latencyHist(m, "skafka.k8s.api.latency",
		"Apiserver call wall-clock per (operation, resource)"); err != nil {
		return err
	}
	return nil
}

// NewReaperMetrics constructs the gh #119 partition-reaper instrument
// bundle on the given meter. Returns nil on any error (caller's
// reaper falls back to log-only). Each instrument's lifecycle is the
// same as the meter — typically the process lifetime.
//
// Lives in observability (not storage) so storage doesn't import the
// observability package; storage exposes the bundle type and the
// observability bootstrap fills it in.
func NewReaperMetrics(m metric.Meter) (enqueued, completed, aborted, retried, givenUp metric.Int64Counter, duration metric.Float64Histogram, err error) {
	if enqueued, err = m.Int64Counter("skafka.reaper.enqueued",
		metric.WithDescription("Partitions enqueued for background reap"),
		metric.WithUnit("{partition}")); err != nil {
		return
	}
	if completed, err = m.Int64Counter("skafka.reaper.completed",
		metric.WithDescription("Partitions successfully reaped"),
		metric.WithUnit("{partition}")); err != nil {
		return
	}
	if aborted, err = m.Int64Counter("skafka.reaper.aborted",
		metric.WithDescription("Reaps aborted because the topic CR reappeared during the reap window"),
		metric.WithUnit("{partition}")); err != nil {
		return
	}
	if retried, err = m.Int64Counter("skafka.reaper.retried",
		metric.WithDescription("Reap operations re-enqueued after a transient I/O error"),
		metric.WithUnit("{operation}")); err != nil {
		return
	}
	if givenUp, err = m.Int64Counter("skafka.reaper.given_up",
		metric.WithDescription("Reaps that exhausted MaxRetries; recovered on next startup SweepTopics"),
		metric.WithUnit("{partition}")); err != nil {
		return
	}
	if duration, err = m.Float64Histogram("skafka.reaper.duration",
		metric.WithDescription("Reap-work wall-clock per partition (closeHandles + os.RemoveAll)"),
		metric.WithUnit("s")); err != nil {
		return
	}
	return
}
