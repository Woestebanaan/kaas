# Phase 10 Breakdown: Observability (OpenTelemetry)

## Current State (end of Phase 9)

No production observability code yet. The broker has `/healthz` and `/readyz`
HTTP endpoints (Phase 8) but they return bare 200s with no detail. No metrics
endpoint; no tracing; no structured request log. `slog` is used throughout but
without OTel correlation.

Phase 10 adds a full observability stack built on the OpenTelemetry SDK: metrics,
traces, and log correlation via a single API that can export to many backends.
The default export path keeps the existing Prometheus-scrape contract intact.

---

## Why OpenTelemetry, not direct Prometheus

Two reasons:

1. **One API, many backends.** Users deploy skafka into different observability
   stacks: Prometheus + Grafana, Grafana Cloud, Datadog, New Relic, Honeycomb.
   Writing against the OTel SDK means any of those can be wired via configuration
   — no code changes, no conditional compilation.

2. **Traces are first-class.** Produce/fetch latency histograms tell you the
   shape of a problem. Distributed traces tell you *where* in the request a
   slow partition lookup, fsync, or forward hop actually happened. Prometheus
   alone cannot do that.

The Prometheus contract is preserved via the OTel SDK's `exporters/prometheus`
package — OTel metric instruments are exposed on `/metrics` in the standard
Prometheus text format. Nothing in an existing Prometheus deployment changes.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│  Broker / Operator pod                                                │
│                                                                       │
│  Handler ──▶ observability.Metrics.ProduceRecords.Add(ctx, n, attrs)  │
│          ──▶ observability.Tracer.Start(ctx, "Produce")                │
│          ──▶ slog.InfoContext(ctx, ...) — trace_id + span_id attrs    │
│                                                                       │
│         OTel SDK (MeterProvider + TracerProvider + LoggerProvider)    │
│              │                    │                    │              │
│              ▼                    ▼                    ▼              │
│   Prometheus exporter      OTLP exporter          slog JSON handler   │
│     (pull, /metrics)        (push, gRPC)          (stdout, to k8s)    │
└─────────┬────────────────────────┬────────────────────────┬───────────┘
          │                        │                        │
          ▼                        ▼                        ▼
   Prometheus scrape      OTel Collector → Tempo/    Cluster log aggregator
   (existing infra)          Jaeger / Datadog / etc.  (Loki, ELK, CloudWatch)
```

The broker container runs *all three* signals in-process. Each backend is
configurable and independent: you can run Prometheus-only (zero OTLP egress), or
full traces + metrics, or anything in between.

---

## New Go dependencies

```
go.opentelemetry.io/otel
go.opentelemetry.io/otel/sdk
go.opentelemetry.io/otel/sdk/metric
go.opentelemetry.io/otel/sdk/trace
go.opentelemetry.io/otel/exporters/prometheus
go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc
go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc
go.opentelemetry.io/contrib/bridges/otelslog
```

All on the stable `v1.*` track. Pin to a single minor version across the tree
via `go.mod` `replace` if needed; otherwise latest is fine.

---

## File Layout for Phase 10

```
internal/observability/
  bootstrap.go              ← Step 10.0: SDK init + exporter wiring
  metrics.go                ← Step 10.1: central instrument registry
  tracing.go                ← Step 10.2: tracer + span helpers
  logging.go                ← Step 10.3: slog handler with OTel correlation
  health.go                 ← Step 10.5: enriched /healthz content
  bootstrap_test.go         ← Step 10.0 unit test
  metrics_test.go           ← Step 10.1 unit test via metric reader
  tracing_test.go           ← Step 10.2 unit test via trace recorder

# Extended files
internal/protocol/handlers/{produce,fetch,consumer_group,sasl}.go
                            ← record counts/bytes/latency per API
internal/storage/engine.go  ← write/read/fsync histograms
internal/storage/segment.go ← segment size gauge per partition
internal/lease/k8s_manager.go ← lease acquisition / loss counters
internal/auth/{engine,acl,scram,serviceaccount}.go
                            ← auth outcome counters + quota throttle
internal/protocol/tls.go    ← handshake counter + cert reload counter
internal/protocol/server.go ← connection counter per listener
cmd/skafka/main.go          ← wire observability.Bootstrap()
cmd/skafka-operator/main.go ← same bootstrap (operator metrics)

deploy/helm/skafka/templates/
  servicemonitor.yaml       ← Step 10.6: Prometheus Operator CRD (optional)
deploy/helm/skafka/values.yaml
                            ← observability.*

deploy/grafana/skafka-dashboard.json
                            ← Step 10.7: Grafana dashboard
```

---

## Step 10.0 — SDK bootstrap

File: `internal/observability/bootstrap.go`

Reads configuration from the standard OTel env vars so deployments are portable:

```
OTEL_SERVICE_NAME                  default: "skafka" or "skafka-operator"
OTEL_RESOURCE_ATTRIBUTES           comma-separated k=v
OTEL_EXPORTER_OTLP_ENDPOINT        default: "" (OTLP disabled if unset)
OTEL_EXPORTER_OTLP_PROTOCOL        default: "grpc"
OTEL_METRIC_EXPORT_INTERVAL        default: 30s (OTLP push only)
OTEL_TRACES_SAMPLER                default: "parentbased_traceidratio"
OTEL_TRACES_SAMPLER_ARG            default: "0.1"
SKAFKA_METRICS_ADDR                default: ":9464" (Prometheus scrape port)
```

```go
// Bootstrap installs global OTel providers and returns a shutdown function
// that flushes remaining signals. Call shutdown() on SIGTERM.
//
// If OTEL_EXPORTER_OTLP_ENDPOINT is set: trace + metric spans are pushed via
// OTLP gRPC. Otherwise, only the Prometheus /metrics endpoint is active.
func Bootstrap(ctx context.Context, service string) (shutdown func(context.Context) error, err error)
```

Resource attributes come from the Kubernetes downward API where available:
`k8s.namespace.name`, `k8s.pod.name`, `k8s.container.name`, plus `service.version`
from the chart's `appVersion`. Brokers also set `skafka.broker.ordinal`.

The Prometheus exporter is wired as a `MeterProvider` reader and starts an HTTP
server on `SKAFKA_METRICS_ADDR` serving `/metrics` in the Prometheus text format.
This is the same endpoint existing Prometheus scrape configs expect.

**Done when:**
- `curl localhost:9464/metrics` returns Prometheus text format when broker runs
- With `OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4317`, OTLP gRPC traces
  reach the collector
- `shutdown(ctx)` flushes spans within 5s and returns nil

---

## Step 10.1 — Metric instrumentation

File: `internal/observability/metrics.go`

Centralised instrument registry — every counter, histogram, and gauge is
defined once, type-checked at compile time, and passed to call sites as a
struct. This replaces scattered `Counter("name").Inc()` calls that are common
in ad-hoc instrumentation.

```go
type Metrics struct {
    // Throughput.
    ProduceRecords  metric.Int64Counter   // attrs: topic
    ProduceBytes    metric.Int64Counter   // attrs: topic
    FetchRecords    metric.Int64Counter   // attrs: topic, consumer_group
    FetchBytes      metric.Int64Counter   // attrs: topic, consumer_group

    // Storage.
    WriteLatency    metric.Float64Histogram // attrs: topic
    ReadLatency     metric.Float64Histogram // attrs: topic
    FsyncLatency    metric.Float64Histogram
    PartitionSize   metric.Int64Gauge       // attrs: topic, partition

    // Leadership.
    LeaseAcquired   metric.Int64Counter   // attrs: topic, partition
    LeaseLost       metric.Int64Counter   // attrs: topic, partition
    PartitionLeader metric.Int64Gauge     // attrs: topic, partition; 1 or 0

    // Consumer groups.
    GroupMembers    metric.Int64Gauge     // attrs: consumer_group
    GroupRebalances metric.Int64Counter   // attrs: consumer_group
    GroupLag        metric.Int64Gauge     // attrs: topic, partition, consumer_group

    // Auth.
    AuthSuccess     metric.Int64Counter   // attrs: mechanism
    AuthFailure     metric.Int64Counter   // attrs: mechanism, reason
    ACLDeny         metric.Int64Counter   // attrs: principal, resource_type
    QuotaThrottle   metric.Int64Counter   // attrs: principal

    // TLS / external access.
    TLSHandshakes       metric.Int64Counter // attrs: result
    CertReloads         metric.Int64Counter
    ExternalConnections metric.Int64Counter // attrs: listener
    NotLeaderReturned   metric.Int64Counter // attrs: topic, partition (informational)

    // Request-level (high-cardinality guard: topic label omitted).
    RequestLatency  metric.Float64Histogram // attrs: api_key, version, error
}

func New(meter metric.Meter) (*Metrics, error)
```

Callers pass a `*Metrics` in at construction time:

```go
// internal/protocol/handlers/produce.go
type ProduceHandler struct {
    ...
    metrics *observability.Metrics
}

func (h *ProduceHandler) Handle(conn, version, body) ([]byte, error) {
    start := time.Now()
    defer func() {
        h.metrics.RequestLatency.Record(ctx, time.Since(start).Seconds(),
            metric.WithAttributes(
                attribute.Int("api_key", 0),
                attribute.Int("version", int(version)),
            ))
    }()
    // ... existing logic ...
    h.metrics.ProduceRecords.Add(ctx, int64(len(records)),
        metric.WithAttributes(attribute.String("topic", topic)))
    h.metrics.ProduceBytes.Add(ctx, int64(totalBytes),
        metric.WithAttributes(attribute.String("topic", topic)))
}
```

### Cardinality discipline

Attribute sets are bounded. Explicitly:
- `topic` is bounded by `KafkaTopic` CRD count (operator-managed)
- `consumer_group` is bounded by `FindCoordinator` call history (naturally low)
- `partition` is bounded by per-topic partition count
- `principal` is bounded by `KafkaUser` CRD count
- **Request latency histogram has no `topic` label** — request-level attribution
  lives in traces, not metrics, to keep the time-series count linear in API
  key + version rather than in topic count × API key × version

### Request log

Logged once per request via slog with fields `principal, api_key, topic,
partition, latency_ms, error`. Emits even when tracing is off; cheap.

**Done when:**
- Produce 100 records to topic `t` — `skafka_produce_records_total{topic="t"}`
  reports 100 at `/metrics`
- Read a cold partition — `skafka_storage_read_latency_seconds_bucket` has
  non-zero samples
- Unit test exercises every counter and histogram via
  `metricdata/metricdatatest.AssertEqual`

---

## Step 10.2 — Tracing

File: `internal/observability/tracing.go`

Every top-level Kafka request is a span. Key child spans within:

```
span: Handle.Produce              (api_key=0, version=9)
├─ span: authorize                (principal, topic)
├─ span: storage.Append           (topic, partition, bytes)
│  ├─ span: segment.write
│  └─ span: segment.fsync
└─ span: metadata.update

span: Handle.Fetch                (api_key=1, version=12)
├─ span: authorize
├─ span: storage.Read             (topic, partition, offset, maxBytes)
└─ span: encode.response

span: group.JoinGroup             (group_id)
├─ span: lease.AcquireCoordinator
└─ span: group.completeRebalance
```

Span attributes follow OTel semantic conventions where applicable, plus a
`skafka.*` namespace for domain concepts (`skafka.topic`, `skafka.partition`,
`skafka.api_key`).

### Trace context propagation

Kafka's wire protocol has no standard trace header slot. Phase 10 does NOT
attempt to thread W3C trace context through Kafka requests — the producing
application's trace and the consuming application's trace remain independent.
Broker-side spans are rooted fresh for each request and linked only via the
shared `correlation_id` (attribute, not a span link).

This is the industry norm for Kafka broker tracing; client-side libraries that
want end-to-end traces use application-level headers (OTel Kafka instrumentation
libraries put the trace context in Kafka message headers), which the broker
simply forwards as opaque data.

**Done when:**
- Produce request produces a span with `skafka.topic` and `skafka.partition`
  attributes
- Storage write is a child span of the Produce handler span
- Sampling works: with `OTEL_TRACES_SAMPLER_ARG=0.1`, approximately 10% of
  requests produce spans at the OTLP exporter (verified via mocked span processor)

---

## Step 10.3 — Structured logging with OTel correlation

File: `internal/observability/logging.go`

A `slog.Handler` that extracts the active span from `context.Context` and adds
`trace_id` and `span_id` as structured attributes:

```go
type CorrelationHandler struct {
    inner slog.Handler
}

func (h *CorrelationHandler) Handle(ctx context.Context, r slog.Record) error {
    if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
        r.AddAttrs(
            slog.String("trace_id", span.SpanContext().TraceID().String()),
            slog.String("span_id", span.SpanContext().SpanID().String()),
        )
    }
    return h.inner.Handle(ctx, r)
}
```

Also adds:
- Default `service.name`, `service.version`, `k8s.namespace`, `k8s.pod` fields
  matching the OTel Resource
- JSON output in production (`SKAFKA_LOG_FORMAT=json`), text for dev
- Configurable level via `SKAFKA_LOG_LEVEL=info|debug|warn|error`

All existing `slog.InfoContext(ctx, ...)` calls immediately gain trace
correlation; calls that don't pass context are unchanged (just no trace fields).

**Done when:**
- A request log line emitted inside a traced span includes `trace_id`
- A request log line emitted without a span has no `trace_id` field (not empty,
  absent)
- Log level switches correctly from env var

---

## Step 10.4 — Instrument the rest of the codebase

The bulk of Phase 10 is walking through the existing code and inserting
`metrics.*.Add/Record` calls at the right boundaries. Approximate scope:

| File | Instruments touched |
|---|---|
| `internal/protocol/handlers/produce.go` | ProduceRecords, ProduceBytes, RequestLatency |
| `internal/protocol/handlers/fetch.go` | FetchRecords, FetchBytes, RequestLatency |
| `internal/protocol/handlers/consumer_group.go` | GroupMembers, GroupRebalances, RequestLatency |
| `internal/protocol/handlers/sasl.go` | AuthSuccess, AuthFailure |
| `internal/storage/engine.go` | WriteLatency, ReadLatency (top-level wrappers) |
| `internal/storage/segment.go` | FsyncLatency, PartitionSize |
| `internal/lease/k8s_manager.go` | LeaseAcquired, LeaseLost, PartitionLeader |
| `internal/coordinator/coordinator.go` | GroupMembers, GroupRebalances, GroupLag |
| `internal/auth/acl.go` | ACLDeny |
| `internal/auth/quota.go` | QuotaThrottle |
| `internal/protocol/tls.go` | TLSHandshakes, CertReloads |
| `internal/protocol/server.go` | ExternalConnections |

Each edit follows the same pattern: accept `*observability.Metrics` in the
constructor, defer a latency record, call the appropriate counter.

**Done when:** running the existing integration tests produces non-zero values
for every metric via a Prometheus-text-format dump of `/metrics`.

---

## Step 10.5 — Enriched health endpoint

File: `internal/observability/health.go`

Replace the current bare `/healthz` with a JSON body that tools can parse:

```json
{
  "status": "ok",
  "broker_id": 1,
  "partitions_led": 4,
  "partitions_assigned": 4,
  "leases_held": 4,
  "tls": {
    "cert_expires_in_hours": 720,
    "cert_subject": "broker-1.kafka.example.com"
  },
  "listeners": ["internal", "external"]
}
```

`/readyz` returns `{"ready": true}` iff `partitions_led == partitions_assigned`
AND `EXTERNAL_ADVERTISED_HOST` is set (on the external listener).

HTTP status code is still 200 or 503 — the body is supplemental detail for
humans debugging. Kubernetes probes only care about the code.

---

## Step 10.6 — Helm values + ServiceMonitor

File: `deploy/helm/skafka/values.yaml` (new `observability` section)

```yaml
observability:
  # Prometheus scrape endpoint. Always on (cheap).
  metrics:
    enabled: true
    port: 9464
    # Create a Prometheus Operator ServiceMonitor. Requires the CRD.
    serviceMonitor:
      enabled: false
      labels: {}
      interval: 30s
      scrapeTimeout: 10s

  # OTLP push — to an OpenTelemetry Collector.
  otlp:
    enabled: false
    endpoint: "otel-collector.observability.svc.cluster.local:4317"
    # TLS config here when the collector is remote.
    insecure: true
    traces:
      # 0.0 = off; 1.0 = sample every trace. Production default 0.1.
      samplerRatio: 0.1

  logs:
    level: info        # debug|info|warn|error
    format: json       # json|text
```

File: `deploy/helm/skafka/templates/servicemonitor.yaml`

```yaml
{{- if and .Values.observability.metrics.enabled
          .Values.observability.metrics.serviceMonitor.enabled }}
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: {{ include "skafka.fullname" . }}
spec:
  selector:
    matchLabels: {{- include "skafka.selectorLabels" . | nindent 6 }}
  endpoints:
    - port: metrics
      path: /metrics
      interval: {{ .Values.observability.metrics.serviceMonitor.interval }}
      scrapeTimeout: {{ .Values.observability.metrics.serviceMonitor.scrapeTimeout }}
{{- end }}
```

The broker StatefulSet and operator Deployment gain a new `metrics` port and
the corresponding env vars for OTLP exporter configuration.

---

## Step 10.7 — Grafana dashboard

File: `deploy/grafana/skafka-dashboard.json`

Panels:

1. **Throughput** — `rate(skafka_produce_bytes_total[5m])` and
   `rate(skafka_fetch_bytes_total[5m])`, stacked by topic
2. **Produce / Fetch latency** — `histogram_quantile(0.99,
   sum(rate(skafka_request_latency_seconds_bucket{api_key=~"0|1"}[5m])) by (api_key, le))`
3. **Storage write/read latency p50 / p99** — same pattern over
   `skafka_storage_{write,read}_latency_seconds_bucket`
4. **Consumer group lag** — `max by (consumer_group) (skafka_group_lag)`,
   sortable table
5. **Partition leadership map** — `skafka_partition_leader` as a heatmap-style
   grid (broker × partition)
6. **Auth failures** — `sum by (mechanism, reason) (rate(skafka_auth_failure_total[5m]))`
7. **TLS handshake success rate** — `sum(rate(skafka_tls_handshakes_total{result="ok"}[5m])) / sum(rate(skafka_tls_handshakes_total[5m]))`
8. **ACL denies** — list of top denied principal/resource pairs
9. **Lease events** — `increase(skafka_lease_acquired_total[1h])` as a timeline

Shipped as JSON in `deploy/grafana/`. Users import manually or via a
`grafana-operator` `GrafanaDashboard` CRD (documented but not templated — that
CRD is optional).

---

## Step 10.8 — Testing

### Unit tests

- **Metric emission** (`metrics_test.go`): drive each instrument through a
  minimal call, assert via `otel/sdk/metric/metricdata/metricdatatest` that
  expected counters/histograms recorded the expected values with expected
  attribute sets.
- **Tracer assertions** (`tracing_test.go`): use
  `otel/sdk/trace/tracetest.NewSpanRecorder` attached to a custom provider;
  run handler code against it; assert span names, parent-child relationships,
  and attributes.
- **Correlation handler** (`logging_test.go`): emit a record inside a traced
  context; parse the JSON output; verify `trace_id` and `span_id` fields are
  present and match the active span.
- **Bootstrap shutdown** (`bootstrap_test.go`): verify `shutdown()` returns
  within 5s and that the Prometheus exporter is serving on the configured
  port.

### Integration tests

- **/metrics has content** (`tests/integration/observability_test.go`): start
  broker with `SKAFKA_METRICS_ADDR=:19464`, run a produce, scrape `/metrics`,
  assert non-zero `skafka_produce_records_total`.
- **OTLP roundtrip** (optional, gated by `-tags otlp`): run a mock collector,
  set `OTEL_EXPORTER_OTLP_ENDPOINT` to it, produce one record, assert a span
  for `Handle.Produce` arrived within 10s.

### What's NOT tested in Phase 10

- Grafana dashboard JSON is not rendered against a live Grafana — only linted
  for syntactic validity via a `jq` check in CI.
- Prometheus alerting rules are out of scope; a sample `PrometheusRule` is
  shipped in docs but not templated.

---

## Step Order Summary

| Step | File(s) | Depends on |
|---|---|---|
| 10.0 SDK bootstrap | `internal/observability/bootstrap.go` | add OTel deps to go.mod |
| 10.1 Metric registry | `internal/observability/metrics.go` | 10.0 |
| 10.2 Tracing | `internal/observability/tracing.go` | 10.0 |
| 10.3 Log correlation | `internal/observability/logging.go` | 10.0 |
| 10.4 Instrument the rest | handlers, storage, lease, auth, tls | 10.1–10.3 |
| 10.5 Enriched /healthz | `internal/observability/health.go` | 10.0 |
| 10.6 Helm values + SM | `values.yaml`, `servicemonitor.yaml` | 10.0, 10.1 |
| 10.7 Grafana dashboard | `deploy/grafana/skafka-dashboard.json` | 10.1 |
| 10.8 Tests | `*_test.go` | 10.1–10.4 |

Steps 10.0–10.3 are sequential. 10.4 is the bulk of the work and can be split
across multiple commits (instrument produce/fetch first, then storage, etc.).
10.5–10.7 are independent of each other and can be done in parallel once 10.4
is underway.

---

## Why this order

Bootstrap first because everything else depends on a working MeterProvider +
TracerProvider. Registry second because the rest of the codebase needs stable
instrument identity before any call sites are written. Tracing and log
correlation can be done in parallel but it's cheap to do tracing first — the
log handler just reads from the context when a span is there.

Instrumenting the rest of the codebase is mechanical but large: plan ~1-2
hours of focused work per subsystem (produce, fetch, storage, lease, auth).
Each insertion is small (a timer.Now() + a defer; one counter.Add() on success
path) but they add up.

Health endpoint enrichment and the ServiceMonitor are small polish items.
The Grafana dashboard is an afternoon of panel design + JSON export from a
live Grafana — not hard, not fast.
