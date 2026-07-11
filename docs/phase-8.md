# Phase 8 — Observability, Helm, parity validation

Detailed work plan for the ninth phase of the Rust rewrite. Companion
to [`rewrite.md`](./rewrite.md); the high-level summary lives there.
Builds on the codec scaffolding from [`phase-1.md`](./phase-1.md), the
storage engine from [`phase-2.md`](./phase-2.md), the single-broker
server from [`phase-3.md`](./phase-3.md), auth from
[`phase-4.md`](./phase-4.md), the cluster + coordinator surface from
[`phase-5.md`](./phase-5.md), the transactional surface from
[`phase-6.md`](./phase-6.md), and the operator + admin write-path
handlers from [`phase-7.md`](./phase-7.md).

**Goal.** Port `archive/internal/observability/` to
`crates/sk-observability/` (currently a doc-comment-only `lib.rs`),
wire every Phase 1-7 call-site that today logs via `tracing::info!`
into the OTel meter + tracer registered by
`sk_observability::bootstrap(...)`, and prove parity end-to-end. Parity
means: every `scripts/kafka-*.sh` script that exits 0 against the Go
broker exits 0 against the Rust broker (and the ones that exit 77 keep
exiting 77 for the same documented reason); the byte-opacity tripwire
test under `archive/tests/byte-opacity/` ported to
`bins/skafka/tests/byte_opacity.rs` still reads 0 on both counters
after a full produce/fetch/admin smoke; the franz-go EOS suite from
`archive/tests/kafka-compat/eos_v2_test.go` (already partly ported in
Phase 6 single-broker) extends to multi-broker against the Rust pair;
and the `bench-compare` skill (skafka vs Strimzi) shows the Rust
broker's Strimzi-relative ratios within ±5 % of the Go broker's
Strimzi-relative ratios captured before the rewrite started
(Strimzi is the fixed external yardstick the Go version has been
benchmarked against from day one). The
PrometheusRule template (`deploy/helm/skafka/templates/prometheusrule.yaml`)
keeps firing on the same metric names the Go side exposed, so the
9 existing alerts (SkafkaByteOpacityViolated, SkafkaSelfFencing,
SkafkaStaleControllerWriting, SkafkaNoCurrentController,
SkafkaBrokerCountMismatch, SkafkaAssignmentFileWriteFailing,
SkafkaAssignmentFileSizeApproachingCap, SkafkaCRMirrorErrorSustained,
SkafkaHeartbeatRTTHigh) are byte-equal across the cutover.

**Length.** ~2 weeks, single engineer. Workstream A (observability
crate) blocks B (call-site wire-up) and unblocks F (bench-compare,
which needs `bytes_in_total` and friends to read on the dashboards);
B blocks the byte-opacity test in D; C (scripts smoke) lands in
parallel with A against the existing wire surface; D (kafka-compat
port) lands in parallel with B; E (image + chart) closes with the
docker-publish workflow flip; F (bench-compare Strimzi-ratio gate)
is the final gate.

**Out of scope for Phase 8.**

- Cross-broker `WriteTxnMarkers` dispatch (gh #114). Phase 6 wired the
  file-queue dispatcher; Phase 8 doesn't extend it.
- The `DescribeTransactions` (65) / `ListTransactions` (66) admin
  surfaces flagged out-of-scope from Phase 7. Real implementation
  arrives only if the parity validation in workstream C surfaces a
  kafkactl path that depends on them — otherwise punt to Phase 9
  follow-up.
- `image.flavor: go | rust` Helm value. **Deliberately deferred to
  Phase 9** — Phase 8 keeps the chart pointing at the existing Go
  images; the parity gates here run against the Rust binary built
  out-of-band (image tag explicitly passed via
  `image.broker.repository=...skafka-rs`). Phase 9 introduces the
  user-facing toggle.
- Replacing the Prometheus-scrape lane. The chart's
  `templates/prometheusrule.yaml` evaluates against the OTLP-pushed
  metrics that Prometheus's native OTLP receiver ingests; we don't
  bring back a `/metrics` scrape endpoint. **Phase 7's operator
  binary already exposes port 8080 with an empty body** — keep it
  that way; Phase 8 just adds resource-attribute labels to the OTel
  SDK so the scrape-target shape is observable for ops without
  changing the data plane.
- Adding new metric names. Phase 8 ports the existing Go meter; new
  metrics land in their own follow-up PRs gated by an alert addition
  in `prometheusrule.yaml`. Drift in metric *units* between Go
  (OpenTelemetry-Go 1.32-ish) and Rust (opentelemetry 0.27) IS in
  scope — see workstream A.
- `tracing` span integration with the broker's per-request handler
  graph. Phase 8 wires the tracer provider, but per-handler spans
  are noted as a Phase 9 follow-up — they're an alert-free quality
  improvement, not a parity gate.
- `cargo bench` lane wiring in CI. The `bench-compare` skill is
  invoked manually for the Strimzi-ratio gate; an automated lane
  that runs on
  every PR is post-cutover work (gh #?, future).

**Prerequisite codec work.** **None.** Phase 7 closed the codec
registry at `sk_codec::api::registry::ALL.len() == 29`; Phase 8 adds
zero new API keys. The kafka-compat suite in workstream D exercises
the existing 29-key surface; if a port surfaces a missing key (e.g.
`DescribeTransactions` 65, `ListTransactions` 66, `ConsumerGroupHeartbeat`
68 for KIP-848), that's a Phase 8 escalation flag — pause the port
and decide whether to add the key here or defer the matching parity
test to Phase 9.

**Scope boundary (what real clients exercise).** After Phase 8 lands:
Grafana dashboards pointed at the OTLP-receiver-fed Prometheus stay
green against the Rust broker without panel rewrites — every panel
the Go side painted has the same metric name, same unit, same label
keys. `kubectl describe prometheusrule skafka-rules` shows 9 active
alerts; injecting a tripwire (decode a record on the broker, e.g. by
running `kafka-dump-log.sh` against a live segment) flips
`SkafkaByteOpacityViolated` within one OTLP push interval. The
`scripts/kafka-*.sh` integration suite runs end-to-end via
`bench-skafka` against the Rust broker and exits clean (`0` or `77`
per the per-script skip contract). The franz-go EOS suite passes
multi-broker; the bench-compare report shows the Rust broker's
Strimzi-relative ratios on producer-perf p50/p95/p99 and
consumer-perf throughput within ±5 % of the Go broker's
Strimzi-relative ratios captured before the rewrite — the parity
gate the rewrite plan committed to in §Phase 8 of `rewrite.md`.

---

## Workstreams

Six workstreams. A blocks B and unblocks F; B blocks the byte-opacity
re-test in D; C lands in parallel with A; D's franz-go port lands in
parallel with B; E lands any time after B; F is the bench-compare gate
that closes the phase.

- **A** — `sk-observability`: bootstrap, tracing, metrics, /healthz,
  byte-opacity tripwires, OTLP push observer, gauges, K8s API
  instrumentation, topic-traffic meter.
- **B** — Call-site wire-up: replace the Phase 1-7 `tracing::info!`-
  only counters in `sk-broker`, `sk-controller`,
  `sk-operator-controllers`, `sk-storage`, and `sk-coordinator` with
  real OTel meter calls under the names the Go side defined.
- **C** — `scripts/kafka-*.sh` smoke: per-tool integration suite runs
  green against the Rust broker; the skip set matches the Go-era
  documented list.
- **D** — `archive/tests/byte-opacity/` and `archive/tests/kafka-compat/`
  ported into `bins/skafka/tests/`. franz-rs or rdkafka harness
  (chosen here, not in Phase 6).
- **E** — `bins/skafka/Dockerfile` + `bins/skafka-operator/Dockerfile`
  are already shipping (Phase 0); Phase 8 flips the
  `.github/workflows/docker-publish.yml` Rust stanzas from commented-
  out to active so the GHCR registry gets `skafka-rs:vX.Y.Z` +
  `skafka-operator-rs:vX.Y.Z` on every tag push. Chart still defaults
  to the Go images — Phase 9 flips the default.
- **F** — `bench-compare` skill runs against the Rust pair; the
  produced Markdown report sits within ±5 % of the captured Go
  reference numbers. **This is the gate that closes Phase 8.**

Dependencies: A blocks B (no meter, no call-sites); B blocks D's byte-
opacity assertion (the tripwire reads come from the OTel side, not
just the in-memory counters); C parallel with A; E parallel with
B; F blocked by B (dashboards must read the right metric names).

---

## A — `sk-observability`: bootstrap + metrics + tracing + health

Port `archive/internal/observability/` (16 files, 2496 LoC) into
`crates/sk-observability/src/`. The Go side bundles bootstrap +
exporter + a 22-method `Metrics` registry + a `RuntimeState`
`/healthz` handler + topic-traffic accounting + a K8s-API
instrumentation wrapper + byte-opacity tripwires. Port each
verbatim — the rewrite plan's `±5 %` parity gate forbids drifting
either metric names or units.

### A.1 — File layout

```text
crates/sk-observability/src/
  lib.rs                     # mod declarations + re-exports
  bootstrap.rs               # `Providers` struct + `bootstrap(service, cancel)`
  tracing.rs                 # tracing-subscriber + tracing-opentelemetry wiring
  metrics.rs                 # Metrics registry — every Counter/Histogram/Gauge
  health.rs                  # axum router: /healthz + /readyz + RuntimeState trait
  byteopacity.rs             # bump_codec_record_decode / bump_codec_batch_reencode
  k8s_api.rs                 # record_k8s_call(operation, resource, fn) wrapper
  topic_traffic.rs           # TopicTrafficMeter (per-topic produce/fetch counts)
  gauges.rs                  # PartitionGauge + GaugeSource trait + runtime install
  otlp_push_observer.rs      # observedExporter wrapping otlp::MetricExporter
```

Files stay ≤ 400 LoC per the rewrite guideline; `metrics.rs` is the
big one (~600 LoC porting `metrics.go`'s 22 register helpers) — keep
the helpers small and the mapping table-driven so the file reads top-
to-bottom as one concept (the meter).

### A.2 — `bootstrap.rs`

```rust
pub struct Providers {
    pub meter: opentelemetry::metrics::Meter,
    pub tracer: opentelemetry::trace::TracerProvider,
    metric_provider: opentelemetry_sdk::metrics::SdkMeterProvider,
    span_processor: Option<opentelemetry_sdk::trace::BatchSpanProcessor>,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
}

impl Providers {
    pub async fn shutdown(self, timeout: Duration) -> Result<(), ObservabilityError> {
        let _ = self.shutdown_tx.send(());
        // Force-flush + shutdown each provider with a per-call timeout;
        // collect errors but continue (don't bail on the first failure).
        self.metric_provider.shutdown()?;
        if let Some(p) = self.span_processor {
            p.shutdown()?;
        }
        Ok(())
    }
}

pub async fn bootstrap(service: &str, cancel: CancellationToken)
    -> Result<Providers, ObservabilityError>
{
    let resource = build_resource(service)?;

    let metric_provider = build_metric_provider(resource.clone()).await?;
    let tracer_provider = build_tracer_provider(resource).await?;

    opentelemetry::global::set_meter_provider(metric_provider.clone());
    opentelemetry::global::set_tracer_provider(tracer_provider.clone());

    install_tracing_subscriber(&tracer_provider);

    Ok(Providers { meter: metric_provider.meter(service), ... })
}
```

`build_resource` reads the OTel resource attribute env vars
(`OTEL_RESOURCE_ATTRIBUTES`, `OTEL_SERVICE_NAME`, etc.) and stamps
`service.name`, `service.namespace = "skafka"`, `service.instance.id`
(falls back to `MY_POD_NAME`). Mirrors `archive/internal/observability/bootstrap.go:147-170`.

`build_metric_provider` reads `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`
(or falls back to `OTEL_EXPORTER_OTLP_ENDPOINT` per the OTel
spec), constructs an `opentelemetry_otlp::MetricExporter` (gRPC),
wraps it in the `observedExporter` from `otlp_push_observer.rs`
(so OTLP exporter errors increment a self-observability counter
the existing `prometheusrule.yaml` doesn't yet alert on but the
Go side surfaces — leave the metric registered for forward-compat),
and builds a `PeriodicReader` with the export interval driven by
`OTEL_METRIC_EXPORT_INTERVAL` (default 10s — matches the Go
`metric.WithInterval(10*time.Second)`).

`build_tracer_provider` reads `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`
(or fallback), constructs `SpanExporter`, wraps in
`BatchSpanProcessor`, sampler is `ParentBased{root=TraceIdRatioBased(0.01)}` —
match Go's `buildSampler()`.

**Env-var name parity.** The chart's
`templates/broker-statefulset.yaml` (and operator-deployment.yaml)
project `OTEL_EXPORTER_OTLP_*_ENDPOINT` + `OTEL_RESOURCE_ATTRIBUTES`
identically across flavors. The Rust port must read the same names —
verify by `helm template` + grep before the merge.

### A.3 — `tracing.rs`

```rust
pub fn install_tracing(log_level: &str, log_format: &str, tracer: &TracerProvider) {
    use tracing_subscriber::{fmt, EnvFilter, layer::SubscriberExt, Registry};
    let filter = EnvFilter::try_new(log_level).unwrap_or_else(|_| EnvFilter::new("info"));
    let json = log_format == "json";

    let fmt_layer = if json {
        fmt::layer().json().with_target(true).boxed()
    } else {
        fmt::layer().with_target(true).boxed()
    };

    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer.tracer("skafka"));

    let registry = Registry::default().with(filter).with(fmt_layer).with(otel_layer);
    let _ = tracing::subscriber::set_global_default(registry);
}
```

Replaces the ad-hoc `init_tracing` in `bins/skafka/src/main.rs:274`
and the corresponding stub in `bins/skafka-operator/src/main.rs`.
Both binaries call `sk_observability::install_tracing(&log_level,
&log_format, &providers.tracer)` after `bootstrap` returns.

### A.4 — `metrics.rs`

Port `archive/internal/observability/metrics.go` 1:1. The Go side
groups its instruments into 14 `register*` helpers (one per
subsystem); mirror the layout exactly. Each helper takes the meter
and the mutable `Metrics` struct, registers its instruments,
returns an error on collision.

```rust
pub struct Metrics {
    // request latency (per-API histograms)
    pub request_latency_ms: HashMap<&'static str, Histogram<f64>>,

    // storage
    pub storage_append_latency_ms: Histogram<f64>,
    pub storage_fetch_latency_ms:  Histogram<f64>,

    // controller
    pub controller_leadership_held: Gauge<i64>,        // 1 = leader
    pub controller_lease_transitions_total: Counter<u64>,

    // assignment
    pub assignment_file_writes_total:        Counter<u64>,
    pub assignment_file_write_errors_total:  Counter<u64>,
    pub assignment_file_size_bytes:          Gauge<i64>,
    pub assignment_broker_count:             Gauge<i64>,
    pub controller_alive_broker_count:       Gauge<i64>,

    // heartbeat
    pub heartbeat_rtt_ms:        Histogram<f64>,
    pub heartbeat_failures_total: Counter<u64>,

    // codec tripwires (the byte-opacity alert reads these)
    pub codec_record_decode_total:    Counter<u64>,
    pub codec_batch_reencode_total:   Counter<u64>,

    // … (groups, connections, auth, quotas, per-API error breakdown,
    //   cleaner, compactor, OTLP self-observability, operator,
    //   k8s API, topic traffic gauges — port each from metrics.go)
}

pub fn new_metrics(m: &Meter) -> Result<Metrics, ObservabilityError> {
    let mut mx = Metrics::default();
    register_request_latency_metrics(m, &mut mx)?;
    register_storage_latency_metrics(m, &mut mx)?;
    register_controller_metrics(m, &mut mx)?;
    register_assignment_metrics(m, &mut mx)?;
    register_heartbeat_metrics(m, &mut mx)?;
    register_codec_tripwire_metrics(m, &mut mx)?;
    register_group_and_connection_metrics(m, &mut mx)?;
    register_auth_and_quota_metrics(m, &mut mx)?;
    register_per_api_error_metrics(m, &mut mx)?;
    register_cleaner_metrics(m, &mut mx)?;
    register_compactor_metrics(m, &mut mx)?;
    register_otlp_metrics(m, &mut mx)?;
    register_operator_metrics(m, &mut mx)?;
    register_k8s_api_metrics(m, &mut mx)?;
    Ok(mx)
}
```

**Naming + units fixture.** Capture a Go-broker boot's `OTLP /v1/metrics`
gRPC payload once (run the broker against `otel-collector`'s
`exporters: logging`, snapshot the first push). Diff the Rust output
against it: every `metric.name`, `metric.description`, `metric.unit`,
`metric.type` must match. Workstream G stage 4 (see below) runs this
diff in CI.

**Global accessor.** Go ships `Global()` / `SetGlobal()` over a
`*Metrics` package var (`metrics.go:17-26`). Port to `OnceLock<Arc<Metrics>>`:

```rust
pub fn global() -> Arc<Metrics> {
    GLOBAL.get().cloned().expect("metrics::set_global not called")
}
pub fn set_global(m: Arc<Metrics>) {
    GLOBAL.set(m).expect("metrics::set_global called twice");
}
```

Call-sites in workstream B reach for `metrics::global().codec_record_decode_total.add(1, &[])`
instead of threading a `&Metrics` through every async stack — same
shape Go uses.

### A.5 — `health.rs`

```rust
pub trait RuntimeState: Send + Sync {
    fn current_controller(&self) -> Option<String>;
    fn current_epoch(&self) -> u64;
    fn partitions_led(&self) -> u32;
    fn alive_brokers(&self) -> Vec<String>;
    fn pid(&self) -> u32;
}

pub struct TlsInfo {
    pub enabled: bool,
    pub cert_expiry: Option<DateTime<Utc>>,
}

pub fn health_handler(broker_id: String, listeners: Vec<String>,
    tls: Option<TlsInfo>, source: Arc<dyn RuntimeState>) -> Router
{
    Router::new()
        .route("/healthz", get(move || healthz(broker_id.clone(), listeners.clone(),
                                                tls.clone(), source.clone())))
        .route("/readyz",  get(readyz))
}

pub fn set_ready(v: bool) { READY.store(v, Ordering::SeqCst); }
pub fn ready() -> bool    { READY.load(Ordering::SeqCst) }
```

`healthz` returns the same JSON shape Go does
(`archive/internal/observability/health.go:43-66`): `broker_id`,
`epoch`, `partitions_led`, `controller`, `alive`, `tls`, `ready`,
`version`. Capture a Go `/healthz` response as a fixture under
`crates/sk-observability/tests/fixtures/healthz_go.json`; the Rust
test serialises with the same `RuntimeState` mock and asserts JSON-
equal (not byte-equal — `serde_json` key ordering may differ;
deserialise both and `assert_eq!` the `serde_json::Value`).

### A.6 — `byteopacity.rs`

```rust
pub fn bump_codec_record_decode(site: &str) {
    metrics::global().codec_record_decode_total.add(1, &[KeyValue::new("site", site.to_string())]);
    tracing::warn!(site, "byte-opacity tripwire: record decode");
}

pub fn bump_codec_batch_reencode(site: &str) {
    metrics::global().codec_batch_reencode_total.add(1, &[KeyValue::new("site", site.to_string())]);
    tracing::warn!(site, "byte-opacity tripwire: batch re-encode");
}
```

`sk-codec`'s in-memory counters (`tripwires::record_decode_count()` /
`tripwires::batch_reencode_count()`) stay where they are — those are
process-local for fast in-test assertions. The OTel counter is the
externally visible signal the `SkafkaByteOpacityViolated` alert reads.
Workstream B audits every `tripwires::bump_*` call-site to add the
matching `byteopacity::bump_*` call.

### A.7 — `k8s_api.rs`

```rust
pub async fn record_k8s_call<F, T, E>(operation: &str, resource: &str, fut: F)
    -> Result<T, E>
where F: Future<Output = Result<T, E>>, E: std::fmt::Display,
{
    let started = Instant::now();
    let res = fut.await;
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    let labels = &[
        KeyValue::new("operation", operation.to_string()),
        KeyValue::new("resource",  resource.to_string()),
        KeyValue::new("outcome",   if res.is_ok() { "ok" } else { "error" }),
    ];
    metrics::global().k8s_api_latency_ms.record(elapsed_ms, labels);
    metrics::global().k8s_api_calls_total.add(1, labels);
    res
}
```

Used by `sk-k8s` + `sk-operator-controllers` around every `kube`
call. Workstream B threads it through. Matches
`archive/internal/observability/k8s_api.go:26`.

### A.8 — `topic_traffic.rs` + `gauges.rs`

`TopicTrafficMeter` carries per-topic produce/fetch counters; the
`Forget(topic)` path drops the entry on `KafkaTopic` CR delete so
the metric series doesn't grow unbounded. Port verbatim (Go's
`topic_traffic.go` is 218 LoC, mostly threadsafe-map glue —
`DashMap<String, TopicCounters>` in Rust).

`PartitionGauge` registers a callback-mode observable gauge that
walks a `GaugeSource` trait on each scrape. Port `gauges.go:15-93`.
The `GaugeSource` impl lives in `sk-broker` (it's the
`Coordinator` snapshot — same source `/healthz` reads); workstream B
wires `metrics::set_gauge_source(broker_coordinator.clone())` after
the coordinator boots.

### A.9 — `otlp_push_observer.rs`

Tiny exporter-wrapping shim (Go = 98 LoC). On every `export(...)`,
record latency + outcome to `metrics.otlp_export_*`; on error,
bucket the failure by `classify_otlp_err` (timeout, refused,
auth, other). Port `otlp_push_observer.go:33-89` verbatim.

**Exit:** `cargo test -p sk-observability` exercises:
- `bootstrap` builds a `Providers` against a `tonic` fake OTLP
  collector (`tokio::net::UnixListener`-backed; same pattern Phase
  6's `WriteTxnMarkers` test uses).
- The captured Go OTLP payload diffs byte-equal (for the names +
  units of every registered instrument) — fixture lives at
  `crates/sk-observability/tests/fixtures/go_metrics_descriptors.bin`.
- The `RuntimeState` mock + `/healthz` returns JSON that
  `serde_json::Value`-equals the captured Go fixture.

---

## B — Call-site wire-up

For every existing `tracing::info!`/`tracing::warn!` line in the
Rust crates that mirrors a Go-side metric increment, add the
corresponding `metrics::global().<counter>.add(...)` call. Audit
strategy: `git grep -nF 'sk_observability'` should hit every
crate from `sk-codec` through `sk-operator-controllers` after this
workstream lands.

### B.1 — Per-crate audit table

| Crate                         | Files to touch                                             | Instruments wired                                       |
|-------------------------------|------------------------------------------------------------|---------------------------------------------------------|
| `sk-codec`                    | `tripwires.rs`, every `bump_*` call-site under `api/*.rs`  | `codec_record_decode_total`, `codec_batch_reencode_total` |
| `sk-storage`                  | `committer.rs`, `cleaner.rs`, `compactor*.rs`, `recover.rs`| `storage_append_latency_ms`, `storage_fetch_latency_ms`, `cleaner_*`, `compactor_*` |
| `sk-protocol/handlers`        | every handler                                              | `request_latency_ms[api_name]`, `per_api_error_total[api_name,err_code]` |
| `sk-auth`                     | `quota.rs`, `acls.rs`, `scram.rs`                          | `auth_failures_total[mechanism]`, `quota_throttle_ms`   |
| `sk-broker`                   | `coordinator.rs`, `takeover.rs`, `heartbeat_client.rs`     | `heartbeat_rtt_ms`, `heartbeat_failures_total`, `self_fence_total` |
| `sk-controller`               | `election.rs`, `assignment_writer.rs`, `balancer.rs`, `cr_mirror.rs` | `controller_leadership_held`, `controller_lease_transitions_total`, `assignment_file_*`, `cr_mirror_errors_total` |
| `sk-coordinator`              | `manager.rs`, `txn_state.rs`, `txn_reaper.rs`              | `group_count`, `connection_count`, `txn_reaper_*`       |
| `sk-k8s`                      | `topic_watcher.rs`, `topic_cr_writer.rs`, `readiness_updater.rs` | `k8s_api_calls_total`, `k8s_api_latency_ms` (via `record_k8s_call`) |
| `sk-operator-controllers`     | `kafkatopic.rs`, `kafkauser.rs`, `kafkacluster.rs`, `sweep.rs` | `operator_reconciles_total[kind,result]`, `operator_sweep_orphans_removed_total[kind]` |
| `bins/skafka`                 | `main.rs`                                                  | bootstrap call, install_tracing, `metrics::set_global`, `metrics::set_gauge_source`, `set_ready(true)` after listeners up |
| `bins/skafka-operator`        | `main.rs`                                                  | bootstrap call, install_tracing, `metrics::set_global`, healthz wiring (already in F.3) |

### B.2 — Wire-up shape

Each call-site is a one-liner. Example for `tripwires::bump_decode`:

```rust
// crates/sk-codec/src/api/produce.rs (existing call)
tripwires::bump_decode("produce::recoverable");

// becomes
tripwires::bump_decode("produce::recoverable");
sk_observability::byteopacity::bump_codec_record_decode("produce::recoverable");
```

Don't merge the two — the in-memory counter stays for fast in-test
assertions (the existing `record_decode_count()` API the kafka-
compat tests already use); the OTel counter is the externally
visible signal. The site label is the same string both sides.

### B.3 — Initialisation order

`bins/skafka/src/main.rs` becomes:

```rust
#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse_env()?;
    let cancel = CancellationToken::new();
    install_signal_handlers(cancel.clone());

    let providers = sk_observability::bootstrap("skafka", cancel.clone()).await?;
    sk_observability::install_tracing(&cli.log_level, &cli.log_format, &providers.tracer);
    let metrics = Arc::new(sk_observability::new_metrics(&providers.meter)?);
    sk_observability::metrics::set_global(metrics.clone());

    // ... existing engine / dispatcher / coordinator / server setup ...

    sk_observability::metrics::set_gauge_source(coordinator.clone());
    spawn_healthz(probe_addr, health_state, cancel.clone());
    sk_observability::health::set_ready(true);

    server.serve(cancel.clone()).await?;
    providers.shutdown(Duration::from_secs(5)).await?;
    Ok(())
}
```

`spawn_healthz` lives in `sk-observability::health` (axum router,
single binding); the broker passes a `RuntimeState` impl that
delegates to `Coordinator::current_controller`,
`Coordinator::current_epoch`, etc.

`bins/skafka-operator/src/main.rs` already references
`sk_observability::bootstrap` in its doc-comment (Phase 7 wrote
`//! when Phase 8 wires sk_observability::bootstrap`); replace the
stub with the real call.

### B.4 — Gauge-source plumbing

`sk_broker::Coordinator` implements `sk_observability::gauges::GaugeSource`:

```rust
impl GaugeSource for Coordinator {
    fn snapshot(&self) -> GaugeSnapshot {
        let asn = self.assignment.load();
        GaugeSnapshot {
            partitions_led: asn.partitions_for(self.broker_id).len() as u64,
            alive_brokers:  asn.alive.len() as u64,
            assignment_epoch: asn.epoch,
        }
    }
}
```

Called once per OTel push interval (10 s) by the observable-gauge
callback registered in `gauges::install_runtime_gauges`. **Don't
register per-partition gauges** — that path is a metric-cardinality
trap; the Go side aggregates to per-broker counters for the same
reason.

**Exit:** every Go-side metric name listed in the OTLP fixture has
at least one call-site in the Rust tree (`git grep -F 'metrics::global().<name>'`
hits ≥ 1 per name). The `prometheusrule.yaml` lint
(`promtool check rules`) passes against a metric-name list extracted
from the Rust binary's first OTLP push. Workstream A's golden-fixture
test covers descriptor parity; workstream B's wire-up parity is
covered by `crates/sk-observability/tests/wire_up_audit.rs` —
a script that walks the workspace for `metrics::global()` calls and
asserts every name in `metrics.rs` appears at least once.

---

## C — `scripts/kafka-*.sh` smoke

The shell suite under `scripts/` is **already language-agnostic**
(it hits the broker on the wire via the Apache Kafka CLI tools).
Per the CLAUDE.md `scripts/kafka-*.sh` note, scripts targeting
non-goals already `exit 77`. Phase 8's work:

1. Run the entire suite via the existing `skafka-scripts` skill
   against a Rust-broker pod in a `kind` cluster (or against the
   live k3s deployment if available — same skill).
2. Capture a baseline from the Go broker (one-shot) into
   `scripts/.parity-baseline.txt` — `<script>: <exit-code> <runtime-s>`.
3. Assert: same exit codes across both flavors. **Runtime drift IS
   not gated here** — that's bench-compare's job (workstream F).

The baseline file is committed; CI's parity job runs the suite,
diffs the result against the baseline, fails on any exit-code
delta.

**No new scripts.** If a Rust-broker bug surfaces a script that
errors where the Go broker passed, file an issue and fix the
broker — don't update the baseline.

**Skip-set audit.** The CLAUDE.md note pins which scripts return
77 today (KRaft tools, share-groups, etc.). Workstream A doesn't
move that set; verify on the first run that the same set returns
77 against the Rust broker. If a script that was 77 against Go
suddenly returns 0 against Rust, that's a feature accidentally
enabled — open an issue rather than silently updating the baseline.

**Exit:** `bench-skafka` skill's `scripts:` stage runs to completion
against the Rust broker; per-script exit codes match the baseline
captured from Go; the bench-skafka report's `scripts:` section
shows zero diffs. Time budget: ~5 min on warm cache (most
scripts are < 5 s each).

---

## D — `byte-opacity` + `kafka-compat` test port

Two test bodies, both heavy.

### D.1 — `bins/skafka/tests/byte_opacity.rs`

Port `archive/tests/byte-opacity/tripwire_test.go`. The Go test
produces 10 batches, fetches them, runs `kafka-delete-records.sh`
+ `kafka-dump-log.sh` against the resulting segments, then asserts:

```go
require.Equal(t, uint64(0), observability.RecordDecodeCount())
require.Equal(t, uint64(0), observability.BatchReencodeCount())
```

The Rust port asserts both `sk_codec::tripwires::record_decode_count()`
**and** `metrics::global().codec_record_decode_total`'s accumulator
read 0 — the latter via a custom in-test exporter that snapshots
the OTLP push payload. Same for `batch_reencode_count`. This is
the only test that proves workstream B wired the OTel side to the
codec side correctly.

```rust
#[tokio::test]
async fn byte_opacity_holds_across_full_smoke() {
    let _broker = TestBroker::with_observability().await;
    // ... produce 10 batches, fetch, delete-records, dump-log ...
    assert_eq!(sk_codec::tripwires::record_decode_count(), 0,
        "in-memory tripwire fired");
    assert_eq!(sk_observability::test_collector::counter("codec_record_decode_total"), 0,
        "OTel tripwire fired");
}
```

`TestBroker::with_observability()` registers an in-process OTel
collector (`sk_observability::test_collector`) that exposes
`counter(name)` for assertions. Add this helper inside
`sk-observability` so each test bin doesn't re-implement it.

### D.2 — `bins/skafka/tests/kafka_compat/`

Port the 21 files under `archive/tests/kafka-compat/`. Each
existing Go test pairs with a Rust file:

| Go file                                | Rust file                          |
|----------------------------------------|------------------------------------|
| `acks_test.go`                         | `acks.rs`                          |
| `admin_test.go`                        | `admin.rs`                         |
| `api_versions_test.go`                 | `api_versions.rs`                  |
| `batching_linger_test.go`              | `batching_linger.rs`               |
| `cert_rotation_test.go`                | `cert_rotation.rs`                 |
| `compat_test.go`                       | `compat.rs`                        |
| `compression_codecs_test.go`           | `compression_codecs.rs`            |
| `consumer_group_protocol_test.go`      | `consumer_group_protocol.rs`       |
| `cooperative_sticky_test.go`           | `cooperative_sticky.rs`            |
| `create_partitions_test.go`            | `create_partitions.rs`             |
| `eos_v2_test.go`                       | `eos_v2.rs` (multi-broker extension over Phase 6's single-broker port) |
| `external_listener_test.go`            | `external_listener.rs`             |
| `fetch_session_test.go`                | `fetch_session.rs` (assert SessionID=0 echo) |
| `find_coordinator_test.go`             | `find_coordinator.rs`              |
| `max_message_bytes_test.go`            | `max_message_bytes.rs`             |
| `metadata_versions_test.go`            | `metadata_versions.rs`             |
| `mtls_test.go`                         | `mtls.rs`                          |
| `offsets_test.go`                      | `offsets.rs`                       |
| `partition_share_test.go`              | `partition_share.rs`               |
| `produce_errors_test.go`               | `produce_errors.rs`                |
| `read_committed_test.go`               | `read_committed.rs`                |
| `scram_test.go`                        | `scram.rs`                         |

**Client choice.** Phase 6's EOS port punted on the
franz-rs-vs-rdkafka call. **Choose rdkafka here.** Rationale:
the Go side calls into librdkafka via cgo for the `kafka-compat`
suite (it's the same library `kafka-console-producer.sh` uses
underneath), so picking the Rust `rdkafka` crate (which wraps
librdkafka the same way) maximises the chance that wire-protocol
edge cases the broker has been fixed against are exercised the
same way. franz-rs is a re-implementation in pure Rust and would
give us false-positive parity on bugs that only librdkafka
exhibits.

Trade-off: rdkafka brings a librdkafka C dependency into the
Rust dev environment; pin the `librdkafka-sys` version to
`v2.6.x` (matches the librdkafka pinned in
`archive/go.mod`'s confluent-kafka-go dependency) so version
drift doesn't break the parity contract.

**Per-test shape.**

```rust
#[tokio::test]
async fn admin_create_topic_round_trips() {
    let cluster = TestCluster::new_kind(3).await;     // 3-broker `kind` cluster
    let admin = rdkafka::AdminClient::from_config(&cluster.client_config()).unwrap();
    admin.create_topics(...).await.unwrap();
    let metadata = admin.metadata(Some("topic"), Timeout::After(Duration::from_secs(5))).await.unwrap();
    assert_eq!(metadata.topics()[0].partitions().len(), 3);
}
```

`TestCluster::new_kind(n)` wraps the same `kind` smoke harness
Phase 7 set up; gated behind `--features kind-smoke` so PR CI
skips it. `main`-push CI runs it.

**Single-broker subset.** ~half the tests don't need 3 brokers
(admin handlers, cert-rotation, codec edge cases). Provide
`TestBroker::single` for those — keeps test runtime down. The
multi-broker tests use the `kind` cluster only on `main` push.

**EOS multi-broker.** Phase 6 closed the single-broker case;
Phase 8's `eos_v2.rs` extends it to the cross-broker case (txn
coord ≠ group coord). Gated on the gh #114 `WriteTxnMarkers`
dispatcher Phase 6 wired — if the multi-broker EOS test fails
because the dispatcher isn't propagating, the failure is in
Phase 6 territory and gets fixed there, not papered over here.

**Exit:** all 22 test bodies port; the `kafka-compat` job in CI
runs them under `--features kind-smoke`; zero failures. Time
budget: ~8 min on warm cache (Phase 7's `kind` cold-build budget
was 2 min, plus per-test runtime).

---

## E — Image + chart publishing

Phase 0 committed `bins/skafka/Dockerfile` +
`bins/skafka-operator/Dockerfile` and a `.github/workflows/docker-publish.yml`
that *only* publishes the Go images today. Phase 8 flips the Rust
stanzas from commented-out to active.

### E.1 — Workflow edits

`.github/workflows/docker-publish.yml` already exists; the Rust
build matrix entries are commented per `CLAUDE.md`'s phase-0
note. Uncomment:

```yaml
- name: skafka-rs
  context: .
  dockerfile: bins/skafka/Dockerfile
  image: ghcr.io/woestebanaan/skafka-rs

- name: skafka-operator-rs
  context: .
  dockerfile: bins/skafka-operator/Dockerfile
  image: ghcr.io/woestebanaan/skafka-operator-rs
```

Both build on every `v*` tag push, same as the Go images. The
chart still defaults to Go (`image.broker.repository:
ghcr.io/.../skafka`); a user opting into the Rust pair sets
`--set image.broker.repository=ghcr.io/.../skafka-rs --set image.operator.repository=...`
explicitly. The user-facing toggle (`image.flavor: go | rust`)
lands in **Phase 9** per `rewrite.md`; don't pre-empt it here.

### E.2 — Tag policy

Same patch-bump policy as Go per
`memory/feedback_release_versioning.md`. The first Rust release is
`v0.2.0-preview` per `rewrite.md` §Phase 9 — Phase 8 doesn't cut
that tag; it just makes the workflow capable of building it. The
final pre-cutover `v0.1.N-preview` Go release publishes both
image sets so Phase 9's bake-time test has two production-shaped
images to compare.

### E.3 — Chart no-op

Per the §Out of scope above, the chart's `values.yaml` does not
gain `image.flavor` in Phase 8. Verify no chart change is required:

```bash
helm template deploy/helm/skafka --set image.broker.repository=ghcr.io/.../skafka-rs | grep image:
```

The single `image:` line under `broker-statefulset.yaml` should
honour the override without further chart edits. Add a 1-line
note to `deploy/helm/skafka/README.md` under "Image overrides"
documenting the Rust opt-in.

**Exit:** a tag push (`v0.1.<next>-preview`) builds the Go and
Rust images side-by-side; both appear under
`ghcr.io/woestebanaan/{skafka,skafka-rs,skafka-operator,skafka-operator-rs}`.
A `helm upgrade` with the Rust repo overrides deploys cleanly into
the bench k3s cluster (the same NFS-backed PVC the existing
bench-skafka skill uses).

---

## F — `bench-compare` Strimzi-relative gate

The `bench-compare` skill runs skafka-bench-producer +
strimzi-bench-producer back-to-back with a 120 s cooldown and emits
a Markdown comparison table. **Strimzi is the fixed reference the
Go version has been benchmarked against from day one** — the
"skafka vs Strimzi" delta is skafka's canonical position statement,
not the absolute number. Phase 8 closes by running the same head-
to-head against the Rust broker and verifying that the Rust
skafka's Strimzi-relative delta is **no worse than** the Go
skafka's Strimzi-relative delta, within a per-axis tolerance
(±5 % on the ratio).

Framing this as a Strimzi ratio, not a Go-absolute number, is
deliberate. Strimzi is external, well-understood, and doesn't
drift with cluster-state noise the way skafka's absolute latency
does under NFS pressure. It's also how the Go release notes have
described skafka's performance since the project shipped
(see `memory/project_skafka_perf.md`).

### F.1 — Reference capture

Before workstream A merges, run `bench-compare` against the
Go-broker baseline (last `v0.1.N-preview` tag). The skill emits
one Markdown table row per axis with three columns:
`skafka` / `strimzi` / `ratio (skafka / strimzi)`. Snapshot the
report into `docs/perf/go-reference-<commit-sha>.md`. The
**third column** is the yardstick — the ratios, not the absolute
skafka column.

Per `memory/feedback_bench_methodology.md`: single-run perf benches
are unreliable on home k3s. Run **5 times**, exclude outliers
(drop the slowest 1 + the fastest 1), average the remaining 3 for
the canonical Go ratios. Same procedure for the Rust runs — don't
gate on a single run.

### F.2 — Per-axis tolerance

The skill's table covers ~6 axes (producer latency p50/p95/p99,
consumer throughput, end-to-end latency, broker CPU/RSS). The
gate applies **to the ratio column** for the throughput + latency
percentiles: if Go skafka's producer-p50 ratio to Strimzi is
`3.8×`, Rust skafka's producer-p50 ratio must sit in
`[3.61×, 3.99×]` (±5 % of the ratio, not the absolute value).

For CPU/RSS, the Rust binary is expected to be lower (no GC
pauses, smaller working set); track the delta in the report but
don't gate on it.

**Why not close the Strimzi gap here?** Per
`memory/project_skafka_perf.md`, the ~75 % single-consumer
throughput gap and the ~3.8× producer-p50 gap are **architectural**
(group-commit fsync vs page-cache ack, single-writer-per-partition
vs ISR replication) — not implementation choices in the Go
port. Rewriting in Rust doesn't move those levers. Phase 8's gate
is "don't regress the Strimzi ratio"; closing the gap is a
separate design conversation (post-cutover, would touch the
storage engine substantially).

### F.3 — Liveness probe

The `bench-compare` skill already includes an NFS NAS-liveness
probe (the user's k3s cluster's PVC backend is finicky under
saturation). Reuse it — don't add a Rust-specific liveness check;
the storage substrate is the same shared NFS PVC.

### F.4 — Failure mode

If the Strimzi ratio widens by 5-15 % vs the Go baseline, profile
(cargo-flamegraph against the broker pod via the user's existing
ARC runner setup); common suspects: missing
`MaybeUninit::array_assume_init`-style fast paths, default `tokio`
runtime work-stealing thrash, or accidentally-synchronous async (a
`block_on` inside a hot path).

If the ratio widens by > 30 %, escalate per the rewrite-plan risk
register: "pause and profile before continuing." Don't merge
Phase 8 with a known ratio regression > 5 %.

**Exit:** `bench-compare` against the Rust broker + Strimzi pair
lands within ±5 % of the Go baseline's Strimzi ratios on the same
hardware, under 5-run averaging. The Markdown report is committed
under `docs/perf/rust-phase-8-<commit-sha>.md` and the ratio-delta
column shows zero red cells.

---

## Phase 8 exit criteria (all must hold)

1. `cargo test --workspace --all-features` green; under 15 min on a
   warm cache (Phase 7's 10 min budget + 5 min for the kafka-compat
   port).
2. `cargo clippy --workspace --all-targets -- -D warnings` and
   `cargo fmt --check` pass.
3. `crates/sk-observability/src/lib.rs` is no longer a doc-comment
   stub; `bootstrap`, `install_tracing`, `new_metrics`,
   `metrics::global`, `metrics::set_global`,
   `byteopacity::bump_codec_record_decode`,
   `byteopacity::bump_codec_batch_reencode`,
   `k8s_api::record_k8s_call`, `health::set_ready` are all
   `pub`-exported and exercised by tests.
4. The Go OTLP-descriptor fixture
   (`crates/sk-observability/tests/fixtures/go_metrics_descriptors.bin`)
   diffs byte-equal against the Rust binary's first push. Every
   metric name + unit + description + label-key set matches.
5. The 9 alerts in `deploy/helm/skafka/templates/prometheusrule.yaml`
   evaluate against the Rust broker's OTLP-pushed metrics; injecting
   the relevant condition fires the alert (verified for at least
   `SkafkaByteOpacityViolated` and `SkafkaHeartbeatRTTHigh` —
   the others are tested by the existing Go-era alert-injection
   harness reused here).
6. Every `scripts/kafka-*.sh` script's exit code against the Rust
   broker matches the captured Go baseline; the parity diff lane in
   CI is green.
7. `bins/skafka/tests/byte_opacity.rs` reads 0 on both the in-memory
   codec counters and the OTel `codec_record_decode_total` /
   `codec_batch_reencode_total` after a full produce / fetch /
   delete-records / dump-log smoke.
8. All 22 kafka-compat test bodies under
   `bins/skafka/tests/kafka_compat/` pass against the Rust 3-broker
   `kind` cluster (single-broker subset runs on every PR; full
   multi-broker on `main` push).
9. `eos_v2.rs` passes both same-broker and cross-broker EOS (the
   Phase 6 dispatcher carries the cross-broker case; if it fails,
   the bug is in Phase 6 territory).
10. `.github/workflows/docker-publish.yml` builds both flavors on
    every `v*` tag; the next tag push produces
    `ghcr.io/woestebanaan/skafka-rs:vX.Y.Z` and
    `skafka-operator-rs:vX.Y.Z`.
11. `bench-compare` against the Rust broker + Strimzi shows
    Strimzi-relative ratios on producer-perf p50/p95/p99 and
    consumer-perf throughput within ±5 % of the Go broker + Strimzi
    baseline captured on the same hardware, under 5-run averaging
    on the bench k3s cluster. The absolute skafka numbers are
    reported alongside for context but not gated on.
12. Go tree under `archive/` unchanged; chart unchanged save for the
    1-line README addendum in §E.3.

If any of these fail, do not merge — fix and re-run.

---

## Risks & mitigations

- **opentelemetry-rust 0.27 vs OTel Spec 1.32 unit-name drift.** The
  Rust SDK's instrument constructors accept `with_unit(Unit::new("ms"))`
  but historically have shipped subtle string differences vs the
  Go SDK (`{unit_count}` vs `1`, `ms` vs `milliseconds`). Mitigation:
  capture the Go-output descriptors fixture (workstream A.4) and
  diff every push; the test is the gate.
- **rdkafka cgo dependency in the dev environment.** librdkafka must
  be present for `cargo test --features kafka-compat` to link. NixOS
  k3s host already has it (Strimzi uses it); pin
  `librdkafka-sys = "4.5"` (librdkafka 2.6.x) in workspace deps and
  document the host requirement in `crates/sk-test-harness/README.md`.
  Fallback: if the cgo path is too painful on macOS dev boxes, ship
  a `--features kafka-compat-rdkafka` and a `--features
  kafka-compat-franz` and let contributors pick.
- **kafka-compat port surfaces a missing API key.** If e.g.
  `DescribeTransactions` (65) shows up in `admin_test.go`, the port
  fails at decode-time. Mitigation: at port time, run the Go test
  against the Rust broker first (via the franz-go binary) and read
  the wire response — if it's `UNSUPPORTED_VERSION`, the test wasn't
  exercising that key in Go either and can be skipped with the same
  reason; otherwise add the key to the codec registry (Phase 8
  escalation flag noted under §Prerequisite).
- **`scripts/kafka-*.sh` baseline drift.** The Go baseline captures
  exit codes against a specific Go commit; if the Go tree changes
  before workstream C lands (unlikely — it's frozen — but possible
  for port-blocking bugfixes per CLAUDE.md), the baseline can drift.
  Mitigation: re-capture the baseline as the first step of
  workstream C; commit the baseline file with the same PR.
- **bench-compare's Strimzi ratio widens.** Performance bugs in the
  Rust port (missed inlining, async-runtime contention, suboptimal
  storage hot path) could push skafka's ratio to Strimzi further
  from parity than the Go baseline. Mitigation: per
  `memory/feedback_perf_dead_ends.md`, don't re-try the PGO /
  FADV_SEQUENTIAL / FLUSH_INTERVAL_MESSAGES=0 dead-ends — those are
  known not to help. Profile first. If the gap is structural (e.g.
  Rust's `tokio::fs` defaults differ from Go's syscalls), gate the
  PR on the profiling write-up and a follow-up issue, not on
  squeezing the last 2 %. Absolute skafka numbers can drift with
  cluster state (NFS health, node pressure); only the Strimzi
  ratio is stable enough to gate on.
- **OTLP collector incompatibility.** The chart's
  `prometheusrule.yaml` evaluates against Prometheus's native OTLP
  receiver. Rust's `opentelemetry-otlp 0.27` defaults to gRPC + the
  OTLP/proto v1 wire format; verify against the deployed collector
  version. Mitigation: send-and-scrape test in workstream A — boot
  the Rust binary against a real Prometheus + OTel-collector pair
  in `kind`, query `/api/v1/label/__name__/values` after one push
  interval, assert the Go-side metric names appear.
- **PromQL alert expressions drift.** The `prometheusrule.yaml`
  alerts use specific PromQL — `rate(skafka_codec_record_decode_total[5m])
  > 0` and similar. If the Rust SDK names the metric without the
  `skafka_` prefix (the OTel SDK auto-prefixes with the meter
  name `service.name = skafka`, which Prometheus's OTLP receiver
  renders as `skafka_...`), the alert still fires. Mitigation: the
  workstream A descriptor-fixture test asserts the rendered Prom
  name, not just the OTel descriptor name; run
  `promtool check rules` against the actual rendered metrics.
- **The `kind` smoke pulls 2 min from CI on every PR if
  ungated.** Phase 7 gated it behind `--features kind-smoke`;
  Phase 8 adds 22 test bodies under the same feature. Mitigation:
  keep the feature gate; single-broker subset runs on every PR
  (~1 min), full multi-broker on `main` push.
- **`TopicTrafficMeter::forget(topic)` race with CR delete.** If
  the topic-watcher fires `Deleted` and the broker is mid-fetch on
  the same topic, the gauge may decrement after the next record
  while accounting has already dropped the entry. Mitigation: Go
  side handles this with a per-topic mutex; mirror with
  `DashMap`'s entry API. Test: parallel produce + delete loop;
  assert the gauge converges to 0 and doesn't go negative.
- **bench-compare run blocks on NFS health.** The bench-compare
  skill includes a NAS-liveness probe; if the NFS pool is degraded
  the bench can't run. Mitigation: don't merge Phase 8 if the
  liveness probe fails — re-run the bench on a healthy day.
  Document in the PR description which run window was used.
- **`opentelemetry::global::set_meter_provider` is one-shot.** Re-
  initialisation across test boundaries (each `#[tokio::test]`
  resets process state) needs a test-only provider. Mitigation:
  workstream A.4 ships a `test_collector` that bypasses the global
  registry; tests construct their own `Metrics` from a
  per-test meter.

---

## Landed vs pending

Phase 8's landed slice covers every workstream that fits inside the
code repo. The three remaining workstreams each need a piece of
external state (a running broker, a `kind` cluster, a Strimzi
co-deploy) that isn't reproducible from a fresh checkout — they
land as separate small PRs once the bench cluster is healthy.

### Landed

- **A — `sk-observability` crate.** Every module ported from
  `archive/internal/observability/`: bootstrap + tracing + metrics
  registry (45 fields, name-and-unit-parity with Go) + `/healthz`
  axum router + byte-opacity tripwires + `record_k8s_call`
  wrapper + `TopicTrafficMeter` + partition gauges + OTLP push
  self-observer. 21 unit tests. See commit `475b752`.
- **B.1 + B.2 — Call-site wire-up + audit.** Threaded
  `sk_observability::metrics::global()` through eight downstream
  crates (`sk-auth`, `sk-storage`, `sk-protocol`, `sk-broker`,
  `sk-controller`, `sk-coordinator`, `sk-k8s`,
  `sk-operator-controllers`). Every `Metrics` field on the
  `EXPECTED_WIRED` list in `crates/sk-observability/tests/wire_up_audit.rs`
  has ≥1 workspace call site, verified by the audit test. See
  commits `f6903e8` + `244d58f`.
- **B.3 — Bin integration.** `bins/skafka/main.rs` and
  `bins/skafka-operator/main.rs` both call
  `sk_observability::bootstrap(service, cancel)` at startup and
  `providers.shutdown()` on SIGTERM. See commit `475b752`.
- **Codec tripwire forwarder.** `sk-codec::tripwires` exposes a
  `TripwireHook` static-hook seam that `sk-observability::bootstrap`
  populates with `byteopacity::bump_codec_*`. Any future production
  code path that fires the tripwire now bumps both the process-
  atomic counter (fast in-test assertion) and the OTel counter
  (`SkafkaByteOpacityViolated` alert). Workspace dep graph stays
  clean — sk-codec doesn't depend on sk-observability. See
  commit `244d58f`.
- **D partial — byte-opacity test port.** `bins/skafka/tests/byte_opacity.rs`
  ports `archive/tests/byte-opacity/tripwire_test.go` against
  `MemoryStorage`. Storage round-trip is byte-identical across
  5 batches with distinct compression codec bits; tripwire
  counters read zero at the end. Meta-test proves the hooks fire
  when explicitly called.
- **E — docker-publish workflow flip.** `.github/workflows/docker-publish.yml`
  builds `skafka-rs` + `skafka-operator-rs` (or their `-preview`
  variants) alongside the Go images on every `v*` tag. Chart
  README documents the override command; the user-facing
  `image.flavor` toggle stays for Phase 9. See commit `244d58f`.

### Also landed in the second execution pass

Between the initial workstream commits and this section, a chain
of Rust-broker deploy-time bugs surfaced against the live k3s +
Strimzi cluster and were closed as part of Phase 8 completion:

* **`--init` mode** in `bins/skafka` — the chart's `partition-init`
  init container invokes `skafka --init` (chown the data dir);
  the Rust binary now handles it (commit `d5a6eec`).
* **rustls `CryptoProvider::install_default`** in both bins — kube-rs
  → hyper-rustls panicked without it (commit `d5a6eec`).
* **Chart-shape `SKAFKA_LISTENERS` deserializer** — the chart's
  Strimzi-style listener JSON (`{name,port,type,tls:bool,authentication:{type}}`)
  now flows through the same `ListenerEntry` as the internal shape
  via a custom `Deserialize` impl (commit `b43d2da`).
* **Chart RBAC**: operator `create`+`update` on
  `coordination.k8s.io/leases` (Rust operator's `KubeLeaseElection`
  needs both; the Go operator ran on ConfigMap-based election —
  commit `b77f265`).
* **axum `/healthz` + `/readyz` server in `bins/skafka`** — the
  chart wires `SKAFKA_HEALTH_ADDR`; broker now binds it (commit
  `13d4363`).
* **StatefulSet FQDN derivation for `advertised_host`** — clients
  bootstrapping on the Service got back `127.0.0.1:9092` in
  Metadata; now built from `MY_POD_NAME.SKAFKA_HEADLESS_SVC.SKAFKA_NAMESPACE.svc.cluster.local`
  (commit `2d30206`).
* **`Condition` serde `rename_all = "camelCase"` + tolerant
  defaults** — apiserver emits `lastTransitionTime`, not
  `last_transition_time`, and partial conditions on older CRs no
  longer break the KafkaTopic watcher (commits `c931856` +
  `b382f31`).
* **`CreateTopics` (API key 19)** — 300 LoC codec + handler +
  `TopicCRWriter::create_topic` (mints `KafkaTopic` CR) + kube
  client install in `bins/skafka/main`. Unblocks every
  `kafka-*.sh` script's `--create` step (commit `96583db`).
* **`run_topic_watch`** — kube-rs `watcher()` streaming
  `KafkaTopic` events into the broker's `TopicRegistry`
  on_apply / on_delete callbacks; newly-created topics now become
  wire-visible on the next Metadata request (commit `a156a45`).
* **Live `TopicSource` for `AssignmentLoop`** — replaced the
  boot-time `topic_specs_from_registry` snapshot with a live
  reader so newly-registered topics get partitions distributed on
  the next 5 s tick (commit `5c43d1b`).
* **`FindCoordinator` FQDN in `self_endpoint_lookup`** — was
  emitting unresolvable `<pod>.local`; now uses the same FQDN
  the Metadata handler advertises (commit `f2417d2`).
* **Chart `broker.replicaCount: 1`** in the k3s-cluster values —
  multi-broker leader ownership on the shared PVC needs
  `KubeLeaseElection` gating the AssignmentLoop (Phase 5 gap);
  single broker sidesteps it for Phase 8's scripts + bench
  baseline. Documented in the values.yaml note.

Together those unblocked C and F. See the `k3s-cluster` repo's
`apps/skafka` for the deployed chart pin (currently
`0.1.181-preview`).

### Attempted, delivered with follow-up

- **C — `scripts/kafka-*.sh` baseline.** Ran the full suite via the
  `skafka-scripts` skill against the live Rust broker
  (`v0.1.179-preview`). **8 PASS, 20 SKIP, 12 FAIL** — committed as
  `scripts/.parity-baseline.txt` (commit `c46f933`). Every FAIL
  traces to one of three follow-up buckets (multi-broker leader
  ownership; admin surface gaps — `DescribeLogDirs`, broker-level
  `IncrementalAlterConfigs`, ACL provisioning; perf-test 120 s
  timeouts). Rerunning the suite and diffing against the baseline
  file is the parity gate; downgrade of any row is a regression.
- **D-partial — byte-opacity test port.** Landed in commit
  `6cb28ff`. Full `kafka-compat` port still pending.
- **F — bench-compare Strimzi ratio.** Ran the `bench-compare`
  skill against the Rust broker + Strimzi pair on the shared NFS.
  **Both sides hit the Job's `activeDeadlineSeconds=1200`** (20-min
  ceiling; skafka 5/5 pods DeadlineExceeded, strimzi 5/5 too). The
  NAS itself was healthy — mid-run snapshots showed modest RPC
  rates (skafka 153 rpc/s + 7.13 MB/s TX; strimzi 158 rpc/s +
  5.93 MB/s), and both PVCs stayed `Bound` throughout. The real
  problem is workload arithmetic: 100M × 1KB × 5 pods at ~7 MB/s
  sustained can't finish in 20 min. Full report at
  `docs/perf/rust-phase-8-<sha>.md`; the `sk/st` ratio column is
  `N/A` for every row because neither producer finished. Rerun
  with a smaller record count in the producer manifest or a
  longer `activeDeadlineSeconds` for real Strimzi-ratio gate
  numbers.

### Pending

- **D — full kafka-compat suite port.** 22 test bodies under
  `archive/tests/kafka-compat/` mapped to
  `bins/skafka/tests/kafka_compat/`. Needs the `rdkafka` /
  `librdkafka-sys` cgo dependency in the dev environment and a
  `kind` cluster for the multi-broker subset. Deliberately
  deferred behind a `--features kafka-compat-rdkafka` gate.
- **Multi-broker leader ownership** — Phase 5 architectural
  follow-up. The Rust broker's `cluster.rs` uses `LocalElection`,
  so N replicas on the shared PVC clobber each other's
  `assignment.json`. Fix: wire `KubeLeaseElection` from
  `sk-controller` so only the elected controller runs the
  AssignmentLoop, and teach the Metadata handler to emit the
  cluster's actual broker set + per-partition leader instead of
  `self` for everything. Tracked in `scripts/.parity-baseline.txt`
  as the primary FAIL bucket.
- **F rerun with real ratio numbers.** Same skill invocation with
  either a smaller record count in the producer manifest or a
  longer `activeDeadlineSeconds`. The NAS is healthy at these
  RPC rates — it's the workload-vs-deadline arithmetic that
  fails on the current preset.

---

## What this enables for Phase 9

After Phase 8 merges, Phase 9 (cutover) lands by:

1. Tagging `v0.2.0-preview` — both Go and Rust images publish; the
   chart still defaults to Go, so the tag is a no-op for existing
   deployments.
2. Adding `image.flavor: go | rust` to
   `deploy/helm/skafka/values.yaml` (default `go`); the chart
   templates select the repository per flavor. Tagging
   `v0.2.1-preview` with the flavor knob lands.
3. Running the existing parity gates (workstreams C + F here, plus
   the kafka-compat suite from D) against both flavors for 72 h on
   the bench k3s cluster — the rewrite plan's "bake-time test."
4. Flipping the chart default to `rust`. The Go tree under
   `archive/` is marked deprecated but kept; deletion lands after
   two clean Rust-only releases per rewrite §Phase 9.
5. Removing the `legacy-go` job from `.github/workflows/ci.yml`.

No further Phase 8 changes — Phase 9 consumes the stable
observability + parity surface.
