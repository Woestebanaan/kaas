# Observability

OTLP metrics and tracing pushed to Prometheus, and the `/healthz` endpoint's rich runtime state.

Everything lives in `crates/kaas-observability`: the OTel SDK bring-up, the
shared metrics registry, the `/healthz`/`/readyz` HTTP surface, and the
byte-opacity tripwire counters.

## Bootstrap: push-mode OTLP

`bootstrap.rs` wires the OTel SDK from environment variables (all emitted by
the Helm chart):

- `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` — metrics are **pushed** via
  OTLP/HTTP (http/protobuf) to Prometheus's native OTLP receiver
  (`/api/v1/otlp/v1/metrics`). Prometheus speaks only http/protobuf on that
  path — exporting gRPC just gets an h2 GoAway.
- `OTEL_EXPORTER_OTLP_ENDPOINT` — traces via OTLP/gRPC.
- `KAAS_METRIC_EXPORT_INTERVAL` — a duration string (`30s`, `1m`); default
  `30s`. The SDK default of 60 s made `rate([1m])` panels see a single sample
  and gap (gh #133).
- `OTEL_SERVICE_VERSION`, `MY_POD_NAME`, `KAAS_NAMESPACE` — folded into
  resource attributes so dashboards can filter by broker and namespace.
- `OTEL_TRACES_SAMPLER_ARG` — trace sampling ratio in `[0, 1]`, default 0.1.

With neither endpoint set the SDK still runs and exports go nowhere — safe
for tests and local dev. Before `bootstrap` runs, the global metrics handle
is a no-op registry, so pre-boot code and tests can record without nil
checks.

One metrics-pipeline invariant is load-bearing enough to repeat here: gauge
callbacks (high watermark, log start) read the storage engine through the
lock-free `ArcSwap` snapshot, never the partition mutex. Before that split, a
stuck NFS fsync holding the mutex stalled the OTel gauge callback and *all*
broker metrics vanished from Prometheus until the stall cleared (gh #134) —
exactly when you needed them most.

## `/healthz` and `/readyz`

Port 8080, axum, in `health.rs`:

- **`/readyz`** is the kubelet probe: flipped once by the broker main after
  listeners are up.
- **`/healthz`** returns a JSON runtime view (the `RuntimeState` trait):
  controller identity (`is_controller`, `controller_id`, `controller_epoch`),
  heartbeat health (`heartbeat_rtt_ms`, `heartbeat_age_ms`), assignment state
  (`assignment_version`, `assignment_age_ms`), partition counts
  (`partitions_led`, `partitions_assigned`, `partitions_recovering`), and
  `storage_stalled` — true when any partition's most recent committer fsync
  tripped the watchdog (gh #95), surfacing "storage backend wedged" before
  the broker accumulates enough queued appenders to look outwardly idle.

Fields with no measurement yet return `-1` internally and render as JSON
`null`, so dashboards show "not measured" rather than a misleading zero. In
local-dev mode (no cluster runtime) the whole runtime view is absent and the
handler serves zero-valued fields — the right answer when no controller,
coordinator, or heartbeat client is running.

`partitions_led` sources from the same `assignment.json`-backed `Coordinator`
as the Produce/Fetch ownership check — `/healthz` never invents its own view
of leadership.

## Byte-opacity tripwires

The broker's load-bearing invariant is *"kaas is a byte mover, not a byte
interpreter"*: no code path decodes individual records or re-encodes a
`RecordBatch` (see [Storage engine hot path](./storage-hot-path.md)).
`byteopacity.rs` gives that invariant teeth as two counters —
`codec_record_decode` and `codec_batch_reencode` — which **must stay at zero
in steady state**. Every increment names the offending call site in a `site`
attribute, and the `KaasByteOpacityViolated` alert fires on non-zero.

As of today no kaas code path calls these functions; the counters exist so
the *first* violation is an alert, not an archaeology project. The codec-side
counterparts live in `crates/kaas-codec/src/tripwires.rs`, and
`bins/kaas/tests/byte_opacity.rs` asserts the invariant in CI.

## Tracing

`install_tracing` (`tracing.rs`) installs one global `tracing-subscriber`
stack: an `EnvFilter` honouring `KAAS_LOG_LEVEL`, a fmt layer honouring
`KAAS_LOG_FORMAT` (`json` or text), and an OTel layer that emits every
`tracing::span!` as an OTel span through the tracer from `bootstrap`. The
correlation contract: every log line carries `trace_id` + `span_id` whenever
a span is active, so a log grep pivots straight into the trace view. Span
sampling defaults to 10% — set `OTEL_TRACES_SAMPLER_ARG=1.0` for a debugging
session.
