//! `tracing-subscriber` + OTel tracing bring-up.
//!
//! Honours `KAAS_LOG_LEVEL` (debug|info|warn|error) and
//! `KAAS_LOG_FORMAT` (json|text). Replaces the ad-hoc `init_tracing`
//! stubs in `bins/kaas/main.rs` and `bins/kaas-operator/main.rs`.
//!
//! The OTel layer emits every `tracing::span!` as an OTel span through
//! the tracer built in [`crate::bootstrap`]. Correlation-ID
//! contract: every log line
//! carries `trace_id` + `span_id` when a span is active.

use opentelemetry_sdk::trace::Tracer as SdkTracer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Install the global `tracing` subscriber. Safe to call once at
/// startup; subsequent calls are a no-op (the harness's global
/// dispatcher only accepts one install).
///
/// `tracer` must be the concrete SDK [`Tracer`] returned by
/// [`crate::bootstrap::Providers::tracer`]; the boxed-dispatch
/// [`opentelemetry::global::BoxedTracer`] doesn't implement
/// `PreSampledTracer` and the layer refuses it.
pub fn install_tracing(log_level: &str, log_format: &str, tracer: SdkTracer) {
    let filter = EnvFilter::try_new(log_level).unwrap_or_else(|_| EnvFilter::new("info"));
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    if log_format.eq_ignore_ascii_case("json") {
        let _ = tracing_subscriber::registry()
            .with(filter)
            .with(otel_layer)
            .with(tracing_subscriber::fmt::layer().json())
            .try_init();
    } else {
        let _ = tracing_subscriber::registry()
            .with(filter)
            .with(otel_layer)
            .with(tracing_subscriber::fmt::layer())
            .try_init();
    }
}
