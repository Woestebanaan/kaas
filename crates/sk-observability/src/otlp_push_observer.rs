//! `PushMetricExporter` wrapper that self-observes success / failure.
//!
//! The
//! SDK's `PeriodicReader` silently swallows Export errors — pre-gh #121
//! PR4 the only symptom was that dashboards stopped receiving new
//! data. Wrapping the exporter here surfaces success / failure /
//! duration on the [`Metrics::otlp_push_*`] instruments so "OTLP push
//! is failing" becomes an alertable signal.
//!
//! Self-referential loop by design: a failure on push cycle N is
//! observed at push cycle N+1. On the first failed cycle dashboards see
//! nothing; on the second they see the previous failure. Acceptable
//! trade-off vs running a separate out-of-band push channel.
//!
//! [`Metrics::otlp_push_*`]: crate::metrics::Metrics::otlp_push_success

use std::time::Instant;

use async_trait::async_trait;
use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::data::ResourceMetrics;
use opentelemetry_sdk::metrics::{exporter::PushMetricExporter, MetricResult, Temporality};

/// Wrap `inner` so every Export call lands on the OTLPPush*
/// instruments.
#[derive(Debug)]
pub struct ObservedExporter<E: PushMetricExporter> {
    inner: E,
}

impl<E: PushMetricExporter> ObservedExporter<E> {
    pub fn new(inner: E) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl<E: PushMetricExporter> PushMetricExporter for ObservedExporter<E> {
    async fn export(&self, metrics: &mut ResourceMetrics) -> MetricResult<()> {
        let started = Instant::now();
        let res = self.inner.export(metrics).await;
        let elapsed_s = started.elapsed().as_secs_f64();

        let mx = crate::metrics::global();
        mx.otlp_push_duration.record(elapsed_s, &[]);
        match &res {
            Ok(()) => mx.otlp_push_success.add(1, &[]),
            Err(err) => mx.otlp_push_failure.add(
                1,
                &[KeyValue::new(
                    "err_class",
                    classify_otlp_err(err).to_string(),
                )],
            ),
        }
        res
    }

    async fn force_flush(&self) -> MetricResult<()> {
        self.inner.force_flush().await
    }

    fn shutdown(&self) -> MetricResult<()> {
        self.inner.shutdown()
    }

    fn temporality(&self) -> Temporality {
        self.inner.temporality()
    }
}

/// Bucket exporter errors into a small label space (timeout / refused
/// / other). Cardinality matters for the counter — we deliberately do
/// NOT label by the raw error string.
fn classify_otlp_err(err: &opentelemetry_sdk::metrics::MetricError) -> &'static str {
    let msg = err.to_string().to_lowercase();
    if msg.contains("timeout") || msg.contains("deadline") {
        "timeout"
    } else if msg.contains("refused") || msg.contains("no such host") || msg.contains("unresolved")
    {
        "refused"
    } else {
        "other"
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use opentelemetry_sdk::metrics::MetricError;

    #[test]
    fn classify_timeout() {
        let err = MetricError::Other("i/o timeout".to_string());
        assert_eq!(classify_otlp_err(&err), "timeout");
    }

    #[test]
    fn classify_refused() {
        let err = MetricError::Other("connection refused".to_string());
        assert_eq!(classify_otlp_err(&err), "refused");
    }

    #[test]
    fn classify_other() {
        let err = MetricError::Other("something went wrong".to_string());
        assert_eq!(classify_otlp_err(&err), "other");
    }
}
