//! K8s API call instrumentation wrapper.
//!
//! Wraps a single
//! apiserver call so the duration + result land on
//! [`Metrics::k8s_api_latency`] / [`Metrics::k8s_api_calls`].
//!
//! Call as:
//!
//! ```ignore
//! observability::record_k8s_call("List", "KafkaTopic", async {
//!     client.list(&params).await
//! }).await?;
//! ```
//!
//! `operation` labels: `"Get"`, `"List"`, `"Watch"`, `"Patch"`,
//! `"Update"`, `"Create"`, `"Delete"`. `resource` labels: the Kind
//! (`"Lease"`, `"Pod"`, `"EndpointSlice"`, `"KafkaTopic"`, ...).
//!
//! Cardinality stays bounded because the operation/resource space is
//! bounded by the broker's actual apiserver footprint.
//!
//! [`Metrics::k8s_api_latency`]: crate::metrics::Metrics::k8s_api_latency
//! [`Metrics::k8s_api_calls`]: crate::metrics::Metrics::k8s_api_calls

use std::future::Future;
use std::time::Instant;

use opentelemetry::KeyValue;

/// Instrument a single K8s API call. `fut`'s return value is wired
/// straight through — the caller's error handling is unchanged.
pub async fn record_k8s_call<F, T, E>(operation: &str, resource: &str, fut: F) -> Result<T, E>
where
    F: Future<Output = Result<T, E>>,
{
    let started = Instant::now();
    let res = fut.await;
    let elapsed_s = started.elapsed().as_secs_f64();

    let m = crate::metrics::global();

    let base_labels = [
        KeyValue::new("operation", operation.to_string()),
        KeyValue::new("resource", resource.to_string()),
    ];
    m.k8s_api_latency.record(elapsed_s, &base_labels);

    let result = if res.is_ok() { "ok" } else { "error" };
    m.k8s_api_calls.add(
        1,
        &[
            KeyValue::new("operation", operation.to_string()),
            KeyValue::new("resource", resource.to_string()),
            KeyValue::new("result", result.to_string()),
        ],
    );
    res
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn wraps_ok_result() {
        let out: Result<i32, &str> =
            record_k8s_call("Get", "Lease", async { Ok::<_, &str>(42) }).await;
        assert_eq!(out, Ok(42));
    }

    #[tokio::test]
    async fn wraps_err_result() {
        let out: Result<i32, &str> =
            record_k8s_call("Patch", "Pod", async { Err::<i32, _>("boom") }).await;
        assert_eq!(out, Err("boom"));
    }
}
