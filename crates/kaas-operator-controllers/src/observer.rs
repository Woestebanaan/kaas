//! Reconcile-event observer.
//!
//! Wraps each `Reconcile` call in a small observer
//! decorator that increments per-kind OTel counters (success / error
//! / requeue) before returning to the caller. In Rust we expose the
//! same shape as a small struct that reconcilers spawn into; Phase 8
//! swaps the `tracing::info!` lines for real OTLP meters behind the
//! same API.

use std::sync::atomic::{AtomicU64, Ordering};

/// Per-kind reconcile counters. Cheap atomics — every reconciler
/// owns one instance.
#[derive(Debug, Default)]
pub struct ReconcileObserver {
    pub kind: &'static str,
    success: AtomicU64,
    error: AtomicU64,
    requeue: AtomicU64,
}

impl ReconcileObserver {
    pub const fn new(kind: &'static str) -> Self {
        Self {
            kind,
            success: AtomicU64::new(0),
            error: AtomicU64::new(0),
            requeue: AtomicU64::new(0),
        }
    }

    pub fn bump_success(&self) {
        self.success.fetch_add(1, Ordering::Relaxed);
        emit(self.kind, "ok");
    }

    pub fn bump_error(&self) {
        self.error.fetch_add(1, Ordering::Relaxed);
        emit(self.kind, "error");
    }

    pub fn bump_requeue(&self) {
        self.requeue.fetch_add(1, Ordering::Relaxed);
        emit(self.kind, "requeue");
    }

    /// Record a reconcile duration alongside the outcome bump. Used
    /// by the reconciler wrapper to feed
    /// `kaas.operator.reconcile.duration`.
    pub fn record_duration(&self, elapsed_s: f64) {
        kaas_observability::metrics::global()
            .operator_reconcile_duration
            .record(
                elapsed_s,
                &[kaas_observability::KeyValue::new("kind", self.kind)],
            );
    }

    pub fn success_count(&self) -> u64 {
        self.success.load(Ordering::Relaxed)
    }
    pub fn error_count(&self) -> u64 {
        self.error.load(Ordering::Relaxed)
    }
    pub fn requeue_count(&self) -> u64 {
        self.requeue.load(Ordering::Relaxed)
    }
}

fn emit(kind: &'static str, result: &'static str) {
    tracing::debug!(kind, result, "reconcile outcome");
    kaas_observability::metrics::global().operator_reconciles.add(
        1,
        &[
            kaas_observability::KeyValue::new("kind", kind),
            kaas_observability::KeyValue::new("result", result),
        ],
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_increment_independently() {
        let obs = ReconcileObserver::new("KafkaTopic");
        obs.bump_success();
        obs.bump_success();
        obs.bump_error();
        assert_eq!(obs.success_count(), 2);
        assert_eq!(obs.error_count(), 1);
        assert_eq!(obs.requeue_count(), 0);
    }
}
