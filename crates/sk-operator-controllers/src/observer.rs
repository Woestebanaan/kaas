//! Reconcile-event observer.
//!
//! Mirrors `archive/operator/controllers/reconcile_observer.go`. The
//! Go side wraps each `Reconcile` call in a small `Observed(...)`
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
        tracing::debug!(kind = self.kind, "reconcile success");
    }

    pub fn bump_error(&self) {
        self.error.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(kind = self.kind, "reconcile error");
    }

    pub fn bump_requeue(&self) {
        self.requeue.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(kind = self.kind, "reconcile requeue");
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
