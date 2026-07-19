//! Pod-readiness gate plumbing.
//!
//! The Helm chart
//! declares a custom `skafka.io/PartitionsReady` `readinessGate` on
//! each broker pod; the broker is not added to the headless
//! `Service`'s endpoints until that gate flips to `True`. Driven by
//! the broker's startup: once `Coordinator::apply_if_new` has
//! applied the first `assignment.json`, the broker calls
//! [`ReadinessGate::set_ready`] with `true`.
//!
//! Phase 5 ships only the trait + the [`NoopReadiness`] stub. The
//! kube-backed implementation lands in the workstream E follow-up
//! that wires `kube::Client` end-to-end.

use async_trait::async_trait;
use thiserror::Error;

/// The `Condition.Type` value the chart's `readinessGate` waits
/// for. Defined here so the kube-backed impl + the chart agree on
/// the exact string.
pub const READINESS_CONDITION: &str = "skafka.io/PartitionsReady";

#[derive(Debug, Error)]
pub enum ReadinessError {
    #[error("kube: {0}")]
    Kube(String),
}

/// Flip the broker pod's readiness gate. Production wires the kube-
/// backed impl that patches the pod's `Status.Conditions`; dev
/// mode uses [`NoopReadiness`].
#[async_trait]
pub trait ReadinessGate: Send + Sync + 'static {
    async fn set_ready(&self, ready: bool) -> Result<(), ReadinessError>;
}

/// Dev-mode / single-broker stub. No-op.
#[derive(Debug, Default)]
pub struct NoopReadiness;

#[async_trait]
impl ReadinessGate for NoopReadiness {
    async fn set_ready(&self, _ready: bool) -> Result<(), ReadinessError> {
        Ok(())
    }
}
