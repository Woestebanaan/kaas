//! CRMirror trait + `NoopMirror` impl.
//!
//! Reflects the current assignment into a `KafkaClusterAssignments`
//! CR for `kubectl describe` debugging. The plan is explicit that
//! mirror failures are fire-and-forget — a successful
//! `AssignmentStore::write` is the authoritative record, and the
//! CR is a convenience for cluster operators.
//!
//! Phase 5 ships only [`NoopMirror`]; the real kube-backed
//! implementation lands in Phase 7 alongside the rest of the
//! operator CR types.

use async_trait::async_trait;

use sk_broker::Assignment;

/// Best-effort write to the `KafkaClusterAssignments` CR. Always
/// returns immediately; failures are swallowed.
#[async_trait]
pub trait CrMirror: Send + Sync + 'static {
    /// Reflect the assignment's status block to the CR. Calls into
    /// the K8s API; errors are intentionally not surfaced.
    async fn mirror(&self, assignment: &Assignment);
}

/// Zero-value mirror used when no real implementation is wired.
/// Production code that never sets a `CrMirror` swaps this in as
/// the default.
#[derive(Debug, Default)]
pub struct NoopMirror;

#[async_trait]
impl CrMirror for NoopMirror {
    async fn mirror(&self, _assignment: &Assignment) {}
}
