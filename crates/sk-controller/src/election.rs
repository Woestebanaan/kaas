//! Controller election seam.
//!
//! Port shape of `archive/internal/controller/election.go`. The Go
//! side hand-rolls a Lease patch loop against the kube API and
//! surfaces `lease_transitions` as the controller epoch passed into
//! [`AssignmentLoop::start`].
//!
//! The kube-backed implementation lands in workstream E follow-up
//! alongside `ControllerWatch` — both wires need `kube::Client` +
//! `kube::Api::<Lease>`. Phase 5 keeps the seam open via a small
//! trait and a [`LocalElection`] stub that always reports "we won
//! at epoch 0", so a single-broker dev binary can construct the
//! Controller and exercise the assignment writer end-to-end.
//!
//! [`AssignmentLoop::start`]: crate::assignment_writer::AssignmentLoop::start

use async_trait::async_trait;

/// Controller-election surface. The implementation either spawns
/// the leader-election loop and fires callbacks (`on_acquired`,
/// `on_lost`) or, for dev mode, returns immediately as if elected.
#[async_trait]
pub trait LeaseElection: Send + Sync + 'static {
    /// Block until elected; return the post-acquire
    /// `lease_transitions` value (the controller epoch). The kube-
    /// backed impl observes the K8s Lease; [`LocalElection`]
    /// returns immediately.
    async fn acquire(&self) -> i64;

    /// Identity stamped into `assignment.json.controller`. Mirrors
    /// the StatefulSet pod-name shape (`"skafka-0"`).
    fn identity(&self) -> String;
}

/// Dev-mode election that always wins at epoch 0. Phase-5 binaries
/// that don't have a kube client wired use this so the assignment
/// writer can still run end-to-end against a single-broker setup.
#[derive(Debug, Clone)]
pub struct LocalElection {
    identity: String,
}

impl LocalElection {
    pub fn new(identity: impl Into<String>) -> Self {
        Self {
            identity: identity.into(),
        }
    }
}

#[async_trait]
impl LeaseElection for LocalElection {
    async fn acquire(&self) -> i64 {
        0
    }

    fn identity(&self) -> String {
        self.identity.clone()
    }
}
