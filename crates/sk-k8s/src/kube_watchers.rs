//! Kube-bound watcher pumps that feed events into
//! [`crate::endpoints::BrokerRegistry`], [`crate::topic_watcher::TopicWatcher`],
//! and the kube-backed [`crate::readiness::ReadinessGate`].
//!
//! Phase 5 ships only the function signatures + a `TODO` body — the
//! real `kube::runtime::watcher::<EndpointSlice>` /
//! `kube::runtime::watcher::<KafkaTopic>` plumbing lands in
//! workstream H's `kind`-driven integration suite alongside the
//! follow-up that wires `kube::Client` end-to-end (task #10). The
//! pump signatures live here so `bins/skafka/main.rs` can call them
//! today and the real implementation drops in without a wire
//! change.

#![cfg(feature = "kube-watchers")]

use std::sync::Arc;

use thiserror::Error;
use tokio::sync::oneshot;

use crate::endpoints::BrokerRegistry;
use crate::readiness::ReadinessGate;
use crate::topic_watcher::TopicWatcher;

#[derive(Debug, Error)]
pub enum KubeWatchError {
    #[error("kube watcher: {0}")]
    Other(String),
}

/// Stream `EndpointSlice` events for the headless service and pump
/// them into the registry. Returns a `oneshot::Sender` the caller
/// uses to signal shutdown.
///
/// **Phase 5 placeholder** — emits the implementation gap on first
/// call so dev-mode binaries don't silently no-op. Real
/// `kube::runtime::watcher::<EndpointSlice>` plumbing lands with
/// the kube-backed `ControllerWatch` follow-up (task #10).
pub async fn watch_endpoints(
    _client: Arc<dyn KubeClient>,
    _namespace: String,
    _headless_service: String,
    _registry: Arc<BrokerRegistry>,
) -> Result<oneshot::Sender<()>, KubeWatchError> {
    Err(KubeWatchError::Other(
        "watch_endpoints: kube-rs pump not yet wired (Phase 5 follow-up #10)".to_owned(),
    ))
}

/// Stream `KafkaTopic` CR events and pump them into the watcher.
pub async fn watch_topics(
    _client: Arc<dyn KubeClient>,
    _namespace: String,
    _watcher: Arc<TopicWatcher>,
) -> Result<oneshot::Sender<()>, KubeWatchError> {
    Err(KubeWatchError::Other(
        "watch_topics: kube-rs pump not yet wired (Phase 5 follow-up #10)".to_owned(),
    ))
}

/// Patch the broker pod's `Status.Conditions` to flip the
/// `skafka.io/PartitionsReady` readinessGate.
pub async fn patch_readiness(
    _client: Arc<dyn KubeClient>,
    _namespace: String,
    _pod_name: String,
    _gate: Arc<dyn ReadinessGate>,
    _ready: bool,
) -> Result<(), KubeWatchError> {
    Err(KubeWatchError::Other(
        "patch_readiness: kube-rs path not yet wired (Phase 5 follow-up #10)".to_owned(),
    ))
}

/// Narrow seam the kube-bound pumps use to reach the kube API. The
/// kube-backed impl wraps a `kube::Client`; tests can wire a mock.
/// Empty for now — methods land with the follow-up that ports
/// `ControllerWatch`.
pub trait KubeClient: Send + Sync + 'static {}
