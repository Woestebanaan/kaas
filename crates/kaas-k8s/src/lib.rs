//! kaas-k8s — broker-side Kubernetes helpers.
//!
//! Phase 5 ships the pure-state cores + the trait seams that
//! `bins/kaas/main.rs` wires together:
//!
//! - [`identity`] — `BrokerIdentity` + `DnsConfig`. Pure code, no
//!   kube dep.
//! - [`endpoints`] — `BrokerRegistry` over the StatefulSet's
//!   headless `Service`. Pure state; the kube-bound pump consumes
//!   `EndpointSlice` events and calls into `apply_slice` /
//!   `delete_slice`.
//! - [`topic_watcher`] — `KafkaTopic` CR cache + divergence
//!   detector. Same shape: pure state, kube pump feeds
//!   observations in.
//! - [`readiness`] — `ReadinessGate` trait + `NoopReadiness` stub.
//!
//! Kube-bound watchers live behind the `kube-watchers` feature
//! (default). The real pump implementations are the workstream E
//! follow-up tracked as task #10 — Phase 5 ships the function
//! signatures so `main.rs` can call them today and the bodies
//! drop in without a wire change.

pub mod endpoints;
pub mod identity;
pub mod readiness;
pub mod topic_watcher;

#[cfg(feature = "kube-watchers")]
pub mod kube_watchers;

pub use endpoints::{BrokerEndpoint, BrokerRegistry, EndpointSliceData, EndpointSliceEntry};
pub use identity::{parse_ordinal, BrokerIdentity, DnsConfig, IdentityError};
pub use readiness::{NoopReadiness, ReadinessError, ReadinessGate, READINESS_CONDITION};
pub use topic_watcher::{TopicEvent, TopicObservation, TopicWatcher};
