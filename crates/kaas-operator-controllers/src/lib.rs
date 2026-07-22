//! kaas-operator-controllers — operator-side helpers and reconcilers.
//!
//! Phase 7 split into two layers:
//!
//! - **Helpers** (this commit): pure-state types that materialise
//!   `/data/__cluster/credentials.json` + `/data/__cluster/acls.json`,
//!   reconcile-observer counters, leader-elected startup sweep.
//!   These are kube-aware only at their public boundary
//!   ([`acls::reconcile_acls`] takes a `kube::Client`); the rest are
//!   plain free functions over filesystem paths.
//! - **Reconcilers** (workstream C, separate commit):
//!   `KafkaTopicReconciler`, `KafkaUserReconciler`,
//!   `KafkaClusterReconciler`.
//!
//! Cleanup model: **no finalizers**, and no delete event is
//! load-bearing. A topic recreated under a name that still has a
//! directory is reclaimed at reconcile time by the `.topic-id.json`
//! identity check (gh #219,
//! [`kafkatopic_controller::KafkaTopicReconciler`]); anything deleted
//! for good is dropped by the leader-elected periodic sweep
//! ([`sweep::sweep_topics`] + [`sweep::sweep_credentials`]).

#![allow(missing_debug_implementations)]

pub mod acls;
pub mod conditions;
pub mod credentials;
pub mod errors;
pub mod kafkacluster_controller;
pub mod kafkatopic_controller;
pub mod kafkauser_controller;
pub mod observer;
pub mod sweep;

pub use acls::{acl_to_entry, reconcile_acls, AclEntry, AclFile, AclResource};
pub use conditions::{set_condition, set_condition_with_now, READY};
pub use credentials::{
    compute_scram, generate_alphanum_password, read_credentials, write_credentials, CredQuotas,
    CredentialsFile, SaCredential, ScramCredential, UserCredential, SCRAM_ITERATIONS,
};
pub use errors::ControllerError;
pub use kafkacluster_controller::{
    broker_hostnames, build_bootstrap_servers, error_policy as kafkacluster_error_policy,
    reconcile_cluster, KafkaClusterReconciler,
};
pub use kafkatopic_controller::{
    error_policy as kafkatopic_error_policy, generate_topic_uuid, reconcile_topic, topic_dir_for,
    KafkaTopicReconciler,
};
pub use kafkauser_controller::{
    error_policy as kafkauser_error_policy, reconcile_user, KafkaUserReconciler,
};
pub use observer::ReconcileObserver;
pub use sweep::{sweep_credentials, sweep_topics, SweepReport};
