//! sk-operator-controllers — operator-side helpers and reconcilers.
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
//! Cleanup model mirrors Go: **no finalizers**. Reconcile-time
//! best-effort cleanup on `Get → NotFound` plus a leader-elected
//! startup sweep ([`sweep::sweep_topics`] +
//! [`sweep::sweep_credentials`]) drop orphans the reconciler missed.

#![allow(missing_debug_implementations)]

pub mod acls;
pub mod conditions;
pub mod credentials;
pub mod errors;
pub mod observer;
pub mod sweep;

pub use acls::{acl_to_entry, reconcile_acls, AclEntry, AclFile, AclResource};
pub use conditions::{set_condition, set_condition_with_now, READY};
pub use credentials::{
    compute_scram, generate_alphanum_password, read_credentials, write_credentials, CredQuotas,
    CredentialsFile, SaCredential, ScramCredential, UserCredential, SCRAM_ITERATIONS,
};
pub use errors::ControllerError;
pub use observer::ReconcileObserver;
pub use sweep::{sweep_credentials, sweep_topics};
