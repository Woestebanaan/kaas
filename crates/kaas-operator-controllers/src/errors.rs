//! Common error type for the operator-side helpers and reconcilers.
//!
//! `thiserror`-built enum; one variant per upstream error source the
//! operator interacts with. The reconcilers' return type is
//! `Result<kube::runtime::controller::Action, ControllerError>` so a
//! single Error type carries everything.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ControllerError {
    #[error("kube api error: {0}")]
    Kube(#[from] kube::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("base64 decode error: {0}")]
    Base64(#[from] base64::DecodeError),

    #[error("rand fill error: {0}")]
    Rand(#[from] rand::Error),

    /// Returned when a reconciler discovers it's pointing at a stale
    /// resource version mid-flight; the controller-runtime requeue
    /// machinery handles the retry.
    #[error("conflict: {0}")]
    Conflict(String),

    /// gh #120: input Secret referenced by `KafkaUser.spec.authentication.password`
    /// doesn't exist. The reconciler converts this to a Condition +
    /// `await_change` instead of returning Err — see the
    /// `kafkauser_controller.rs` body.
    #[error("secret not found: {namespace}/{name}")]
    SecretNotFound { namespace: String, name: String },

    #[error("unsupported authentication type: {0}")]
    UnsupportedAuthType(String),

    #[error("malformed credential: {0}")]
    MalformedCredential(String),

    #[error("other: {0}")]
    Other(String),
}
