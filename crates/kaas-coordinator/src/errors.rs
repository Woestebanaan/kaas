//! Coordinator error types.
//!
//! Cross-cutting errors that surface through the offset store, group
//! state machine, and handler shells. Wire-level error codes
//! (NOT_COORDINATOR = 16, COORDINATOR_NOT_AVAILABLE = 15, …) stay on
//! the response structs from `kaas-codec` — this enum models
//! programmer-facing failure modes only.

use std::io;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum CoordError {
    #[error("io: {0}")]
    Io(#[from] io::Error),

    #[error("json: {0}")]
    Json(#[from] serde_json::Error),

    #[error("group {0:?} not owned by this broker")]
    NotCoordinator(String),

    #[error("group {0:?} is not empty (members present)")]
    NonEmptyGroup(String),

    #[error("unknown member {0:?} in group {1:?}")]
    UnknownMember(String, String),
}
