//! sk-protocol — per-API request/response routing, dispatch, and
//! multi-listener server bring-up.
//!
//! Phase 3 of the rewrite. See [`docs/phase-3.md`](../../../docs/phase-3.md).
//!
//! - [`connstate`] — per-connection mutable state (`ConnState`).
//! - [`frame`] — `Connection<S>` wraps an async stream + frame reader,
//!   parses request headers via [`sk_codec::api::registry`], writes
//!   responses with the appropriate header version.
//! - [`dispatch`] — API-key router; mirrors the `errorResponse` /
//!   `errorResponseRaw` contract in the Go dispatcher.
//!
//! Workstream E (server) and F (handlers) land in follow-up commits.

pub mod connstate;
pub mod dispatch;
pub mod frame;
pub mod server;

pub use connstate::{ConnState, Principal};
pub use dispatch::{
    Dispatcher, Handler, HandlerError, ERR_CLUSTER_AUTHORIZATION_FAILED, ERR_UNSUPPORTED_VERSION,
};
pub use frame::{Connection, ProtoError};
pub use server::{BoundServer, ListenerConfig, Server, ServerConfigBuilder};
