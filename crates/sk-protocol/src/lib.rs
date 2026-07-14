//! sk-protocol — per-API request/response routing, dispatch, and
//! multi-listener server bring-up.
//!
//! - [`connstate`] — per-connection mutable state (`ConnState`).
//! - [`frame`] — `Connection<S>` wraps an async stream + frame reader,
//!   parses request headers via [`sk_codec::api::registry`], writes
//!   responses with the appropriate header version.
//! - [`dispatch`] — API-key router; implements the error-response
//!   contract (error_code-only body).
//!
//! Workstream E (server) and F (handlers) land in follow-up commits.

pub mod connstate;
pub mod dispatch;
pub mod frame;
pub mod server;

pub use connstate::{ConnState, Principal};
pub use dispatch::{
    is_pre_auth, Dispatcher, Handler, HandlerError, ERR_CLUSTER_AUTHORIZATION_FAILED,
    ERR_UNSUPPORTED_VERSION, PRE_AUTH_KEYS,
};
pub use frame::{Connection, ProtoError};
pub use server::{BoundServer, ListenerConfig, MtlsConfig, Server, ServerConfigBuilder};
