//! Per-connection mutable state.
//!
//! Phase 3 ships a minimal shape — `listener_name`, `peer_addr`,
//! `principal`, `sasl_done`. The auth fields stay at their stub
//! defaults until Phase 4 wires the SASL/mTLS engines.
//!
//! Lives behind `Arc<Mutex<ConnState>>` so handlers can mutate
//! `sasl_done` mid-connection (Phase 4) without re-threading the
//! state through the dispatcher signature.

use std::net::SocketAddr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    pub principal_type: String,
    pub name: String,
}

impl Principal {
    /// Sentinel used before authentication completes (or when running
    /// on an anonymous listener — Phase 3's default).
    pub fn anonymous() -> Self {
        Self {
            principal_type: "User".to_owned(),
            name: "ANONYMOUS".to_owned(),
        }
    }
}

#[derive(Debug)]
pub struct ConnState {
    /// Free-form listener tag from `ListenerConfig::name`. The
    /// metadata handler keys per-listener port lookups on this string
    /// (Phase 5 gh #125; Phase 3 just echoes it back).
    pub listener_name: String,
    pub peer_addr: SocketAddr,
    /// Authenticated principal. `None` until SASL/mTLS completes
    /// (Phase 4). Phase 3 keeps it `None` everywhere.
    pub principal: Option<Principal>,
    /// `true` after a successful `SaslAuthenticate` (or after the
    /// mTLS handshake on a mTLS listener). Phase 3 never flips this.
    pub sasl_done: bool,
}

impl ConnState {
    pub fn new(listener_name: impl Into<String>, peer_addr: SocketAddr) -> Self {
        Self {
            listener_name: listener_name.into(),
            peer_addr,
            principal: None,
            sasl_done: false,
        }
    }
}
