//! Axum-backed `/healthz` + `/readyz` handlers.
//!
//! The `/healthz` JSON shape
//! is pinned so dashboards + scripts written
//! against the plan-v3 schema keep working.
//!
//! [`RuntimeState`] is the v3 broker view — implementations must be
//! safe to call from any async task (the axum handler owns an
//! `Arc<dyn RuntimeState>`). A `None` runtime is acceptable and yields
//! zero-valued fields, which is the right answer in local-dev mode
//! where no controller / coordinator / heartbeat client is running.
//!
//! Fields that have no measurement yet return `-1` from the trait; the
//! handler folds those into `null` on the wire (via `Option<i64>`) so
//! dashboards render "not measured" rather than a misleading zero.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::get,
    Router,
};
use serde::Serialize;

/// The v3 broker runtime view surfaced on `/healthz`.
///
/// All methods must be safe to call from any thread. Methods that
/// haven't measured anything yet return `-1` (for `*_ms` fields); the
/// handler renders those as JSON `null`.
pub trait RuntimeState: Send + Sync {
    fn is_controller(&self) -> bool;
    fn controller_id(&self) -> String;
    fn controller_epoch(&self) -> i64;
    fn heartbeat_rtt_ms(&self) -> i64;
    fn heartbeat_age_ms(&self) -> i64;
    fn assignment_version(&self) -> u64;
    fn assignment_age_ms(&self) -> i64;
    fn partitions_led(&self) -> i32;
    fn partitions_assigned(&self) -> i32;
    fn partitions_recovering(&self) -> i32;
    /// Reports whether at least one partition's most recent committer
    /// fsync timed out per `Config.FsyncMaxLatency` (gh #95). Lets
    /// healthz surface a "storage backend wedged" signal before the
    /// broker accumulates enough queued appenders to look outwardly
    /// idle. Implementations that don't track storage health return
    /// `false`.
    fn storage_stalled(&self) -> bool;
}

/// TLS listener readiness snapshot (populated when an external
/// listener is bound).
#[derive(Debug, Clone, Serialize)]
pub struct TlsInfo {
    pub enabled: bool,
    #[serde(rename = "external_host", skip_serializing_if = "String::is_empty")]
    pub external_host: String,
}

#[derive(Debug, Serialize)]
struct HealthState {
    status: &'static str,
    #[serde(skip_serializing_if = "String::is_empty")]
    broker_id: String,
    listeners: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tls: Option<TlsInfo>,

    is_controller: bool,
    #[serde(skip_serializing_if = "String::is_empty")]
    controller_id: String,
    #[serde(skip_serializing_if = "is_zero_i64")]
    controller_epoch: i64,

    #[serde(skip_serializing_if = "Option::is_none")]
    heartbeat_rtt_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    heartbeat_age_ms: Option<i64>,

    #[serde(skip_serializing_if = "is_zero_u64")]
    assignment_version: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    assignment_age_ms: Option<i64>,

    partitions_led: i32,
    partitions_assigned: i32,
    partitions_recovering: i32,

    #[serde(skip_serializing_if = "is_false")]
    storage_stalled: bool,
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_zero_i64(v: &i64) -> bool {
    *v == 0
}
#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_zero_u64(v: &u64) -> bool {
    *v == 0
}
#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_false(v: &bool) -> bool {
    !*v
}

/// Shared state for the axum router — the fields the handler folds
/// into every response.
#[derive(Clone)]
pub struct HealthConfig {
    pub broker_id: String,
    pub listeners: Vec<String>,
    pub tls: Option<TlsInfo>,
    pub source: Option<Arc<dyn RuntimeState>>,
}

impl std::fmt::Debug for HealthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HealthConfig")
            .field("broker_id", &self.broker_id)
            .field("listeners", &self.listeners)
            .field("tls", &self.tls)
            .field(
                "source",
                &self.source.as_ref().map(|_| "<dyn RuntimeState>"),
            )
            .finish()
    }
}

/// Build the axum router with `/healthz` and `/readyz` bound.
pub fn health_router(cfg: HealthConfig) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .with_state(cfg)
}

async fn healthz(State(cfg): State<HealthConfig>) -> impl IntoResponse {
    let mut state = HealthState {
        status: "ok",
        broker_id: cfg.broker_id.clone(),
        listeners: cfg.listeners.clone(),
        tls: cfg.tls.clone(),
        is_controller: false,
        controller_id: String::new(),
        controller_epoch: 0,
        heartbeat_rtt_ms: None,
        heartbeat_age_ms: None,
        assignment_version: 0,
        assignment_age_ms: None,
        partitions_led: 0,
        partitions_assigned: 0,
        partitions_recovering: 0,
        storage_stalled: false,
    };
    if let Some(src) = cfg.source.as_ref() {
        state.is_controller = src.is_controller();
        state.controller_id = src.controller_id();
        state.controller_epoch = src.controller_epoch();
        state.heartbeat_rtt_ms = pos_i64(src.heartbeat_rtt_ms());
        state.heartbeat_age_ms = pos_i64(src.heartbeat_age_ms());
        state.assignment_version = src.assignment_version();
        state.assignment_age_ms = pos_i64(src.assignment_age_ms());
        state.partitions_led = src.partitions_led();
        state.partitions_assigned = src.partitions_assigned();
        state.partitions_recovering = src.partitions_recovering();
        state.storage_stalled = src.storage_stalled();
    }
    Json(state)
}

fn pos_i64(v: i64) -> Option<i64> {
    if v >= 0 {
        Some(v)
    } else {
        None
    }
}

#[derive(Debug, Serialize)]
struct ReadyState {
    ready: bool,
}

async fn readyz() -> impl IntoResponse {
    // gh #131: only require EXTERNAL_HOSTNAME_PATTERN when
    // KAAS_TLS_PORT is set (i.e. an external TLS listener is
    // active). Internal-only TLS deployments set the cert but never
    // an external hostname; without this guard they'd report unready
    // forever.
    let mut is_ready = ready();
    let tls_port = std::env::var("KAAS_TLS_PORT").unwrap_or_default();
    let ext_host = std::env::var("EXTERNAL_HOSTNAME_PATTERN").unwrap_or_default();
    if !tls_port.is_empty() && ext_host.is_empty() {
        is_ready = false;
    }
    let status = if is_ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(ReadyState { ready: is_ready }))
}

static READY_SNAPSHOT: AtomicBool = AtomicBool::new(false);

/// Flip the `/readyz` state. Called by the broker main once all
/// listeners are up and any required env vars are present.
pub fn set_ready(v: bool) {
    READY_SNAPSHOT.store(v, Ordering::SeqCst);
}

/// Report the current `/readyz` state.
#[must_use]
pub fn ready() -> bool {
    READY_SNAPSHOT.load(Ordering::SeqCst)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn set_ready_round_trips() {
        set_ready(true);
        assert!(ready());
        set_ready(false);
        assert!(!ready());
    }

    #[test]
    fn pos_i64_folds_negatives_to_none() {
        assert_eq!(pos_i64(0), Some(0));
        assert_eq!(pos_i64(42), Some(42));
        assert_eq!(pos_i64(-1), None);
    }

    #[test]
    fn health_json_shape_omits_unmeasured_fields() {
        let cfg = HealthConfig {
            broker_id: "skafka-0".to_string(),
            listeners: vec!["plain".to_string()],
            tls: None,
            source: None,
        };
        let state = HealthState {
            status: "ok",
            broker_id: cfg.broker_id.clone(),
            listeners: cfg.listeners.clone(),
            tls: None,
            is_controller: false,
            controller_id: String::new(),
            controller_epoch: 0,
            heartbeat_rtt_ms: None,
            heartbeat_age_ms: None,
            assignment_version: 0,
            assignment_age_ms: None,
            partitions_led: 0,
            partitions_assigned: 0,
            partitions_recovering: 0,
            storage_stalled: false,
        };
        let json = serde_json::to_value(&state).unwrap();
        assert_eq!(json["status"], "ok");
        assert_eq!(json["broker_id"], "skafka-0");
        assert_eq!(json["listeners"][0], "plain");
        assert!(json.get("heartbeat_rtt_ms").is_none());
        assert!(json.get("controller_epoch").is_none());
        assert!(json.get("storage_stalled").is_none());
    }
}
