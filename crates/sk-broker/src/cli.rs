//! Env-var parsing for `bins/skafka/main.rs`.
//!
//! All knobs are env-only — Phase 3 ships no flag parser. Names
//! match the Go broker (`SKAFKA_*`) so the chart's env block
//! doesn't churn between flavours.

use std::env;
use std::path::PathBuf;

use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("SKAFKA_LISTENERS: {0}")]
    Listeners(serde_json::Error),
    #[error("SKAFKA_LISTENERS empty — set at least one entry")]
    NoListeners,
    #[error("SKAFKA_BROKER_ID: {0}")]
    BrokerId(std::num::ParseIntError),
    #[error("SKAFKA_FLUSH_INTERVAL_MESSAGES: {0}")]
    FlushInterval(std::num::ParseIntError),
}

/// JSON entry in `SKAFKA_LISTENERS`. Mirrors the Helm chart's
/// listener array shape (gh #126), narrowed to the Phase 3 fields.
#[derive(Debug, Clone, Deserialize)]
pub struct ListenerEntry {
    pub name: String,
    /// `host:port`. Use `0.0.0.0:9092` to bind all interfaces.
    pub addr: String,
    /// Optional advertised host (defaults to listener `addr`'s host).
    /// Phase 5 wires the per-broker external hostname template here;
    /// Phase 3 always echoes `addr.host` so single-broker Metadata
    /// responses make sense.
    #[serde(default)]
    pub advertised_host: Option<String>,
}

#[derive(Debug)]
pub struct Cli {
    pub listeners: Vec<ListenerEntry>,
    pub data_dir: Option<PathBuf>,
    pub flush_interval_messages: i64,
    pub cluster_id: String,
    pub broker_id: i32,
    pub topics_seed: String,
    pub log_level: String,
}

impl Cli {
    pub fn from_env() -> Result<Self, ConfigError> {
        let listeners_json =
            env::var("SKAFKA_LISTENERS").unwrap_or_else(|_| default_listeners().to_owned());
        let listeners: Vec<ListenerEntry> =
            serde_json::from_str(&listeners_json).map_err(ConfigError::Listeners)?;
        if listeners.is_empty() {
            return Err(ConfigError::NoListeners);
        }

        let data_dir = env::var("SKAFKA_DATA_DIR").ok().and_then(|s| {
            if s.is_empty() {
                None
            } else {
                Some(PathBuf::from(s))
            }
        });

        let flush_interval_messages = env::var("SKAFKA_FLUSH_INTERVAL_MESSAGES")
            .ok()
            .map(|s| s.parse::<i64>())
            .transpose()
            .map_err(ConfigError::FlushInterval)?
            .unwrap_or(1);

        let cluster_id =
            env::var("SKAFKA_CLUSTER_ID").unwrap_or_else(|_| "skafka-rust-dev".to_owned());

        let broker_id = env::var("SKAFKA_BROKER_ID")
            .ok()
            .map(|s| s.parse::<i32>())
            .transpose()
            .map_err(ConfigError::BrokerId)?
            .unwrap_or(0);

        let topics_seed = env::var("SKAFKA_TOPICS").unwrap_or_default();
        let log_level = env::var("RUST_LOG").unwrap_or_else(|_| "info".to_owned());

        Ok(Self {
            listeners,
            data_dir,
            flush_interval_messages,
            cluster_id,
            broker_id,
            topics_seed,
            log_level,
        })
    }
}

fn default_listeners() -> &'static str {
    r#"[{"name":"internal","addr":"0.0.0.0:9092"}]"#
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_listener_parses() {
        let v: Vec<ListenerEntry> = serde_json::from_str(default_listeners()).unwrap();
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].name, "internal");
        assert!(v[0].advertised_host.is_none());
    }

    #[test]
    fn json_with_advertised_host_round_trips() {
        let v: Vec<ListenerEntry> = serde_json::from_str(
            r#"[{"name":"x","addr":"0.0.0.0:9094","advertised_host":"broker-0.cluster.local"}]"#,
        )
        .unwrap();
        assert_eq!(
            v[0].advertised_host.as_deref(),
            Some("broker-0.cluster.local")
        );
    }
}
