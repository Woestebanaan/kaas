//! In-memory topic registry seeded from `KAAS_TOPICS` env JSON.
//!
//! Phase 3 stand-in for the `KafkaTopic` CR watcher that lands in
//! Phase 5/7. The shape is intentionally narrow — just what the
//! Metadata handler reads.

use std::collections::HashMap;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("topics seed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("topics seed: partitions must be > 0 for topic {0}")]
    InvalidPartitions(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicMeta {
    pub name: String,
    pub partition_count: i32,
    /// 16-byte UUID. All-zero is the gh #105 fallback for legacy CRs
    /// without `Status.TopicID`. Phase 3 always emits all-zero (the
    /// operator will mint real ids in Phase 7).
    #[serde(default = "TopicMeta::null_topic_id")]
    pub topic_id: [u8; 16],
}

impl TopicMeta {
    fn null_topic_id() -> [u8; 16] {
        [0; 16]
    }
}

/// JSON shape accepted in `KAAS_TOPICS`. Mirrors the simplest
/// possible KafkaTopic CR projection — name + partitions. Extra
/// fields are ignored so the env-var can grow without breaking
/// downgrade.
#[derive(Debug, Deserialize)]
struct TopicSeedEntry {
    name: String,
    partitions: i32,
}

#[derive(Debug)]
pub struct TopicRegistry {
    inner: RwLock<HashMap<String, TopicMeta>>,
    /// gh #221 phase 2: topic → (partition-as-string → log-dir name),
    /// stashed from `KafkaTopic.status.volumeAssignments` by the
    /// topic watch. Side map (not a `TopicMeta` field) so the meta
    /// struct keeps its stable literal shape across the many
    /// construction sites. Keys are strings because the CR status
    /// map is JSON.
    volume_assignments: RwLock<HashMap<String, std::collections::BTreeMap<String, String>>>,
}

impl Default for TopicRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl TopicRegistry {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            volume_assignments: RwLock::new(HashMap::new()),
        }
    }

    pub fn from_env_json(json: &str) -> Result<Self, ConfigError> {
        let entries: Vec<TopicSeedEntry> = if json.trim().is_empty() {
            Vec::new()
        } else {
            serde_json::from_str(json)?
        };
        let mut map = HashMap::with_capacity(entries.len());
        for e in entries {
            if e.partitions <= 0 {
                return Err(ConfigError::InvalidPartitions(e.name));
            }
            map.insert(
                e.name.clone(),
                TopicMeta {
                    name: e.name,
                    partition_count: e.partitions,
                    topic_id: [0; 16],
                },
            );
        }
        Ok(Self {
            inner: RwLock::new(map),
            volume_assignments: RwLock::new(HashMap::new()),
        })
    }

    pub fn get(&self, name: &str) -> Option<TopicMeta> {
        self.inner.read().get(name).cloned()
    }

    pub fn all(&self) -> Vec<TopicMeta> {
        let g = self.inner.read();
        let mut out: Vec<TopicMeta> = g.values().cloned().collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    pub fn insert(&self, m: TopicMeta) {
        self.inner.write().insert(m.name.clone(), m);
    }

    pub fn remove(&self, name: &str) {
        self.inner.write().remove(name);
        self.volume_assignments.write().remove(name);
    }

    /// Replace the stashed partition→log-dir placement for `topic`
    /// (gh #221 phase 2). The topic watch calls this on every CR
    /// apply; an empty map clears the entry.
    pub fn set_volume_assignments(
        &self,
        topic: &str,
        map: std::collections::BTreeMap<String, String>,
    ) {
        if map.is_empty() {
            self.volume_assignments.write().remove(topic);
        } else {
            self.volume_assignments
                .write()
                .insert(topic.to_owned(), map);
        }
    }

    /// Point one partition at a log dir (gh #221 phase 3 — the
    /// AlterReplicaLogDirs cutover updates the local view immediately
    /// instead of waiting for the CR-status watch echo).
    pub fn set_volume_assignment(&self, topic: &str, partition: i32, log_dir: &str) {
        self.volume_assignments
            .write()
            .entry(topic.to_owned())
            .or_default()
            .insert(partition.to_string(), log_dir.to_owned());
    }

    /// Log-dir name hosting `(topic, partition)`, if explicitly
    /// placed. `None` → the default log dir.
    pub fn volume_assignment(&self, topic: &str, partition: i32) -> Option<String> {
        self.volume_assignments
            .read()
            .get(topic)?
            .get(&partition.to_string())
            .cloned()
    }

    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }
}

/// gh #221 phase 2: the registry IS the broker's placement source —
/// the topic watch stashes `KafkaTopic.status.volumeAssignments`
/// here, and the storage engine resolves partition roots through it.
impl kaas_storage::PlacementResolver for TopicRegistry {
    fn log_dir_of(&self, topic: &str, partition: i32) -> Option<String> {
        self.volume_assignment(topic, partition)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_json_is_empty_registry() {
        let r = TopicRegistry::from_env_json("").unwrap();
        assert!(r.is_empty());
    }

    #[test]
    fn seed_parses_two_topics() {
        let r = TopicRegistry::from_env_json(
            r#"[{"name":"t1","partitions":3},{"name":"t2","partitions":1}]"#,
        )
        .unwrap();
        assert_eq!(r.len(), 2);
        let t1 = r.get("t1").unwrap();
        assert_eq!(t1.partition_count, 3);
        assert_eq!(t1.topic_id, [0; 16]);
    }

    #[test]
    fn zero_partitions_rejected() {
        let err = TopicRegistry::from_env_json(r#"[{"name":"x","partitions":0}]"#).unwrap_err();
        assert!(matches!(err, ConfigError::InvalidPartitions(_)));
    }

    #[test]
    fn all_returns_sorted_by_name() {
        let r = TopicRegistry::from_env_json(
            r#"[{"name":"z","partitions":1},{"name":"a","partitions":1}]"#,
        )
        .unwrap();
        let all = r.all();
        assert_eq!(all[0].name, "a");
        assert_eq!(all[1].name, "z");
    }
}
