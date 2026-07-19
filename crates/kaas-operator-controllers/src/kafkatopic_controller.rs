//! Reconciler that materialises a `KafkaTopic` CR into:
//!
//! - Partition directories at `<data_dir>/<effective_topic_name>/<0..partitions>/`.
//! - A per-topic `.config.json` next to the topic dir, consumed by the
//!   broker's cleaner / compactor via `kaas_storage::TopicConfigFile`.
//! - `Status.TopicID` — a v4 UUID minted on first reconcile, never
//!   rotated (gh #105, KIP-516).
//!
//! Cleanup model is reconcile-time + sweep — no finalizers. The
//! `NotFound` branch removes the topic dir best-effort; the
//! [`crate::sweep::sweep_topics`] pass catches orphans the operator
//! missed while down. See the gh #76 / gh #86 notes inline.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use kube::api::{Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::{Api, Client};
use kaas_operator_api::{Condition, KafkaTopic};
use kaas_storage::TopicConfigFile;

use crate::conditions::{set_condition, READY};
use crate::errors::ControllerError;
use crate::observer::ReconcileObserver;

/// Reconciler context. One per controller; cloned per-reconcile via
/// `Arc<Ctx>`.
pub struct KafkaTopicReconciler {
    pub client: Client,
    pub data_dir: PathBuf,
    pub observer: ReconcileObserver,
}

impl KafkaTopicReconciler {
    pub fn new(client: Client, data_dir: PathBuf) -> Self {
        Self {
            client,
            data_dir,
            observer: ReconcileObserver::new("KafkaTopic"),
        }
    }

    /// Reconcile entry point. The top-level [`reconcile_topic`]
    /// wrapper adapts this to kube-rs's `Controller::run` signature.
    pub async fn reconcile(&self, topic: Arc<KafkaTopic>) -> Result<Action, ControllerError> {
        let started = std::time::Instant::now();
        let out = self.reconcile_inner(topic).await;
        self.observer
            .record_duration(started.elapsed().as_secs_f64());
        out
    }

    async fn reconcile_inner(&self, topic: Arc<KafkaTopic>) -> Result<Action, ControllerError> {
        // gh #76 / NFS silly-rename guard: when the CR is mid-delete
        // (deletionTimestamp non-nil) the broker's TopicWatcher fires
        // its own Deleted event so the broker closes its open file
        // descriptors BEFORE our remove_dir_all swings in. The
        // reconciler itself doesn't have to do anything special —
        // the actual cleanup happens in the NotFound branch below
        // once the object disappears.
        if topic.metadata.deletion_timestamp.is_some() {
            self.observer.bump_requeue();
            return Ok(Action::await_change());
        }

        let topic_name = topic.effective_topic_name().to_string();
        if topic_name.is_empty() {
            // Defensive: a CR without metadata.name AND without spec.topic_name
            // wouldn't have made it past the apiserver, but if it does we
            // can't dispatch to any directory. Skip without erroring.
            self.observer.bump_error();
            return Ok(Action::await_change());
        }

        // Partition-decrease guard:
        // ConditionFalse with reason `InvalidPartitionCount` and DO NOT
        // mutate the filesystem. Caller is expected to fix the CR.
        let existing_count = topic
            .status
            .as_ref()
            .map(|s| s.partition_count)
            .unwrap_or(0);
        if existing_count > 0 && topic.spec.partitions < existing_count {
            let cond = Condition {
                type_: READY.into(),
                status: Condition::STATUS_FALSE.into(),
                observed_generation: topic.metadata.generation,
                last_transition_time: String::new(),
                reason: "InvalidPartitionCount".into(),
                message: "reducing partition count is not supported".into(),
            };
            self.patch_status(&topic, |st| set_condition(&mut st.conditions, cond.clone()))
                .await?;
            self.observer.bump_error();
            // No requeue — user has to fix the CR; await_change wakes us on the next edit.
            return Ok(Action::await_change());
        }

        // 1. mkdir partition dirs (idempotent).
        let topic_dir = self.data_dir.join(&topic_name);
        for p in 0..topic.spec.partitions {
            let part_dir = topic_dir.join(p.to_string());
            std::fs::create_dir_all(&part_dir)?;
        }

        // 2. Write per-topic .config.json. The broker watches this
        // file and applies retention/segment/compaction knobs on
        // partition open.
        let cfg = TopicConfigFile {
            retention_ms: topic.spec.config.retention_ms,
            retention_bytes: topic.spec.config.retention_bytes,
            segment_bytes: topic.spec.config.segment_bytes,
            cleanup_policy: topic.spec.config.cleanup_policy.clone(),
            min_compaction_lag_ms: topic.spec.config.min_compaction_lag_ms,
            delete_retention_ms: topic.spec.config.delete_retention_ms,
        };
        let fs = kaas_storage::fs::RealFs::new();
        kaas_storage::write_topic_config(&fs, &topic_dir, &cfg)?;

        // 3. Status update: partition count + TopicID (v4 UUID, minted
        // on first reconcile, NEVER rotated per gh #105).
        let next_count = topic.spec.partitions;
        let next_topic_id = topic
            .status
            .as_ref()
            .filter(|s| !s.topic_id.is_empty())
            .map(|s| s.topic_id.clone())
            .unwrap_or_else(generate_topic_uuid);

        let ready = Condition {
            type_: READY.into(),
            status: Condition::STATUS_TRUE.into(),
            observed_generation: topic.metadata.generation,
            last_transition_time: String::new(),
            reason: "PartitionsCreated".into(),
            message: format!("{} partition directories created", next_count),
        };

        self.patch_status(&topic, |st| {
            st.partition_count = next_count;
            st.topic_id = next_topic_id.clone();
            set_condition(&mut st.conditions, ready.clone());
        })
        .await?;

        self.observer.bump_success();
        // 5 min default requeue (controller-runtime-style
        // SyncPeriod fallback); watch events are
        // the primary driver, this is the safety net.
        Ok(Action::requeue(Duration::from_secs(300)))
    }

    /// NotFound branch: remove the topic dir best-effort.
    ///
    /// We only have `metadata.name` here (the watch's tombstone
    /// carries that, never `spec.topic_name`). For the common case
    /// where `metadata.name == effective_topic_name`, the directory
    /// is at `<data_dir>/<name>` and `remove_dir_all` succeeds.
    /// For gh #86 synthetic-name CRs the directory is at the
    /// `spec.topic_name` path which we no longer know — the
    /// `sweep_topics` startup pass catches those.
    pub fn handle_not_found(&self, name: &str) -> Result<(), ControllerError> {
        let path = self.data_dir.join(name);
        match std::fs::remove_dir_all(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(ControllerError::Io(e)),
        }
    }

    async fn patch_status(
        &self,
        topic: &KafkaTopic,
        mutate: impl FnOnce(&mut kaas_operator_api::KafkaTopicStatus),
    ) -> Result<(), ControllerError> {
        let Some(name) = topic.metadata.name.as_deref() else {
            return Ok(());
        };
        let namespace = topic.metadata.namespace.as_deref().unwrap_or("default");
        let api: Api<KafkaTopic> = Api::namespaced(self.client.clone(), namespace);

        let mut status = topic.status.clone().unwrap_or_default();
        mutate(&mut status);

        // Server-side apply requires apiVersion + kind in the body —
        // without them the API server answers
        // `invalid object type: /, Kind=` (400) and every reconcile
        // hot-loops through the error policy.
        let body = serde_json::json!({
            "apiVersion": "kaas.rs/v1alpha1",
            "kind": "KafkaTopic",
            "status": status,
        });
        api.patch_status(
            name,
            &PatchParams::apply("kaas-operator").force(),
            &Patch::Apply(&body),
        )
        .await?;
        Ok(())
    }
}

/// Generate a canonical hyphenated v4-shape UUID. Mirrors
/// `kafkatopic_controller.go:generateTopicUUID` — kube-rs has
/// `uuid::Uuid::new_v4()` which produces the same RFC 4122 layout,
/// but spell it out byte-for-byte so the regex match against
/// `^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`
/// is unambiguous.
pub fn generate_topic_uuid() -> String {
    uuid::Uuid::new_v4().hyphenated().to_string()
}

/// Free function passed to `kube::runtime::Controller::run`. The
/// `Ctx` is the reconciler itself behind `Arc`.
pub async fn reconcile_topic(
    topic: Arc<KafkaTopic>,
    ctx: Arc<KafkaTopicReconciler>,
) -> Result<Action, ControllerError> {
    ctx.reconcile(topic).await
}

/// Error policy for `kube::runtime::Controller::run`. Errors
/// requeue after 10 s; matches controller-runtime's default backoff
/// floor.
pub fn error_policy(
    _topic: Arc<KafkaTopic>,
    err: &ControllerError,
    ctx: Arc<KafkaTopicReconciler>,
) -> Action {
    tracing::warn!(error = %err, "KafkaTopic reconcile failed");
    ctx.observer.bump_error();
    Action::requeue(Duration::from_secs(10))
}

/// Bonus helper for the `bins/kaas-operator` main: derive the
/// CR's filesystem path so other tools (e.g. the sweep) can share
/// the resolution rule.
pub fn topic_dir_for(data_dir: &Path, topic: &KafkaTopic) -> PathBuf {
    data_dir.join(topic.effective_topic_name())
}

#[cfg(test)]
mod tests {
    use super::*;
    use kube::api::ObjectMeta;
    use kaas_operator_api::{KafkaTopic, KafkaTopicConfig, KafkaTopicSpec, KafkaTopicStatus};

    #[test]
    fn generate_topic_uuid_matches_v4_pattern() {
        let pat = regex_lite_v4();
        for _ in 0..100 {
            let id = generate_topic_uuid();
            assert!(pat.is_match(&id), "uuid {id} does not match v4 pattern");
        }
    }

    fn regex_lite_v4() -> regex::Regex {
        regex::Regex::new("^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
            .unwrap()
    }

    #[test]
    fn topic_dir_for_uses_effective_name() {
        // metadata.name only — falls back to it.
        let t = KafkaTopic {
            metadata: ObjectMeta {
                name: Some("meta-name".into()),
                ..ObjectMeta::default()
            },
            spec: KafkaTopicSpec {
                topic_name: String::new(),
                partitions: 1,
                config: KafkaTopicConfig::default(),
            },
            status: None,
        };
        assert_eq!(
            topic_dir_for(Path::new("/data"), &t),
            Path::new("/data/meta-name")
        );

        // spec.topic_name overrides metadata.name (gh #86 path).
        let synth = KafkaTopic {
            metadata: ObjectMeta {
                name: Some("hash-of-thing".into()),
                ..ObjectMeta::default()
            },
            spec: KafkaTopicSpec {
                topic_name: "My.Real.Topic".into(),
                partitions: 1,
                config: KafkaTopicConfig::default(),
            },
            status: None,
        };
        assert_eq!(
            topic_dir_for(Path::new("/data"), &synth),
            Path::new("/data/My.Real.Topic")
        );
    }

    #[test]
    fn partition_decrease_predicate() {
        // Verify the guard condition: existing > 0 AND spec < existing.
        let cases: &[(i32, i32, bool)] = &[
            (0, 1, false), // never reconciled → first run
            (1, 1, false), // same
            (1, 3, false), // grow
            (3, 1, true),  // shrink
            (5, 0, true),  // shrink to 0 (theoretically prevented by min=1 in the schema)
        ];
        for (existing, spec, expected) in cases {
            let got = *existing > 0 && *spec < *existing;
            assert_eq!(got, *expected, "existing={existing} spec={spec}");
        }
    }

    #[test]
    fn topic_id_preserved_when_already_set() {
        let original = "11111111-2222-4333-8444-555555555555".to_string();
        let topic = KafkaTopic {
            metadata: ObjectMeta::default(),
            spec: KafkaTopicSpec {
                topic_name: "t".into(),
                partitions: 1,
                config: KafkaTopicConfig::default(),
            },
            status: Some(KafkaTopicStatus {
                partition_count: 1,
                topic_id: original.clone(),
                conditions: vec![],
            }),
        };
        let preserved = topic
            .status
            .as_ref()
            .filter(|s| !s.topic_id.is_empty())
            .map(|s| s.topic_id.clone())
            .unwrap_or_else(generate_topic_uuid);
        assert_eq!(preserved, original);
    }

    #[test]
    fn topic_id_minted_when_status_missing_or_blank() {
        let topic = KafkaTopic {
            metadata: ObjectMeta::default(),
            spec: KafkaTopicSpec {
                topic_name: "t".into(),
                partitions: 1,
                config: KafkaTopicConfig::default(),
            },
            status: None,
        };
        let id = topic
            .status
            .as_ref()
            .filter(|s| !s.topic_id.is_empty())
            .map(|s| s.topic_id.clone())
            .unwrap_or_else(generate_topic_uuid);
        assert!(regex_lite_v4().is_match(&id));
    }
}
