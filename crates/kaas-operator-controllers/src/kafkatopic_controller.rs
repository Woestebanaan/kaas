//! Reconciler that materialises a `KafkaTopic` CR into:
//!
//! - Partition directories at `<data_dir>/<effective_topic_name>/<0..partitions>/`.
//! - A per-topic `.config.json` next to the topic dir, consumed by the
//!   broker's cleaner / compactor via `kaas_storage::TopicConfigFile`.
//! - `Status.TopicID` — a v4 UUID minted on first reconcile, never
//!   rotated (gh #105, KIP-516), and stamped onto the topic dir as
//!   `.topic-id.json` so a recreated topic can be told apart from its
//!   predecessor.
//!
//! Cleanup model is reconcile-time + sweep — no finalizers, and no
//! delete event is load-bearing:
//!
//! - **Deleted and recreated** under the same name (gh #219): the
//!   reconciler sees a stamp that isn't this incarnation's TopicID and
//!   stages the old directory aside *before* the new one uses it. See
//!   `KafkaTopicReconciler::reclaim_stale_incarnation`.
//! - **Deleted for good**: the leader-elected periodic
//!   [`crate::sweep::sweep_topics`] pass reclaims the orphan. Latency
//!   there is fine — nothing is waiting on the directory.
//!
//! See the gh #76 / gh #86 / gh #203 notes inline.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use kaas_operator_api::{Condition, KafkaTopic};
use kaas_storage::TopicConfigFile;
use kube::api::{Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::{Api, Client};

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
        // A CR mid-delete (deletionTimestamp non-nil) has nothing to
        // materialise. Its directory is reclaimed later — by the sweep
        // if the topic stays gone, or by `reclaim_stale_incarnation`
        // when a topic of the same name is recreated. Neither path
        // depends on this reconcile, so bail out cleanly.
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

        // TopicID first: it is this incarnation's identity, and step 1
        // needs it to tell "our directory" from "the previous
        // incarnation's directory sitting at the same path".
        let next_topic_id = topic
            .status
            .as_ref()
            .filter(|s| !s.topic_id.is_empty())
            .map(|s| s.topic_id.clone())
            .unwrap_or_else(generate_topic_uuid);

        // 1. Reclaim a stale incarnation, then mkdir partition dirs
        // (both idempotent).
        let topic_dir = self.data_dir.join(&topic_name);
        let fs = kaas_storage::fs::RealFs::new();
        let identity =
            self.reclaim_stale_incarnation(&topic_name, &topic_dir, &next_topic_id, &fs)?;
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
        kaas_storage::write_topic_config(&fs, &topic_dir, &cfg)?;

        // 3. Status update: partition count + TopicID (v4 UUID, minted
        // on first reconcile, NEVER rotated per gh #105).
        let next_count = topic.spec.partitions;

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

        // Stamp the directory only once the TopicID is durable in the
        // CR status (gh #219). Stamping a *locally minted* ID before
        // that would be a data-loss trap: if the status patch keeps
        // failing, every reconcile mints a different ID, and each one
        // would see the previous one's stamp as a stale incarnation and
        // reclaim a live topic. Post-patch, a failed patch simply
        // leaves the dir unstamped — which is always adopted, never
        // reclaimed — and the next reconcile stamps it for real.
        //
        // Skipped when the stamp already matches: the steady-state
        // 5-minute requeue shouldn't rewrite a file per topic on the
        // shared volume for no reason.
        if identity != kaas_storage::IdentityVerdict::Match {
            kaas_storage::write_topic_identity(&fs, &topic_dir, &next_topic_id)?;
        }

        self.observer.bump_success();
        // 5 min default requeue (controller-runtime-style
        // SyncPeriod fallback); watch events are
        // the primary driver, this is the safety net.
        Ok(Action::requeue(Duration::from_secs(300)))
    }

    /// gh #219: reclaim a directory left behind by a **previous
    /// incarnation** of this topic name before the current one uses it.
    ///
    /// Partition dirs are addressed by name, so a delete→recreate under
    /// the same name (Kafka Streams' `application-reset` does this on
    /// every run) inherits the old incarnation's segments, manifest
    /// (high watermark / log start offset / epoch) and
    /// `producer-state.snapshot` — the last of which rejects the new
    /// producer's first batch with `OUT_OF_ORDER_SEQUENCE_NUMBER` or
    /// silently swallows it as a duplicate. `Status.TopicID` is minted
    /// fresh for a recreated CR (gh #105), so a stamp mismatch is an
    /// exact, race-free "this is not my directory" signal.
    ///
    /// Reconcile-driven on purpose: it needs no delete event, so there
    /// is no ordering hazard between a delete watch and the reconciler
    /// re-creating the dir. Re-running it is a no-op (NFS rule 2).
    ///
    /// **Unstamped is never stale.** A directory with no stamp predates
    /// this check, or was created by a broker's `Partition::open`;
    /// adopting it is the only safe reading — deleting on "unknown"
    /// would eat live data on upgrade.
    ///
    /// Returns what the directory's stamp said, so the caller can skip
    /// re-stamping a directory that already agrees.
    fn reclaim_stale_incarnation(
        &self,
        name: &str,
        topic_dir: &Path,
        topic_id: &str,
        fs: &dyn kaas_storage::fs::Fs,
    ) -> Result<kaas_storage::IdentityVerdict, ControllerError> {
        reclaim_stale_incarnation(&self.data_dir, name, topic_dir, topic_id, fs)
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

/// gh #203: reclaim a topic's directory without racing a broker's
/// `Partition::open`.
///
/// Atomically renames `<data_dir>/<name>` aside to a
/// `.deleting-<name>.<nanos>` sibling (a same-parent rename is the one
/// atomic NFS primitive), then best-effort recurses on the staged
/// copy — which no broker will ever open. The destructive
/// `remove_dir_all` therefore never runs on the live path. Any staged
/// dir the inline delete can't finish (FDs still held, gh #76) is
/// retried by the resumable sweep. See
/// `docs/src/architecture/nfs-substrate.md`.
/// Body of [`KafkaTopicReconciler::reclaim_stale_incarnation`] — free
/// so it is testable without a `kube::Client`. See that method for the
/// contract.
pub(crate) fn reclaim_stale_incarnation(
    data_dir: &Path,
    name: &str,
    topic_dir: &Path,
    topic_id: &str,
    fs: &dyn kaas_storage::fs::Fs,
) -> Result<kaas_storage::IdentityVerdict, ControllerError> {
    use kaas_storage::IdentityVerdict;

    if topic_id.is_empty() {
        return Ok(IdentityVerdict::Unstamped);
    }
    let verdict = match kaas_storage::classify_topic_identity(fs, topic_dir, topic_id) {
        Ok(v) => v,
        // An unreadable stamp is "unknown", not "stale".
        Err(err) => {
            tracing::warn!(topic = name, %err, "topic identity unreadable; adopting the dir");
            return Ok(IdentityVerdict::Unstamped);
        }
    };
    if verdict != IdentityVerdict::Stale {
        return Ok(verdict);
    }
    tracing::info!(
        topic = name,
        topic_id,
        "topic dir belongs to a previous incarnation; reclaiming before reuse"
    );
    stage_and_delete_topic_dir(data_dir, name)?;
    Ok(IdentityVerdict::Stale)
}

pub(crate) fn stage_and_delete_topic_dir(
    data_dir: &Path,
    name: &str,
) -> Result<(), ControllerError> {
    let path = data_dir.join(name);
    if !path.exists() {
        return Ok(());
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let staged = data_dir.join(format!(
        "{}{name}.{nanos}",
        crate::sweep::STAGED_DELETE_PREFIX
    ));
    match std::fs::rename(&path, &staged) {
        Ok(()) => {}
        // Already gone (a concurrent actor moved/removed it) → done.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(ControllerError::Io(e)),
    }
    // Best-effort recurse on the staged copy. A failure here (FDs
    // still open) is not fatal — the sweep retries `.deleting-*`.
    let _ = std::fs::remove_dir_all(&staged);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use kaas_operator_api::{KafkaTopic, KafkaTopicConfig, KafkaTopicSpec, KafkaTopicStatus};
    use kube::api::ObjectMeta;

    /// gh #203: deleting a topic dir frees the LIVE path immediately
    /// (atomic rename) so a concurrent broker open can't race the
    /// recursive delete. With no open FDs the staged copy is fully
    /// gone afterwards.
    #[test]
    fn stage_and_delete_frees_live_path_and_removes_staged() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let topic = root.join("doomed");
        std::fs::create_dir_all(topic.join("0")).unwrap();
        std::fs::write(topic.join("0").join("manifest.json"), b"{}").unwrap();

        stage_and_delete_topic_dir(root, "doomed").unwrap();

        // Live path is free — a broker mkdir on it now creates a fresh
        // dir instead of racing a delete-in-progress.
        assert!(!topic.exists(), "live topic path freed by the rename");
        std::fs::create_dir_all(&topic).unwrap();
        assert!(topic.exists());

        // No FDs were held, so the staged copy is fully reclaimed.
        let staged: Vec<_> = std::fs::read_dir(root)
            .unwrap()
            .flatten()
            .filter(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with(crate::sweep::STAGED_DELETE_PREFIX)
            })
            .collect();
        assert!(staged.is_empty(), "staged copy removed when no FDs held");
    }

    /// Seed `<root>/<name>` with a partition dir carrying leftover
    /// state, stamped with `stamp` (empty = unstamped).
    fn seed_topic_dir(root: &Path, name: &str, stamp: &str) -> PathBuf {
        let dir = root.join(name);
        std::fs::create_dir_all(dir.join("0")).unwrap();
        std::fs::write(
            dir.join("0").join("manifest.json"),
            br#"{"highWatermark":500}"#,
        )
        .unwrap();
        std::fs::write(dir.join("0").join("producer-state.snapshot"), b"{}").unwrap();
        if !stamp.is_empty() {
            kaas_storage::write_topic_identity(&kaas_storage::fs::RealFs::new(), &dir, stamp)
                .unwrap();
        }
        dir
    }

    /// gh #219: a topic deleted and recreated under the same name gets
    /// a fresh `Status.TopicID`, so the directory left by the previous
    /// incarnation must be staged aside — otherwise the new topic
    /// inherits its high watermark and, worse, its idempotence dedupe
    /// window (`OUT_OF_ORDER_SEQUENCE_NUMBER` on the first produce).
    #[test]
    fn stale_incarnation_dir_is_reclaimed() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let fs = kaas_storage::fs::RealFs::new();
        let dir = seed_topic_dir(root, "reset-me", "old-incarnation");

        let verdict =
            reclaim_stale_incarnation(root, "reset-me", &dir, "new-incarnation", &fs).unwrap();

        assert_eq!(verdict, kaas_storage::IdentityVerdict::Stale);
        assert!(
            !dir.exists(),
            "the previous incarnation's dir must not survive into the new one"
        );
    }

    /// Steady state: the same incarnation reconciling again must not
    /// touch its own data.
    #[test]
    fn matching_incarnation_dir_is_kept() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let fs = kaas_storage::fs::RealFs::new();
        let dir = seed_topic_dir(root, "steady", "same-id");

        let verdict = reclaim_stale_incarnation(root, "steady", &dir, "same-id", &fs).unwrap();

        // `Match` is also what tells the reconciler not to re-stamp the
        // directory on every 5-minute requeue.
        assert_eq!(verdict, kaas_storage::IdentityVerdict::Match);
        assert!(dir.join("0").join("manifest.json").exists(), "data kept");
    }

    /// Upgrade safety: directories written before the stamp existed
    /// (and directories a broker created on `Partition::open`) carry no
    /// identity. "Unknown" must adopt, never delete — anything else
    /// would wipe every live topic on the first reconcile after
    /// upgrade.
    #[test]
    fn unstamped_dir_is_adopted_not_deleted() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let fs = kaas_storage::fs::RealFs::new();
        let dir = seed_topic_dir(root, "legacy", "");

        reclaim_stale_incarnation(root, "legacy", &dir, "fresh-id", &fs).unwrap();

        assert!(
            dir.join("0").join("manifest.json").exists(),
            "an unstamped dir is adopted, never reclaimed"
        );
    }

    /// A CR whose status hasn't been written yet has no identity to
    /// compare against; that must be a no-op, not a reclaim.
    #[test]
    fn empty_topic_id_never_reclaims() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let fs = kaas_storage::fs::RealFs::new();
        let dir = seed_topic_dir(root, "no-id-yet", "some-id");

        reclaim_stale_incarnation(root, "no-id-yet", &dir, "", &fs).unwrap();

        assert!(dir.join("0").join("manifest.json").exists());
    }

    #[test]
    fn reclaim_on_a_missing_dir_is_ok() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = kaas_storage::fs::RealFs::new();
        let dir = tmp.path().join("never-existed");
        reclaim_stale_incarnation(tmp.path(), "never-existed", &dir, "id", &fs).unwrap();
    }

    #[test]
    fn stage_and_delete_missing_dir_is_ok() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(stage_and_delete_topic_dir(tmp.path(), "never-existed").is_ok());
    }

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
