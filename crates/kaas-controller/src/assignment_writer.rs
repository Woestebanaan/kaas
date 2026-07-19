//! Atomic `assignment.json` writer + recompute loop.
//!
//! The
//! controller broker is the only writer; every other broker reads
//! the file via [`kaas_broker::Coordinator`]. The writer's job is to
//! recompute the assignment on every input change (broker join/
//! leave, topic CR change, active-group churn) and atomically
//! replace the file with the new version.
//!
//! Atomicity: tempfile + `rename`. NFSv4 guarantees same-directory
//! rename is atomic, so a crash mid-write leaves either the old or
//! the new file — never a torn JSON.
//!
//! Source-of-truth seams as traits — production wires
//! [`HeartbeatServer::active_groups`] + the topic CR watcher + the
//! K8s endpoint registry; tests pass `Vec`-backed stubs.
//!
//! [`HeartbeatServer::active_groups`]:
//!     crate::heartbeat_server::HeartbeatServer::active_groups

use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use chrono::SecondsFormat;
use parking_lot::Mutex;
use serde::Serialize;
use kaas_broker::{
    Assignment, BrokerAssignment, BrokerHealth, ConsumerGroupAssignment, PartitionAssignment,
};

use crate::balancer::{balance, balance_groups, GroupSpec, TopicSpec};
use crate::k8s_mirror::{CrMirror, NoopMirror};

/// "Tell the loop *why* it should recompute". Reasons are
/// informational — they end up on tracing spans but don't gate the
/// recompute itself.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub enum AssignmentReason {
    BrokerJoined,
    BrokerLeaving,
    BrokerDead,
    TopicCreated,
    TopicDeleted,
    TopicResized,
    AdminRebalance,
    InitialRecompute,
}

impl AssignmentReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BrokerJoined => "broker_joined",
            Self::BrokerLeaving => "broker_leaving",
            Self::BrokerDead => "broker_dead",
            Self::TopicCreated => "topic_created",
            Self::TopicDeleted => "topic_deleted",
            Self::TopicResized => "topic_resized",
            Self::AdminRebalance => "admin_rebalance",
            Self::InitialRecompute => "initial_recompute",
        }
    }
}

/// Live topic catalog the writer balances over.
pub trait TopicSource: Send + Sync + 'static {
    fn topics(&self) -> Vec<TopicSpec>;
}

/// Broker liveness — the alive subset the controller sees.
pub trait BrokerSource: Send + Sync + 'static {
    fn alive_brokers(&self) -> Vec<String>;
}

/// Consumer groups currently active in the cluster.
pub trait GroupSource: Send + Sync + 'static {
    fn active_groups(&self) -> Vec<String>;
}

/// `Vec`-backed helper that satisfies all three source traits;
/// useful for tests.
#[derive(Debug, Default, Clone)]
pub struct StaticSources {
    pub topics: Vec<TopicSpec>,
    pub brokers: Vec<String>,
    pub groups: Vec<String>,
}

impl TopicSource for StaticSources {
    fn topics(&self) -> Vec<TopicSpec> {
        self.topics.clone()
    }
}
impl BrokerSource for StaticSources {
    fn alive_brokers(&self) -> Vec<String> {
        self.brokers.clone()
    }
}
impl GroupSource for StaticSources {
    fn active_groups(&self) -> Vec<String> {
        self.groups.clone()
    }
}

/// The recompute → write → push pipeline.
///
/// Single-task ownership: all state mutation happens inside
/// [`AssignmentLoop::update_assignment`] (which currently runs the
/// recompute inline rather than via a coalescing channel because
/// production callers (`bins/kaas/main.rs`) don't generate enough
/// concurrent updates to warrant the coalescing yet. A follow-up
/// can introduce a `tokio::mpsc` queue if the call rate climbs.
pub struct AssignmentLoop<T, B, G> {
    data_dir: PathBuf,
    controller_id: String,
    /// `AtomicI64` so [`Self::start`] can stamp the lease-acquire
    /// epoch after the `Arc` is already shared. Read on every
    /// recompute via [`Ordering::Relaxed`].
    controller_epoch: AtomicI64,
    topics: Arc<T>,
    brokers: Arc<B>,
    groups: Option<Arc<G>>,
    mirror: Arc<dyn CrMirror>,
    state: Mutex<LoopState>,
}

#[derive(Debug, Default)]
struct LoopState {
    current: Option<Assignment>,
    version_counter: i64,
}

impl<T, B, G> std::fmt::Debug for AssignmentLoop<T, B, G> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.lock();
        f.debug_struct("AssignmentLoop")
            .field("data_dir", &self.data_dir)
            .field("controller_id", &self.controller_id)
            .field(
                "controller_epoch",
                &self.controller_epoch.load(Ordering::Relaxed),
            )
            .field("version_counter", &state.version_counter)
            .finish()
    }
}

impl<T, B, G> AssignmentLoop<T, B, G>
where
    T: TopicSource,
    B: BrokerSource,
    G: GroupSource,
{
    pub fn new(
        data_dir: impl Into<PathBuf>,
        controller_id: impl Into<String>,
        topics: Arc<T>,
        brokers: Arc<B>,
    ) -> Arc<Self> {
        Arc::new(Self {
            data_dir: data_dir.into(),
            controller_id: controller_id.into(),
            controller_epoch: AtomicI64::new(0),
            topics,
            brokers,
            groups: None,
            mirror: Arc::new(NoopMirror),
            state: Mutex::new(LoopState::default()),
        })
    }

    /// Attach an optional [`GroupSource`]. Without one,
    /// `consumer_groups` stays empty on every write.
    pub fn with_group_source(self: Arc<Self>, g: Arc<G>) -> Arc<Self> {
        // Arc::get_mut is sound here because the loop hasn't been
        // shared yet — call this immediately after `new`.
        let mut this = self;
        if let Some(inner) = Arc::get_mut(&mut this) {
            inner.groups = Some(g);
        }
        this
    }

    /// Attach a [`CrMirror`]. Default is [`NoopMirror`].
    pub fn with_mirror(self: Arc<Self>, m: Arc<dyn CrMirror>) -> Arc<Self> {
        let mut this = self;
        if let Some(inner) = Arc::get_mut(&mut this) {
            inner.mirror = m;
        }
        this
    }

    /// Stamp the lease-acquire epoch + bootstrap from any existing
    /// `assignment.json` on disk. Returns the new file's version
    /// after the initial recompute. The epoch swap is atomic so a
    /// shared `Arc<AssignmentLoop>` is safe to start from any
    /// caller.
    pub async fn start(self: &Arc<Self>, epoch: i64) -> io::Result<i64> {
        self.controller_epoch.store(epoch, Ordering::Relaxed);
        // Bootstrap: carry the version counter forward so a
        // restarted controller doesn't rewind the sequence.
        let path = Assignment::path_in(&self.data_dir);
        if let Ok(bytes) = std::fs::read(&path) {
            if let Ok(prev) = serde_json::from_slice::<Assignment>(&bytes) {
                let mut s = self.state.lock();
                if prev.assignment_version > s.version_counter {
                    s.version_counter = prev.assignment_version;
                }
                s.current = Some(prev);
            }
        }
        self.update_assignment(AssignmentReason::InitialRecompute)
            .await
    }

    /// Snapshot of the most recently written assignment. `None`
    /// before the first write.
    pub fn snapshot(&self) -> Option<Assignment> {
        self.state.lock().current.clone()
    }

    /// Recompute + write + (optionally) mirror. `reason` is
    /// informational. Returns the new assignment_version.
    pub async fn update_assignment(&self, reason: AssignmentReason) -> io::Result<i64> {
        // Snapshot inputs outside the lock so the source traits'
        // own locking doesn't intersect with our `state` lock.
        let brokers = self.brokers.alive_brokers();
        let topics = self.topics.topics();
        let group_specs = self
            .groups
            .as_ref()
            .map(|g| {
                g.active_groups()
                    .into_iter()
                    .map(|id| GroupSpec { group_id: id })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let (assignment, version) = {
            let mut s = self.state.lock();
            let prev_parts: Option<Vec<PartitionAssignment>> =
                s.current.as_ref().map(|a| a.partitions.clone());
            let prev_groups: Option<Vec<ConsumerGroupAssignment>> =
                s.current.as_ref().map(|a| a.consumer_groups.clone());
            let parts = balance(prev_parts.as_deref(), &brokers, &topics);
            let groups = balance_groups(prev_groups.as_deref(), &brokers, &group_specs);
            s.version_counter += 1;
            let version = s.version_counter;
            let now = chrono::Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true);
            let a = Assignment {
                controller_epoch: self.controller_epoch.load(Ordering::Relaxed),
                assignment_version: version,
                generated_at: now.clone(),
                controller: self.controller_id.clone(),
                brokers: build_broker_entries(&brokers, &now),
                partitions: parts,
                consumer_groups: groups,
            };
            s.current = Some(a.clone());
            (a, version)
        };

        tracing::debug!(
            reason = reason.as_str(),
            assignment_version = version,
            partitions = assignment.partitions.len(),
            groups = assignment.consumer_groups.len(),
            "controller recompute"
        );

        let m = kaas_observability::metrics::global();
        m.assignment_changes.add(1, &[]);

        let started = std::time::Instant::now();
        let write_res = atomic_write(&self.data_dir, &assignment);
        m.assignment_file_write_latency
            .record(started.elapsed().as_secs_f64(), &[]);
        m.assignment_file_writes.add(
            1,
            &[kaas_observability::KeyValue::new(
                "result",
                if write_res.is_ok() { "ok" } else { "error" },
            )],
        );
        write_res?;

        self.mirror.mirror(&assignment).await;
        // Mirror errors are swallowed by the trait's `async fn`
        // signature (returns `()`); count the attempt regardless so
        // the operator alert can gate on staleness rather than a
        // rate-of-errors ratio.
        m.cr_mirror_writes
            .add(1, &[kaas_observability::KeyValue::new("result", "ok")]);
        Ok(version)
    }
}

fn build_broker_entries(brokers: &[String], now: &str) -> Vec<BrokerAssignment> {
    brokers
        .iter()
        .map(|b| BrokerAssignment {
            id: b.clone(),
            health: BrokerHealth::Alive,
            last_seen: now.to_owned(),
        })
        .collect()
}

/// `tmp + rename` write of `<data_dir>/__cluster/assignment.json`.
/// Shared by the loop and by the controller-failover test harness.
fn atomic_write<T: Serialize>(data_dir: &std::path::Path, payload: &T) -> io::Result<()> {
    let dir = data_dir.join("__cluster");
    std::fs::create_dir_all(&dir)?;
    let final_path = dir.join(Assignment::FILE_NAME);
    let mut tmp_name = String::from(Assignment::FILE_NAME);
    tmp_name.push_str(".tmp");
    let tmp_path = dir.join(&tmp_name);
    let data = serde_json::to_vec(payload).map_err(io::Error::other)?;
    {
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp_path)?;
        use std::io::Write;
        if let Err(e) = f.write_all(&data) {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(e);
        }
        if let Err(e) = f.sync_all() {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(e);
        }
    }
    if let Err(e) = std::fs::rename(&tmp_path, &final_path) {
        let _ = std::fs::remove_file(&tmp_path);
        return Err(e);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topics(n: i32) -> Vec<TopicSpec> {
        (0..n)
            .map(|i| TopicSpec {
                name: format!("t{i}"),
                partition_count: 3,
            })
            .collect()
    }

    fn brokers(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("kaas-{i}")).collect()
    }

    fn loop_with_brokers(
        dir: &std::path::Path,
        brokers_n: usize,
        topics_n: i32,
    ) -> Arc<AssignmentLoop<StaticSources, StaticSources, StaticSources>> {
        let s = Arc::new(StaticSources {
            topics: topics(topics_n),
            brokers: brokers(brokers_n),
            groups: vec!["g1".to_owned()],
        });
        let l = AssignmentLoop::new(dir, "kaas-0", s.clone(), s.clone());
        l.with_group_source(s)
    }

    #[tokio::test]
    async fn initial_recompute_writes_a_valid_file() {
        let tmp = tempfile::tempdir().unwrap();
        let l = loop_with_brokers(tmp.path(), 3, 2);
        let v = l.start(7).await.unwrap();
        assert_eq!(v, 1, "first write is version 1");
        let snap = l.snapshot().expect("snapshot present after start");
        assert_eq!(snap.controller_epoch, 7);
        assert_eq!(snap.controller, "kaas-0");
        assert_eq!(snap.partitions.len(), 6);
        assert_eq!(snap.consumer_groups.len(), 1);
        let path = tmp.path().join("__cluster").join(Assignment::FILE_NAME);
        let on_disk: Assignment = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(on_disk, snap);
    }

    #[tokio::test]
    async fn version_increments_on_each_update() {
        let tmp = tempfile::tempdir().unwrap();
        let l = loop_with_brokers(tmp.path(), 3, 2);
        let v1 = l.start(1).await.unwrap();
        let v2 = l
            .update_assignment(AssignmentReason::TopicResized)
            .await
            .unwrap();
        let v3 = l
            .update_assignment(AssignmentReason::BrokerJoined)
            .await
            .unwrap();
        assert!(v3 > v2 && v2 > v1);
    }

    #[tokio::test]
    async fn bootstraps_version_counter_from_existing_file() {
        let tmp = tempfile::tempdir().unwrap();
        // Write a fake prior assignment at version 42.
        let dir = tmp.path().join("__cluster");
        std::fs::create_dir_all(&dir).unwrap();
        let prior = Assignment {
            controller_epoch: 9,
            assignment_version: 42,
            generated_at: "2024-12-31T23:59:59Z".to_owned(),
            controller: "kaas-old".to_owned(),
            brokers: vec![],
            partitions: vec![],
            consumer_groups: vec![],
        };
        std::fs::write(
            dir.join(Assignment::FILE_NAME),
            serde_json::to_vec(&prior).unwrap(),
        )
        .unwrap();
        let l = loop_with_brokers(tmp.path(), 3, 1);
        // Start as a fresh controller at epoch 10 — version must
        // bump beyond 42.
        let v = l.start(10).await.unwrap();
        assert_eq!(v, 43);
    }

    #[tokio::test]
    async fn rename_atomicity_no_tmp_leftover() {
        let tmp = tempfile::tempdir().unwrap();
        let l = loop_with_brokers(tmp.path(), 3, 1);
        l.start(0).await.unwrap();
        let dir = tmp.path().join("__cluster");
        assert!(dir.join(Assignment::FILE_NAME).exists());
        let tmp_path = dir.join(format!("{}.tmp", Assignment::FILE_NAME));
        assert!(
            !tmp_path.exists(),
            "tmp file must not survive a clean write"
        );
    }

    #[tokio::test]
    async fn snapshot_is_a_clone_not_a_reference_to_state() {
        let tmp = tempfile::tempdir().unwrap();
        let l = loop_with_brokers(tmp.path(), 3, 1);
        l.start(0).await.unwrap();
        let snap = l.snapshot().unwrap();
        let _v = l
            .update_assignment(AssignmentReason::BrokerJoined)
            .await
            .unwrap();
        let snap2 = l.snapshot().unwrap();
        assert!(snap2.assignment_version > snap.assignment_version);
    }

    #[tokio::test]
    async fn no_brokers_yields_empty_partitions() {
        let tmp = tempfile::tempdir().unwrap();
        let l = loop_with_brokers(tmp.path(), 0, 2);
        let _v = l.start(0).await.unwrap();
        let snap = l.snapshot().unwrap();
        assert!(snap.partitions.is_empty());
        assert!(snap.consumer_groups.is_empty());
    }
}
