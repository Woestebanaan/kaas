//! Phase 5 §H — stale-controller-race port.
//!
//! Two simulated controllers race on the same `assignment.json`.
//! The losing controller writes with an older `controller_epoch`;
//! the Coordinator's epoch fence must reject that write so brokers
//! never see a stale view of cluster state.
//!
//! Kube-free port: instead of two real Lease-driven controllers, the
//! test drives two [`kaas_controller::AssignmentLoop`] instances at
//! distinct `controller_epoch` values. The Coordinator's
//! `LeaseEpochSource` reports the higher of the two, so the lower-
//! epoch loop's writes get fenced by [`Coordinator::apply_if_new`].

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use kaas_broker::{Coordinator, LeaseEpochSource, LocalHeartbeat};
use kaas_controller::{AssignmentLoop, AssignmentReason, StaticSources, TopicSpec};

/// `LeaseEpochSource` backed by an `AtomicI64` so the test can
/// flip the observed lease epoch mid-run.
#[derive(Debug, Default)]
struct AtomicEpoch(AtomicI64);

impl LeaseEpochSource for AtomicEpoch {
    fn current_epoch(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}

fn sources(self_id: &str) -> Arc<StaticSources> {
    Arc::new(StaticSources {
        topics: vec![TopicSpec {
            name: "t1".to_owned(),
            partition_count: 4,
        }],
        brokers: vec![self_id.to_owned()],
        groups: Vec::new(),
    })
}

fn build_loop(
    dir: std::path::PathBuf,
    self_id: &str,
    s: Arc<StaticSources>,
) -> Arc<AssignmentLoop<StaticSources, StaticSources, StaticSources>> {
    AssignmentLoop::new(dir, self_id.to_owned(), s.clone(), s.clone()).with_group_source(s)
}

#[tokio::test]
async fn stale_epoch_write_is_rejected_by_coordinator() {
    let tmp = tempfile::tempdir().unwrap();

    // The "current" controller — has the higher epoch (the one
    // the Lease would have transitioned to).
    let lease = Arc::new(AtomicEpoch(AtomicI64::new(2)));
    let coord = Coordinator::new(
        "kaas-0",
        tmp.path().to_path_buf(),
        lease.clone(),
        Arc::new(LocalHeartbeat),
    );

    // A real controller at epoch 2 writes.
    let real_sources = sources("kaas-0");
    let real = build_loop(tmp.path().to_path_buf(), "kaas-0", real_sources.clone());
    real.start(2).await.unwrap();
    assert!(coord.apply_if_new(), "first apply at epoch 2 accepted");
    assert!(coord.owns("t1", 0));

    // The stale ex-controller still thinks it owns the cluster at
    // epoch 1 and writes. Same directory, same file, lower epoch.
    let stale_sources = sources("kaas-9");
    let stale = build_loop(tmp.path().to_path_buf(), "kaas-9", stale_sources.clone());
    stale.start(1).await.unwrap();
    // It would have overwritten the file — Coordinator must
    // reject the read so brokers don't lose ownership.
    assert!(
        !coord.apply_if_new(),
        "stale-epoch write (epoch 1 < lease epoch 2) must be rejected"
    );
    // Snapshot still reflects the real controller.
    let snap = coord.snapshot().expect("snapshot present");
    assert_eq!(snap.controller, "kaas-0");
    assert_eq!(snap.controller_epoch, 2);
    // Ownership is unchanged.
    assert!(
        coord.owns("t1", 0),
        "Coordinator must preserve the real controller's ownership"
    );
}

#[tokio::test]
async fn higher_version_at_same_epoch_overrides_lower_version() {
    // Same-epoch writes follow the (version > last_applied)
    // contract — a stale-version write from the same controller
    // is also rejected.
    let tmp = tempfile::tempdir().unwrap();
    let lease = Arc::new(AtomicEpoch(AtomicI64::new(1)));
    let coord = Coordinator::new(
        "kaas-0",
        tmp.path().to_path_buf(),
        lease.clone(),
        Arc::new(LocalHeartbeat),
    );

    let s = sources("kaas-0");
    let loop_a = build_loop(tmp.path().to_path_buf(), "kaas-0", s.clone());
    loop_a.start(1).await.unwrap();
    assert!(coord.apply_if_new(), "v1 accepted");

    // The same loop bumps the version (recompute writes
    // version 2).
    let _ = loop_a
        .update_assignment(AssignmentReason::AdminRebalance)
        .await
        .unwrap();
    assert!(coord.apply_if_new(), "v2 accepted");

    // Re-applying without any new write is a no-op.
    assert!(
        !coord.apply_if_new(),
        "re-read of the same file must be a no-op"
    );
}

#[tokio::test]
async fn epoch_bump_unfences_the_new_controller() {
    // Controller failover: lease_transitions ticks from 1 → 2.
    // The new controller's writes (epoch 2) become the
    // authoritative view; the old controller's writes (epoch 1)
    // get fenced from there on.
    let tmp = tempfile::tempdir().unwrap();
    let lease = Arc::new(AtomicEpoch(AtomicI64::new(1)));
    let coord = Coordinator::new(
        "kaas-0",
        tmp.path().to_path_buf(),
        lease.clone(),
        Arc::new(LocalHeartbeat),
    );

    // Old controller at epoch 1 publishes.
    let old_sources = sources("kaas-0");
    let old = build_loop(tmp.path().to_path_buf(), "kaas-0", old_sources.clone());
    old.start(1).await.unwrap();
    assert!(coord.apply_if_new());
    let snap = coord.snapshot().expect("snapshot present");
    assert_eq!(snap.controller_epoch, 1);

    // Lease transitions: new controller wins.
    lease.0.store(2, Ordering::Relaxed);

    // New controller publishes at epoch 2.
    let new_sources = sources("kaas-1");
    let new = build_loop(tmp.path().to_path_buf(), "kaas-1", new_sources.clone());
    new.start(2).await.unwrap();
    assert!(
        coord.apply_if_new(),
        "post-bump write at epoch 2 must be accepted"
    );
    let snap = coord.snapshot().expect("snapshot present");
    assert_eq!(snap.controller, "kaas-1");
    assert_eq!(snap.controller_epoch, 2);

    // From this point forward, an old-epoch write (epoch 1) must
    // be fenced.
    let zombie = build_loop(tmp.path().to_path_buf(), "kaas-0", old_sources.clone());
    zombie.start(1).await.unwrap();
    assert!(
        !coord.apply_if_new(),
        "ex-controller's write must be fenced after lease transition"
    );
}
