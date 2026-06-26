//! Phase 6 §G cross-broker producer-epoch fence broadcast.
//!
//! End-to-end check that an epoch bump emitted by broker A reaches
//! broker B's [`FenceWatcher`] dispatch path via the shared
//! `producer_fences/` directory on the data dir. Unit tests cover
//! each side independently — [`FenceLog::append`] (sk-coordinator)
//! and [`FenceWatcher::tick`] (sk-broker). This file wires them
//! together against one shared tempdir to confirm the on-disk
//! shape, file-naming convention, and self-skip filter match across
//! the crate boundary.
//!
//! The wired [`ProducerEpochFencer`] is a capturing fake — the
//! production storage-engine adapter is open follow-up (the
//! `StorageEngine` trait still lacks a cross-partition
//! `fence_producer_epoch` walker).

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::sync::Arc;

use sk_broker::{FenceWatcher, ProducerEpochFencer};
use sk_coordinator::{fence_log_dir, FenceLog};

#[derive(Default)]
struct Capturing {
    calls: parking_lot::Mutex<Vec<(i64, i16)>>,
}

impl Capturing {
    fn drain(&self) -> Vec<(i64, i16)> {
        let mut v = std::mem::take(&mut *self.calls.lock());
        v.sort_unstable();
        v
    }
}

impl ProducerEpochFencer for Capturing {
    fn fence_producer_epoch(&self, pid: i64, epoch: i16) {
        self.calls.lock().push((pid, epoch));
    }
}

fn build_watcher(
    cluster_dir: &std::path::Path,
    self_id: &str,
) -> (Arc<FenceWatcher>, Arc<Capturing>) {
    let fenced = Arc::new(Capturing::default());
    let watcher = Arc::new(FenceWatcher::new(
        fence_log_dir(cluster_dir),
        self_id,
        fenced.clone(),
    ));
    (watcher, fenced)
}

/// Two brokers in the same cluster dir. Broker A writes a fence;
/// broker B's watcher.tick() picks it up. The reverse direction
/// also has to hold so InitProducerId on B can later fence A.
#[test]
fn two_brokers_share_fences_through_the_pvc() {
    let tmp = tempfile::tempdir().unwrap();
    let cluster = tmp.path();
    let log_a = FenceLog::open(&fence_log_dir(cluster), "skafka-a").unwrap();
    let log_b = FenceLog::open(&fence_log_dir(cluster), "skafka-b").unwrap();

    let (watcher_a, fenced_a) = build_watcher(cluster, "skafka-a");
    let (watcher_b, fenced_b) = build_watcher(cluster, "skafka-b");

    // A bumps PID 42 → epoch 3. B should observe.
    log_a.append(42, 3).unwrap();
    watcher_b.tick();
    assert_eq!(fenced_b.drain(), vec![(42, 3)]);
    // ...and not loop back to A — A's watcher skips its own file.
    watcher_a.tick();
    assert!(fenced_a.drain().is_empty(), "self-fence must not feed back");

    // B bumps a different PID. A picks it up; B doesn't double-fire
    // (already applied in-process before append, no need to re-read).
    log_b.append(99, 1).unwrap();
    watcher_a.tick();
    assert_eq!(fenced_a.drain(), vec![(99, 1)]);
    watcher_b.tick();
    assert!(
        fenced_b.drain().is_empty(),
        "B should not re-fire its own outbound entries"
    );
}

/// A second tick after a no-op append doesn't re-fire — the
/// per-peer applied cache catches the duplicate.
#[test]
fn duplicate_appends_dont_refire() {
    let tmp = tempfile::tempdir().unwrap();
    let cluster = tmp.path();
    let log_a = FenceLog::open(&fence_log_dir(cluster), "skafka-a").unwrap();
    let (watcher_b, fenced_b) = build_watcher(cluster, "skafka-b");

    log_a.append(42, 3).unwrap();
    watcher_b.tick();
    log_a.append(42, 3).unwrap(); // idempotent on the writer side
    log_a.append(42, 2).unwrap(); // older epoch — also no-op
    watcher_b.tick();
    assert_eq!(fenced_b.drain(), vec![(42, 3)]);
}

/// Bumping the epoch later does fire — the watcher's cache only
/// suppresses entries `<=` the highest already applied.
#[test]
fn higher_epoch_after_first_apply_fires_again() {
    let tmp = tempfile::tempdir().unwrap();
    let cluster = tmp.path();
    let log_a = FenceLog::open(&fence_log_dir(cluster), "skafka-a").unwrap();
    let (watcher_b, fenced_b) = build_watcher(cluster, "skafka-b");

    log_a.append(42, 3).unwrap();
    watcher_b.tick();
    log_a.append(42, 5).unwrap();
    watcher_b.tick();
    assert_eq!(fenced_b.drain(), vec![(42, 3), (42, 5)]);
}

/// Two peers, multiple PIDs each, one tick. Order between PIDs is
/// HashMap-iteration-dependent so we assert on the sorted set.
#[test]
fn multiple_peers_multiple_pids_one_tick() {
    let tmp = tempfile::tempdir().unwrap();
    let cluster = tmp.path();
    let log_a = FenceLog::open(&fence_log_dir(cluster), "skafka-a").unwrap();
    let log_c = FenceLog::open(&fence_log_dir(cluster), "skafka-c").unwrap();
    let (watcher_b, fenced_b) = build_watcher(cluster, "skafka-b");

    log_a.append(1, 7).unwrap();
    log_a.append(2, 1).unwrap();
    log_c.append(99, 2).unwrap();
    log_c.append(100, 4).unwrap();

    watcher_b.tick();
    assert_eq!(fenced_b.drain(), vec![(1, 7), (2, 1), (99, 2), (100, 4)]);
}

/// FenceLog snapshot mirrors the on-disk JSON shape that
/// FenceWatcher will read on the other broker. Sanity check that
/// the two surfaces agree without going through `tick()`.
#[test]
fn snapshot_round_trips_to_peer_watcher() {
    let tmp = tempfile::tempdir().unwrap();
    let cluster = tmp.path();
    let log_a = FenceLog::open(&fence_log_dir(cluster), "skafka-a").unwrap();
    log_a.append(42, 3).unwrap();
    log_a.append(99, 7).unwrap();
    let snap = log_a.snapshot();
    let expected: HashMap<i64, i16> = [(42, 3), (99, 7)].into_iter().collect();
    assert_eq!(snap, expected);

    // And a peer reads exactly that set off disk.
    let (watcher_b, fenced_b) = build_watcher(cluster, "skafka-b");
    watcher_b.tick();
    let got: HashMap<i64, i16> = fenced_b.drain().into_iter().collect();
    assert_eq!(got, expected);
}
