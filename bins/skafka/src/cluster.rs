//! Phase 5 cluster + consumer-group bring-up.
//!
//! Wires together the [`Manager`], [`OffsetStore`], [`Coordinator`],
//! [`TakeoverDriver`], [`GroupTakeoverDriver`], and (in single-
//! broker dev mode) the [`AssignmentLoop`] that writes the
//! authoritative `assignment.json` the Coordinator watches.
//!
//! Three modes, picked at boot:
//!
//! - **In-memory** (`MemoryStorage`) — Manager + LocalGroupSource;
//!   no Coordinator wired. Produce/Fetch fall through to the
//!   "always lead" `LocalLeaseManager` path. Consumer-group APIs
//!   (`Manager`) work fully.
//! - **Single-broker disk** (`SKAFKA_DATA_DIR` set, `MY_POD_NAME`
//!   unset) — adds a Coordinator + AssignmentLoop. The loop writes
//!   a self-only `assignment.json` every recompute; the Coordinator
//!   watcher picks it up and stamps ownership. Manager
//!   hot-swaps to the Coordinator-backed source via gh #92.
//! - **Cluster** (`MY_POD_NAME` set) — same as single-broker disk
//!   from a wiring perspective, but the kube-bound bits
//!   (`ControllerWatch`, `LeaseElection`, peer `BrokerRegistry`,
//!   `TopicWatcher`) come online with follow-up #10. Today the
//!   binary boots successfully but degrades to LocalLeaseEpoch /
//!   LocalHeartbeat / a single-broker assignment until those
//!   wires land.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use sk_broker::{
    build_control_batch, ApplyOutcome, Broker, Coordinator, FenceWatcher, GroupTakeoverDriver,
    LocalHeartbeat, LocalLeaseEpoch, MarkerApplier, MarkerWatcher, ProducerEpochFencer,
    TakeoverDriver,
};
use sk_controller::{AssignmentLoop, AssignmentReason, LocalElection, StaticSources};
use sk_coordinator::{
    fence_log_dir, BrokerEndpoint, BrokerLookup, FenceLog, FnLookup, LocalGroupSource,
    LocalTxnSource, Manager, MarkerEntry, MarkerQueue, OffsetStore, TxnOffsetHook, TxnStateStore,
};
use sk_storage::StorageEngine;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use sk_broker::TopicRegistry;

/// Cadence of the txn-timeout reaper. Matches Apache Kafka's
/// `transaction.abort.timed.out.transaction.cleanup.interval.ms`
/// default.
const TXN_REAPER_INTERVAL: Duration = Duration::from_secs(10);

/// What was installed. Returned so `main.rs` can drop the handles
/// on shutdown.
pub struct ClusterRuntime {
    /// Held so the Arc stays alive at least as long as `main`
    /// runs; the Broker installs and owns its own clone.
    #[allow(dead_code)]
    pub manager: Arc<Manager>,
    /// Same — Broker holds an installed Arc; this is a convenient
    /// handle for the harness.
    #[allow(dead_code)]
    pub coordinator: Option<Arc<Coordinator>>,
    /// Phase 6 transactional-state store. `Some` whenever a data
    /// dir is configured (dev `MemoryStorage` paths use a tempdir so
    /// the gh #22 rejoin contract still works under unit tests).
    /// Broker holds an installed Arc; this is a convenient handle.
    #[allow(dead_code)]
    pub txn_state: Arc<TxnStateStore>,
    /// Phase 6 outbound fence log (gh #108). Held so the Arc lives
    /// at least as long as `main`.
    #[allow(dead_code)]
    pub fence_log: Arc<FenceLog>,
    /// gh #175 cross-broker marker queue. Held for the same reason.
    #[allow(dead_code)]
    pub marker_queue: MarkerQueue,
    /// Background tasks the runtime owns. Aborted on shutdown.
    pub tasks: Vec<tokio::task::JoinHandle<()>>,
}

/// Build the consumer-group + offsets Manager and (when a data dir
/// is configured) the Coordinator + AssignmentLoop. Installs both
/// on the [`Broker`] so handlers can read them.
pub fn install(
    broker: Arc<Broker>,
    topics: Arc<TopicRegistry>,
    engine: Arc<dyn StorageEngine>,
    data_dir: Option<std::path::PathBuf>,
    broker_id: i32,
    cluster_id: &str,
    cancel: CancellationToken,
) -> Result<ClusterRuntime> {
    let self_id = format!("skafka-{broker_id}");
    let offset_dir = data_dir
        .clone()
        .unwrap_or_else(|| std::path::PathBuf::from("/tmp/skafka-offsets-mem"));

    let offsets = Arc::new(OffsetStore::new(&offset_dir));
    let lookup: Arc<dyn BrokerLookup> = self_endpoint_lookup(&self_id, /* port */ 9092);
    let manager = Manager::new(
        self_id.clone(),
        offsets,
        lookup,
        LocalGroupSource::new(self_id.clone()),
    );
    // Phase 6 bootstrap: txn assignment source. Hot-swapped to the
    // Coordinator below when a data dir is configured.
    manager.set_txn_assignment_source(LocalTxnSource::new(self_id.clone()));
    broker.install_coord_manager(manager.clone());
    info!(
        broker_id = self_id.as_str(),
        cluster_id, "installed Manager (LocalGroupSource + LocalTxnSource bootstrap)"
    );

    // Phase 6 transactional-state store + fence log. We always
    // construct one — dev mode (no SKAFKA_DATA_DIR) gets a tempdir
    // so the gh #22 rejoin contract still works under unit tests
    // and single-binary smoke runs. Same shape as the offset store
    // fallback above.
    let cluster_dir = data_dir
        .clone()
        .map(|d| d.join("__cluster"))
        .unwrap_or_else(|| std::path::PathBuf::from("/tmp/skafka-cluster-mem"));
    std::fs::create_dir_all(&cluster_dir)?;
    let txn_state = Arc::new(TxnStateStore::open(&cluster_dir, 0)?);
    broker.install_txn_state(txn_state.clone());
    info!(
        slots = txn_state.num_slots(),
        cluster_dir = %cluster_dir.display(),
        "installed TxnStateStore",
    );

    // EndTxn / reaper → group-coordinator offset commit/discard.
    let hook: Arc<dyn TxnOffsetHook> = Arc::new(OffsetStoreHook {
        manager: manager.clone(),
    });
    txn_state.set_offset_hook(hook);

    let fence_dir = fence_log_dir(&cluster_dir);
    let fence_log = Arc::new(FenceLog::open(&fence_dir, &self_id)?);
    broker.install_fence_log(fence_log.clone());
    info!(path = %fence_log.path().display(), "opened FenceLog");

    let marker_queue = MarkerQueue::open(&cluster_dir)?;
    broker.install_marker_queue(marker_queue.clone());
    info!(
        inbox = %marker_queue.inbox(&self_id).display(),
        "opened MarkerQueue"
    );

    let mut tasks = Vec::new();

    // FenceWatcher: poll peer brokers' producer_fences/from-*.json
    // every 2s and dispatch new (pid, epoch) pairs into the local
    // storage engine's cross-partition fence walker (gh #170).
    let fencer: Arc<dyn ProducerEpochFencer> = Arc::new(EngineFencer {
        engine: engine.clone(),
    });
    let watcher = Arc::new(FenceWatcher::new(fence_dir, &self_id, fencer));
    let watcher_cancel = cancel.clone();
    let watcher_clone = watcher.clone();
    tasks.push(tokio::spawn(async move {
        watcher_clone.run(watcher_cancel).await;
    }));

    // gh #175 MarkerWatcher: poll the per-broker marker inbox every
    // 2 s and apply each commit/abort marker to the partitions we
    // currently lead.
    let applier: Arc<dyn MarkerApplier> = Arc::new(BrokerMarkerApplier {
        broker: broker.clone(),
    });
    let marker_watcher = Arc::new(MarkerWatcher::new(marker_queue.inbox(&self_id), applier));
    let mw_cancel = cancel.clone();
    let mw_clone = marker_watcher.clone();
    tasks.push(tokio::spawn(async move {
        mw_clone.run(mw_cancel).await;
    }));

    // Txn-timeout reaper (gh #28). Walks every slot every 10 s,
    // aborts Ongoing entries past their TransactionTimeoutMs.
    let reaper_cancel = cancel.clone();
    let reaper_store = txn_state.clone();
    tasks.push(tokio::spawn(async move {
        run_txn_reaper(reaper_store, reaper_cancel).await;
    }));
    let coordinator = match data_dir {
        None => {
            info!("dev mode (MemoryStorage) — Coordinator + AssignmentLoop skipped");
            None
        }
        Some(dir) => {
            // Coordinator watches <data_dir>/__cluster/assignment.json
            // via a 1 s poll. LocalLeaseEpoch / LocalHeartbeat keep
            // the kube-bound seams stubbed; follow-up #10 swaps in
            // ControllerWatch + HeartbeatClient.
            let coordinator = Coordinator::new(
                self_id.clone(),
                dir.clone(),
                Arc::new(LocalLeaseEpoch),
                Arc::new(LocalHeartbeat),
            );
            broker.install_coordinator(coordinator.clone());

            // Hot-swap the Manager's group + txn sources to the
            // assignment-aware Coordinator (gh #92, gh #91). Without
            // the txn swap, FindCoordinator(KeyType=transaction)
            // would keep returning self for every txnID — the
            // LocalTxnSource bootstrap — instead of routing
            // through hash(txnID) % numBrokers.
            manager.set_group_assignment_source(coordinator.clone());
            manager.set_txn_assignment_source(coordinator.clone());

            // Drivers fire on every assignment apply.
            let takeover = TakeoverDriver::new(engine.clone(), self_id.clone());
            coordinator.on_assignment_change(takeover.as_handler());
            let group_takeover = GroupTakeoverDriver::new(manager.clone(), self_id.clone());
            coordinator.on_assignment_change(group_takeover.as_handler());

            tasks.push(coordinator.spawn_watcher());
            info!(
                data_dir = %dir.display(),
                "Coordinator + takeover drivers wired"
            );

            // Single-broker AssignmentLoop drives the
            // assignment.json the Coordinator watches. Multi-broker
            // mode wires this to the elected controller broker only
            // (follow-up #10); for Phase 5 every broker writes its
            // own assignment.json under its data_dir, which is fine
            // because each broker only reads its own (the chart's
            // shared PVC layout is workstream H).
            if let Some(handle) = spawn_single_broker_assignment_loop(
                dir.clone(),
                self_id.clone(),
                topics.clone(),
                cancel.clone(),
            )? {
                tasks.push(handle);
            }

            Some(coordinator)
        }
    };

    Ok(ClusterRuntime {
        manager,
        coordinator,
        txn_state,
        fence_log,
        marker_queue,
        tasks,
    })
}

/// Bridges [`TxnOffsetHook`] (from the transactional coordinator)
/// to the consumer-group `Manager`'s `OffsetStore`. On `EndTxn`
/// commit, the txn coord fires the hook for each `(group, pid)`
/// that staged offsets via `TxnOffsetCommit`; the hook materialises
/// the pending entry into the durable offset map. On abort, it
/// discards the pending entry.
struct OffsetStoreHook {
    manager: Arc<Manager>,
}

impl TxnOffsetHook for OffsetStoreHook {
    fn on_end_txn(&self, group_id: &str, producer_id: i64, commit: bool) {
        if commit {
            if let Err(err) = self.manager.offsets.commit_pending(group_id, producer_id) {
                warn!(
                    group_id, producer_id, %err,
                    "txn offset hook: commit_pending failed; staged offsets remain pending"
                );
            }
        } else {
            self.manager.offsets.discard_pending(group_id, producer_id);
        }
    }
}

/// [`ProducerEpochFencer`] adapter that bridges the
/// [`FenceWatcher`]'s per-peer fence dispatch into the storage
/// engine's cross-partition `fence_producer_epoch` walker
/// (gh #170). Inbound peer fences are applied to every partition
/// this broker leads so a zombie batch from an old session is
/// rejected even on partitions the new session hasn't yet
/// touched (gh #30).
struct EngineFencer {
    engine: Arc<dyn StorageEngine>,
}

impl ProducerEpochFencer for EngineFencer {
    fn fence_producer_epoch(&self, pid: i64, epoch: i16) {
        self.engine.fence_producer_epoch(pid, epoch);
    }
}

/// [`MarkerApplier`] adapter that bridges the inbound marker queue
/// into the storage engine. For each `(topic, partition)` in the
/// entry that this broker currently leads, builds a control batch
/// via [`build_control_batch`] and appends it with `acks = -1`.
/// Partitions led by another broker are silently skipped — the
/// dispatcher should not have targeted us with them; logging
/// preserves the breadcrumb.
struct BrokerMarkerApplier {
    broker: Arc<Broker>,
}

#[async_trait::async_trait]
impl MarkerApplier for BrokerMarkerApplier {
    async fn apply(&self, entry: &MarkerEntry) -> ApplyOutcome {
        let batch = Bytes::from(build_control_batch(
            entry.producer_id,
            entry.producer_epoch,
            entry.commit,
            entry.coordinator_epoch,
        ));
        let coord = self.broker.coordinator();
        for topic in &entry.partitions {
            for &p in &topic.partitions {
                let owns = coord.as_ref().is_none_or(|c| c.owns(&topic.topic, p));
                if !owns {
                    tracing::warn!(
                        topic = %topic.topic,
                        partition = p,
                        pid = entry.producer_id,
                        "MarkerWatcher: queue entry targeted us but we don't lead this \
                         partition; assignment view may have drifted — marker dropped"
                    );
                    continue;
                }
                let epoch = coord
                    .as_ref()
                    .and_then(|c| c.current_epoch(&topic.topic, p))
                    .unwrap_or_else(|| self.broker.local_lease.current_epoch());
                let _ = self.broker.engine.create_partition(&topic.topic, p).await;
                if let Err(err) = self
                    .broker
                    .engine
                    .append(&topic.topic, p, epoch, -1, batch.clone())
                    .await
                {
                    tracing::warn!(
                        topic = %topic.topic,
                        partition = p,
                        %err,
                        "MarkerWatcher: control-batch append failed; keeping \
                         queue file for retry next tick"
                    );
                    return ApplyOutcome::Retry;
                }
            }
        }
        ApplyOutcome::Applied
    }
}

async fn run_txn_reaper(store: Arc<TxnStateStore>, cancel: CancellationToken) {
    let mut tick = tokio::time::interval(TXN_REAPER_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = cancel.cancelled() => return,
            _ = tick.tick() => {
                // `now_ms` is wall-clock millis. UNIX_EPOCH
                // conversion can't fail in practice (it would mean
                // the system clock is pre-1970).
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
                    .unwrap_or(0);
                let aborted = store.abort_overdue(now_ms);
                if !aborted.is_empty() {
                    info!(
                        count = aborted.len(),
                        "txn-timeout reaper aborted overdue Ongoing transactions"
                    );
                }
            }
        }
    }
}

/// Spawn an [`AssignmentLoop`] that runs in single-broker mode. The
/// loop ticks every 5 s and writes a fresh `assignment.json`
/// listing every known topic with this broker as leader. The
/// Coordinator's 1 s poll picks the file up and stamps ownership
/// on the broker.
fn spawn_single_broker_assignment_loop(
    data_dir: std::path::PathBuf,
    self_id: String,
    topics: Arc<TopicRegistry>,
    cancel: CancellationToken,
) -> Result<Option<tokio::task::JoinHandle<()>>> {
    let sources = Arc::new(StaticSources {
        topics: topic_specs_from_registry(&topics),
        brokers: vec![self_id.clone()],
        groups: Vec::new(),
    });
    let loop_handle = AssignmentLoop::new(
        data_dir.clone(),
        self_id.clone(),
        sources.clone(),
        sources.clone(),
    )
    .with_group_source(sources.clone());
    let election = LocalElection::new(self_id.clone());

    let task = tokio::spawn(async move {
        // Local election always wins immediately; the awaited
        // future returns the post-acquire epoch.
        use sk_controller::LeaseElection;
        let epoch = election.acquire().await;
        if let Err(err) = loop_handle.start(epoch).await {
            warn!(%err, "single-broker assignment loop: initial start failed");
            return;
        }
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(5));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tick.tick() => {
                    if let Err(err) = loop_handle
                        .update_assignment(AssignmentReason::InitialRecompute)
                        .await
                    {
                        warn!(%err, "single-broker assignment loop: update failed");
                    }
                }
            }
        }
    });
    Ok(Some(task))
}

fn topic_specs_from_registry(topics: &TopicRegistry) -> Vec<sk_controller::TopicSpec> {
    topics
        .all()
        .into_iter()
        .map(|m| sk_controller::TopicSpec {
            name: m.name,
            partition_count: m.partition_count,
        })
        .collect()
}

fn self_endpoint_lookup(self_id: &str, port: i32) -> Arc<dyn BrokerLookup> {
    let id = self_id.to_owned();
    Arc::new(FnLookup::new(move |req: &str| {
        if req == id {
            Some(BrokerEndpoint {
                node_id: 0,
                host: format!("{id}.local"),
                port,
            })
        } else {
            None
        }
    }))
}
