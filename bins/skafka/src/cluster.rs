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

use anyhow::Result;
use sk_broker::{
    Broker, Coordinator, GroupTakeoverDriver, LocalHeartbeat, LocalLeaseEpoch, TakeoverDriver,
};
use sk_controller::{AssignmentLoop, AssignmentReason, LocalElection, StaticSources};
use sk_coordinator::{
    BrokerEndpoint, BrokerLookup, FnLookup, LocalGroupSource, Manager, OffsetStore,
};
use sk_storage::StorageEngine;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use sk_broker::TopicRegistry;

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
    broker.install_coord_manager(manager.clone());
    info!(
        broker_id = self_id.as_str(),
        cluster_id, "installed Manager (LocalGroupSource bootstrap)"
    );

    let mut tasks = Vec::new();
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

            // Hot-swap the Manager's group source to the
            // assignment-aware Coordinator (gh #92).
            manager.set_group_assignment_source(coordinator.clone());

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
        tasks,
    })
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
