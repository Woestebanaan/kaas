//! `TakeoverDriver` — reacts to assignment changes by driving the
//! storage engine through `take_over` / `relinquish`.
//!
//! Registered as an
//! [`AssignmentChangeHandler`] on the [`crate::coordinator::Coordinator`];
//! runs synchronously on the watcher task. The driver does *not*
//! perform recovery itself — it dispatches per-partition
//! `take_over(topic, partition, epoch)` / `relinquish(topic,
//! partition)` calls into the [`StorageEngine`].
//!
//! [`AssignmentChangeHandler`]: crate::assignment::AssignmentChangeHandler

use std::collections::HashMap;
use std::sync::Arc;

use sk_storage::StorageEngine;
use tracing::warn;

use crate::assignment::Assignment;
use crate::coordinator::partition_key;

/// Per-`(topic, partition)` reference carried across the prev→next
/// diff. Internal to the driver.
#[derive(Debug, Clone)]
struct OwnedRef {
    topic: String,
    partition: i32,
    epoch: u32,
}

/// Bound to a [`StorageEngine`] + the broker's own ID; the driver
/// itself is stateless. The Coordinator owns one `Arc<TakeoverDriver>`
/// and registers it via `on_assignment_change`.
pub struct TakeoverDriver {
    engine: Arc<dyn StorageEngine>,
    broker_id: String,
}

impl std::fmt::Debug for TakeoverDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TakeoverDriver")
            .field("broker_id", &self.broker_id)
            .finish_non_exhaustive()
    }
}

impl TakeoverDriver {
    pub fn new(engine: Arc<dyn StorageEngine>, broker_id: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            engine,
            broker_id: broker_id.into(),
        })
    }

    /// Build the [`AssignmentChangeHandler`] closure for this driver.
    /// Register it with `coordinator.on_assignment_change(...)` at
    /// boot.
    pub fn as_handler(self: &Arc<Self>) -> crate::assignment::AssignmentChangeHandler {
        let me = self.clone();
        Arc::new(move |prev, next| me.on_change(prev, next))
    }

    /// Diff `prev` vs `next.partitions` and dispatch
    /// take-over / relinquish calls. The storage methods are async;
    /// each is spawned as a `tokio::task` so the watcher can move on
    /// to the next tick — Apache's contract is "the next heartbeat
    /// reports RECOVERING / ERROR if recovery is slow", which we
    /// preserve by surfacing errors via the storage engine's
    /// per-partition state rather than blocking here.
    pub fn on_change(&self, prev: Option<&Assignment>, next: &Assignment) {
        let prev_owned = self.owned_by(prev);
        let next_owned = self.owned_by(Some(next));

        for (k, ne) in &next_owned {
            let needs_takeover = match prev_owned.get(k) {
                Some(pe) => pe.epoch != ne.epoch,
                None => true,
            };
            if !needs_takeover {
                continue;
            }
            let engine = self.engine.clone();
            let topic = ne.topic.clone();
            let partition = ne.partition;
            let epoch = ne.epoch;
            tokio::spawn(async move {
                if let Err(e) = engine.take_over(&topic, partition, epoch).await {
                    warn!(
                        topic = topic.as_str(),
                        partition,
                        epoch,
                        %e,
                        "take_over failed"
                    );
                }
            });
        }

        for (k, pe) in &prev_owned {
            if next_owned.contains_key(k) {
                continue;
            }
            let engine = self.engine.clone();
            let topic = pe.topic.clone();
            let partition = pe.partition;
            tokio::spawn(async move {
                if let Err(e) = engine.relinquish(&topic, partition).await {
                    warn!(
                        topic = topic.as_str(),
                        partition,
                        %e,
                        "relinquish failed"
                    );
                }
            });
        }
    }

    fn owned_by(&self, a: Option<&Assignment>) -> HashMap<String, OwnedRef> {
        let mut out = HashMap::new();
        let a = match a {
            None => return out,
            Some(a) => a,
        };
        for p in &a.partitions {
            if p.broker != self.broker_id {
                continue;
            }
            out.insert(
                partition_key(&p.topic, p.partition),
                OwnedRef {
                    topic: p.topic.clone(),
                    partition: p.partition,
                    epoch: p.epoch,
                },
            );
        }
        out
    }
}
