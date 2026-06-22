//! Broker — the runtime context every handler reads from.
//!
//! Phase 3 shape: owns the storage engine, the in-memory topic
//! registry, a `LocalLeaseManager`, and a monotonic producer-id
//! counter. Phase 5 adds the `Coordinator`; Phase 6 adds
//! `TxnStateStore`.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use sk_storage::StorageEngine;

use crate::local_lease::LocalLeaseManager;
use crate::topic_registry::TopicRegistry;

pub struct Broker {
    pub engine: Arc<dyn StorageEngine>,
    pub topics: Arc<TopicRegistry>,
    pub local_lease: LocalLeaseManager,
    pub cluster_id: String,
    pub broker_id: i32,
    producer_id_counter: AtomicI64,
}

impl std::fmt::Debug for Broker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Broker")
            .field("cluster_id", &self.cluster_id)
            .field("broker_id", &self.broker_id)
            .field("topics", &self.topics.len())
            .field(
                "producer_id_counter",
                &self.producer_id_counter.load(Ordering::Relaxed),
            )
            .finish()
    }
}

impl Broker {
    pub fn new(
        engine: Arc<dyn StorageEngine>,
        topics: Arc<TopicRegistry>,
        cluster_id: impl Into<String>,
        broker_id: i32,
    ) -> Self {
        Self {
            engine,
            topics,
            local_lease: LocalLeaseManager,
            cluster_id: cluster_id.into(),
            broker_id,
            // Start at 1 so 0 stays available as an "unset" sentinel
            // for clients that read uninitialised pid.
            producer_id_counter: AtomicI64::new(1),
        }
    }

    /// Hand out the next non-transactional producer id. Monotonic,
    /// resets to 1 on broker restart — same behaviour as Apache for
    /// non-transactional producers (transactional ids are persisted
    /// in Phase 6).
    pub fn next_producer_id(&self) -> i64 {
        self.producer_id_counter.fetch_add(1, Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sk_storage::MemoryStorage;

    fn test_broker() -> Broker {
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        Broker::new(engine, Arc::new(TopicRegistry::new()), "skafka-test", 0)
    }

    #[test]
    fn producer_ids_are_monotonic() {
        let b = test_broker();
        let a = b.next_producer_id();
        let c = b.next_producer_id();
        assert!(c > a);
    }

    #[test]
    fn first_producer_id_is_one() {
        let b = test_broker();
        assert_eq!(b.next_producer_id(), 1);
    }
}
