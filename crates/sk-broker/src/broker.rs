//! Broker — the runtime context every handler reads from.
//!
//! Phase 3 shape: owns the storage engine, the in-memory topic
//! registry, a `LocalLeaseManager`, and a monotonic producer-id
//! counter. Phase 5 adds hot-swappable [`Coordinator`] +
//! [`Manager`] slots — `None` in dev mode (Phase-3/4 tests) so
//! handlers gracefully degrade to local-lease ownership / consumer-
//! group `NOT_COORDINATOR` retry path. `bins/skafka/main.rs` populates
//! them at boot via [`Broker::install_coord_manager`] /
//! [`Broker::install_coordinator`]. Phase 6 adds `TxnStateStore`.
//!
//! [`Coordinator`]: crate::coordinator::Coordinator
//! [`Manager`]: sk_coordinator::Manager

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use sk_auth::{AllowAllAuthorizer, Authorizer, NoQuotaChecker, QuotaChecker};
use sk_coordinator::Manager;
use sk_storage::StorageEngine;

use crate::coordinator::Coordinator;
use crate::local_lease::LocalLeaseManager;
use crate::topic_registry::TopicRegistry;

pub struct Broker {
    pub engine: Arc<dyn StorageEngine>,
    pub topics: Arc<TopicRegistry>,
    pub local_lease: LocalLeaseManager,
    pub cluster_id: String,
    pub broker_id: i32,
    /// Broker identity string (e.g. `"skafka-0"`). Distinct from
    /// `broker_id: i32` which is the wire-level node id; the
    /// coordinator + assignment.json use the StatefulSet pod-name
    /// shape.
    pub self_id: String,
    /// Cluster-wide authorization decision surface. `AllowAllAuthorizer`
    /// is the default when no `authorization` config is set
    /// (Strimzi-compat semantic).
    pub authorizer: Arc<dyn Authorizer>,
    /// Cluster-wide quota check. `NoQuotaChecker` is the default for
    /// anonymous-only brokers.
    pub quotas: Arc<dyn QuotaChecker>,
    /// Consumer-group + offsets coordinator. `None` until the
    /// production wiring in `bins/skafka/main.rs` installs one;
    /// handlers that read this fall back to `NOT_COORDINATOR` (16)
    /// so a client retries via FindCoordinator. Phase-3/4 tests
    /// leave it `None` and exercise only the produce/fetch surface.
    coord_manager: RwLock<Option<Arc<Manager>>>,
    /// Broker-side assignment watcher + ownership lookup + group/txn
    /// source. `None` in dev mode (`Broker::new`); production
    /// installs at boot. When `Some`, produce / fetch ownership goes
    /// through this; when `None`, the local-lease "always lead" path
    /// stays in effect — the gh #92 fallback contract.
    coordinator: RwLock<Option<Arc<Coordinator>>>,
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
    /// Build a broker with the default open-everything authorization
    /// surface (`AllowAllAuthorizer` + `NoQuotaChecker`). Tests and
    /// dev mode call this; `bins/skafka/main.rs` uses
    /// [`Broker::with_auth`] to swap in real implementations.
    pub fn new(
        engine: Arc<dyn StorageEngine>,
        topics: Arc<TopicRegistry>,
        cluster_id: impl Into<String>,
        broker_id: i32,
    ) -> Self {
        Self::with_auth(
            engine,
            topics,
            cluster_id,
            broker_id,
            Arc::new(AllowAllAuthorizer),
            Arc::new(NoQuotaChecker),
        )
    }

    pub fn with_auth(
        engine: Arc<dyn StorageEngine>,
        topics: Arc<TopicRegistry>,
        cluster_id: impl Into<String>,
        broker_id: i32,
        authorizer: Arc<dyn Authorizer>,
        quotas: Arc<dyn QuotaChecker>,
    ) -> Self {
        let broker_id_val = broker_id;
        Self {
            engine,
            topics,
            local_lease: LocalLeaseManager,
            cluster_id: cluster_id.into(),
            broker_id,
            self_id: format!("skafka-{broker_id_val}"),
            authorizer,
            quotas,
            coord_manager: RwLock::new(None),
            coordinator: RwLock::new(None),
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

    /// Install the consumer-group + offsets coordinator. Called once
    /// from `bins/skafka/main.rs` at boot; tests can call it to wire
    /// a per-test [`Manager`].
    ///
    /// [`Manager`]: sk_coordinator::Manager
    pub fn install_coord_manager(&self, mgr: Arc<Manager>) {
        *self.coord_manager.write() = Some(mgr);
    }

    /// Read the installed [`Manager`]. Returns `None` when no
    /// coordinator is wired (Phase-3/4 dev path); handlers map that
    /// to `NOT_COORDINATOR` (16) so the client retries via
    /// FindCoordinator.
    ///
    /// [`Manager`]: sk_coordinator::Manager
    pub fn coord_manager(&self) -> Option<Arc<Manager>> {
        self.coord_manager.read().clone()
    }

    /// Install the broker-side assignment-watching [`Coordinator`].
    /// See [`Self::install_coord_manager`] for the equivalent group/
    /// offsets surface.
    pub fn install_coordinator(&self, c: Arc<Coordinator>) {
        *self.coordinator.write() = Some(c);
    }

    /// Read the installed [`Coordinator`]. Returns `None` when no
    /// coordinator is wired; produce / fetch / metadata handlers
    /// fall back to the `LocalLeaseManager` "always lead" path.
    pub fn coordinator(&self) -> Option<Arc<Coordinator>> {
        self.coordinator.read().clone()
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
