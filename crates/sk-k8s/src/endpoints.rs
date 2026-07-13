//! Live map of broker endpoints derived from `EndpointSlice` events.
//!
//! Port of `archive/internal/k8s/endpoints.go`. Two layers:
//!
//! 1. [`BrokerRegistry`] — the pure-state map keyed on
//!    StatefulSet ordinal. Apply events via
//!    [`BrokerRegistry::apply_slice`] / [`BrokerRegistry::delete_slice`].
//!    No kube dep — fully unit-testable.
//! 2. [`crate::kube_watchers::watch_endpoints`] — the kube-bound
//!    pump that consumes `kube::runtime::watcher` events and calls
//!    into the registry. Lives behind the `kube-watchers` feature.
//!
//! gh #97 / gh #128: each broker advertises the StatefulSet pod's
//! FQDN (e.g. `"skafka-1.skafka-headless.skafka.svc.cluster.local"`)
//! built from [`crate::identity::DnsConfig`] — NOT the pod IP from
//! `EndpointSlice.Endpoints[].Addresses[0]`, which would break under
//! pod restart. Tests that pass an empty `DnsConfig` fall back to
//! the raw address.

use std::collections::HashMap;

use parking_lot::RwLock;

use crate::identity::{parse_ordinal, DnsConfig};

/// One broker's endpoint as advertised in the Metadata response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerEndpoint {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub ready: bool,
}

/// One ready-or-not endpoint extracted from an `EndpointSlice` —
/// the kube-bound watcher converts each `Endpoints[]` row into one
/// of these before calling into [`BrokerRegistry::apply_slice`].
/// Splitting this out keeps the registry kube-free.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointSliceEntry {
    /// Pod hostname (`EndpointSlice.Endpoints[].Hostname`), e.g.
    /// `"skafka-0"`. Used to extract the ordinal.
    pub hostname: String,
    /// First `Addresses[]` entry. Used as a fallback host when
    /// `DnsConfig` is unset.
    pub address: String,
    /// `EndpointSlice.Endpoints[].Conditions.Ready`.
    pub ready: bool,
}

/// A whole slice's worth of entries + the Kafka port the slice
/// advertises. Mirrors the kube `EndpointSlice` shape one-to-one.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EndpointSliceData {
    pub entries: Vec<EndpointSliceEntry>,
    pub kafka_port: Option<i32>,
}

#[derive(Debug, Default)]
struct Inner {
    brokers: HashMap<i32, BrokerEndpoint>,
}

/// Callback type fired on every registry change. Receives a fresh
/// snapshot sorted by `node_id`. Boxed for object safety so the
/// kube watcher can hand in arbitrary closures.
pub type OnChangeCallback = Box<dyn Fn(&[BrokerEndpoint]) + Send + Sync + 'static>;

/// Live broker endpoint map. The owning broker always appears at
/// `self.node_id`; peers come and go with `EndpointSlice` events.
pub struct BrokerRegistry {
    self_endpoint: BrokerEndpoint,
    dns: DnsConfig,
    on_change: parking_lot::Mutex<Option<OnChangeCallback>>,
    inner: RwLock<Inner>,
}

impl std::fmt::Debug for BrokerRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.read();
        f.debug_struct("BrokerRegistry")
            .field("self", &self.self_endpoint)
            .field("brokers", &inner.brokers.len())
            .finish()
    }
}

impl BrokerRegistry {
    pub fn new(self_endpoint: BrokerEndpoint, dns: DnsConfig) -> Self {
        let mut brokers = HashMap::new();
        brokers.insert(self_endpoint.node_id, self_endpoint.clone());
        Self {
            self_endpoint,
            dns,
            on_change: parking_lot::Mutex::new(None),
            inner: RwLock::new(Inner { brokers }),
        }
    }

    /// Register the on-change callback. Replaces any prior
    /// registration — there's at most one subscriber (the
    /// dispatcher or the metadata handler).
    pub fn on_change<F>(&self, f: F)
    where
        F: Fn(&[BrokerEndpoint]) + Send + Sync + 'static,
    {
        *self.on_change.lock() = Some(Box::new(f));
    }

    /// Owning broker's endpoint.
    pub fn self_endpoint(&self) -> BrokerEndpoint {
        self.self_endpoint.clone()
    }

    /// All known endpoints sorted by `node_id`.
    pub fn all(&self) -> Vec<BrokerEndpoint> {
        let inner = self.inner.read();
        let mut out: Vec<BrokerEndpoint> = inner.brokers.values().cloned().collect();
        out.sort_by_key(|e| e.node_id);
        out
    }

    /// Number of known ready brokers.
    pub fn count(&self) -> usize {
        self.inner.read().brokers.len()
    }

    /// Manual upsert — used by tests + local-dev binaries that
    /// don't run the kube watcher.
    pub fn upsert(&self, endpoint: BrokerEndpoint) {
        {
            let mut inner = self.inner.write();
            inner.brokers.insert(endpoint.node_id, endpoint);
        }
        self.fire_on_change();
    }

    /// Apply an `Added` / `Modified` `EndpointSlice` event. Ready
    /// entries land in the map keyed on their ordinal (extracted
    /// from `hostname`); not-ready entries are removed — except
    /// SELF, which is pinned: a readiness-probe blip on this pod
    /// must not make it forget its own existence. (Observed live:
    /// self-eviction → controller balanced over an empty set →
    /// unassigned all partitions → the takeover storm failed the
    /// next probe too — a self-sustaining death spiral.)
    pub fn apply_slice(&self, slice: &EndpointSliceData) {
        let port = slice.kafka_port.unwrap_or(self.self_endpoint.port);
        {
            let mut inner = self.inner.write();
            for ep in &slice.entries {
                let Some(ordinal) = parse_ordinal(&ep.hostname) else {
                    continue;
                };
                if !ep.ready {
                    if ordinal != self.self_endpoint.node_id {
                        inner.brokers.remove(&ordinal);
                    }
                    continue;
                }
                // gh #128: advertise the headless-DNS FQDN when
                // available; fall back to the raw address for
                // tests / dev where `DnsConfig` is empty.
                let host = if !self.dns.headless_service.is_empty()
                    && !self.dns.pod_name_pattern.is_empty()
                {
                    self.dns.fqdn(ordinal)
                } else {
                    ep.address.clone()
                };
                inner.brokers.insert(
                    ordinal,
                    BrokerEndpoint {
                        node_id: ordinal,
                        host,
                        port,
                        ready: true,
                    },
                );
            }
        }
        self.fire_on_change();
    }

    /// Apply a `Deleted` `EndpointSlice` event. Every entry's
    /// ordinal is removed from the map.
    pub fn delete_slice(&self, slice: &EndpointSliceData) {
        {
            let mut inner = self.inner.write();
            for ep in &slice.entries {
                if let Some(ordinal) = parse_ordinal(&ep.hostname) {
                    // Same self-pin as apply_slice.
                    if ordinal != self.self_endpoint.node_id {
                        inner.brokers.remove(&ordinal);
                    }
                }
            }
        }
        self.fire_on_change();
    }

    fn fire_on_change(&self) {
        let snapshot = self.all();
        if let Some(cb) = self.on_change.lock().as_ref() {
            cb(&snapshot);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dns() -> DnsConfig {
        DnsConfig {
            namespace: "skafka".to_owned(),
            headless_service: "skafka-headless".to_owned(),
            pod_name_pattern: "skafka-{ordinal}".to_owned(),
            cluster_domain: "cluster.local".to_owned(),
        }
    }

    fn self_ep() -> BrokerEndpoint {
        BrokerEndpoint {
            node_id: 0,
            host: "skafka-0.skafka-headless.skafka.svc.cluster.local".to_owned(),
            port: 9092,
            ready: true,
        }
    }

    fn slice_with(entries: Vec<EndpointSliceEntry>) -> EndpointSliceData {
        EndpointSliceData {
            entries,
            kafka_port: Some(9092),
        }
    }

    #[test]
    fn fresh_registry_has_self() {
        let r = BrokerRegistry::new(self_ep(), dns());
        assert_eq!(r.count(), 1);
        assert_eq!(r.all()[0], self_ep());
    }

    #[test]
    fn apply_slice_adds_peers_with_fqdn_host() {
        let r = BrokerRegistry::new(self_ep(), dns());
        r.apply_slice(&slice_with(vec![
            EndpointSliceEntry {
                hostname: "skafka-1".to_owned(),
                address: "10.0.0.5".to_owned(),
                ready: true,
            },
            EndpointSliceEntry {
                hostname: "skafka-2".to_owned(),
                address: "10.0.0.6".to_owned(),
                ready: true,
            },
        ]));
        let all = r.all();
        assert_eq!(all.len(), 3);
        // Peers use the headless-DNS FQDN, not the raw address.
        assert_eq!(
            all[1].host,
            "skafka-1.skafka-headless.skafka.svc.cluster.local"
        );
        assert_eq!(
            all[2].host,
            "skafka-2.skafka-headless.skafka.svc.cluster.local"
        );
    }

    #[test]
    fn not_ready_entries_are_removed() {
        let r = BrokerRegistry::new(self_ep(), dns());
        r.apply_slice(&slice_with(vec![EndpointSliceEntry {
            hostname: "skafka-1".to_owned(),
            address: "10.0.0.5".to_owned(),
            ready: true,
        }]));
        assert_eq!(r.count(), 2);
        r.apply_slice(&slice_with(vec![EndpointSliceEntry {
            hostname: "skafka-1".to_owned(),
            address: "10.0.0.5".to_owned(),
            ready: false,
        }]));
        assert_eq!(r.count(), 1);
    }

    #[test]
    fn delete_slice_removes_every_ordinal() {
        let r = BrokerRegistry::new(self_ep(), dns());
        r.apply_slice(&slice_with(vec![EndpointSliceEntry {
            hostname: "skafka-1".to_owned(),
            address: "10.0.0.5".to_owned(),
            ready: true,
        }]));
        r.delete_slice(&slice_with(vec![EndpointSliceEntry {
            hostname: "skafka-1".to_owned(),
            address: "10.0.0.5".to_owned(),
            ready: true,
        }]));
        assert_eq!(r.count(), 1);
    }

    #[test]
    fn empty_dns_falls_back_to_raw_address() {
        let empty_dns = DnsConfig {
            namespace: String::new(),
            headless_service: String::new(),
            pod_name_pattern: String::new(),
            cluster_domain: String::new(),
        };
        let r = BrokerRegistry::new(self_ep(), empty_dns);
        r.apply_slice(&slice_with(vec![EndpointSliceEntry {
            hostname: "skafka-1".to_owned(),
            address: "10.0.0.5".to_owned(),
            ready: true,
        }]));
        let all = r.all();
        let peer = all.iter().find(|e| e.node_id == 1).unwrap();
        assert_eq!(peer.host, "10.0.0.5");
    }

    #[test]
    fn on_change_fires_with_sorted_snapshot() {
        let r = BrokerRegistry::new(self_ep(), dns());
        let observed = std::sync::Arc::new(parking_lot::Mutex::new(Vec::<i32>::new()));
        let observed_c = observed.clone();
        r.on_change(move |all| {
            *observed_c.lock() = all.iter().map(|e| e.node_id).collect();
        });
        r.apply_slice(&slice_with(vec![
            EndpointSliceEntry {
                hostname: "skafka-2".to_owned(),
                address: "10.0.0.6".to_owned(),
                ready: true,
            },
            EndpointSliceEntry {
                hostname: "skafka-1".to_owned(),
                address: "10.0.0.5".to_owned(),
                ready: true,
            },
        ]));
        assert_eq!(*observed.lock(), vec![0, 1, 2]);
    }

    #[test]
    fn unparseable_hostnames_are_skipped() {
        let r = BrokerRegistry::new(self_ep(), dns());
        r.apply_slice(&slice_with(vec![EndpointSliceEntry {
            hostname: "noordinal".to_owned(),
            address: "10.0.0.7".to_owned(),
            ready: true,
        }]));
        assert_eq!(r.count(), 1);
    }
}
