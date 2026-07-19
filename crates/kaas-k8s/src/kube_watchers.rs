//! Kube-bound implementations: `EndpointSlice` watcher,
//! `Lease`-poll epoch source, and the `Pod` readiness patcher.
//!
//! Each helper takes a [`kube::Client`] (or a closure that produces
//! one) and runs as a long-lived `tokio` task. Cancellation is via
//! [`tokio_util::sync::CancellationToken`] — same shape the broker
//! binary already uses everywhere else.
//!
//! The `KafkaTopic` watcher is parked for Phase 7, where the CR
//! type lands in `kaas-operator-api`. Phase 5 doesn't read the CR
//! anyway — the broker takes its topic catalog from
//! `KAAS_TOPICS` env JSON until the operator surfaces in
//! Phase 7.

// Module-gating is already done via `#[cfg(...)]` on the
// `pub mod kube_watchers;` in lib.rs; the file-scope
// `#![cfg(...)]` only repeats it and clippy flags it as
// duplicated.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::coordination::v1::Lease;
use k8s_openapi::api::discovery::v1::EndpointSlice;
use kube::api::{Api, ListParams};
use kube::runtime::watcher::{watcher, Config as WatcherConfig, Event};
use kube::Client;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::endpoints::{BrokerRegistry, EndpointSliceData, EndpointSliceEntry};
use kaas_broker::coordinator::LeaseEpochSource;

#[derive(Debug, Error)]
pub enum KubeWatchError {
    #[error("kube error: {0}")]
    Kube(#[from] kube::Error),

    #[error("kube watcher: {0}")]
    Other(String),
}

/// Lease-backed [`LeaseEpochSource`] — polls the
/// `skafka-controller` Lease on a 1 s cadence and surfaces
/// `spec.leaseTransitions` as the current controller epoch.
///
/// Phase 5 brokers read `current_epoch` on every
/// [`kaas_broker::Coordinator::apply_if_new`] call to reject writes
/// from a partitioned ex-controller. Updates are atomic and lock-
/// free; the poll loop is the only writer.
///
/// [`LeaseEpochSource`]:
///     kaas_broker::coordinator::LeaseEpochSource
#[derive(Debug)]
pub struct KubeLeaseEpoch {
    epoch: AtomicI64,
    holder: parking_lot::Mutex<Option<String>>,
}

impl Default for KubeLeaseEpoch {
    fn default() -> Self {
        Self::new()
    }
}

impl KubeLeaseEpoch {
    pub fn new() -> Self {
        Self {
            epoch: AtomicI64::new(0),
            holder: parking_lot::Mutex::new(None),
        }
    }

    pub fn current_epoch(&self) -> i64 {
        self.epoch.load(Ordering::Relaxed)
    }

    pub fn current_holder(&self) -> Option<String> {
        self.holder.lock().clone()
    }

    fn store(&self, epoch: i64, holder: Option<String>) {
        self.epoch.store(epoch, Ordering::Relaxed);
        *self.holder.lock() = holder;
    }
}

impl LeaseEpochSource for KubeLeaseEpoch {
    fn current_epoch(&self) -> i64 {
        Self::current_epoch(self)
    }
}

/// Poll the `skafka-controller` Lease on a 1 s cadence and update
/// the supplied [`KubeLeaseEpoch`]. Returns when `cancel` fires.
pub async fn run_lease_watch(
    client: Client,
    namespace: String,
    lease_name: String,
    epoch: Arc<KubeLeaseEpoch>,
    cancel: CancellationToken,
) {
    let api: Api<Lease> = Api::namespaced(client, &namespace);
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tick.tick() => {
                match kaas_observability::record_k8s_call("Get", "Lease", api.get_opt(&lease_name)).await {
                    Ok(Some(lease)) => {
                        let transitions = lease
                            .spec
                            .as_ref()
                            .and_then(|s| s.lease_transitions)
                            .unwrap_or(0);
                        let holder = lease
                            .spec
                            .as_ref()
                            .and_then(|s| s.holder_identity.clone());
                        epoch.store(i64::from(transitions), holder);
                    }
                    Ok(None) => {
                        debug!(
                            lease = lease_name.as_str(),
                            "lease watch: lease not found yet (cluster still booting)"
                        );
                    }
                    Err(err) => {
                        warn!(%err, lease = lease_name.as_str(),
                              "lease watch: get failed; will retry next tick");
                    }
                }
            }
        }
    }
}

/// Stream `EndpointSlice` events for the headless service and feed
/// them into the registry. Selects slices by the standard
/// `kubernetes.io/service-name` label so the broker only sees its
/// own headless service's slices.
pub async fn run_endpoint_watch(
    client: Client,
    namespace: String,
    headless_service: String,
    registry: Arc<BrokerRegistry>,
    cancel: CancellationToken,
) {
    let api: Api<EndpointSlice> = Api::namespaced(client, &namespace);
    let lp =
        ListParams::default().labels(&format!("kubernetes.io/service-name={headless_service}"));
    let cfg = WatcherConfig {
        label_selector: Some(lp.label_selector.unwrap_or_default()),
        ..Default::default()
    };

    let mut stream = watcher(api, cfg).boxed();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            evt = stream.next() => {
                let evt = match evt {
                    None => {
                        debug!("endpoint watch: stream ended; restarting");
                        return;
                    }
                    Some(Ok(e)) => e,
                    Some(Err(err)) => {
                        warn!(%err, "endpoint watch: error from stream");
                        continue;
                    }
                };
                handle_endpoint_event(&registry, evt);
            }
        }
    }
}

fn handle_endpoint_event(registry: &BrokerRegistry, event: Event<EndpointSlice>) {
    match event {
        Event::Apply(slice) | Event::InitApply(slice) => {
            registry.apply_slice(&convert_slice(&slice));
        }
        Event::Delete(slice) => {
            registry.delete_slice(&convert_slice(&slice));
        }
        Event::Init => {}
        Event::InitDone => {}
    }
}

fn convert_slice(slice: &EndpointSlice) -> EndpointSliceData {
    let kafka_port = slice.ports.as_ref().and_then(|ps| {
        ps.iter()
            .find(|p| p.name.as_deref() == Some("kafka"))
            .and_then(|p| p.port)
    });
    let entries = slice
        .endpoints
        .iter()
        .filter_map(|ep| {
            let hostname = ep.hostname.clone()?;
            let address = ep.addresses.first()?.clone();
            let ready = ep
                .conditions
                .as_ref()
                .and_then(|c| c.ready)
                .unwrap_or(false);
            Some(EndpointSliceEntry {
                hostname,
                address,
                ready,
            })
        })
        .collect();
    EndpointSliceData {
        entries,
        kafka_port,
    }
}

/// Patch the broker pod's `Status.Conditions` to flip the
/// `skafka.io/PartitionsReady` readinessGate.
///
/// Returns `Ok(())` on the first successful patch; the caller can
/// retry on failure.
pub async fn patch_readiness(
    client: Client,
    namespace: String,
    pod_name: String,
    ready: bool,
) -> Result<(), KubeWatchError> {
    use k8s_openapi::api::core::v1::Pod;
    use kube::api::{Patch, PatchParams};

    let api: Api<Pod> = Api::namespaced(client, &namespace);
    let condition_status = if ready { "True" } else { "False" };
    // Server-side apply requires apiVersion + kind in the body —
    // without them the API server answers
    // `invalid object type: /, Kind=` (400).
    let patch = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": pod_name, "namespace": namespace },
        "status": {
            "conditions": [
                {
                    "type": crate::readiness::READINESS_CONDITION,
                    "status": condition_status,
                    "lastTransitionTime": chrono::Utc::now()
                        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                    "reason": "AssignmentApplied",
                    "message": "broker has applied at least one assignment.json",
                }
            ]
        }
    });
    kaas_observability::record_k8s_call(
        "Patch",
        "Pod",
        api.patch_status(
            &pod_name,
            &PatchParams::apply("kaas").force(),
            &Patch::Apply(&patch),
        ),
    )
    .await?;
    Ok(())
}

/// Stream `KafkaTopic` CR events into the broker's topic-registry
/// callback. Called with a `on_apply` closure that receives `(name,
/// partition_count)` on Apply/InitApply and an `on_delete` closure
/// that receives `name` on Delete.
///
/// Runs until `cancel` fires or the stream terminates permanently.
/// Individual watcher errors are logged and swallowed — the stream
/// restarts on the next tick.
pub async fn run_topic_watch<A, D>(
    client: Client,
    namespace: String,
    on_apply: A,
    on_delete: D,
    cancel: CancellationToken,
) -> Result<(), KubeWatchError>
where
    A: Fn(&str, i32) + Send + Sync + 'static,
    D: Fn(&str) + Send + Sync + 'static,
{
    let api: Api<kaas_operator_api::KafkaTopic> = Api::namespaced(client, &namespace);
    let mut stream = watcher(api, WatcherConfig::default()).boxed();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            evt = stream.next() => {
                let evt = match evt {
                    None => {
                        debug!("topic watch: stream ended; caller may restart");
                        return Ok(());
                    }
                    Some(Ok(e)) => e,
                    Some(Err(err)) => {
                        warn!(%err, "topic watch: error from stream");
                        continue;
                    }
                };
                handle_topic_event(&on_apply, &on_delete, evt);
            }
        }
    }
}

fn handle_topic_event<A, D>(on_apply: &A, on_delete: &D, event: Event<kaas_operator_api::KafkaTopic>)
where
    A: Fn(&str, i32),
    D: Fn(&str),
{
    match event {
        Event::Apply(t) | Event::InitApply(t) => {
            let Some(name) = t.metadata.name.as_deref() else {
                return;
            };
            on_apply(name, t.spec.partitions);
        }
        Event::Delete(t) => {
            let Some(name) = t.metadata.name.as_deref() else {
                return;
            };
            on_delete(name);
        }
        Event::Init | Event::InitDone => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::discovery::v1::{Endpoint, EndpointConditions, EndpointPort};
    use kube::api::ObjectMeta;

    #[test]
    fn convert_slice_extracts_kafka_port_when_named() {
        let slice = EndpointSlice {
            metadata: ObjectMeta::default(),
            address_type: "IPv4".to_owned(),
            endpoints: vec![Endpoint {
                hostname: Some("skafka-1".to_owned()),
                addresses: vec!["10.0.0.5".to_owned()],
                conditions: Some(EndpointConditions {
                    ready: Some(true),
                    serving: None,
                    terminating: None,
                }),
                ..Default::default()
            }],
            ports: Some(vec![
                EndpointPort {
                    name: Some("kafka".to_owned()),
                    port: Some(9092),
                    ..Default::default()
                },
                EndpointPort {
                    name: Some("metrics".to_owned()),
                    port: Some(9090),
                    ..Default::default()
                },
            ]),
        };
        let data = convert_slice(&slice);
        assert_eq!(data.kafka_port, Some(9092));
        assert_eq!(data.entries.len(), 1);
        assert_eq!(data.entries[0].hostname, "skafka-1");
        assert_eq!(data.entries[0].address, "10.0.0.5");
        assert!(data.entries[0].ready);
    }

    #[test]
    fn convert_slice_skips_entries_without_hostname() {
        let slice = EndpointSlice {
            metadata: ObjectMeta::default(),
            address_type: "IPv4".to_owned(),
            endpoints: vec![Endpoint {
                hostname: None,
                addresses: vec!["10.0.0.5".to_owned()],
                ..Default::default()
            }],
            ports: None,
        };
        assert!(convert_slice(&slice).entries.is_empty());
    }

    #[test]
    fn kube_lease_epoch_starts_at_zero() {
        let e = KubeLeaseEpoch::new();
        assert_eq!(e.current_epoch(), 0);
        assert!(e.current_holder().is_none());
    }

    #[test]
    fn kube_lease_epoch_stores_value() {
        let e = KubeLeaseEpoch::new();
        e.store(7, Some("skafka-0".to_owned()));
        assert_eq!(e.current_epoch(), 7);
        assert_eq!(e.current_holder().as_deref(), Some("skafka-0"));
    }
}
