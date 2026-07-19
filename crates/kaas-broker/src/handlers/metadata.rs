//! Metadata handler (key 3).
//!
//! Single-broker shape: every partition leads on `broker_id = self`,
//! every topic carries the all-zero `topic_id` sentinel until Phase 7
//! mints real UUIDs.
//!
//! Per-listener port advertisement (gh #125): the handler picks the
//! port matching `ConnState::listener_name` from the listener table
//! it was constructed with, so a client that bootstrapped on
//! the authed listener gets back the authed port, not the anonymous
//! one. Phase 3 single-listener clusters resolve to the single entry.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_codec::api::metadata;
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use crate::broker::Broker;
use crate::cli::ListenerEntry;

const ERR_UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;

/// Per-listener advertised endpoint precomputed at handler-build
/// time. Keyed by the listener `name` stored on each connection.
#[derive(Debug, Clone)]
struct ListenerAdvert {
    name: String,
    host: String,
    port: i32,
}

#[derive(Debug)]
pub struct MetadataHandler {
    broker: Arc<Broker>,
    listeners: Vec<ListenerAdvert>,
}

impl MetadataHandler {
    pub fn new(broker: Arc<Broker>, listeners: &[ListenerEntry]) -> Self {
        let listeners = listeners.iter().map(advert_from).collect();
        Self { broker, listeners }
    }

    fn advert_for(&self, listener_name: &str) -> ListenerAdvert {
        self.listeners
            .iter()
            .find(|l| l.name == listener_name)
            .cloned()
            .unwrap_or_else(|| {
                // The connection's listener tag didn't match any
                // configured entry — should only happen with a
                // programming error in main.rs. Fall back to the
                // first listener so the response is still well-formed.
                self.listeners.first().cloned().unwrap_or(ListenerAdvert {
                    name: "internal".to_owned(),
                    host: "127.0.0.1".to_owned(),
                    port: 9092,
                })
            })
    }
}

fn self_broker_row(node_id: i32, advert: &ListenerAdvert) -> metadata::Broker {
    metadata::Broker {
        node_id,
        host: advert.host.clone(),
        port: advert.port,
        rack: None,
    }
}

/// `"kaas-2"` → `2`. Broker identity strings carry the ordinal as
/// the trailing hyphen segment (StatefulSet pod-name shape); a
/// malformed id yields `None` and the caller falls back to self.
fn trailing_ordinal(id: &str) -> Option<i32> {
    id.rsplit('-').next()?.parse().ok()
}

fn advert_from(entry: &ListenerEntry) -> ListenerAdvert {
    // Best-effort parse: bad addrs (which shouldn't occur — `Cli`
    // validates earlier) degrade to localhost:9092 so the Metadata
    // response stays well-formed.
    let addr: std::net::SocketAddr = entry
        .addr
        .parse()
        .unwrap_or_else(|_| std::net::SocketAddr::from(([127, 0, 0, 1], 9092)));
    let port = i32::from(addr.port());
    let host = match entry.advertised_host.as_deref() {
        Some(h) if !h.is_empty() => h.to_owned(),
        // 0.0.0.0 is a wildcard bind, not a routable target.
        // For dev clients connecting on the same box, localhost is
        // the right echo.
        _ if addr.ip().is_unspecified() => "127.0.0.1".to_owned(),
        _ => addr.ip().to_string(),
    };
    ListenerAdvert {
        name: entry.name.clone(),
        host,
        port,
    }
}

#[async_trait]
impl Handler for MetadataHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = metadata::decode_request(&mut body, version)?;
        let listener_name = conn.lock().listener_name.clone();
        let advert = self.advert_for(&listener_name);

        // Cluster mode: advertise the live broker set. Peers run the
        // same chart, so each peer is advertised at its stable FQDN
        // with the port of the listener this client connected on
        // (gh #125 symmetry). Self keeps the listener's own
        // advertised host so external hostname templates still win.
        // (External per-broker hostname templates for peers are a
        // follow-up — the external listener ships disabled.)
        let brokers = match self.broker.broker_view() {
            Some(view) => {
                let mut v: Vec<metadata::Broker> = view
                    .brokers()
                    .into_iter()
                    .map(|b| metadata::Broker {
                        node_id: b.node_id,
                        host: if b.node_id == self.broker.broker_id {
                            advert.host.clone()
                        } else {
                            b.host
                        },
                        port: advert.port,
                        rack: None,
                    })
                    .collect();
                if v.is_empty() {
                    v.push(self_broker_row(self.broker.broker_id, &advert));
                }
                v
            }
            None => vec![self_broker_row(self.broker.broker_id, &advert)],
        };

        // Per-partition leader from the applied assignment; self
        // when no coordinator is wired (dev) or the partition is
        // missing from the assignment (fresh topic, next recompute
        // pending).
        let coord = self.broker.coordinator();
        let controller_id = coord
            .as_ref()
            .and_then(|c| c.snapshot())
            .and_then(|a| trailing_ordinal(&a.controller))
            .unwrap_or(self.broker.broker_id);

        // If the request topic list is empty, return every known topic
        // (Apache: an empty list means "all topics"). If non-empty,
        // return exactly the requested topics, with UNKNOWN_TOPIC_OR_PARTITION
        // for any that aren't in our registry.
        let topic_names: Vec<String> = if req.topics.is_empty() {
            self.broker
                .topics
                .all()
                .into_iter()
                .map(|t| t.name)
                .collect()
        } else {
            req.topics
        };

        let mut topics = Vec::with_capacity(topic_names.len());
        for name in topic_names {
            match self.broker.topics.get(&name) {
                Some(meta) => {
                    let mut partitions =
                        Vec::with_capacity(usize::try_from(meta.partition_count).unwrap_or(0));
                    for i in 0..meta.partition_count {
                        let leader_id = coord
                            .as_ref()
                            .and_then(|c| c.leader_for(&name, i))
                            .and_then(|owner| trailing_ordinal(&owner))
                            .unwrap_or(self.broker.broker_id);
                        partitions.push(metadata::Partition {
                            error_code: 0,
                            partition_index: i,
                            leader_id,
                            leader_epoch: 0,
                            replica_nodes: vec![leader_id],
                            isr_nodes: vec![leader_id],
                            offline_replicas: Vec::new(),
                        });
                    }
                    topics.push(metadata::Topic {
                        error_code: 0,
                        name: meta.name,
                        topic_id: meta.topic_id,
                        is_internal: false,
                        partitions,
                        topic_authorized_operations: 0,
                    });
                }
                None => topics.push(metadata::Topic {
                    error_code: ERR_UNKNOWN_TOPIC_OR_PARTITION,
                    name,
                    topic_id: [0; 16],
                    is_internal: false,
                    partitions: Vec::new(),
                    topic_authorized_operations: 0,
                }),
            }
        }

        let resp = metadata::Response {
            throttle_time_ms: 0,
            brokers,
            cluster_id: Some(self.broker.cluster_id.clone()),
            controller_id,
            topics,
            cluster_authorized_operations: 0,
        };

        let mut out = BytesMut::new();
        metadata::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::ListenerEntry;
    use crate::topic_registry::{TopicMeta, TopicRegistry};
    use kaas_codec::api::common::{write_array_len, write_str};
    use kaas_codec::primitives::write_i8;
    use kaas_codec::tagged;
    use kaas_storage::{MemoryStorage, StorageEngine};
    use std::net::SocketAddr;
    use std::str::FromStr;

    fn conn(listener: &str) -> Mutex<ConnState> {
        Mutex::new(ConnState::new(
            listener,
            SocketAddr::from_str("127.0.0.1:9092").unwrap(),
        ))
    }

    fn broker_with(topics: Vec<(&str, i32)>) -> Arc<Broker> {
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let r = Arc::new(TopicRegistry::new());
        for (n, p) in topics {
            r.insert(TopicMeta {
                name: n.to_owned(),
                partition_count: p,
                topic_id: [0; 16],
            });
        }
        Arc::new(Broker::new(engine, r, "kaas-dev", 0))
    }

    fn listeners() -> Vec<ListenerEntry> {
        vec![
            ListenerEntry {
                name: "internal".to_owned(),
                addr: "0.0.0.0:9092".to_owned(),
                advertised_host: None,
                tls: None,
                authentication_type: None,
            },
            ListenerEntry {
                name: "external".to_owned(),
                addr: "0.0.0.0:9094".to_owned(),
                advertised_host: Some("broker-0.cluster.local".to_owned()),
                tls: None,
                authentication_type: None,
            },
        ]
    }

    fn encode_request_v9(topics: &[&str]) -> Bytes {
        let flexible = true; // v9 flexible
        let mut w = BytesMut::new();
        write_array_len(&mut w, topics.len(), flexible).unwrap();
        for n in topics {
            write_str(&mut w, n, flexible).unwrap();
            tagged::write_empty(&mut w);
        }
        write_i8(&mut w, 0); // allow_auto_topic_creation (v4+)
        write_i8(&mut w, 0); // include_cluster_authorized_operations (v8-10)
        write_i8(&mut w, 0); // include_topic_authorized_operations (v8+)
        tagged::write_empty(&mut w);
        w.freeze()
    }

    #[tokio::test]
    async fn returns_self_as_only_broker_and_leader() {
        let h = MetadataHandler::new(broker_with(vec![("events", 3)]), &listeners());
        let body = encode_request_v9(&["events"]);
        let out = h.handle(&conn("internal"), 9, body).await.unwrap();
        let mut r = out.freeze();
        let resp = metadata::decode_response(&mut r, 9).unwrap();
        assert_eq!(resp.brokers.len(), 1);
        assert_eq!(resp.brokers[0].node_id, 0);
        assert_eq!(resp.cluster_id.as_deref(), Some("kaas-dev"));
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(resp.topics[0].name, "events");
        assert_eq!(resp.topics[0].partitions.len(), 3);
        for p in &resp.topics[0].partitions {
            assert_eq!(p.leader_id, 0);
            assert_eq!(p.replica_nodes, vec![0]);
            assert_eq!(p.isr_nodes, vec![0]);
        }
    }

    #[tokio::test]
    async fn per_listener_port_echoed_back() {
        let h = MetadataHandler::new(broker_with(vec![("events", 1)]), &listeners());
        let body = encode_request_v9(&["events"]);

        let internal = h.handle(&conn("internal"), 9, body.clone()).await.unwrap();
        let mut r = internal.freeze();
        let resp_int = metadata::decode_response(&mut r, 9).unwrap();
        assert_eq!(resp_int.brokers[0].port, 9092);
        assert_eq!(resp_int.brokers[0].host, "127.0.0.1");

        let external = h.handle(&conn("external"), 9, body).await.unwrap();
        let mut r = external.freeze();
        let resp_ext = metadata::decode_response(&mut r, 9).unwrap();
        assert_eq!(resp_ext.brokers[0].port, 9094);
        assert_eq!(resp_ext.brokers[0].host, "broker-0.cluster.local");
    }

    #[tokio::test]
    async fn unknown_topic_returns_per_topic_error_3() {
        let h = MetadataHandler::new(broker_with(vec![("events", 1)]), &listeners());
        let body = encode_request_v9(&["nope"]);
        let out = h.handle(&conn("internal"), 9, body).await.unwrap();
        let mut r = out.freeze();
        let resp = metadata::decode_response(&mut r, 9).unwrap();
        assert_eq!(resp.topics[0].error_code, ERR_UNKNOWN_TOPIC_OR_PARTITION);
        assert!(resp.topics[0].partitions.is_empty());
    }

    #[tokio::test]
    async fn empty_topic_list_returns_all_known() {
        let h = MetadataHandler::new(broker_with(vec![("a", 1), ("b", 2)]), &listeners());
        let body = encode_request_v9(&[]);
        let out = h.handle(&conn("internal"), 9, body).await.unwrap();
        let mut r = out.freeze();
        let resp = metadata::decode_response(&mut r, 9).unwrap();
        assert_eq!(resp.topics.len(), 2);
        let names: Vec<&str> = resp.topics.iter().map(|t| t.name.as_str()).collect();
        assert!(names.contains(&"a"));
        assert!(names.contains(&"b"));
    }
}
