//! TxnOffsetCommit handler (key 28, v0–v3).
//!
//! Stages the consumer-group offset commit in the [`OffsetStore`]'s
//! pending layer keyed by `(group_id, producer_id)`. Pending offsets
//! are **not** visible to `OffsetFetch` until the txn coordinator
//! fires the offset hook from `EndTxn(commit = true)` — see
//! [`OffsetStore::store_pending`] / `commit_pending` / `discard_pending`.
//!
//! Routing: this handler runs on the **group** coordinator (not the
//! txn coordinator). Cross-broker case (group coord ≠ txn coord on
//! `EndTxn`) is currently handled by the same-broker fast path
//! when they coincide; the gh #114 cross-broker `WriteTxnMarkers`
//! RPC completes the loop for the general case.
//!
//! [`OffsetStore`]: sk_coordinator::OffsetStore
//! [`OffsetStore::store_pending`]: sk_coordinator::OffsetStore::store_pending

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;
use sk_codec::api::txn_offset_commit;
use sk_coordinator::offset_key;
use sk_protocol::{ConnState, Handler, HandlerError};

use crate::broker::Broker;

const ERR_NOT_COORDINATOR: i16 = 16;

#[derive(Debug)]
pub struct TxnOffsetCommitHandler {
    broker: Arc<Broker>,
}

impl TxnOffsetCommitHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for TxnOffsetCommitHandler {
    async fn handle(
        &self,
        _conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = txn_offset_commit::decode_request(&mut body, version)?;

        // Group-coordinator gate. Mirrors OffsetCommit: when no
        // Manager is wired, fall through with NOT_COORDINATOR so the
        // client retries via FindCoordinator.
        let mgr = match self.broker.coord_manager() {
            Some(m) => m,
            None => {
                return write_response(version, &req, ERR_NOT_COORDINATOR);
            }
        };
        if !mgr.is_coordinator(&req.group_id) {
            return write_response(version, &req, ERR_NOT_COORDINATOR);
        }

        // Flatten (topic, partition) → offset_key. Mirrors the
        // OffsetCommit handler's transform so the offset store sees
        // the same key shape regardless of which API staged it.
        let mut offsets: HashMap<String, i64> = HashMap::new();
        for t in &req.topics {
            for p in &t.partitions {
                offsets.insert(offset_key(&t.name, p.partition_index), p.committed_offset);
            }
        }
        mgr.offsets
            .store_pending(&req.group_id, req.producer_id, offsets);

        write_response(version, &req, 0)
    }
}

fn write_response(
    version: i16,
    req: &txn_offset_commit::Request,
    err_code: i16,
) -> Result<BytesMut, HandlerError> {
    let topics = req
        .topics
        .iter()
        .map(|t| txn_offset_commit::ResponseTopic {
            name: t.name.clone(),
            partitions: t
                .partitions
                .iter()
                .map(|p| txn_offset_commit::ResponsePartition {
                    partition_index: p.partition_index,
                    error_code: err_code,
                })
                .collect(),
        })
        .collect();
    let resp = txn_offset_commit::Response {
        throttle_time_ms: 0,
        topics,
    };
    let mut out = BytesMut::new();
    txn_offset_commit::encode_response(&mut out, &resp, version)?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic_registry::TopicRegistry;
    use sk_coordinator::{offset_store::OffsetStore, FnLookup, LocalGroupSource, Manager};
    use sk_storage::{MemoryStorage, StorageEngine};
    use std::net::SocketAddr;
    use std::str::FromStr;

    fn conn() -> Mutex<ConnState> {
        Mutex::new(ConnState::new(
            "internal",
            SocketAddr::from_str("127.0.0.1:9092").unwrap(),
        ))
    }

    fn broker_with_manager() -> (tempfile::TempDir, Arc<Broker>, Arc<Manager>) {
        let tmp = tempfile::tempdir().unwrap();
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let b = Arc::new(Broker::new(
            engine,
            Arc::new(TopicRegistry::new()),
            "test",
            0,
        ));
        let offsets = Arc::new(OffsetStore::new(tmp.path()));
        let lookup = Arc::new(FnLookup::new(|_| None));
        let mgr = Manager::new(
            "skafka-0",
            offsets,
            lookup,
            LocalGroupSource::new("skafka-0"),
        );
        b.install_coord_manager(mgr.clone());
        (tmp, b, mgr)
    }

    fn encode_request(req: &txn_offset_commit::Request, version: i16) -> Bytes {
        use sk_codec::api::common::{write_array_len, write_nullable_str, write_str};
        use sk_codec::primitives::{write_i16, write_i32, write_i64};
        use sk_codec::tagged;
        let flexible = version >= txn_offset_commit::MIN_FLEXIBLE;
        let mut w = BytesMut::new();
        write_str(&mut w, &req.transactional_id, flexible).unwrap();
        write_str(&mut w, &req.group_id, flexible).unwrap();
        write_i64(&mut w, req.producer_id);
        write_i16(&mut w, req.producer_epoch);
        if version >= 3 {
            write_i32(&mut w, req.generation_id);
            write_str(&mut w, &req.member_id, flexible).unwrap();
            write_nullable_str(&mut w, req.group_instance_id.as_deref(), flexible).unwrap();
        }
        write_array_len(&mut w, req.topics.len(), flexible).unwrap();
        for t in &req.topics {
            write_str(&mut w, &t.name, flexible).unwrap();
            write_array_len(&mut w, t.partitions.len(), flexible).unwrap();
            for p in &t.partitions {
                write_i32(&mut w, p.partition_index);
                write_i64(&mut w, p.committed_offset);
                if version >= 2 {
                    write_i32(&mut w, p.committed_leader_epoch);
                }
                write_nullable_str(&mut w, p.committed_metadata.as_deref(), flexible).unwrap();
                if flexible {
                    tagged::write_empty(&mut w);
                }
            }
            if flexible {
                tagged::write_empty(&mut w);
            }
        }
        if flexible {
            tagged::write_empty(&mut w);
        }
        w.freeze()
    }

    async fn call(
        h: &TxnOffsetCommitHandler,
        req: &txn_offset_commit::Request,
    ) -> txn_offset_commit::Response {
        let body = encode_request(req, 3);
        let out = h.handle(&conn(), 3, body).await.unwrap();
        let mut r = out.freeze();
        txn_offset_commit::decode_response(&mut r, 3).unwrap()
    }

    #[tokio::test]
    async fn happy_path_stages_pending_offsets() {
        let (_t, b, mgr) = broker_with_manager();
        let h = TxnOffsetCommitHandler::new(b);
        let req = txn_offset_commit::Request {
            transactional_id: "tx-1".into(),
            group_id: "g1".into(),
            producer_id: 42,
            producer_epoch: 0,
            generation_id: 5,
            member_id: "m1".into(),
            group_instance_id: None,
            topics: vec![txn_offset_commit::Topic {
                name: "t".into(),
                partitions: vec![
                    txn_offset_commit::Partition {
                        partition_index: 0,
                        committed_offset: 100,
                        committed_leader_epoch: -1,
                        committed_metadata: None,
                    },
                    txn_offset_commit::Partition {
                        partition_index: 1,
                        committed_offset: 200,
                        committed_leader_epoch: -1,
                        committed_metadata: None,
                    },
                ],
            }],
        };
        let resp = call(&h, &req).await;
        for tr in &resp.topics {
            for pr in &tr.partitions {
                assert_eq!(pr.error_code, 0);
            }
        }
        // Pending offsets must NOT be visible to a regular OffsetFetch
        // until commit_pending fires (gh #27 contract).
        let pending = mgr.offsets.pending_for("g1", 42).expect("pending entry");
        assert_eq!(pending.get(&offset_key("t", 0)), Some(&100));
        assert_eq!(pending.get(&offset_key("t", 1)), Some(&200));
    }

    #[tokio::test]
    async fn no_manager_returns_not_coordinator() {
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let b = Arc::new(Broker::new(
            engine,
            Arc::new(TopicRegistry::new()),
            "test",
            0,
        ));
        let h = TxnOffsetCommitHandler::new(b);
        let req = txn_offset_commit::Request {
            transactional_id: "tx-1".into(),
            group_id: "g1".into(),
            producer_id: 42,
            producer_epoch: 0,
            generation_id: -1,
            member_id: String::new(),
            group_instance_id: None,
            topics: vec![txn_offset_commit::Topic {
                name: "t".into(),
                partitions: vec![txn_offset_commit::Partition {
                    partition_index: 0,
                    committed_offset: 1,
                    committed_leader_epoch: -1,
                    committed_metadata: None,
                }],
            }],
        };
        let resp = call(&h, &req).await;
        for tr in &resp.topics {
            for pr in &tr.partitions {
                assert_eq!(pr.error_code, ERR_NOT_COORDINATOR);
            }
        }
    }
}
