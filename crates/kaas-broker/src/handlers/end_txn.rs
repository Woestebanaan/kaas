//! EndTxn handler (key 26, v0–v3).
//!
//! EndTxn (API key 26), including a same-broker marker fast path
//! (an improvement over v0.1).
//!
//! Sequence:
//! 1. Validate ownership + (pid, epoch) and transition state via
//!    [`TxnStateStore::end_txn`]. The wired [`TxnOffsetHook`]
//!    (workstream F) materialises or discards pending offsets.
//! 2. For every partition this broker leads from the snapshotted
//!    partition list, build a COMMIT / ABORT control batch via
//!    [`build_control_batch`] and `engine.append` it with
//!    `acks = -1` so a `read_committed` consumer can immediately see
//!    the commit. Partitions led by another broker are silently
//!    skipped — the gh #114 cross-broker `WriteTxnMarkers` RPC picks
//!    them up.
//!
//! `acks = -1` for marker dispatch: control markers commit
//! transactions, so they must be durable before we ack the producer.
//!
//! [`TxnOffsetHook`]: kaas_coordinator::TxnOffsetHook
//! [`build_control_batch`]: crate::control_batch::build_control_batch

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;
use kaas_codec::api::end_txn;
use kaas_coordinator::{EndTxnOutcome, MarkerEntry, TxnStateError, TxnTopic};
use kaas_protocol::{ConnState, Handler, HandlerError};

use std::collections::HashMap;

use crate::broker::Broker;
use crate::control_batch::build_control_batch;

const ERR_INVALID_REQUEST: i16 = 42;
const ERR_NOT_COORDINATOR: i16 = 16;
const ERR_COORDINATOR_NOT_AVAILABLE: i16 = 15;
const ERR_INVALID_PRODUCER_ID_MAPPING: i16 = 49;
const ERR_PRODUCER_FENCED: i16 = 90;
const ERR_CONCURRENT_TRANSACTIONS: i16 = 51;
const ERR_INVALID_TXN_STATE: i16 = 50;
const ACKS_ALL: i16 = -1;

#[derive(Debug)]
pub struct EndTxnHandler {
    broker: Arc<Broker>,
}

impl EndTxnHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for EndTxnHandler {
    async fn handle(
        &self,
        _conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = end_txn::decode_request(&mut body, version)?;

        let error_code = match self.classify(&req) {
            Some(code) => code,
            None => self.transition_and_dispatch(&req).await,
        };

        let resp = end_txn::Response {
            throttle_time_ms: 0,
            error_code,
        };
        let mut out = BytesMut::new();
        end_txn::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

impl EndTxnHandler {
    fn classify(&self, req: &end_txn::Request) -> Option<i16> {
        if req.transactional_id.is_empty() {
            return Some(ERR_INVALID_REQUEST);
        }
        if !self.broker.owns_txn(&req.transactional_id) {
            return Some(ERR_NOT_COORDINATOR);
        }
        if self.broker.txn_state().is_none() {
            return Some(ERR_COORDINATOR_NOT_AVAILABLE);
        }
        None
    }

    async fn transition_and_dispatch(&self, req: &end_txn::Request) -> i16 {
        let store = match self.broker.txn_state() {
            Some(s) => s,
            None => return ERR_COORDINATOR_NOT_AVAILABLE,
        };
        let outcome = match store.end_txn(
            &req.transactional_id,
            req.producer_id,
            req.producer_epoch,
            req.committed,
        ) {
            Ok(o) => o,
            Err(e) => return map_store_error(&e),
        };
        // Same-broker fast path: write markers for every partition we
        // currently lead. Cross-broker partitions are left to gh #114.
        // Idempotent retry returns `transition_fired = false` with an
        // empty partition list, so this loop is a no-op there.
        self.dispatch_markers(req, &outcome).await;
        0
    }

    async fn dispatch_markers(&self, req: &end_txn::Request, outcome: &EndTxnOutcome) {
        if !outcome.transition_fired || outcome.partitions.is_empty() {
            return;
        }

        // Group partitions by which broker leads them. Same-broker
        // partitions are written locally (low latency); peer-broker
        // partitions go through the marker_queue (gh #175 file-queue
        // dispatch). Coordinator-less dev mode treats every partition
        // as same-broker.
        let mut by_target: HashMap<Option<String>, Vec<(String, i32)>> = HashMap::new();
        let coord = self.broker.coordinator();
        for TxnTopic { topic, partitions } in &outcome.partitions {
            for &p in partitions {
                let leader = coord.as_ref().and_then(|c| c.leader_for(topic, p));
                by_target
                    .entry(leader)
                    .or_default()
                    .push((topic.clone(), p));
            }
        }

        let self_id = self.broker.self_id.as_str();
        // Splits: (local writes, per-target queue entries).
        let mut local_partitions: Vec<(String, i32)> = Vec::new();
        let mut queued: HashMap<String, Vec<(String, i32)>> = HashMap::new();
        for (target, parts) in by_target {
            match target {
                None => local_partitions.extend(parts), // dev mode
                Some(id) if id == self_id => local_partitions.extend(parts),
                Some(id) => {
                    queued.entry(id).or_default().extend(parts);
                }
            }
        }

        // Same-broker write — happens before the queue write so a
        // crash mid-dispatch still leaves the local marker in place.
        if !local_partitions.is_empty() {
            self.write_local_markers(req, &local_partitions).await;
        }

        // Cross-broker dispatch via the shared-PVC queue. Receiver's
        // MarkerWatcher picks it up within ~2 s and applies it on the
        // peer leader (gh #175).
        if !queued.is_empty() {
            self.enqueue_cross_broker_markers(req, &queued);
        }
    }

    async fn write_local_markers(&self, req: &end_txn::Request, partitions: &[(String, i32)]) {
        let batch = Bytes::from(build_control_batch(
            req.producer_id,
            req.producer_epoch,
            req.committed,
            // CoordinatorEpoch — Apache populates it from the txn
            // coordinator's lease epoch. Phase 6 doesn't track that
            // distinctly from the assignment epoch; 0 keeps the wire
            // shape valid (consumers don't act on the field).
            0,
        ));
        for (topic, p) in partitions {
            let epoch = self
                .broker
                .coordinator()
                .and_then(|c| c.current_epoch(topic, *p))
                .unwrap_or_else(|| self.broker.local_lease.current_epoch());
            let _ = self.broker.engine.create_partition(topic, *p).await;
            if let Err(err) = self
                .broker
                .engine
                .append(topic, *p, epoch, ACKS_ALL, batch.clone())
                .await
            {
                tracing::warn!(
                    topic,
                    partition = p,
                    %err,
                    "EndTxn marker append failed; consumers in read_committed mode \
                     will not see the txn as committed until the producer retries",
                );
            }
        }
    }

    fn enqueue_cross_broker_markers(
        &self,
        req: &end_txn::Request,
        queued: &HashMap<String, Vec<(String, i32)>>,
    ) {
        let queue = match self.broker.marker_queue() {
            Some(q) => q,
            None => {
                tracing::warn!(
                    txn_id = %req.transactional_id,
                    "EndTxn: cross-broker markers needed but no MarkerQueue is wired; \
                     peer partitions will not see this commit/abort until the txn \
                     coord retries",
                );
                return;
            }
        };
        for (target_broker, parts) in queued {
            // Pack into TxnTopic so the schema matches what
            // MarkerWatcher applies on the other side.
            let mut by_topic: HashMap<String, Vec<i32>> = HashMap::new();
            for (topic, p) in parts {
                by_topic.entry(topic.clone()).or_default().push(*p);
            }
            let partitions: Vec<TxnTopic> = by_topic
                .into_iter()
                .map(|(topic, partitions)| TxnTopic { topic, partitions })
                .collect();
            let entry = MarkerEntry {
                transactional_id: req.transactional_id.clone(),
                producer_id: req.producer_id,
                producer_epoch: req.producer_epoch,
                commit: req.committed,
                coordinator_epoch: 0,
                partitions,
            };
            if let Err(err) = queue.enqueue(target_broker, &entry) {
                tracing::warn!(
                    target = %target_broker,
                    txn_id = %req.transactional_id,
                    %err,
                    "EndTxn: marker queue enqueue failed; peer will not see this \
                     commit/abort until the txn coord retries"
                );
            }
        }
    }
}

fn map_store_error(err: &TxnStateError) -> i16 {
    match err {
        TxnStateError::EmptyTxnId => ERR_INVALID_REQUEST,
        TxnStateError::UnknownProducer => ERR_INVALID_PRODUCER_ID_MAPPING,
        TxnStateError::EpochFenced => ERR_PRODUCER_FENCED,
        TxnStateError::Concurrent => ERR_CONCURRENT_TRANSACTIONS,
        TxnStateError::InvalidState => ERR_INVALID_TXN_STATE,
        TxnStateError::Io(_) | TxnStateError::Decode(_) => ERR_COORDINATOR_NOT_AVAILABLE,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic_registry::TopicRegistry;
    use kaas_coordinator::{TxnStateStore, TxnTopic};
    use kaas_storage::{MemoryStorage, StorageEngine};
    use std::net::SocketAddr;
    use std::str::FromStr;

    fn conn() -> Mutex<ConnState> {
        Mutex::new(ConnState::new(
            "internal",
            SocketAddr::from_str("127.0.0.1:9092").unwrap(),
        ))
    }

    fn broker_with_txn() -> (tempfile::TempDir, Arc<Broker>) {
        let tmp = tempfile::tempdir().unwrap();
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let b = Arc::new(Broker::new(
            engine,
            Arc::new(TopicRegistry::new()),
            "test",
            0,
        ));
        b.install_txn_state(Arc::new(TxnStateStore::open(tmp.path(), 0).unwrap()));
        (tmp, b)
    }

    fn encode_request(req: &end_txn::Request, version: i16) -> Bytes {
        use kaas_codec::api::common::write_str;
        use kaas_codec::primitives::{write_i16, write_i64, write_i8};
        use kaas_codec::tagged;
        let flexible = version >= end_txn::MIN_FLEXIBLE;
        let mut w = BytesMut::new();
        write_str(&mut w, &req.transactional_id, flexible).unwrap();
        write_i64(&mut w, req.producer_id);
        write_i16(&mut w, req.producer_epoch);
        write_i8(&mut w, if req.committed { 1 } else { 0 });
        if flexible {
            tagged::write_empty(&mut w);
        }
        w.freeze()
    }

    async fn call(h: &EndTxnHandler, req: &end_txn::Request) -> end_txn::Response {
        let body = encode_request(req, 3);
        let out = h.handle(&conn(), 3, body).await.unwrap();
        let mut r = out.freeze();
        end_txn::decode_response(&mut r, 3).unwrap()
    }

    #[tokio::test]
    async fn commit_happy_path_writes_marker_to_owned_partition() {
        let (_t, b) = broker_with_txn();
        let store = b.txn_state().unwrap();
        let (pid, epoch) = store.get_or_allocate("tx-1", || 1).unwrap();
        store
            .add_partitions(
                "tx-1",
                pid,
                epoch,
                &[TxnTopic {
                    topic: "t".into(),
                    partitions: vec![0],
                }],
                100,
            )
            .unwrap();

        // Pre-create the partition so the marker append has somewhere
        // to land.
        b.engine.create_partition("t", 0).await.unwrap();
        let hwm_before = b.engine.high_watermark("t", 0).unwrap();

        let h = EndTxnHandler::new(b.clone());
        let resp = call(
            &h,
            &end_txn::Request {
                transactional_id: "tx-1".into(),
                producer_id: pid,
                producer_epoch: epoch,
                committed: true,
            },
        )
        .await;
        assert_eq!(resp.error_code, 0);

        // State was cleared and a marker batch appended (HWM advanced).
        let snap = store.snapshot();
        assert!(snap["tx-1"].partitions.is_empty());
        let hwm_after = b.engine.high_watermark("t", 0).unwrap();
        assert!(
            hwm_after > hwm_before,
            "expected HWM to advance after marker append; before={hwm_before} after={hwm_after}"
        );
    }

    #[tokio::test]
    async fn end_txn_against_empty_returns_invalid_txn_state() {
        let (_t, b) = broker_with_txn();
        let store = b.txn_state().unwrap();
        let (pid, epoch) = store.get_or_allocate("tx-1", || 1).unwrap();
        let h = EndTxnHandler::new(b);
        let resp = call(
            &h,
            &end_txn::Request {
                transactional_id: "tx-1".into(),
                producer_id: pid,
                producer_epoch: epoch,
                committed: true,
            },
        )
        .await;
        assert_eq!(resp.error_code, ERR_INVALID_TXN_STATE);
    }

    #[tokio::test]
    async fn epoch_mismatch_returns_producer_fenced() {
        let (_t, b) = broker_with_txn();
        let store = b.txn_state().unwrap();
        let (pid, _epoch) = store.get_or_allocate("tx-1", || 1).unwrap();
        let h = EndTxnHandler::new(b);
        let resp = call(
            &h,
            &end_txn::Request {
                transactional_id: "tx-1".into(),
                producer_id: pid,
                producer_epoch: 99,
                committed: true,
            },
        )
        .await;
        assert_eq!(resp.error_code, ERR_PRODUCER_FENCED);
    }

    #[tokio::test]
    async fn idempotent_retry_after_commit_is_noop() {
        let (_t, b) = broker_with_txn();
        let store = b.txn_state().unwrap();
        let (pid, epoch) = store.get_or_allocate("tx-1", || 1).unwrap();
        store
            .add_partitions(
                "tx-1",
                pid,
                epoch,
                &[TxnTopic {
                    topic: "t".into(),
                    partitions: vec![0],
                }],
                100,
            )
            .unwrap();
        b.engine.create_partition("t", 0).await.unwrap();
        let h = EndTxnHandler::new(b.clone());
        call(
            &h,
            &end_txn::Request {
                transactional_id: "tx-1".into(),
                producer_id: pid,
                producer_epoch: epoch,
                committed: true,
            },
        )
        .await;
        let hwm_after_first = b.engine.high_watermark("t", 0).unwrap();
        // Retry — should be Ok with no extra marker write.
        let resp = call(
            &h,
            &end_txn::Request {
                transactional_id: "tx-1".into(),
                producer_id: pid,
                producer_epoch: epoch,
                committed: true,
            },
        )
        .await;
        assert_eq!(resp.error_code, 0);
        let hwm_after_retry = b.engine.high_watermark("t", 0).unwrap();
        assert_eq!(
            hwm_after_first, hwm_after_retry,
            "idempotent retry must not write a second marker"
        );
    }
}
