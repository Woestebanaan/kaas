//! EndTxn handler (key 26, v0–v3).
//!
//! Port of `archive/internal/protocol/handlers/end_txn.go`. Phase 6
//! workstream C implements the state-machine path: validate ownership
//! and (pid, epoch); transition Ongoing → CompleteCommit /
//! CompleteAbort via [`TxnStateStore::end_txn`]; rely on the wired
//! [`TxnOffsetHook`] (workstream F) to materialise or discard
//! pending offsets staged by `TxnOffsetCommit`.
//!
//! The same-broker control-batch marker write — Apache's
//! `WriteTxnMarkers` fast path — lands in workstream D. Without it,
//! a `read_committed` consumer can't yet observe the commit; this
//! handler returns `Ok` once the state has transitioned so callers
//! see a successful response even though the records aren't yet
//! marked.
//!
//! [`TxnOffsetHook`]: sk_coordinator::TxnOffsetHook

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;
use sk_codec::api::end_txn;
use sk_coordinator::TxnStateError;
use sk_protocol::{ConnState, Handler, HandlerError};

use crate::broker::Broker;

const ERR_INVALID_REQUEST: i16 = 42;
const ERR_NOT_COORDINATOR: i16 = 16;
const ERR_COORDINATOR_NOT_AVAILABLE: i16 = 15;
const ERR_INVALID_PRODUCER_ID_MAPPING: i16 = 49;
const ERR_PRODUCER_FENCED: i16 = 90;
const ERR_CONCURRENT_TRANSACTIONS: i16 = 51;
const ERR_INVALID_TXN_STATE: i16 = 50;

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
            None => match self.broker.txn_state() {
                Some(store) => match store.end_txn(
                    &req.transactional_id,
                    req.producer_id,
                    req.producer_epoch,
                    req.committed,
                ) {
                    Ok(()) => 0,
                    Err(e) => map_store_error(&e),
                },
                None => ERR_COORDINATOR_NOT_AVAILABLE,
            },
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
    use sk_coordinator::{TxnStateStore, TxnTopic};
    use sk_storage::{MemoryStorage, StorageEngine};
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
        use sk_codec::api::common::write_str;
        use sk_codec::primitives::{write_i16, write_i64, write_i8};
        use sk_codec::tagged;
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
    async fn commit_happy_path_clears_partitions() {
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
        let snap = store.snapshot();
        let entry = &snap["tx-1"];
        assert!(entry.partitions.is_empty());
    }

    #[tokio::test]
    async fn end_txn_against_empty_returns_invalid_txn_state() {
        let (_t, b) = broker_with_txn();
        let store = b.txn_state().unwrap();
        let (pid, epoch) = store.get_or_allocate("tx-1", || 1).unwrap();
        // No AddPartitions — state stays Empty.
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
                producer_epoch: 99, // stale
                committed: true,
            },
        )
        .await;
        assert_eq!(resp.error_code, ERR_PRODUCER_FENCED);
    }
}
