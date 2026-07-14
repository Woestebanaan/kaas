//! AddOffsetsToTxn handler (key 25, v0–v3).
//!
//! Single top-level `ErrorCode` (no per-partition shape). The
//! `EndTxn` path uses the recorded group list to fire the offset
//! hook on commit/abort.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;
use sk_codec::api::add_offsets_to_txn;
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
pub struct AddOffsetsToTxnHandler {
    broker: Arc<Broker>,
}

impl AddOffsetsToTxnHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for AddOffsetsToTxnHandler {
    async fn handle(
        &self,
        _conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = add_offsets_to_txn::decode_request(&mut body, version)?;

        let error_code = match self.classify(&req) {
            Some(code) => code,
            None => match self.broker.txn_state() {
                Some(store) => match store.add_offsets_to_txn(
                    &req.transactional_id,
                    req.producer_id,
                    req.producer_epoch,
                    &req.group_id,
                    now_ms(),
                ) {
                    Ok(()) => 0,
                    Err(e) => map_store_error(&e),
                },
                None => ERR_COORDINATOR_NOT_AVAILABLE,
            },
        };

        let resp = add_offsets_to_txn::Response {
            throttle_time_ms: 0,
            error_code,
        };
        let mut out = BytesMut::new();
        add_offsets_to_txn::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

impl AddOffsetsToTxnHandler {
    fn classify(&self, req: &add_offsets_to_txn::Request) -> Option<i16> {
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

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic_registry::TopicRegistry;
    use sk_coordinator::TxnStateStore;
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

    fn encode_request(req: &add_offsets_to_txn::Request, version: i16) -> Bytes {
        use sk_codec::api::common::write_str;
        use sk_codec::primitives::{write_i16, write_i64};
        use sk_codec::tagged;
        let flexible = version >= add_offsets_to_txn::MIN_FLEXIBLE;
        let mut w = BytesMut::new();
        write_str(&mut w, &req.transactional_id, flexible).unwrap();
        write_i64(&mut w, req.producer_id);
        write_i16(&mut w, req.producer_epoch);
        write_str(&mut w, &req.group_id, flexible).unwrap();
        if flexible {
            tagged::write_empty(&mut w);
        }
        w.freeze()
    }

    async fn call(
        h: &AddOffsetsToTxnHandler,
        req: &add_offsets_to_txn::Request,
    ) -> add_offsets_to_txn::Response {
        let body = encode_request(req, 3);
        let out = h.handle(&conn(), 3, body).await.unwrap();
        let mut r = out.freeze();
        add_offsets_to_txn::decode_response(&mut r, 3).unwrap()
    }

    #[tokio::test]
    async fn happy_path_records_group() {
        let (_t, b) = broker_with_txn();
        let store = b.txn_state().unwrap();
        let (pid, epoch) = store.get_or_allocate("tx-1", || 42).unwrap();
        let h = AddOffsetsToTxnHandler::new(b.clone());
        let req = add_offsets_to_txn::Request {
            transactional_id: "tx-1".into(),
            producer_id: pid,
            producer_epoch: epoch,
            group_id: "g1".into(),
        };
        let resp = call(&h, &req).await;
        assert_eq!(resp.error_code, 0);
        let snap = store.snapshot();
        assert_eq!(snap["tx-1"].groups, vec!["g1".to_owned()]);
    }

    #[tokio::test]
    async fn unknown_producer_returns_invalid_producer_id_mapping() {
        let (_t, b) = broker_with_txn();
        let h = AddOffsetsToTxnHandler::new(b);
        let req = add_offsets_to_txn::Request {
            transactional_id: "tx-1".into(),
            producer_id: 999,
            producer_epoch: 0,
            group_id: "g1".into(),
        };
        let resp = call(&h, &req).await;
        assert_eq!(resp.error_code, ERR_INVALID_PRODUCER_ID_MAPPING);
    }
}
