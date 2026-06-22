//! InitProducerId handler (key 22).
//!
//! Phase 3: non-transactional producers get a fresh PID with epoch=0.
//! Transactional requests (`transactional_id: Some(_)`) return
//! `TRANSACTIONAL_ID_NOT_FOUND` (74) — Phase 6 wires the gh #22
//! rejoin contract + real `TxnStateStore`.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;
use sk_codec::api::init_producer_id;
use sk_protocol::{ConnState, Handler, HandlerError};

use crate::broker::Broker;

/// Apache wire error code for "the transactional id is unknown to
/// the coordinator". The Java client surfaces this distinctly from
/// generic errors so it can avoid retrying transactional sends on a
/// broker that's not running the txn coordinator yet.
pub const ERR_TRANSACTIONAL_ID_NOT_FOUND: i16 = 74;

#[derive(Debug)]
pub struct InitProducerIdHandler {
    broker: Arc<Broker>,
}

impl InitProducerIdHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for InitProducerIdHandler {
    async fn handle(
        &self,
        _conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = init_producer_id::decode_request(&mut body, version)?;
        let resp = if req.transactional_id.is_some() {
            init_producer_id::Response {
                throttle_time_ms: 0,
                error_code: ERR_TRANSACTIONAL_ID_NOT_FOUND,
                producer_id: -1,
                producer_epoch: -1,
            }
        } else {
            init_producer_id::Response {
                throttle_time_ms: 0,
                error_code: 0,
                producer_id: self.broker.next_producer_id(),
                producer_epoch: 0,
            }
        };
        let mut out = BytesMut::new();
        init_producer_id::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic_registry::TopicRegistry;
    use sk_storage::{MemoryStorage, StorageEngine};
    use std::net::SocketAddr;
    use std::str::FromStr;

    fn conn() -> Mutex<ConnState> {
        Mutex::new(ConnState::new(
            "internal",
            SocketAddr::from_str("127.0.0.1:9092").unwrap(),
        ))
    }

    fn broker() -> Arc<Broker> {
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        Arc::new(Broker::new(
            engine,
            Arc::new(TopicRegistry::new()),
            "test",
            0,
        ))
    }

    fn encode_request(req: &init_producer_id::Request, version: i16) -> Bytes {
        // Re-encoder for tests. Matches the sk-codec test helpers.
        use sk_codec::api::common::write_nullable_str;
        use sk_codec::primitives::{write_i16, write_i32, write_i64};
        use sk_codec::tagged;
        let flexible = version >= init_producer_id::MIN_FLEXIBLE;
        let mut w = BytesMut::new();
        write_nullable_str(&mut w, req.transactional_id.as_deref(), flexible).unwrap();
        write_i32(&mut w, req.transaction_timeout_ms);
        if version >= 3 {
            write_i64(&mut w, req.producer_id);
            write_i16(&mut w, req.producer_epoch);
        }
        if flexible {
            tagged::write_empty(&mut w);
        }
        w.freeze()
    }

    #[tokio::test]
    async fn non_transactional_returns_fresh_pid() {
        let h = InitProducerIdHandler::new(broker());
        let req = init_producer_id::Request {
            transactional_id: None,
            transaction_timeout_ms: 0,
            producer_id: -1,
            producer_epoch: -1,
        };
        let body = encode_request(&req, 4);
        let out = h.handle(&conn(), 4, body).await.unwrap();
        let mut r = out.freeze();
        let resp = init_producer_id::decode_response(&mut r, 4).unwrap();
        assert_eq!(resp.error_code, 0);
        assert!(resp.producer_id >= 1);
        assert_eq!(resp.producer_epoch, 0);
    }

    #[tokio::test]
    async fn transactional_returns_not_found() {
        let h = InitProducerIdHandler::new(broker());
        let req = init_producer_id::Request {
            transactional_id: Some("tx-1".to_owned()),
            transaction_timeout_ms: 60_000,
            producer_id: -1,
            producer_epoch: -1,
        };
        let body = encode_request(&req, 4);
        let out = h.handle(&conn(), 4, body).await.unwrap();
        let mut r = out.freeze();
        let resp = init_producer_id::decode_response(&mut r, 4).unwrap();
        assert_eq!(resp.error_code, ERR_TRANSACTIONAL_ID_NOT_FOUND);
    }
}
