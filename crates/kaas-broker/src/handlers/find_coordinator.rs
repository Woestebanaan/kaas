//! FindCoordinator handler (key 10).
//!
//! Translates the codec request into one or more
//! [`Manager::find_coordinator`] lookups. v0..=v3 carries a single
//! key on the legacy response shape; v4+ batches many keys into the
//! `coordinators[]` response field.
//!
//! With no coord manager installed, returns
//! `COORDINATOR_NOT_AVAILABLE` (15) so the client retries — same
//! shape as the boot window between `Server::serve` and the first
//! `assignment.json` apply.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_codec::api::find_coordinator;
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use crate::broker::Broker;

const ERR_COORD_NOT_AVAILABLE: i16 = 15;

#[derive(Debug)]
pub struct FindCoordinatorHandler {
    broker: Arc<Broker>,
}

impl FindCoordinatorHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for FindCoordinatorHandler {
    async fn handle(
        &self,
        _conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = find_coordinator::decode_request(&mut body, version)?;

        let mgr = self.broker.coord_manager();
        let resolve = |key: &str| -> find_coordinator::CoordinatorResult {
            match &mgr {
                None => find_coordinator::CoordinatorResult {
                    key: key.to_owned(),
                    node_id: 0,
                    host: String::new(),
                    port: 0,
                    error_code: ERR_COORD_NOT_AVAILABLE,
                    error_message: None,
                },
                Some(m) => {
                    let r = m.find_coordinator(key, req.key_type);
                    find_coordinator::CoordinatorResult {
                        key: key.to_owned(),
                        node_id: r.node_id,
                        host: r.host,
                        port: r.port,
                        error_code: r.error_code,
                        error_message: None,
                    }
                }
            }
        };

        let resp = if version >= 4 {
            let coordinators: Vec<find_coordinator::CoordinatorResult> =
                req.coordinator_keys.iter().map(|k| resolve(k)).collect();
            find_coordinator::Response {
                throttle_time_ms: 0,
                coordinators,
                ..find_coordinator::Response::default()
            }
        } else {
            let r = resolve(&req.key);
            find_coordinator::Response {
                throttle_time_ms: 0,
                error_code: r.error_code,
                error_message: None,
                node_id: r.node_id,
                host: r.host,
                port: r.port,
                coordinators: Vec::new(),
            }
        };
        let mut out = BytesMut::new();
        find_coordinator::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}
