//! Heartbeat handler (key 12).
//!
//! Decodes the codec request, dispatches to
//! [`Manager::heartbeat`] when a coord manager is installed, and
//! returns the wire-level error code on the response. With no
//! coord manager (Phase-3/4 dev mode) the handler returns
//! `NOT_COORDINATOR` (16) so the client retries via
//! `FindCoordinator`.
//!
//! [`Manager::heartbeat`]: kaas_coordinator::Manager::heartbeat

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;
use kaas_codec::api::heartbeat;
use kaas_coordinator::HeartbeatRequest;
use kaas_protocol::{ConnState, Handler, HandlerError};

use crate::broker::Broker;

const ERR_NOT_COORDINATOR: i16 = 16;

#[derive(Debug)]
pub struct HeartbeatHandler {
    broker: Arc<Broker>,
}

impl HeartbeatHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for HeartbeatHandler {
    async fn handle(
        &self,
        _conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = heartbeat::decode_request(&mut body, version)?;
        let error_code = match self.broker.coord_manager() {
            None => ERR_NOT_COORDINATOR,
            Some(mgr) => mgr.heartbeat(
                &req.group_id,
                HeartbeatRequest {
                    member_id: req.member_id,
                    generation_id: req.generation_id,
                    group_instance_id: req.group_instance_id,
                },
            ),
        };
        let resp = heartbeat::Response {
            throttle_time_ms: 0,
            error_code,
        };
        let mut out = BytesMut::new();
        heartbeat::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}
