//! DeleteGroups handler (key 42, gh #89).

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;
use kaas_codec::api::delete_groups;
use kaas_protocol::{ConnState, Handler, HandlerError};

use crate::broker::Broker;

const ERR_NOT_COORDINATOR: i16 = 16;

#[derive(Debug)]
pub struct DeleteGroupsHandler {
    broker: Arc<Broker>,
}

impl DeleteGroupsHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for DeleteGroupsHandler {
    async fn handle(
        &self,
        _conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = delete_groups::decode_request(&mut body, version)?;
        let results: Vec<delete_groups::DeleteGroupsResult> = match self.broker.coord_manager() {
            None => req
                .group_names
                .into_iter()
                .map(|n| delete_groups::DeleteGroupsResult {
                    group_id: n,
                    error_code: ERR_NOT_COORDINATOR,
                })
                .collect(),
            Some(mgr) => req
                .group_names
                .into_iter()
                .map(|name| {
                    let code = mgr.delete_group(&name);
                    delete_groups::DeleteGroupsResult {
                        group_id: name,
                        error_code: code,
                    }
                })
                .collect(),
        };
        let resp = delete_groups::Response {
            throttle_time_ms: 0,
            results,
        };
        let mut out = BytesMut::new();
        delete_groups::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}
