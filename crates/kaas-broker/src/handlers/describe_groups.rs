//! DescribeGroups handler (key 15).
//!
//! Maps the Manager's per-group snapshot onto the codec response
//! shape. Groups this broker doesn't coordinate get the per-group
//! `NOT_COORDINATOR` (16) error.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_codec::api::describe_groups;
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use crate::broker::Broker;

const ERR_NOT_COORDINATOR: i16 = 16;

#[derive(Debug)]
pub struct DescribeGroupsHandler {
    broker: Arc<Broker>,
}

impl DescribeGroupsHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for DescribeGroupsHandler {
    async fn handle(
        &self,
        _conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = describe_groups::decode_request(&mut body, version)?;

        let groups: Vec<describe_groups::DescribedGroup> = match self.broker.coord_manager() {
            None => req
                .groups
                .into_iter()
                .map(|id| describe_groups::DescribedGroup {
                    error_code: ERR_NOT_COORDINATOR,
                    group_id: id,
                    group_state: String::new(),
                    protocol_type: String::new(),
                    protocol_data: String::new(),
                    members: Vec::new(),
                    authorized_operations: 0,
                })
                .collect(),
            Some(mgr) => mgr
                .describe_groups(&req.groups)
                .into_iter()
                .zip(req.groups)
                .map(|(snap, id)| match snap {
                    None => describe_groups::DescribedGroup {
                        error_code: ERR_NOT_COORDINATOR,
                        group_id: id,
                        group_state: String::new(),
                        protocol_type: String::new(),
                        protocol_data: String::new(),
                        members: Vec::new(),
                        authorized_operations: 0,
                    },
                    Some(s) => describe_groups::DescribedGroup {
                        error_code: 0,
                        group_id: s.id,
                        group_state: s.state.to_owned(),
                        protocol_type: s.protocol_type,
                        protocol_data: s.protocol_name,
                        members: s
                            .members
                            .into_iter()
                            .map(|m| describe_groups::DescribedGroupMember {
                                member_id: m.member_id,
                                group_instance_id: m.group_instance_id,
                                client_id: m.client_id,
                                client_host: String::new(),
                                member_metadata: Bytes::new(),
                                member_assignment: m.assignment,
                            })
                            .collect(),
                        authorized_operations: 0,
                    },
                })
                .collect(),
        };
        let resp = describe_groups::Response {
            throttle_time_ms: 0,
            groups,
        };
        let mut out = BytesMut::new();
        describe_groups::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}
