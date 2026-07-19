//! OffsetDelete handler (key 47, gh #100).
//!
//! v0 only. Drops specific `(topic, partition)` committed offset
//! entries without dropping the whole group. Group-level errors:
//! `NOT_COORDINATOR` (16), `GROUP_ID_NOT_FOUND` (69),
//! `NON_EMPTY_GROUP` (67). Per-partition errors —
//! `UNKNOWN_TOPIC_OR_PARTITION` (3) — only fire when the group-level
//! error is 0.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;
use kaas_codec::api::offset_delete;
use kaas_coordinator::build_offset_key;
use kaas_protocol::{ConnState, Handler, HandlerError};

use crate::broker::Broker;

const ERR_NOT_COORDINATOR: i16 = 16;
const ERR_UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;

#[derive(Debug)]
pub struct OffsetDeleteHandler {
    broker: Arc<Broker>,
}

impl OffsetDeleteHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for OffsetDeleteHandler {
    async fn handle(
        &self,
        _conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = offset_delete::decode_request(&mut body, version)?;

        let mgr = match self.broker.coord_manager() {
            None => {
                let resp = offset_delete::Response {
                    error_code: ERR_NOT_COORDINATOR,
                    throttle_time_ms: 0,
                    topics: Vec::new(),
                };
                let mut out = BytesMut::new();
                offset_delete::encode_response(&mut out, &resp, version)?;
                return Ok(out);
            }
            Some(m) => m,
        };

        // Build the `topic/partition` key list expected by
        // OffsetStore::delete_partitions.
        let keys: Vec<String> = req
            .topics
            .iter()
            .flat_map(|t| {
                t.partitions
                    .iter()
                    .map(move |&p| build_offset_key(&t.name, p))
            })
            .collect();
        let (group_error, removed) = mgr.delete_offsets(&req.group_id, &keys);

        let topics_resp: Vec<offset_delete::OffsetDeleteTopicResponse> = req
            .topics
            .iter()
            .map(|t| {
                let partitions = t
                    .partitions
                    .iter()
                    .map(|&p| {
                        let key = build_offset_key(&t.name, p);
                        let error_code = if group_error != 0 {
                            // Per Apache's contract per-partition
                            // errors are suppressed when the group-
                            // level error is non-zero.
                            0
                        } else if *removed.get(&key).unwrap_or(&false) {
                            0
                        } else {
                            ERR_UNKNOWN_TOPIC_OR_PARTITION
                        };
                        offset_delete::OffsetDeletePartitionResponse {
                            partition_index: p,
                            error_code,
                        }
                    })
                    .collect();
                offset_delete::OffsetDeleteTopicResponse {
                    name: t.name.clone(),
                    partitions,
                }
            })
            .collect();

        let resp = offset_delete::Response {
            error_code: group_error,
            throttle_time_ms: 0,
            topics: topics_resp,
        };
        let mut out = BytesMut::new();
        offset_delete::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}
