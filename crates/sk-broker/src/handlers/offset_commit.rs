//! OffsetCommit handler (key 8).
//!
//! Flattens the codec's nested `(topic, partition)` shape into a
//! single `offset_key → offset` map, delegates to
//! [`Manager::offset_commit`], and echoes the per-partition layout
//! back with the resulting error code stamped uniformly across all
//! partitions.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;
use sk_codec::api::offset_commit;
use sk_coordinator::build_offset_key;
use sk_protocol::{ConnState, Handler, HandlerError};

use crate::broker::Broker;

const ERR_NOT_COORDINATOR: i16 = 16;

#[derive(Debug)]
pub struct OffsetCommitHandler {
    broker: Arc<Broker>,
}

impl OffsetCommitHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for OffsetCommitHandler {
    async fn handle(
        &self,
        _conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = offset_commit::decode_request(&mut body, version)?;

        // Build the offsets + metadata maps the Manager expects.
        let mut offsets: HashMap<String, i64> = HashMap::new();
        let mut metadata: HashMap<String, String> = HashMap::new();
        for t in &req.topics {
            for p in &t.partitions {
                let key = build_offset_key(&t.name, p.partition_index);
                offsets.insert(key.clone(), p.committed_offset);
                if let Some(m) = &p.committed_metadata {
                    if !m.is_empty() {
                        metadata.insert(key, m.clone());
                    }
                }
            }
        }

        let group_error = match self.broker.coord_manager() {
            None => ERR_NOT_COORDINATOR,
            Some(mgr) => mgr.offset_commit(&req.group_id, offsets, metadata),
        };

        let topics_resp: Vec<offset_commit::OffsetCommitTopicResponse> = req
            .topics
            .into_iter()
            .map(|t| {
                let partitions = t
                    .partitions
                    .iter()
                    .map(|p| offset_commit::OffsetCommitPartitionResponse {
                        partition_index: p.partition_index,
                        error_code: group_error,
                    })
                    .collect();
                offset_commit::OffsetCommitTopicResponse {
                    name: t.name,
                    partitions,
                }
            })
            .collect();

        let resp = offset_commit::Response {
            throttle_time_ms: 0,
            topics: topics_resp,
        };
        let mut out = BytesMut::new();
        offset_commit::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}
