//! DescribeLogDirs handler (key 35).
//!
//! One log dir per kaas broker (the engine's data dir); one topic
//! entry per requested topic — or every known topic when the request
//! carries a null filter. Partition sizes come straight from
//! [`StorageEngine::partition_size`].
//!
//! DescribeLogDirs (API key 35).
//!
//! [`StorageEngine::partition_size`]: kaas_storage::StorageEngine::partition_size

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_codec::api::describe_log_dirs;
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use crate::broker::Broker;

#[derive(Debug)]
pub struct DescribeLogDirsHandler {
    broker: Arc<Broker>,
}

impl DescribeLogDirsHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }

    /// Resolve the request's topic/partition filter against the
    /// registry: null topics → every known topic, every partition;
    /// a named topic with an empty partition list → every partition
    /// of that topic; unknown topics are dropped.
    fn wanted(&self, req: &describe_log_dirs::Request) -> Vec<(String, Vec<i32>)> {
        match req.topics.as_ref() {
            None => self
                .broker
                .topics
                .all()
                .into_iter()
                .map(|t| (t.name.clone(), (0..t.partition_count).collect()))
                .collect(),
            Some(topics) => topics
                .iter()
                .filter_map(|t| {
                    let known = self.broker.topics.get(&t.name)?;
                    let parts = if t.partitions.is_empty() {
                        (0..known.partition_count).collect()
                    } else {
                        t.partitions.clone()
                    };
                    Some((t.name.clone(), parts))
                })
                .collect(),
        }
    }
}

#[async_trait]
impl Handler for DescribeLogDirsHandler {
    async fn handle(
        &self,
        _conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = describe_log_dirs::decode_request(&mut body, version)?;

        // gh #221 phase 2: one LogDirResult per log dir (pool
        // member), every member reported even when empty — Kafka
        // reports all configured log.dirs. Partitions group under
        // the dir the placement resolver assigns them to.
        let wanted = self.wanted(&req);
        let results = self
            .broker
            .engine
            .log_dirs()
            .into_iter()
            .map(|dir| {
                // KIP-827 (v4+): per-dir filesystem capacity. The
                // codec drops the fields below v4; (-1, -1) is the
                // "unknown" sentinel (e.g. dev-mode memory://).
                let (total_bytes, usable_bytes) = kaas_storage::fs_capacity(&dir.path);
                let topics = wanted
                    .iter()
                    .filter_map(|(name, parts)| {
                        let partitions: Vec<_> = parts
                            .iter()
                            .filter(|p| self.broker.engine.partition_log_dir(name, **p) == dir.name)
                            .map(|p| describe_log_dirs::ResponsePartition {
                                partition_index: *p,
                                partition_size: self.broker.engine.partition_size(name, *p),
                                offset_lag: 0,
                                is_future_key: false,
                            })
                            .collect();
                        if partitions.is_empty() {
                            return None;
                        }
                        Some(describe_log_dirs::ResponseTopic {
                            name: name.clone(),
                            partitions,
                        })
                    })
                    .collect();
                describe_log_dirs::LogDirResult {
                    error_code: 0,
                    log_dir: dir.path.display().to_string(),
                    topics,
                    total_bytes,
                    usable_bytes,
                }
            })
            .collect();

        let resp = describe_log_dirs::Response {
            throttle_time_ms: 0,
            error_code: 0,
            results,
        };
        let mut out = BytesMut::new();
        describe_log_dirs::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}
