//! Fetch handler (key 1).
//!
//! Stateless full-fetch per gh #4: `session_id = 0` regardless of
//! what the client sent. Read-uncommitted only in Phase 3 — the
//! `aborted_transactions` list is always empty (Phase 6 wires the
//! `LastStableOffset` differentiator).

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;
use sk_codec::api::fetch;
use sk_protocol::{ConnState, Handler, HandlerError};
use sk_storage::StorageError;

use crate::broker::Broker;

const ERR_UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
const ERR_OFFSET_OUT_OF_RANGE: i16 = 1;
const ERR_NOT_LEADER_FOR_PARTITION: i16 = 6;

#[derive(Debug)]
pub struct FetchHandler {
    broker: Arc<Broker>,
}

impl FetchHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for FetchHandler {
    async fn handle(
        &self,
        _conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = fetch::decode_request(&mut body, version)?;

        let mut responses = Vec::with_capacity(req.topics.len());
        for t in &req.topics {
            let mut parts = Vec::with_capacity(t.partitions.len());
            for p in &t.partitions {
                parts.push(self.read_one(&t.name, p).await);
            }
            responses.push(fetch::TopicResponse {
                name: t.name.clone(),
                partitions: parts,
            });
        }

        let resp = fetch::Response {
            throttle_time_ms: 0,
            error_code: 0,
            session_id: 0, // gh #4 — stateless contract
            responses,
        };
        let mut out = BytesMut::new();
        fetch::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

impl FetchHandler {
    async fn read_one(&self, topic: &str, p: &fetch::Partition) -> fetch::PartitionResponse {
        if self.broker.topics.get(topic).is_none() {
            return error_partition(p.partition_index, ERR_UNKNOWN_TOPIC_OR_PARTITION);
        }

        // Best-effort metadata; HWM = 0 if the partition has never
        // been written to.
        let hwm = self
            .broker
            .engine
            .high_watermark(topic, p.partition_index)
            .unwrap_or(0);
        let lso = self
            .broker
            .engine
            .log_start_offset(topic, p.partition_index)
            .unwrap_or(0);

        // Reading from the high-watermark forward is the "I'm caught
        // up, give me nothing" case. Engine returns empty Bytes.
        let bytes = match self
            .broker
            .engine
            .read(
                topic,
                p.partition_index,
                p.fetch_offset,
                usize::try_from(p.partition_max_bytes.max(0)).unwrap_or(0),
            )
            .await
        {
            Ok(b) => b,
            Err(StorageError::OffsetOutOfRange) => {
                return error_partition(p.partition_index, ERR_OFFSET_OUT_OF_RANGE);
            }
            Err(StorageError::UnknownTopicOrPartition) => {
                return error_partition(p.partition_index, ERR_UNKNOWN_TOPIC_OR_PARTITION);
            }
            Err(StorageError::EpochMismatch) => {
                return error_partition(p.partition_index, ERR_NOT_LEADER_FOR_PARTITION);
            }
            Err(err) => {
                tracing::warn!(%err, topic, partition = p.partition_index, "fetch read failed");
                return error_partition(p.partition_index, -1);
            }
        };

        fetch::PartitionResponse {
            partition_index: p.partition_index,
            error_code: 0,
            high_watermark: hwm,
            last_stable_offset: hwm, // read-uncommitted in Phase 3
            log_start_offset: lso,
            aborted_transactions: Vec::new(),
            preferred_read_replica: -1,
            records: if bytes.is_empty() { None } else { Some(bytes) },
        }
    }
}

fn error_partition(partition_index: i32, error_code: i16) -> fetch::PartitionResponse {
    fetch::PartitionResponse {
        partition_index,
        error_code,
        high_watermark: -1,
        last_stable_offset: -1,
        log_start_offset: -1,
        aborted_transactions: Vec::new(),
        preferred_read_replica: -1,
        records: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic_registry::{TopicMeta, TopicRegistry};
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
        let topics = Arc::new(TopicRegistry::new());
        topics.insert(TopicMeta {
            name: "t".to_owned(),
            partition_count: 1,
            topic_id: [0; 16],
        });
        Arc::new(Broker::new(engine, topics, "test", 0))
    }

    #[tokio::test]
    async fn unknown_topic_returns_error_3() {
        // Build a minimal request: replica_id, max_wait, min_bytes,
        // max_bytes, isolation, session_id, session_epoch, one topic
        // with one partition.
        use sk_codec::api::common::{write_array_len, write_str};
        use sk_codec::primitives::{write_i32, write_i64, write_i8};
        use sk_codec::tagged;
        let mut w = BytesMut::new();
        write_i32(&mut w, -1); // replica_id
        write_i32(&mut w, 500); // max_wait_ms
        write_i32(&mut w, 1); // min_bytes
        write_i32(&mut w, 1024 * 1024); // max_bytes
        write_i8(&mut w, 0); // isolation_level
        write_i32(&mut w, 0); // session_id
        write_i32(&mut w, -1); // session_epoch
                               // topics (1)
        write_array_len(&mut w, 1, true).unwrap();
        write_str(&mut w, "unknown", true).unwrap();
        write_array_len(&mut w, 1, true).unwrap();
        write_i32(&mut w, 0); // partition_index
        write_i32(&mut w, -1); // current_leader_epoch (v9+)
        write_i64(&mut w, 0); // fetch_offset
        write_i32(&mut w, -1); // last_fetched_epoch (v12+)
        write_i64(&mut w, 0); // log_start_offset (v5+)
        write_i32(&mut w, 64 * 1024); // partition_max_bytes
        tagged::write_empty(&mut w); // partition tag
        tagged::write_empty(&mut w); // topic tag
                                     // forgotten_topics (0)
        write_array_len(&mut w, 0, true).unwrap();
        // rack_id (v11+)
        write_str(&mut w, "", true).unwrap();
        tagged::write_empty(&mut w); // request tag

        let body = w.freeze();
        let h = FetchHandler::new(broker());
        let out = h.handle(&conn(), 12, body).await.unwrap();
        let mut r = out.freeze();
        let resp = fetch::decode_response(&mut r, 12).unwrap();
        assert_eq!(resp.session_id, 0, "session_id must be 0 (gh #4)");
        assert_eq!(resp.responses[0].partitions[0].error_code, 3);
    }
}
