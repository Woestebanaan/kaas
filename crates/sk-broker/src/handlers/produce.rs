//! Produce handler (key 0).
//!
//! Iterates `req.topic_data`, calls `engine.append` per partition,
//! maps `StorageError` variants to wire error codes. Records bytes
//! flow through as opaque `Option<Bytes>` — no decode, no re-encode.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;
use sk_auth::{Operation, Principal, Resource};
use sk_codec::api::produce;
use sk_protocol::{ConnState, Handler, HandlerError};
use sk_storage::StorageError;

use crate::broker::Broker;

// Wire error codes (subset). Same numeric values Apache uses.
const ERR_UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
const ERR_LEADER_NOT_AVAILABLE: i16 = 5;
const ERR_NOT_LEADER_FOR_PARTITION: i16 = 6;
const ERR_TOPIC_AUTHORIZATION_FAILED: i16 = 29;
const ERR_OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;
const ERR_DUPLICATE_SEQUENCE_NUMBER: i16 = 46;
const ERR_INVALID_PRODUCER_EPOCH: i16 = 47;
const ERR_GENERIC: i16 = -1; // KAFKA_STORAGE_ERROR equivalent

#[derive(Debug)]
pub struct ProduceHandler {
    broker: Arc<Broker>,
}

impl ProduceHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for ProduceHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = produce::decode_request(&mut body, version)?;

        let acks = req.acks;
        let principal = conn
            .lock()
            .principal
            .clone()
            .unwrap_or_else(Principal::anonymous);

        // Cumulative byte count for the per-principal produce quota.
        let total_bytes: usize = req
            .topic_data
            .iter()
            .flat_map(|t| t.partition_data.iter())
            .filter_map(|p| p.records.as_ref().map(|b| b.len()))
            .sum();

        let mut responses = Vec::with_capacity(req.topic_data.len());
        for t in &req.topic_data {
            let mut partition_responses = Vec::with_capacity(t.partition_data.len());
            for p in &t.partition_data {
                let pr = self.append_one(&principal, &t.name, p, acks).await;
                partition_responses.push(pr);
            }
            responses.push(produce::TopicResponse {
                name: t.name.clone(),
                partition_responses,
            });
        }

        let throttle_time_ms = self
            .broker
            .quotas
            .check_produce_quota(&principal, total_bytes);

        let resp = produce::Response {
            responses,
            throttle_time_ms,
        };
        let mut out = BytesMut::new();
        produce::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

impl ProduceHandler {
    async fn append_one(
        &self,
        principal: &Principal,
        topic: &str,
        p: &produce::PartitionData,
        acks: i16,
    ) -> produce::PartitionResponse {
        // Quick "topic exists" check before going to the engine —
        // matches Apache's UNKNOWN_TOPIC_OR_PARTITION granularity.
        if self.broker.topics.get(topic).is_none() {
            return error_partition(p.index, ERR_UNKNOWN_TOPIC_OR_PARTITION);
        }

        // Phase 5 cluster check: a real `Coordinator` answers "do I
        // lead this partition?" against the most recently applied
        // assignment.json. Dev mode (no coordinator wired) keeps the
        // `LocalLeaseManager` "always lead" path.
        if let Some(c) = self.broker.coordinator() {
            if !c.owns(topic, p.index) {
                return error_partition(p.index, ERR_NOT_LEADER_FOR_PARTITION);
            }
        }

        // Cluster-wide ACL check (gh #126). Topic-level Write is the
        // canonical Apache mapping for Produce.
        let resource = Resource::topic(topic);
        if !self
            .broker
            .authorizer
            .authorize(principal, &resource, Operation::Write)
        {
            return error_partition(p.index, ERR_TOPIC_AUTHORIZATION_FAILED);
        }

        let Some(records) = p.records.clone() else {
            // Null records is a no-op produce per Apache. Echo the
            // current HWM so the client's offset accounting stays
            // sane.
            let base = self
                .broker
                .engine
                .high_watermark(topic, p.index)
                .unwrap_or(0);
            return ok_partition(p.index, base, 0);
        };

        // Ensure the partition is open (create_partition is idempotent
        // on the engine side).
        if let Err(err) = self.broker.engine.create_partition(topic, p.index).await {
            tracing::warn!(%err, topic, partition = p.index, "create_partition failed");
            return map_error(p.index, &err);
        }

        // Real cluster epoch from `Coordinator::current_epoch` when
        // wired; LocalLeaseManager (epoch 0) otherwise.
        let epoch = self
            .broker
            .coordinator()
            .and_then(|c| c.current_epoch(topic, p.index))
            .unwrap_or_else(|| self.broker.local_lease.current_epoch());
        match self
            .broker
            .engine
            .append(topic, p.index, epoch, acks, records)
            .await
        {
            Ok(base_offset) => {
                let log_start = self
                    .broker
                    .engine
                    .log_start_offset(topic, p.index)
                    .unwrap_or(0);
                ok_partition(p.index, base_offset, log_start)
            }
            Err(err) => {
                tracing::warn!(%err, topic, partition = p.index, "append failed");
                map_error(p.index, &err)
            }
        }
    }
}

fn ok_partition(index: i32, base_offset: i64, log_start_offset: i64) -> produce::PartitionResponse {
    produce::PartitionResponse {
        index,
        error_code: 0,
        base_offset,
        log_append_time_ms: -1,
        log_start_offset,
        record_errors: Vec::new(),
        error_message: None,
    }
}

fn error_partition(index: i32, error_code: i16) -> produce::PartitionResponse {
    produce::PartitionResponse {
        index,
        error_code,
        base_offset: -1,
        log_append_time_ms: -1,
        log_start_offset: -1,
        record_errors: Vec::new(),
        error_message: None,
    }
}

fn map_error(index: i32, err: &StorageError) -> produce::PartitionResponse {
    let code = match err {
        StorageError::EpochMismatch => ERR_NOT_LEADER_FOR_PARTITION,
        StorageError::OutOfOrderSequence => ERR_OUT_OF_ORDER_SEQUENCE_NUMBER,
        StorageError::DuplicateSequence => ERR_DUPLICATE_SEQUENCE_NUMBER,
        StorageError::InvalidProducerEpoch => ERR_INVALID_PRODUCER_EPOCH,
        StorageError::UnknownTopicOrPartition => ERR_UNKNOWN_TOPIC_OR_PARTITION,
        StorageError::Stalled => ERR_LEADER_NOT_AVAILABLE,
        _ => ERR_GENERIC,
    };
    error_partition(index, code)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic_registry::{TopicMeta, TopicRegistry};
    use sk_codec::api::common::{
        write_array_len, write_nullable_bytes, write_nullable_str, write_str,
    };
    use sk_codec::primitives::{write_i16, write_i32};
    use sk_codec::tagged;
    use sk_storage::{MemoryStorage, StorageEngine};
    use std::net::SocketAddr;
    use std::str::FromStr;

    fn conn() -> Mutex<ConnState> {
        Mutex::new(ConnState::new(
            "internal",
            SocketAddr::from_str("127.0.0.1:9092").unwrap(),
        ))
    }

    fn broker_with_topic(name: &str, partitions: i32) -> Arc<Broker> {
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let topics = Arc::new(TopicRegistry::new());
        topics.insert(TopicMeta {
            name: name.to_owned(),
            partition_count: partitions,
            topic_id: [0; 16],
        });
        Arc::new(Broker::new(engine, topics, "test", 0))
    }

    fn encode_request(req: &produce::Request, version: i16) -> Bytes {
        let flexible = version >= produce::MIN_FLEXIBLE;
        let mut w = BytesMut::new();
        if version >= 3 {
            write_nullable_str(&mut w, req.transactional_id.as_deref(), flexible).unwrap();
        }
        write_i16(&mut w, req.acks);
        write_i32(&mut w, req.timeout_ms);
        write_array_len(&mut w, req.topic_data.len(), flexible).unwrap();
        for t in &req.topic_data {
            write_str(&mut w, &t.name, flexible).unwrap();
            write_array_len(&mut w, t.partition_data.len(), flexible).unwrap();
            for p in &t.partition_data {
                write_i32(&mut w, p.index);
                write_nullable_bytes(&mut w, p.records.as_deref(), flexible).unwrap();
                if flexible {
                    tagged::write_empty(&mut w);
                }
            }
            if flexible {
                tagged::write_empty(&mut w);
            }
        }
        if flexible {
            tagged::write_empty(&mut w);
        }
        w.freeze()
    }

    #[tokio::test]
    async fn unknown_topic_returns_error_code_3() {
        let h = ProduceHandler::new(broker_with_topic("known", 1));
        let req = produce::Request {
            transactional_id: None,
            acks: -1,
            timeout_ms: 1000,
            topic_data: vec![produce::TopicData {
                name: "unknown".to_owned(),
                partition_data: vec![produce::PartitionData {
                    index: 0,
                    records: Some(Bytes::from_static(&[0; 64])),
                }],
            }],
        };
        let body = encode_request(&req, 9);
        let out = h.handle(&conn(), 9, body).await.unwrap();
        let mut r = out.freeze();
        let resp = produce::decode_response(&mut r, 9).unwrap();
        assert_eq!(resp.responses[0].partition_responses[0].error_code, 3);
    }
}
