//! AlterReplicaLogDirs handler (key 34, KIP-113) — the gh #221
//! phase-3 volume-pool migration verb.
//!
//! Per requested `(destination path, topic, partition)`:
//!
//! 1. Resolve the destination path against the engine's log dirs —
//!    unknown paths answer `LOG_DIR_NOT_FOUND` (57), cordoned members
//!    `INVALID_REQUEST` (42) (KIP-1066: no new placements).
//! 2. Leadership gate: only the partition's current leader holds its
//!    files; everyone else answers `REPLICA_NOT_AVAILABLE` (9).
//! 3. `StorageEngine::move_partition_to_log_dir` — close, fresh-copy
//!    to the target root (appends fail retriable-`Migrating` during
//!    the window).
//! 4. Flip the placement record: `KafkaTopic.status.volumeAssignments`
//!    via the CR writer (durable truth), then the local registry (the
//!    engine's resolver — so the next open lands on the new root
//!    without waiting for the watch echo).
//! 5. Reclaim the source directory, best-effort — a failure leaves
//!    orphan bytes, never a correctness problem, and is logged.
//!
//! If the CR flip fails after the copy, the copy is rolled back and
//! the partition stays where it was — placement truth and data
//! location never diverge.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_codec::api::alter_replica_log_dirs as arld;
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;
use tracing::{info, warn};

use crate::broker::Broker;

const ERR_NONE: i16 = 0;
const ERR_UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
const ERR_REPLICA_NOT_AVAILABLE: i16 = 9;
const ERR_INVALID_REQUEST: i16 = 42;
const ERR_KAFKA_STORAGE_ERROR: i16 = 56;
const ERR_LOG_DIR_NOT_FOUND: i16 = 57;

#[derive(Debug)]
pub struct AlterReplicaLogDirsHandler {
    broker: Arc<Broker>,
}

impl AlterReplicaLogDirsHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }

    async fn move_one(&self, topic: &str, partition: i32, log_dir_name: &str) -> i16 {
        if self.broker.topics.get(topic).is_none() {
            return ERR_UNKNOWN_TOPIC_OR_PARTITION;
        }
        // Only the current leader holds the partition's files. Dev /
        // single-broker mode (no coordinator) trivially leads.
        if let Some(c) = self.broker.coordinator() {
            if !c.owns(topic, partition) {
                return ERR_REPLICA_NOT_AVAILABLE;
            }
        }
        let src_dir = match self
            .broker
            .engine
            .move_partition_to_log_dir(topic, partition, log_dir_name)
            .await
        {
            Ok(src) => src,
            Err(kaas_storage::StorageError::UnknownTopicOrPartition) => {
                return ERR_UNKNOWN_TOPIC_OR_PARTITION;
            }
            Err(kaas_storage::StorageError::Unsupported(_)) => return ERR_LOG_DIR_NOT_FOUND,
            Err(err) => {
                warn!(topic, partition, %err, "AlterReplicaLogDirs: move failed");
                return ERR_KAFKA_STORAGE_ERROR;
            }
        };

        // Durable truth first: flip the CR status record. On failure,
        // roll the copy back so data location and placement record
        // never diverge (the copy is fresh-per-attempt, so a retry
        // redoes it from scratch).
        if let Some(writer) = self.broker.cr_writer() {
            if let Err(err) = writer
                .set_partition_log_dir(topic, partition, log_dir_name)
                .await
            {
                warn!(topic, partition, %err,
                    "AlterReplicaLogDirs: placement-record flip failed; rolling back copy");
                let target = self
                    .broker
                    .engine
                    .log_dirs()
                    .into_iter()
                    .find(|d| d.name == log_dir_name)
                    .map(|d| d.path.join(topic).join(partition.to_string()));
                if let Some(dst) = target {
                    let _ = tokio::task::spawn_blocking(move || std::fs::remove_dir_all(dst)).await;
                }
                return ERR_KAFKA_STORAGE_ERROR;
            }
        } else {
            // No CR writer (dev mode): the local registry is the only
            // placement record there is.
            info!(
                topic,
                partition, log_dir_name, "AlterReplicaLogDirs: dev-mode local flip"
            );
        }
        self.broker
            .topics
            .set_volume_assignment(topic, partition, log_dir_name);

        // Source reclaim is best-effort: a failure (gh #76-style busy
        // handles on NFS) leaves orphan bytes on the old volume, never
        // an inconsistency. Logged so an operator can chase it.
        let src = src_dir.clone();
        if let Err(err) = tokio::task::spawn_blocking(move || std::fs::remove_dir_all(&src)).await {
            warn!(topic, partition, %err, "AlterReplicaLogDirs: source reclaim join error");
        }
        info!(topic, partition, log_dir_name, src = %src_dir.display(),
            "AlterReplicaLogDirs: partition moved");
        ERR_NONE
    }
}

#[async_trait]
impl Handler for AlterReplicaLogDirsHandler {
    async fn handle(
        &self,
        _conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = arld::decode_request(&mut body, version)?;

        // path → log dir, from the same list DescribeLogDirs reports.
        let dirs = self.broker.engine.log_dirs();

        // Aggregate per topic across the request's dir entries.
        let mut results: BTreeMap<String, Vec<arld::ResponsePartition>> = BTreeMap::new();
        for dir_req in &req.dirs {
            let target = dirs
                .iter()
                .find(|d| d.path.as_os_str() == std::ffi::OsStr::new(&dir_req.path));
            for t in &dir_req.topics {
                for p in &t.partitions {
                    let error_code = match target {
                        None => ERR_LOG_DIR_NOT_FOUND,
                        // KIP-1066: cordoned log dirs accept no new
                        // placements — moves included.
                        Some(d) if d.cordoned => ERR_INVALID_REQUEST,
                        Some(d) => self.move_one(&t.name, *p, &d.name).await,
                    };
                    results
                        .entry(t.name.clone())
                        .or_default()
                        .push(arld::ResponsePartition {
                            partition_index: *p,
                            error_code,
                        });
                }
            }
        }

        let resp = arld::Response {
            throttle_time_ms: 0,
            results: results
                .into_iter()
                .map(|(topic_name, partitions)| arld::ResponseTopic {
                    topic_name,
                    partitions,
                })
                .collect(),
        };
        let mut out = BytesMut::new();
        arld::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}
