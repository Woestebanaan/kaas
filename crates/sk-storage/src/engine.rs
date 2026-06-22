//! The `StorageEngine` trait — the contract every storage backend
//! implements.
//!
//! Phase 2 ships two impls:
//!
//! - [`crate::memory::MemoryStorage`] — `Vec<Bytes>` per partition, used
//!   by dev mode (no `MY_POD_NAME`) and unit tests.
//! - `DiskStorageEngine` (follow-up commit) — segment files, manifest,
//!   producer snapshot, group-commit committer task, takeover.
//!
//! The trait is `dyn`-compatible (no generics, no `Self: Sized` methods,
//! async fn via `async_trait`). Every consumer holds
//! `Arc<dyn StorageEngine>`. This is the seam tests mock and the seam
//! the byte-opacity cross-engine test exercises.
//!
//! # Byte-opacity
//!
//! `append` takes raw RecordBatch bytes as `Bytes`. The engine rewrites
//! the first 8 bytes (baseOffset) to the broker's chosen offset before
//! storing — that's "brokers own offsets", and the v2 RecordBatch CRC
//! deliberately starts at byte 21 so this overwrite doesn't invalidate
//! it. Records bytes (everything past the 61-byte header) are never
//! inspected or modified.

use std::path::Path;

use async_trait::async_trait;
use bytes::Bytes;

use crate::errors::StorageError;

#[async_trait]
pub trait StorageEngine: Send + Sync + 'static {
    /// Append a raw RecordBatch to `(topic, partition)`. The engine
    /// rewrites bytes `[0..8]` (baseOffset) to the partition's current
    /// HWM before storing.
    ///
    /// `epoch` is the leader epoch the caller believes it owns; the
    /// disk engine rejects with `EpochMismatch` if its current epoch is
    /// higher. `MemoryStorage` ignores it (single-process dev mode).
    ///
    /// `acks` matches the producer field: `-1` waits for durable
    /// storage before returning; `0` and `1` return as soon as the
    /// bytes are accepted. `MemoryStorage` is always synchronous so
    /// the distinction is moot for it.
    ///
    /// Returns the assigned `base_offset` (== HWM at the moment of
    /// append).
    async fn append(
        &self,
        topic: &str,
        partition: i32,
        epoch: u32,
        acks: i16,
        batch: Bytes,
    ) -> Result<i64, StorageError>;

    /// Read raw RecordBatch bytes starting at `start_offset` for up to
    /// `max_bytes` of payload. Returns the concatenated bytes of all
    /// batches whose last_offset is `>= start_offset`, until the cap
    /// is reached.
    async fn read(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
        max_bytes: usize,
    ) -> Result<Bytes, StorageError>;

    fn high_watermark(&self, topic: &str, partition: i32) -> Result<i64, StorageError>;

    fn log_start_offset(&self, topic: &str, partition: i32) -> Result<i64, StorageError>;

    /// gh #5: timestamp-to-offset lookup (`ListOffsets` v1+). Returns
    /// `(offset, timestamp)` for the first record at or after
    /// `timestamp_ms`. `(-1, -1)` is the documented "no matching
    /// record" sentinel.
    fn offset_for_timestamp(
        &self,
        topic: &str,
        partition: i32,
        timestamp_ms: i64,
    ) -> Result<(i64, i64), StorageError>;

    /// KIP-101 / gh #101: for a Java consumer holding `leader_epoch =
    /// E`, return `(result_epoch, end_offset)` marking the offset just
    /// past the last record at epoch `E`. `(-1, -1)` is the documented
    /// "nothing to truncate to" sentinel.
    fn offset_for_leader_epoch(
        &self,
        topic: &str,
        partition: i32,
        leader_epoch: i32,
    ) -> Result<(i32, i64), StorageError>;

    /// gh #31: `DeleteRecords` (API key 21). Advances the partition's
    /// log start offset to at least `target_offset` so records below
    /// it become invisible to Fetch. `target_offset == -1` is the
    /// purge-to-HWM sentinel (KIP-107). Returns the new log start
    /// offset.
    async fn delete_records(
        &self,
        topic: &str,
        partition: i32,
        target_offset: i64,
    ) -> Result<i64, StorageError>;

    async fn create_partition(&self, topic: &str, partition: i32) -> Result<(), StorageError>;

    async fn delete_partition(&self, topic: &str, partition: i32) -> Result<(), StorageError>;

    /// Total bytes occupied by the partition's storage. `0` for unknown
    /// partitions so callers can iterate without filtering.
    fn partition_size(&self, topic: &str, partition: i32) -> i64;

    /// Engine's data directory. Advertised as the "log dir" in
    /// `DescribeLogDirs`. `MemoryStorage` returns the sentinel
    /// `memory://`.
    fn data_dir(&self) -> &Path;

    /// Claim write ownership of `(topic, partition)` under `epoch`.
    /// On disk, this opens FDs and runs recovery; on memory storage
    /// it's a no-op that returns the current HWM.
    async fn take_over(&self, topic: &str, partition: i32, epoch: u32)
        -> Result<i64, StorageError>;

    /// Release write ownership.
    async fn relinquish(&self, topic: &str, partition: i32) -> Result<(), StorageError>;
}
