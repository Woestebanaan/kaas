//! `DiskStorageEngine` — the production [`StorageEngine`] impl.
//!
//! Thin glue over [`crate::partition::Partition`]. Holds a
//! `DashMap<(String, i32), Arc<Partition>>` and routes
//! [`StorageEngine`] method calls to the relevant partition.
//!
//! Phase 2 follow-up commits on gh #157 will extend this with: the
//! topic-config hot-reload watcher (per-partition `.config.json`),
//! `OffsetForTimestamp` / `OffsetForLeaderEpoch` semantics richer than
//! the current sentinels, and the `ReadSegmentRef`-based splice path
//! that the Phase 3 Fetch handler will use for sendfile.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;

use crate::engine::StorageEngine;
use crate::errors::StorageError;
use crate::fs::Fs;
use crate::partition::{Partition, PartitionConfig};

pub struct DiskStorageEngine {
    fs: Arc<dyn Fs>,
    data_dir: PathBuf,
    cfg: PartitionConfig,
    partitions: DashMap<(String, i32), Arc<Partition>>,
}

impl std::fmt::Debug for DiskStorageEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskStorageEngine")
            .field("data_dir", &self.data_dir)
            .field("partitions", &self.partitions.len())
            .finish()
    }
}

impl DiskStorageEngine {
    /// Construct a new engine rooted at `data_dir`. The directory is
    /// created on first partition open; no I/O happens here.
    pub fn new(fs: Arc<dyn Fs>, data_dir: PathBuf, cfg: PartitionConfig) -> Self {
        Self {
            fs,
            data_dir,
            cfg,
            partitions: DashMap::new(),
        }
    }

    fn partition_dir(&self, topic: &str, partition: i32) -> PathBuf {
        self.data_dir.join(topic).join(partition.to_string())
    }

    /// Get-or-open the partition. The race window between "no entry"
    /// and "insert" is resolved by re-checking after the open — the
    /// loser drops its partition (the active-segment FDs close in
    /// Drop, and the committer task exits when the channel sender
    /// drops). Theoretical contention only; both partitions just
    /// opened their handles, neither has written yet.
    async fn ensure_open(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<Arc<Partition>, StorageError> {
        let key = (topic.to_owned(), partition);
        if let Some(entry) = self.partitions.get(&key) {
            return Ok(entry.value().clone());
        }
        let dir = self.partition_dir(topic, partition);
        let p = Arc::new(
            Partition::open(
                self.fs.clone(),
                topic.to_owned(),
                partition,
                dir,
                self.cfg.clone(),
            )
            .await?,
        );
        match self.partitions.entry(key) {
            dashmap::Entry::Occupied(e) => Ok(e.get().clone()),
            dashmap::Entry::Vacant(e) => {
                e.insert(p.clone());
                Ok(p)
            }
        }
    }

    /// Snapshot the currently-open partition keys. Lets the retention
    /// cleaner iterate without holding DashMap shards across await
    /// points.
    pub fn iter_partition_keys(&self) -> Vec<(String, i32)> {
        self.partitions.iter().map(|kv| kv.key().clone()).collect()
    }

    /// Look up a currently-open [`Partition`] by `(topic, partition)`.
    /// Returns `None` if the partition is not open in this engine
    /// (the cleaner / compactor skip it).
    pub fn partition(&self, topic: &str, partition: i32) -> Option<Arc<Partition>> {
        self.partitions
            .get(&(topic.to_owned(), partition))
            .map(|e| e.value().clone())
    }

    /// Close every open partition (drain committers, persist state,
    /// release FDs). Used by the SIGTERM drain path (gh #61 + gh #139).
    pub async fn relinquish_all(&self) -> Result<(), StorageError> {
        // Drain into a vec so we don't hold dashmap shards across
        // the async close. clone the Arc's; remove from map.
        let entries: Vec<((String, i32), Arc<Partition>)> = self
            .partitions
            .iter()
            .map(|kv| (kv.key().clone(), kv.value().clone()))
            .collect();
        for (key, p) in entries {
            let _ = p.close().await;
            self.partitions.remove(&key);
        }
        Ok(())
    }
}

#[async_trait]
impl StorageEngine for DiskStorageEngine {
    async fn append(
        &self,
        topic: &str,
        partition: i32,
        epoch: u32,
        acks: i16,
        batch: Bytes,
    ) -> Result<i64, StorageError> {
        let p = self.ensure_open(topic, partition).await?;
        p.append(epoch, acks, batch).await
    }

    async fn read(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
        max_bytes: usize,
    ) -> Result<Bytes, StorageError> {
        // Read on an unknown partition returns empty (matches
        // MemoryStorage and the Go side's "no data" semantics —
        // clients receive an empty Fetch response, not an error).
        if let Some(entry) = self.partitions.get(&(topic.to_owned(), partition)) {
            entry.value().read(start_offset, max_bytes).await
        } else {
            Ok(Bytes::new())
        }
    }

    fn high_watermark(&self, topic: &str, partition: i32) -> Result<i64, StorageError> {
        Ok(self
            .partitions
            .get(&(topic.to_owned(), partition))
            .map(|e| e.value().high_watermark())
            .unwrap_or(0))
    }

    fn log_start_offset(&self, topic: &str, partition: i32) -> Result<i64, StorageError> {
        Ok(self
            .partitions
            .get(&(topic.to_owned(), partition))
            .map(|e| e.value().log_start_offset())
            .unwrap_or(0))
    }

    fn offset_for_timestamp(
        &self,
        _topic: &str,
        _partition: i32,
        _timestamp_ms: i64,
    ) -> Result<(i64, i64), StorageError> {
        // Phase 2 ships the sentinel; segment-level max_timestamp
        // tracking is in place (gh #5) but the index that maps
        // timestamps to offsets is a Phase 5 follow-up.
        Ok((-1, -1))
    }

    fn offset_for_leader_epoch(
        &self,
        _topic: &str,
        _partition: i32,
        _leader_epoch: i32,
    ) -> Result<(i32, i64), StorageError> {
        // KIP-101 lookup is Phase 5 follow-up; the segment epochs
        // are recorded but the leader-epoch cache isn't materialised
        // yet.
        Ok((-1, -1))
    }

    async fn delete_records(
        &self,
        topic: &str,
        partition: i32,
        target_offset: i64,
    ) -> Result<i64, StorageError> {
        let p = self.ensure_open(topic, partition).await?;
        p.delete_records(target_offset).await
    }

    fn fence_producer_epoch(&self, pid: i64, new_epoch: i16) {
        // Walk every open partition. Each Partition::fence_producer
        // takes its own inner mutex; the DashMap iter holds shard
        // read locks for the duration, which is fine because
        // fence_producer doesn't recursively touch the partitions
        // map. Partitions that aren't currently open on this broker
        // pick up the fence on their next take_over via the
        // producer-state snapshot path (gh #12).
        for kv in self.partitions.iter() {
            kv.value().fence_producer(pid, new_epoch);
        }
    }

    fn last_stable_offset(&self, topic: &str, partition: i32) -> Result<i64, StorageError> {
        match self.partitions.get(&(topic.to_owned(), partition)) {
            Some(p) => Ok(p.last_stable_offset()),
            None => self.high_watermark(topic, partition),
        }
    }

    fn aborted_transactions_in_range(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
        end_offset: i64,
    ) -> Vec<crate::txn_index::AbortedTxn> {
        match self.partitions.get(&(topic.to_owned(), partition)) {
            Some(p) => p.aborted_in_range(start_offset, end_offset),
            None => Vec::new(),
        }
    }

    async fn create_partition(&self, topic: &str, partition: i32) -> Result<(), StorageError> {
        // Opening is creation — Partition::open creates the dir.
        self.ensure_open(topic, partition).await?;
        Ok(())
    }

    async fn delete_partition(&self, topic: &str, partition: i32) -> Result<(), StorageError> {
        let key = (topic.to_owned(), partition);
        if let Some((_, p)) = self.partitions.remove(&key) {
            let _ = p.close().await;
        }
        // Best-effort directory removal — leave the dir if cleanup
        // fails (segment files survive on NFS silly-rename).
        let dir = self.partition_dir(topic, partition);
        for path in self.fs.readdir(&dir).unwrap_or_default() {
            let _ = self.fs.remove(&path);
        }
        Ok(())
    }

    fn partition_size(&self, topic: &str, partition: i32) -> i64 {
        self.partitions
            .get(&(topic.to_owned(), partition))
            .map(|e| e.value().partition_size())
            .unwrap_or(0)
    }

    fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    async fn take_over(
        &self,
        topic: &str,
        partition: i32,
        _epoch: u32,
    ) -> Result<i64, StorageError> {
        // Open opens at the manifest's epoch; explicit epoch-bump
        // semantics land alongside the cluster-runtime wire-up in
        // Phase 5. For now take_over == ensure_open + report HWM.
        let p = self.ensure_open(topic, partition).await?;
        Ok(p.high_watermark())
    }

    async fn relinquish(&self, topic: &str, partition: i32) -> Result<(), StorageError> {
        let key = (topic.to_owned(), partition);
        if let Some((_, p)) = self.partitions.remove(&key) {
            let _ = p.close().await;
        }
        Ok(())
    }

    async fn drain(&self) -> Result<(), StorageError> {
        self.relinquish_all().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::RealFs;
    use crate::memory::MemoryStorage;
    use crate::partition::PartitionConfig;

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    }

    /// Build a v2 batch carrying (num_records). Producer ID = -1
    /// (non-idempotent) so the engine doesn't gate on a missing PID.
    fn build_batch(num_records: i32, max_timestamp: i64) -> Bytes {
        let body_size = 49 + 16;
        let total = 12 + body_size;
        let mut buf = vec![0u8; total];
        buf[0..8].copy_from_slice(&0i64.to_be_bytes());
        let body_len_i32 = i32::try_from(body_size).unwrap();
        buf[8..12].copy_from_slice(&body_len_i32.to_be_bytes());
        buf[16] = 2;
        let last_offset_delta = num_records - 1;
        buf[23..27].copy_from_slice(&last_offset_delta.to_be_bytes());
        buf[35..43].copy_from_slice(&max_timestamp.to_be_bytes());
        buf[43..51].copy_from_slice(&(-1i64).to_be_bytes());
        Bytes::from(buf)
    }

    #[test]
    fn fresh_engine_unknown_partition_returns_empty() {
        rt().block_on(async {
            let tmp = tempfile::tempdir().unwrap();
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let e =
                DiskStorageEngine::new(fs, tmp.path().to_path_buf(), PartitionConfig::default());
            // Read before any append on an unknown partition.
            let got = e.read("nope", 0, 0, 4096).await.unwrap();
            assert!(got.is_empty());
            assert_eq!(e.high_watermark("nope", 0).unwrap(), 0);
        });
    }

    #[test]
    fn append_then_read_byte_identical() {
        rt().block_on(async {
            let tmp = tempfile::tempdir().unwrap();
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let e =
                DiskStorageEngine::new(fs, tmp.path().to_path_buf(), PartitionConfig::default());
            for _ in 0..4 {
                e.append("t", 0, 0, -1, build_batch(2, 1_000))
                    .await
                    .unwrap();
            }
            assert_eq!(e.high_watermark("t", 0).unwrap(), 8);
            let got = e.read("t", 0, 0, 4096).await.unwrap();
            // Each batch contributes its full bytes; expect 4 batches.
            let one_len = build_batch(2, 1_000).len();
            assert_eq!(got.len(), one_len * 4);
            e.relinquish_all().await.unwrap();
        });
    }

    #[test]
    fn data_dir_returns_configured_path() {
        let tmp = tempfile::tempdir().unwrap();
        let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
        let e = DiskStorageEngine::new(fs, tmp.path().to_path_buf(), PartitionConfig::default());
        assert_eq!(e.data_dir(), tmp.path());
    }

    /// Cross-engine equivalence: identical input sequence produces
    /// identical bytes on `read`. The broker-owns-offsets rewrite
    /// happens identically in both impls.
    #[test]
    fn memory_and_disk_produce_byte_equal_reads() {
        rt().block_on(async {
            let tmp = tempfile::tempdir().unwrap();
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let mem = MemoryStorage::new();
            let disk =
                DiskStorageEngine::new(fs, tmp.path().to_path_buf(), PartitionConfig::default());

            let sequence = [
                build_batch(3, 1_000),
                build_batch(1, 2_000),
                build_batch(5, 3_000),
                build_batch(2, 4_000),
            ];
            for b in &sequence {
                mem.append("t", 0, 0, -1, b.clone()).await.unwrap();
                disk.append("t", 0, 0, -1, b.clone()).await.unwrap();
            }

            assert_eq!(
                mem.high_watermark("t", 0).unwrap(),
                disk.high_watermark("t", 0).unwrap()
            );
            let mem_bytes = mem.read("t", 0, 0, 1 << 16).await.unwrap();
            let disk_bytes = disk.read("t", 0, 0, 1 << 16).await.unwrap();
            assert_eq!(
                mem_bytes, disk_bytes,
                "MemoryStorage and DiskStorageEngine bytes diverged"
            );
            disk.relinquish_all().await.unwrap();
        });
    }

    /// Reopen test: state survives engine restart via manifest +
    /// scan_high_watermark recovery.
    #[test]
    fn reopen_recovers_state() {
        rt().block_on(async {
            let tmp = tempfile::tempdir().unwrap();
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            {
                let e = DiskStorageEngine::new(
                    fs.clone(),
                    tmp.path().to_path_buf(),
                    PartitionConfig::default(),
                );
                for _ in 0..3 {
                    e.append("t", 0, 0, -1, build_batch(2, 1_000))
                        .await
                        .unwrap();
                }
                e.delete_records("t", 0, 2).await.unwrap();
                e.relinquish_all().await.unwrap();
            }
            // Reopen — HWM + log_start recovered from manifest.
            let e2 =
                DiskStorageEngine::new(fs, tmp.path().to_path_buf(), PartitionConfig::default());
            // Force the partition to open.
            let _ = e2.take_over("t", 0, 0).await.unwrap();
            assert_eq!(e2.high_watermark("t", 0).unwrap(), 6);
            assert_eq!(e2.log_start_offset("t", 0).unwrap(), 2);
            e2.relinquish_all().await.unwrap();
        });
    }

    /// Phantom HWM: manifest claims HWM past actual log → reopen
    /// rewinds via scan_high_watermark.
    #[test]
    fn phantom_hwm_is_healed_on_open() {
        rt().block_on(async {
            let tmp = tempfile::tempdir().unwrap();
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            {
                let e = DiskStorageEngine::new(
                    fs.clone(),
                    tmp.path().to_path_buf(),
                    PartitionConfig::default(),
                );
                for _ in 0..3 {
                    e.append("t", 0, 0, -1, build_batch(1, 1_000))
                        .await
                        .unwrap();
                }
                e.relinquish_all().await.unwrap();
            }
            // Hand-edit the manifest to claim HWM=999.
            let manifest_path = tmp.path().join("t").join("0").join("manifest.json");
            let json = std::fs::read_to_string(&manifest_path).unwrap();
            let phantom = json.replace("\"highWatermark\":3", "\"highWatermark\":999");
            assert_ne!(phantom, json, "expected the patch to take effect");
            std::fs::write(&manifest_path, phantom).unwrap();

            // Reopen — the phantom HWM should be rewound to 3.
            let e2 =
                DiskStorageEngine::new(fs, tmp.path().to_path_buf(), PartitionConfig::default());
            let _ = e2.take_over("t", 0, 0).await.unwrap();
            assert_eq!(
                e2.high_watermark("t", 0).unwrap(),
                3,
                "phantom HWM should be healed by scan_high_watermark"
            );
            e2.relinquish_all().await.unwrap();
        });
    }

    #[test]
    fn arc_dyn_storage_engine_compiles() {
        let tmp = tempfile::tempdir().unwrap();
        let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
        let _e: Arc<dyn StorageEngine> = Arc::new(DiskStorageEngine::new(
            fs,
            tmp.path().to_path_buf(),
            PartitionConfig::default(),
        ));
    }

    #[test]
    fn relinquish_then_reopen_via_take_over_roundtrips() {
        rt().block_on(async {
            let tmp = tempfile::tempdir().unwrap();
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let e =
                DiskStorageEngine::new(fs, tmp.path().to_path_buf(), PartitionConfig::default());
            e.append("t", 0, 0, -1, build_batch(3, 1_000))
                .await
                .unwrap();
            e.relinquish("t", 0).await.unwrap();
            // Re-take.
            let hwm = e.take_over("t", 0, 0).await.unwrap();
            assert_eq!(hwm, 3);
        });
    }

    #[test]
    fn partition_size_sums_segment_sizes() {
        rt().block_on(async {
            let tmp = tempfile::tempdir().unwrap();
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let e =
                DiskStorageEngine::new(fs, tmp.path().to_path_buf(), PartitionConfig::default());
            let one_len = i64::try_from(build_batch(1, 1_000).len()).unwrap();
            for _ in 0..4 {
                e.append("t", 0, 0, -1, build_batch(1, 1_000))
                    .await
                    .unwrap();
            }
            assert_eq!(e.partition_size("t", 0), one_len * 4);
            e.relinquish_all().await.unwrap();
        });
    }
}
