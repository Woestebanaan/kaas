//! Per-group committed offsets, persisted under
//! `<cluster_dir>/__consumer_offsets/<group>.json` with `tmp + rename`
//! atomicity.
//!
//! The root is the cluster-state directory (`/data/__cluster` by
//! default), NOT the data dir: a sibling of the topic dirs is exactly
//! what the operator's orphan-topic sweep reclaims, and offsets lived
//! there once — every sweep pass deleted them (gh #223).
//! [`migrate_legacy_offsets_dir`] adopts a surviving pre-gh #223
//! layout on boot.
//!
//! Wire-readable JSON shape pinned across releases: a v0.1-written
//! file decodes here unchanged, and vice-versa. Two on-disk schemas survive: the gh #21 v2 envelope
//! `{"offsets":{...}, "metadata":{...}}` and the legacy v1 plain
//! `map[string]int64`. Read paths accept both; new writes always
//! emit v2.
//!
//! Three layers of state:
//!
//! 1. The visible cache + metadata maps backed by `<group>.json`.
//!    `Commit` / `commit_with_metadata` writes here; `fetch` /
//!    `fetch_metadata` reads here.
//! 2. The transactional **pending** layer keyed on `(group_id, pid)`
//!    — `store_pending` stages, `commit_pending` materialises into
//!    layer 1, `discard_pending` drops. Memory-only: an unfinished
//!    transaction reset on broker restart (matches Apache's "in-
//!    flight offsets aren't recovered" contract).
//! 3. The on-disk file. Lock is dropped before disk I/O — the
//!    "snap then write" pattern, so a concurrent `fetch` doesn't
//!    block on filesystem latency.

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::atomic_write::atomic_write_json;

/// Per-(topic, partition) request shape used by `fetch` /
/// `fetch_metadata`.
#[derive(Debug, Clone)]
pub struct FetchSpec {
    pub topic: String,
    pub partitions: Vec<i32>,
}

/// Build the canonical `"topic/partition"` cache + on-disk JSON key.
/// Handlers and tests must use this exact helper so wire-level
/// `OffsetDelete` (key 47) lookups round trip against the cache
/// (gh #100).
pub fn offset_key(topic: &str, partition: i32) -> String {
    format!("{topic}/{partition}")
}

/// gh #21 v2 envelope. Older v1 files store a plain
/// `HashMap<String, i64>`; the decoder falls back on parse failure.
#[derive(Debug, Serialize, Deserialize, Default)]
struct OffsetFileV2 {
    #[serde(default)]
    offsets: HashMap<String, i64>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    metadata: HashMap<String, String>,
}

#[derive(Debug, Default)]
struct Inner {
    /// `group → offset_key → offset`.
    cache: HashMap<String, HashMap<String, i64>>,
    /// `group → offset_key → metadata`. Mirrors `cache` shape; empty
    /// strings are stored as "no entry" so the wire null sentinel
    /// round-trips.
    metadata: HashMap<String, HashMap<String, String>>,
    /// gh #27 in-flight transactional offset commits keyed on
    /// `(group_id, producer_id)`. Memory-only.
    pending: HashMap<PendingKey, HashMap<String, i64>>,
}

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
struct PendingKey {
    group_id: String,
    producer_id: i64,
}

#[derive(Debug)]
pub struct OffsetStore {
    root: PathBuf,
    state: RwLock<Inner>,
}

impl OffsetStore {
    /// `root` is the cluster-state directory; group files land at
    /// `<root>/__consumer_offsets/<group>.json`.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            state: RwLock::new(Inner::default()),
        }
    }

    fn dir(&self) -> PathBuf {
        self.root.join("__consumer_offsets")
    }

    // --- pending (gh #27 transactional offsets) -----------------------

    /// Stage offsets from `TxnOffsetCommit` (key 28). They are NOT
    /// visible to `OffsetFetch` until `commit_pending` runs.
    pub fn store_pending(&self, group_id: &str, producer_id: i64, offsets: HashMap<String, i64>) {
        let key = PendingKey {
            group_id: group_id.to_owned(),
            producer_id,
        };
        let mut s = self.state.write();
        let slot = s.pending.entry(key).or_default();
        for (k, v) in offsets {
            slot.insert(k, v);
        }
    }

    /// Materialise the staged offsets for `(group, pid)` as committed.
    /// Called from the `EndTxn(commit)` handler. Idempotent.
    pub fn commit_pending(&self, group_id: &str, producer_id: i64) -> io::Result<()> {
        let key = PendingKey {
            group_id: group_id.to_owned(),
            producer_id,
        };
        let pending = {
            let mut s = self.state.write();
            s.pending.remove(&key)
        };
        match pending {
            None => Ok(()),
            Some(offsets) => self.commit(group_id, offsets),
        }
    }

    /// Drop staged offsets for `(group, pid)` without materialising.
    /// Called from `EndTxn(abort)`. Idempotent.
    pub fn discard_pending(&self, group_id: &str, producer_id: i64) {
        let key = PendingKey {
            group_id: group_id.to_owned(),
            producer_id,
        };
        self.state.write().pending.remove(&key);
    }

    /// Read-only snapshot of staged offsets for `(group, pid)`.
    /// Exposed for tests; production wires `commit_pending` /
    /// `discard_pending`. Returns `None` when no pending entry exists.
    pub fn pending_for(&self, group_id: &str, producer_id: i64) -> Option<HashMap<String, i64>> {
        let key = PendingKey {
            group_id: group_id.to_owned(),
            producer_id,
        };
        self.state.read().pending.get(&key).cloned()
    }

    // --- committed --------------------------------------------------

    /// Equivalent to `commit_with_metadata(group, offsets, &empty)`.
    /// Preserved for callers that don't carry metadata (txn commit
    /// path, internal compaction paths).
    pub fn commit(&self, group_id: &str, offsets: HashMap<String, i64>) -> io::Result<()> {
        self.commit_with_metadata(group_id, offsets, HashMap::new())
    }

    /// Atomically write the committed offsets for a group + an
    /// optional per-partition metadata string (gh #21). Empty
    /// metadata values clear the entry — round-trip back as the wire
    /// null sentinel.
    pub fn commit_with_metadata(
        &self,
        group_id: &str,
        offsets: HashMap<String, i64>,
        metadata: HashMap<String, String>,
    ) -> io::Result<()> {
        let (merged_offsets, merged_meta) = {
            let mut s = self.state.write();
            let cached = s.cache.entry(group_id.to_owned()).or_default();
            for (k, v) in offsets {
                cached.insert(k, v);
            }
            let cached_meta = s.metadata.entry(group_id.to_owned()).or_default();
            for (k, v) in metadata {
                if v.is_empty() {
                    cached_meta.remove(&k);
                } else {
                    cached_meta.insert(k, v);
                }
            }
            let off = s.cache.get(group_id).cloned().unwrap_or_default();
            let meta = s.metadata.get(group_id).cloned().unwrap_or_default();
            (off, meta)
        };

        let payload = OffsetFileV2 {
            offsets: merged_offsets,
            metadata: merged_meta,
        };
        let name = format!("{group_id}.json");
        atomic_write_json(&self.dir(), &name, &payload)
    }

    /// Committed offsets for the given `(topic, partitions[])` set.
    /// Returns `-1` for any partition without a committed offset.
    pub fn fetch(&self, group_id: &str, specs: &[FetchSpec]) -> HashMap<String, i64> {
        let s = self.state.read();
        let group = s.cache.get(group_id);
        let mut out = HashMap::new();
        for spec in specs {
            for &p in &spec.partitions {
                let k = offset_key(&spec.topic, p);
                let v = group.and_then(|g| g.get(&k)).copied().unwrap_or(-1);
                out.insert(k, v);
            }
        }
        out
    }

    /// Per-partition metadata blob committed alongside each offset
    /// (gh #21). Keys missing from the returned map have no metadata
    /// — the wire null sentinel.
    pub fn fetch_metadata(&self, group_id: &str, specs: &[FetchSpec]) -> HashMap<String, String> {
        let s = self.state.read();
        let group = match s.metadata.get(group_id) {
            None => return HashMap::new(),
            Some(g) => g,
        };
        let mut out = HashMap::new();
        for spec in specs {
            for &p in &spec.partitions {
                let k = offset_key(&spec.topic, p);
                if let Some(v) = group.get(&k) {
                    out.insert(k, v.clone());
                }
            }
        }
        out
    }

    /// Does the in-memory cache have any offsets for `group_id`?
    pub fn has_group(&self, group_id: &str) -> bool {
        self.state.read().cache.contains_key(group_id)
    }

    /// Drop a group's offsets from cache + disk. Idempotent — deleting
    /// an unknown group is `Ok(())` so partial-delete retries from
    /// AdminClient don't surface spurious errors.
    pub fn delete(&self, group_id: &str) -> io::Result<()> {
        {
            let mut s = self.state.write();
            s.cache.remove(group_id);
            s.metadata.remove(group_id);
        }
        let path = self.dir().join(format!("{group_id}.json"));
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Remove specific `(topic, partition)` offset entries from a
    /// group's committed offsets (gh #100 — `OffsetDelete` key 47).
    /// Returns the set of keys actually removed; absent keys are
    /// silently ignored — wire-level `UNKNOWN_TOPIC_OR_PARTITION`
    /// mapping is the handler's job.
    pub fn delete_partitions(
        &self,
        group_id: &str,
        keys: &[String],
    ) -> io::Result<HashMap<String, bool>> {
        let mut removed = HashMap::new();
        let snap_offsets = {
            let mut s = self.state.write();
            let group = match s.cache.get_mut(group_id) {
                None => return Ok(removed),
                Some(g) => g,
            };
            for k in keys {
                if group.remove(k).is_some() {
                    removed.insert(k.clone(), true);
                }
            }
            group.clone()
        };
        // Legacy plain-map shape on disk after a partition-delete keeps
        // the
        // forward-compat read path working from either side of the
        // cutover. New full Commits restamp the v2 envelope.
        let name = format!("{group_id}.json");
        atomic_write_json(&self.dir(), &name, &snap_offsets)?;
        Ok(removed)
    }

    /// Read a group's offsets from disk into the in-memory cache.
    /// Called when this broker becomes coordinator for the group.
    /// Tolerates both the gh #21 v2 envelope and the legacy v1 plain
    /// map.
    pub fn load(&self, group_id: &str) -> io::Result<()> {
        let path = self.dir().join(format!("{group_id}.json"));
        let data = match std::fs::read(&path) {
            Ok(d) => d,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
        };
        let (offsets, metadata) = decode_offsets_file(&data)?;
        let mut s = self.state.write();
        s.cache.insert(group_id.to_owned(), offsets);
        if let Some(m) = metadata {
            s.metadata.insert(group_id.to_owned(), m);
        }
        Ok(())
    }
}

/// Parse a `<group>.json` blob. Tries the v2 envelope first; falls
/// back to the legacy v1 plain `HashMap<String, i64>`. Returning
/// `None` metadata for the legacy shape lets `load` skip touching
/// `s.metadata` for unmigrated groups.
type OffsetsAndMetadata = (HashMap<String, i64>, Option<HashMap<String, String>>);

fn decode_offsets_file(data: &[u8]) -> io::Result<OffsetsAndMetadata> {
    if let Ok(v2) = serde_json::from_slice::<OffsetFileV2>(data) {
        if !v2.offsets.is_empty() || !v2.metadata.is_empty() {
            let meta = if v2.metadata.is_empty() {
                None
            } else {
                Some(v2.metadata)
            };
            return Ok((v2.offsets, meta));
        }
    }
    let v1: HashMap<String, i64> = serde_json::from_slice(data).map_err(io::Error::other)?;
    Ok((v1, None))
}

/// gh #223 boot-time adoption of the pre-fix layout: offsets used to
/// live at `<data_dir>/__consumer_offsets`, a sibling of the topic
/// dirs, where the operator's orphan-topic sweep deleted them every
/// pass. Move a surviving legacy dir under the cluster-state root.
///
/// `rename` first (atomic on the same mount — the default layout);
/// on `EXDEV` (cluster dir on its own volume) fall back to
/// copy-then-remove, per-file, resumable: a crash mid-copy leaves
/// both dirs and the next boot re-runs the copy idempotently
/// (`<group>.json` files are only ever whole-file replaced).
pub fn migrate_legacy_offsets_dir(data_dir: &std::path::Path, cluster_dir: &std::path::Path) {
    let legacy = data_dir.join("__consumer_offsets");
    let target = cluster_dir.join("__consumer_offsets");
    if !legacy.is_dir() || legacy == target {
        return;
    }
    if !target.exists() {
        match std::fs::rename(&legacy, &target) {
            Ok(()) => {
                tracing::info!(from = %legacy.display(), to = %target.display(),
                    "migrated legacy consumer-offsets dir (gh #223)");
                return;
            }
            Err(e) if e.kind() == io::ErrorKind::CrossesDevices => {}
            Err(e) => {
                tracing::warn!(%e, from = %legacy.display(),
                    "legacy consumer-offsets migration failed; leaving in place");
                return;
            }
        }
    }
    // Cross-device (or a half-migrated target from a previous crash):
    // copy file-by-file, never overwriting a file the new store may
    // already have rewritten, then drop the legacy dir.
    if let Err(e) = copy_offsets_dir(&legacy, &target).and_then(|()| std::fs::remove_dir_all(&legacy))
    {
        tracing::warn!(%e, from = %legacy.display(),
            "legacy consumer-offsets copy-migration incomplete; will retry next boot");
    } else {
        tracing::info!(from = %legacy.display(), to = %target.display(),
            "copy-migrated legacy consumer-offsets dir (gh #223)");
    }
}

fn copy_offsets_dir(from: &std::path::Path, to: &std::path::Path) -> io::Result<()> {
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)?.flatten() {
        let dst = to.join(entry.file_name());
        if entry.file_type()?.is_file() && !dst.exists() {
            std::fs::copy(entry.path(), &dst)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(dir: &std::path::Path) -> OffsetStore {
        OffsetStore::new(dir)
    }

    fn one(topic: &str, partition: i32, offset: i64) -> HashMap<String, i64> {
        let mut m = HashMap::new();
        m.insert(offset_key(topic, partition), offset);
        m
    }

    #[test]
    fn commit_then_fetch_roundtrips() {
        let tmp = tempfile::tempdir().unwrap();
        let s = store(tmp.path());
        s.commit("g1", one("t1", 0, 42)).unwrap();
        let got = s.fetch(
            "g1",
            &[FetchSpec {
                topic: "t1".to_owned(),
                partitions: vec![0, 1],
            }],
        );
        assert_eq!(got.get("t1/0"), Some(&42));
        // unknown partition → -1 sentinel
        assert_eq!(got.get("t1/1"), Some(&-1));
    }

    #[test]
    fn commit_with_metadata_persists() {
        let tmp = tempfile::tempdir().unwrap();
        let s = store(tmp.path());
        let mut md = HashMap::new();
        md.insert(offset_key("t1", 0), "consumer-1".to_owned());
        s.commit_with_metadata("g1", one("t1", 0, 42), md).unwrap();
        let got_md = s.fetch_metadata(
            "g1",
            &[FetchSpec {
                topic: "t1".to_owned(),
                partitions: vec![0],
            }],
        );
        assert_eq!(got_md.get("t1/0").map(String::as_str), Some("consumer-1"));
    }

    #[test]
    fn empty_metadata_clears_entry() {
        let tmp = tempfile::tempdir().unwrap();
        let s = store(tmp.path());
        let mut md = HashMap::new();
        md.insert(offset_key("t1", 0), "tag".to_owned());
        s.commit_with_metadata("g1", one("t1", 0, 1), md.clone())
            .unwrap();
        // Empty string clears it.
        md.insert(offset_key("t1", 0), String::new());
        s.commit_with_metadata("g1", one("t1", 0, 2), md).unwrap();
        let got_md = s.fetch_metadata(
            "g1",
            &[FetchSpec {
                topic: "t1".to_owned(),
                partitions: vec![0],
            }],
        );
        assert!(got_md.is_empty());
    }

    #[test]
    fn load_reads_v2_envelope_from_disk() {
        let tmp = tempfile::tempdir().unwrap();
        let s1 = store(tmp.path());
        s1.commit("g1", one("t1", 0, 7)).unwrap();

        let s2 = store(tmp.path());
        s2.load("g1").unwrap();
        let got = s2.fetch(
            "g1",
            &[FetchSpec {
                topic: "t1".to_owned(),
                partitions: vec![0],
            }],
        );
        assert_eq!(got.get("t1/0"), Some(&7));
    }

    #[test]
    fn load_reads_legacy_v1_plain_map() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("__consumer_offsets");
        std::fs::create_dir_all(&dir).unwrap();
        // Pre-#21 plain-map shape.
        std::fs::write(dir.join("g1.json"), r#"{"t1/0": 99}"#).unwrap();
        let s = store(tmp.path());
        s.load("g1").unwrap();
        let got = s.fetch(
            "g1",
            &[FetchSpec {
                topic: "t1".to_owned(),
                partitions: vec![0],
            }],
        );
        assert_eq!(got.get("t1/0"), Some(&99));
    }

    #[test]
    fn delete_drops_cache_and_file() {
        let tmp = tempfile::tempdir().unwrap();
        let s = store(tmp.path());
        s.commit("g1", one("t1", 0, 1)).unwrap();
        let path = tmp.path().join("__consumer_offsets/g1.json");
        assert!(path.exists());
        s.delete("g1").unwrap();
        assert!(!path.exists());
        assert!(!s.has_group("g1"));
        // Idempotent on missing group.
        s.delete("g1").unwrap();
    }

    #[test]
    fn delete_partitions_removes_only_requested_keys() {
        let tmp = tempfile::tempdir().unwrap();
        let s = store(tmp.path());
        let mut both = HashMap::new();
        both.insert(offset_key("t1", 0), 10);
        both.insert(offset_key("t1", 1), 20);
        s.commit("g1", both).unwrap();
        let removed = s
            .delete_partitions("g1", &[offset_key("t1", 0), offset_key("t1", 99)])
            .unwrap();
        assert_eq!(removed.len(), 1);
        assert_eq!(removed.get("t1/0"), Some(&true));
        let got = s.fetch(
            "g1",
            &[FetchSpec {
                topic: "t1".to_owned(),
                partitions: vec![0, 1],
            }],
        );
        assert_eq!(got.get("t1/0"), Some(&-1));
        assert_eq!(got.get("t1/1"), Some(&20));
    }

    #[test]
    fn pending_invisible_to_fetch_until_commit_pending() {
        let tmp = tempfile::tempdir().unwrap();
        let s = store(tmp.path());
        s.store_pending("g1", 100, one("t1", 0, 555));
        let got = s.fetch(
            "g1",
            &[FetchSpec {
                topic: "t1".to_owned(),
                partitions: vec![0],
            }],
        );
        assert_eq!(got.get("t1/0"), Some(&-1));
        s.commit_pending("g1", 100).unwrap();
        let got = s.fetch(
            "g1",
            &[FetchSpec {
                topic: "t1".to_owned(),
                partitions: vec![0],
            }],
        );
        assert_eq!(got.get("t1/0"), Some(&555));
    }

    #[test]
    fn discard_pending_drops_unmaterialised_offsets() {
        let tmp = tempfile::tempdir().unwrap();
        let s = store(tmp.path());
        s.store_pending("g1", 100, one("t1", 0, 555));
        s.discard_pending("g1", 100);
        s.commit_pending("g1", 100).unwrap(); // no-op, idempotent
        let got = s.fetch(
            "g1",
            &[FetchSpec {
                topic: "t1".to_owned(),
                partitions: vec![0],
            }],
        );
        assert_eq!(got.get("t1/0"), Some(&-1));
    }

    /// gh #223: a pre-fix `<data_dir>/__consumer_offsets` dir is
    /// adopted under the cluster dir on boot, and the store then
    /// serves the migrated offsets.
    #[test]
    fn migrates_legacy_offsets_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path();
        let cluster_dir = data_dir.join("__cluster");
        std::fs::create_dir_all(&cluster_dir).unwrap();
        let legacy = data_dir.join("__consumer_offsets");
        std::fs::create_dir_all(&legacy).unwrap();
        std::fs::write(legacy.join("g1.json"), r#"{"t1/0": 42}"#).unwrap();

        migrate_legacy_offsets_dir(data_dir, &cluster_dir);
        assert!(!legacy.exists());
        assert!(cluster_dir.join("__consumer_offsets/g1.json").exists());

        let s = store(&cluster_dir);
        s.load("g1").unwrap();
        let got = s.fetch(
            "g1",
            &[FetchSpec {
                topic: "t1".to_owned(),
                partitions: vec![0],
            }],
        );
        assert_eq!(got.get("t1/0"), Some(&42));

        // Idempotent: a second run with nothing to do is a no-op.
        migrate_legacy_offsets_dir(data_dir, &cluster_dir);
        assert!(cluster_dir.join("__consumer_offsets/g1.json").exists());
    }

    /// gh #223: a half-migrated target (crash between copy and
    /// remove) merges without clobbering files the new store already
    /// rewrote.
    #[test]
    fn legacy_migration_merges_without_clobbering() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path();
        let cluster_dir = data_dir.join("__cluster");
        let target = cluster_dir.join("__consumer_offsets");
        std::fs::create_dir_all(&target).unwrap();
        std::fs::write(target.join("g1.json"), r#"{"t1/0": 99}"#).unwrap();
        let legacy = data_dir.join("__consumer_offsets");
        std::fs::create_dir_all(&legacy).unwrap();
        std::fs::write(legacy.join("g1.json"), r#"{"t1/0": 1}"#).unwrap();
        std::fs::write(legacy.join("g2.json"), r#"{"t2/0": 7}"#).unwrap();

        migrate_legacy_offsets_dir(data_dir, &cluster_dir);
        assert!(!legacy.exists());
        // g1 kept the newer (target) copy; g2 was carried over.
        let s = store(&cluster_dir);
        s.load("g1").unwrap();
        s.load("g2").unwrap();
        assert_eq!(
            s.fetch(
                "g1",
                &[FetchSpec {
                    topic: "t1".to_owned(),
                    partitions: vec![0]
                }]
            )
            .get("t1/0"),
            Some(&99)
        );
        assert_eq!(
            s.fetch(
                "g2",
                &[FetchSpec {
                    topic: "t2".to_owned(),
                    partitions: vec![0]
                }]
            )
            .get("t2/0"),
            Some(&7)
        );
    }
}
