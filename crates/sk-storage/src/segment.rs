//! Segment filename helpers and the on-disk [`SegmentMeta`] struct.
//!
//! Phase 2 initial slice ports only the structural primitives. The full
//! segment file I/O — `create_segment`, `open_handles`, `append_batch`,
//! `roll_fast`, recovery — lands in the workstream-B follow-up commit.
//!
//! # Filenames
//!
//! Each segment is a pair of files in the partition directory:
//!
//! - `{epoch:08x}-{base_offset:020d}.log`
//! - `{epoch:08x}-{base_offset:020d}.index`
//!
//! Encoding the leader epoch in the filename prevents a stale ex-leader's
//! writes from colliding with a new leader's file (the gh #75 / gh #76
//! single-writer story). Pre-Phase-4 partitions on disk use the legacy
//! unprefixed form `{base_offset:020d}.log`; [`parse_segment_stem`]
//! accepts both so an in-place upgrade Just Works.
//!
//! The epoch is rendered as an 8-char `%08x` hex of the **lower 32 bits**
//! of the epoch — Go uses `uint32(epoch)`. Phase 2 mirrors that exactly.

use std::path::{Path, PathBuf};

/// `.log` filename suffix.
pub const LOG_EXT: &str = ".log";

/// `.index` filename suffix.
pub const INDEX_EXT: &str = ".index";

/// On-disk pointer to a single segment. Owned by the partition's
/// closed-segment list and (via the active variant landed later) the
/// active segment too.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentMeta {
    pub base_offset: i64,
    pub epoch: i64,
    /// Log file size in bytes. `0` is a valid value at creation;
    /// callers update this after appends.
    pub size: u64,
    pub log_path: PathBuf,
    pub index_path: PathBuf,
}

/// Construct the `.log` path for `(dir, base_offset, epoch)`.
pub fn segment_log_path(dir: &Path, base_offset: i64, epoch: i64) -> PathBuf {
    let stem = epoch_prefixed_stem(base_offset, epoch);
    dir.join(format!("{stem}{LOG_EXT}"))
}

/// Construct the `.index` path matching [`segment_log_path`].
pub fn segment_index_path(dir: &Path, base_offset: i64, epoch: i64) -> PathBuf {
    let stem = epoch_prefixed_stem(base_offset, epoch);
    dir.join(format!("{stem}{INDEX_EXT}"))
}

/// Legacy pre-Phase-4 path with no epoch prefix. Kept for the
/// migration-fixture tests; new writes always use [`segment_log_path`].
pub fn legacy_segment_log_path(dir: &Path, base_offset: i64) -> PathBuf {
    dir.join(format!("{base_offset:020}{LOG_EXT}"))
}

/// Recover `(base_offset, epoch)` from a stem (filename without `.log` /
/// `.index` suffix). Accepts both layouts:
///
/// - Epoch-prefixed: `"00000005-00000000000000000123"` → `(123, 5)`
/// - Legacy: `"00000000000000000123"` → `(123, 0)`
///
/// Returns `None` for any other shape; the caller (segment listing) skips
/// unknown files.
pub fn parse_segment_stem(stem: &str) -> Option<(i64, i64)> {
    if let Some((epoch_part, base_part)) = stem.split_once('-') {
        let epoch = u32::from_str_radix(epoch_part, 16).ok()?;
        let base = base_part.parse::<i64>().ok()?;
        Some((base, i64::from(epoch)))
    } else {
        let base = stem.parse::<i64>().ok()?;
        Some((base, 0))
    }
}

fn epoch_prefixed_stem(base_offset: i64, epoch: i64) -> String {
    // Go uses uint32(epoch) — the lower 32 bits — for the filename
    // prefix. Re-do that reduction without an `as` cast by going
    // through `to_le_bytes` reinterpret.
    let epoch_u32 = i64_low_u32(epoch);
    format!("{epoch_u32:08x}-{base_offset:020}")
}

fn i64_low_u32(v: i64) -> u32 {
    let bytes = v.to_le_bytes();
    let mut lo = [0u8; 4];
    lo.copy_from_slice(&bytes[..4]);
    u32::from_le_bytes(lo)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_prefixed_log_path_matches_go_format() {
        // Go: filepath.Join(dir, fmt.Sprintf("%08x-%020d.log", uint32(epoch), baseOffset))
        let dir = Path::new("/data/topic-a/0");
        let p = segment_log_path(dir, 123, 5);
        assert_eq!(
            p.to_str().unwrap(),
            "/data/topic-a/0/00000005-00000000000000000123.log"
        );
    }

    #[test]
    fn epoch_prefixed_index_path_matches_go_format() {
        let dir = Path::new("/data/topic-a/0");
        let p = segment_index_path(dir, 123, 5);
        assert_eq!(
            p.to_str().unwrap(),
            "/data/topic-a/0/00000005-00000000000000000123.index"
        );
    }

    #[test]
    fn legacy_log_path_has_no_epoch_prefix() {
        let dir = Path::new("/data/topic-a/0");
        let p = legacy_segment_log_path(dir, 999);
        assert_eq!(
            p.to_str().unwrap(),
            "/data/topic-a/0/00000000000000000999.log"
        );
    }

    #[test]
    fn parse_epoch_prefixed_stem() {
        assert_eq!(
            parse_segment_stem("00000005-00000000000000000123"),
            Some((123, 5))
        );
    }

    #[test]
    fn parse_legacy_stem_yields_zero_epoch() {
        assert_eq!(parse_segment_stem("00000000000000000123"), Some((123, 0)));
    }

    #[test]
    fn parse_zero_base_offset() {
        assert_eq!(
            parse_segment_stem("00000000-00000000000000000000"),
            Some((0, 0))
        );
        assert_eq!(parse_segment_stem("00000000000000000000"), Some((0, 0)));
    }

    #[test]
    fn parse_rejects_garbage() {
        assert_eq!(parse_segment_stem("not-a-number"), None);
        assert_eq!(parse_segment_stem("XYZ-123"), None);
        assert_eq!(parse_segment_stem(""), None);
        // Three components — should fail (we use split_once, so the
        // second '-' falls into the base-offset string and that parse
        // rejects it).
        assert_eq!(parse_segment_stem("00000001-12-34"), None);
    }

    #[test]
    fn round_trip_path_then_parse() {
        let dir = Path::new("/d");
        let max_u32_as_i64: i64 = i64::from(u32::MAX);
        for (base, epoch) in &[(0i64, 0i64), (1, 0), (123, 5), (i64::MAX, max_u32_as_i64)] {
            let p = segment_log_path(dir, *base, *epoch);
            let stem = p.file_stem().unwrap().to_str().unwrap();
            // Reduce expected epoch through the same uint32 truncation Go does.
            let expected_epoch = i64::from(i64_low_u32(*epoch));
            assert_eq!(parse_segment_stem(stem), Some((*base, expected_epoch)));
        }
    }

    #[test]
    fn segment_meta_construction() {
        let dir = Path::new("/d");
        let meta = SegmentMeta {
            base_offset: 100,
            epoch: 3,
            size: 0,
            log_path: segment_log_path(dir, 100, 3),
            index_path: segment_index_path(dir, 100, 3),
        };
        assert!(meta
            .log_path
            .to_str()
            .unwrap()
            .ends_with("00000003-00000000000000000100.log"));
        assert_eq!(meta.base_offset, 100);
        assert_eq!(meta.epoch, 3);
    }
}
