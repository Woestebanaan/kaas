//! Per-partition recovery checkpoint: `recovery-checkpoint.json`.
//!
//! Kafka's recovery-point idea, per partition: a durable marker that
//! says "in the active segment, everything up to `byte_pos` is fsynced
//! and the log-end-offset there is `high_watermark`." On takeover,
//! recovery scans forward from `byte_pos` instead of from the segment
//! start — O(bytes since the checkpoint) instead of O(active segment).
//! A clean close writes it at EOF, so a graceful restart re-scans
//! nothing.
//!
//! It is only a hint. If it is missing, stale, or refers to a
//! *different* active segment (a roll happened since it was written),
//! recovery ignores it and falls back to a full scan of the current
//! active segment — always correct, and with a bounded `segment.bytes`,
//! cheap. So the file is never load-bearing for correctness; it goes
//! through [`atomic_write_json`] anyway (NFS-substrate rule 1) so a
//! torn write is never even read.

use std::io;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::atomic_write::atomic_write_json;
use crate::fs::Fs;

pub const RECOVERY_CHECKPOINT_FILENAME: &str = "recovery-checkpoint.json";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecoveryCheckpoint {
    /// `base_offset` of the active segment this checkpoint refers to.
    /// Recovery discards the checkpoint when the current active segment
    /// has a different base — i.e. a roll happened since it was written.
    pub segment_base: i64,
    /// Byte offset within the active segment's log file up to which the
    /// data is fsynced. Recovery seeks here and scans forward.
    pub byte_pos: i64,
    /// Log-end-offset (high-watermark) as of `byte_pos`. Seeds the
    /// forward scan's running maximum.
    pub high_watermark: i64,
}

/// Read the checkpoint. `None` when the file is absent or unreadable —
/// recovery then falls back to a full scan, so a missing or corrupt
/// checkpoint is never a correctness problem.
#[must_use]
pub fn read(fs: &dyn Fs, dir: &Path) -> Option<RecoveryCheckpoint> {
    let path = dir.join(RECOVERY_CHECKPOINT_FILENAME);
    let mut f = fs.open_read(&path).ok()?;
    let mut buf = Vec::new();
    io::Read::read_to_end(&mut f, &mut buf).ok()?;
    serde_json::from_slice(&buf).ok()
}

/// Write the checkpoint atomically (tmp + fsync + rename).
pub fn write(fs: &dyn Fs, dir: &Path, cp: &RecoveryCheckpoint) -> io::Result<()> {
    atomic_write_json(fs, dir, RECOVERY_CHECKPOINT_FILENAME, cp)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::RealFs;

    #[test]
    fn missing_checkpoint_reads_none() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(read(&RealFs::new(), tmp.path()).is_none());
    }

    #[test]
    fn write_then_read_roundtrips() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        let cp = RecoveryCheckpoint {
            segment_base: 100,
            byte_pos: 4096,
            high_watermark: 250,
        };
        write(&fs, tmp.path(), &cp).unwrap();
        assert_eq!(read(&fs, tmp.path()), Some(cp));
    }

    #[test]
    fn corrupt_checkpoint_reads_none() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            tmp.path().join(RECOVERY_CHECKPOINT_FILENAME),
            b"not json at all",
        )
        .unwrap();
        assert!(read(&RealFs::new(), tmp.path()).is_none());
    }
}
