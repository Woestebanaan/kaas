//! Per-partition manifest: `manifest.json`.
//!
//! The manifest is the
//! source of truth for `(epoch, high_watermark, log_start_offset)` on
//! partition open; segment scanning is the fallback when the manifest
//! is missing or unreadable.
//!
//! On TakeOver, segment roll, and partition close, the manifest is
//! rewritten so the next open is fast. The write uses
//! [`crate::atomic_write::atomic_write_json`] — tmp + fsync + rename
//! in the same directory, matching NFSv4 same-dir rename atomicity.
//!
//! # On-disk compatibility
//!
//! The JSON layout is pinned to the v0.1 output verbatim:
//!
//! ```json
//! {"epoch":5,"highWatermark":1000,"logStartOffset":0}
//! ```
//!
//! camelCase via serde rename rules; field order matches the v0.1
//! declaration order so the byte
//! diff against a captured fixture passes.

use std::io;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::atomic_write::atomic_write_json;
use crate::fs::Fs;

pub const MANIFEST_FILENAME: &str = "manifest.json";

/// Pre-Phase-4 single-field file. `readManifest` falls back to this
/// when `manifest.json` is missing so partitions opened with an older
/// kaas don't lose their epoch.
pub const LEGACY_EPOCH_FILENAME: &str = ".leader-epoch";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub epoch: i64,
    #[serde(rename = "highWatermark")]
    pub high_watermark: i64,
    #[serde(rename = "logStartOffset")]
    pub log_start_offset: i64,
}

/// Read result. Distinguishes "no manifest at all" from
/// "manifest read OK" so callers can fall back to a segment scan.
#[derive(Debug)]
pub enum ReadResult {
    /// No `manifest.json` and no legacy `.leader-epoch` — fresh
    /// partition or rmrf'd. Caller proceeds with HWM=0, epoch=0.
    NotFound,
    /// Read from `manifest.json`.
    Present(Manifest),
    /// Read from the legacy single-field file. HWM and
    /// `log_start_offset` are 0 placeholders; the caller is expected
    /// to fill them in from a segment scan.
    Legacy(Manifest),
}

/// Read the manifest for `dir`. Falls back to the legacy single-field
/// file when `manifest.json` is absent. Both files absent → `NotFound`.
pub fn read(fs: &dyn Fs, dir: &Path) -> Result<ReadResult, ManifestError> {
    let path = dir.join(MANIFEST_FILENAME);
    match fs.open_read(&path) {
        Ok(mut f) => {
            let mut buf = Vec::new();
            io::Read::read_to_end(&mut f, &mut buf)?;
            let m: Manifest = serde_json::from_slice(&buf)?;
            Ok(ReadResult::Present(m))
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            read_legacy_epoch(fs, dir).map(|opt| match opt {
                Some(m) => ReadResult::Legacy(m),
                None => ReadResult::NotFound,
            })
        }
        Err(e) => Err(e.into()),
    }
}

fn read_legacy_epoch(fs: &dyn Fs, dir: &Path) -> Result<Option<Manifest>, ManifestError> {
    let path = dir.join(LEGACY_EPOCH_FILENAME);
    match fs.open_read(&path) {
        Ok(mut f) => {
            let mut buf = Vec::new();
            io::Read::read_to_end(&mut f, &mut buf)?;
            if buf.len() < 8 {
                return Err(ManifestError::LegacyEpochTruncated { got: buf.len() });
            }
            let mut epoch_bytes = [0u8; 8];
            epoch_bytes.copy_from_slice(&buf[0..8]);
            let epoch = i64::from_be_bytes(epoch_bytes);
            Ok(Some(Manifest {
                epoch,
                high_watermark: 0,
                log_start_offset: 0,
            }))
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Atomically write the manifest. Best-effort drops the legacy
/// `.leader-epoch` file once the new manifest is authoritative.
pub fn write(fs: &dyn Fs, dir: &Path, m: &Manifest) -> Result<(), ManifestError> {
    atomic_write_json(fs, dir, MANIFEST_FILENAME, m)?;
    // Drop the legacy file. Failure isn't fatal — the migration path
    // in `read` prefers manifest.json over the legacy file anyway.
    let _ = fs.remove(&dir.join(LEGACY_EPOCH_FILENAME));
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("legacy .leader-epoch file truncated: {got} bytes (want 8)")]
    LegacyEpochTruncated { got: usize },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::RealFs;
    use std::io::Write;

    #[test]
    fn json_matches_v01_camelcase_layout() {
        // manifest.json bytes are pinned to the v0.1 layout.
        let m = Manifest {
            epoch: 5,
            high_watermark: 1000,
            log_start_offset: 0,
        };
        let json = serde_json::to_string(&m).unwrap();
        assert_eq!(
            json, r#"{"epoch":5,"highWatermark":1000,"logStartOffset":0}"#,
            "manifest JSON shape diverged from the pinned v0.1 layout"
        );
    }

    #[test]
    fn roundtrip_via_atomic_write() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        let m = Manifest {
            epoch: 7,
            high_watermark: 42,
            log_start_offset: 10,
        };
        write(&fs, tmp.path(), &m).unwrap();
        let got = match read(&fs, tmp.path()).unwrap() {
            ReadResult::Present(got) => got,
            other => unreachable!("expected Present, got {:?}", other),
        };
        assert_eq!(got, m);
    }

    #[test]
    fn missing_manifest_returns_not_found() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        assert!(matches!(
            read(&fs, tmp.path()).unwrap(),
            ReadResult::NotFound
        ));
    }

    #[test]
    fn legacy_epoch_file_is_migrated_on_read() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        let legacy_path = tmp.path().join(LEGACY_EPOCH_FILENAME);
        {
            let mut f = fs.create(&legacy_path).unwrap();
            f.write_all(&12i64.to_be_bytes()).unwrap();
        }
        let m = match read(&fs, tmp.path()).unwrap() {
            ReadResult::Legacy(m) => m,
            other => unreachable!("expected Legacy, got {:?}", other),
        };
        assert_eq!(m.epoch, 12);
        assert_eq!(m.high_watermark, 0);
        assert_eq!(m.log_start_offset, 0);
    }

    #[test]
    fn write_removes_legacy_epoch_file() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        let legacy_path = tmp.path().join(LEGACY_EPOCH_FILENAME);
        {
            let mut f = fs.create(&legacy_path).unwrap();
            f.write_all(&3i64.to_be_bytes()).unwrap();
        }
        write(
            &fs,
            tmp.path(),
            &Manifest {
                epoch: 4,
                high_watermark: 0,
                log_start_offset: 0,
            },
        )
        .unwrap();
        assert!(!fs.exists(&legacy_path), "legacy file should be removed");
        assert!(fs.exists(&tmp.path().join(MANIFEST_FILENAME)));
    }

    #[test]
    fn truncated_legacy_file_is_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        {
            let mut f = fs.create(&tmp.path().join(LEGACY_EPOCH_FILENAME)).unwrap();
            f.write_all(&[0, 0, 0, 0]).unwrap();
        }
        let err = read(&fs, tmp.path()).unwrap_err();
        assert!(matches!(
            err,
            ManifestError::LegacyEpochTruncated { got: 4 }
        ));
    }
}
