//! `tmp + fsync + rename` writer for partition metadata files.
//!
//! Direct port of the dance in
//! `archive/internal/storage/manifest.go:78-119`. NFSv4 guarantees
//! that a same-directory rename is atomic, so a crash mid-write
//! leaves either the old file or the new one — never a torn JSON
//! payload.
//!
//! All three persisted JSON files in skafka go through here:
//! `manifest.json`, `producer-state.snapshot`, and the operator-
//! written `.config.json`.

use std::io::{self, Write};
use std::path::Path;

use serde::Serialize;

use crate::fs::{fsync_path, Fs};

/// Atomically write a serde-serialisable payload to `dir/name`.
///
/// 1. `mkdir_all(dir)`
/// 2. JSON-encode payload
/// 3. Write into `dir/<name>.tmp`
/// 4. `fsync` the tmp file
/// 5. `rename(<name>.tmp, <name>)`
///
/// On any failure after the tmp file is opened, the tmp file is
/// removed before the error is propagated so a partial write never
/// leaks. Mirrors the cleanup branches in Go's `writeManifest`.
pub fn atomic_write_json<T: Serialize>(
    fs: &dyn Fs,
    dir: &Path,
    name: &str,
    payload: &T,
) -> io::Result<()> {
    fs.mkdir_all(dir)?;
    let data = serde_json::to_vec(payload).map_err(io::Error::other)?;

    let mut tmp_name = String::from(name);
    tmp_name.push_str(".tmp");
    let tmp_path = dir.join(&tmp_name);
    let final_path = dir.join(name);

    // Step 3: write the bytes.
    {
        let mut f = fs.create(&tmp_path)?;
        if let Err(e) = f.write_all(&data) {
            let _ = fs.remove(&tmp_path);
            return Err(e);
        }
        if let Err(e) = f.flush() {
            let _ = fs.remove(&tmp_path);
            return Err(e);
        }
    }

    // Step 4: fsync the tmp file via the path-based helper. The trait-
    // object route (`Fs::fsync`) is intentionally no-op on `RealFs`
    // because the trait hides the concrete `std::fs::File`. See the
    // comment above `fs::fsync_path`.
    if let Err(e) = fsync_path(&tmp_path) {
        let _ = fs.remove(&tmp_path);
        return Err(e);
    }

    // Step 5: rename.
    if let Err(e) = fs.rename(&tmp_path, &final_path) {
        let _ = fs.remove(&tmp_path);
        return Err(e);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::RealFs;
    use serde::Deserialize;
    use std::io::Read;

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    struct Sample {
        a: i64,
        b: String,
    }

    #[test]
    fn roundtrip_writes_and_reads() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        let payload = Sample {
            a: 42,
            b: "skafka".into(),
        };
        atomic_write_json(&fs, tmp.path(), "thing.json", &payload).unwrap();

        let mut buf = String::new();
        fs.open_read(&tmp.path().join("thing.json"))
            .unwrap()
            .read_to_string(&mut buf)
            .unwrap();
        let got: Sample = serde_json::from_str(&buf).unwrap();
        assert_eq!(got, payload);
    }

    #[test]
    fn replacing_an_existing_file_is_atomic_in_observable_state() {
        // After atomic_write_json returns successfully, the destination
        // file exists and the .tmp file does not.
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        atomic_write_json(
            &fs,
            tmp.path(),
            "x.json",
            &Sample {
                a: 1,
                b: "v1".into(),
            },
        )
        .unwrap();
        atomic_write_json(
            &fs,
            tmp.path(),
            "x.json",
            &Sample {
                a: 2,
                b: "v2".into(),
            },
        )
        .unwrap();

        assert!(fs.exists(&tmp.path().join("x.json")));
        assert!(!fs.exists(&tmp.path().join("x.json.tmp")));

        let mut buf = String::new();
        fs.open_read(&tmp.path().join("x.json"))
            .unwrap()
            .read_to_string(&mut buf)
            .unwrap();
        let got: Sample = serde_json::from_str(&buf).unwrap();
        assert_eq!(
            got,
            Sample {
                a: 2,
                b: "v2".into()
            }
        );
    }

    #[test]
    fn mkdir_all_creates_parent_directories() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        let nested = tmp.path().join("topic-a/partition-0");
        atomic_write_json(
            &fs,
            &nested,
            "manifest.json",
            &Sample {
                a: 5,
                b: "deep".into(),
            },
        )
        .unwrap();
        assert!(fs.exists(&nested.join("manifest.json")));
    }
}
