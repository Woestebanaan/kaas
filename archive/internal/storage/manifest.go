package storage

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// Manifest is the per-partition metadata file (`manifest.json`) defined by the
// v3.3 plan §"Per-partition manifest". Three fields, atomic tmp + rename
// writes:
//
//	{
//	  "epoch": 5,
//	  "highWatermark": 1000,
//	  "logStartOffset": 0
//	}
//
// On partition open, the manifest is the source of truth for HWM and
// logStartOffset; segment scanning happens only as a fallback when the
// manifest is missing or unreadable. On TakeOver, segment roll, and
// partition close, the manifest is rewritten so the next open is fast.
//
// The manifest replaces the legacy `.leader-epoch` 8-byte file. A one-shot
// migration in readManifest() reads the legacy file when manifest.json is
// missing so partitions opened with an older skafka don't lose their epoch.
type Manifest struct {
	Epoch          int64 `json:"epoch"`
	HighWatermark  int64 `json:"highWatermark"`
	LogStartOffset int64 `json:"logStartOffset"`
}

const manifestFilename = "manifest.json"
const legacyEpochFilename = ".leader-epoch"

// readManifest loads the partition manifest from dir. Returns os.ErrNotExist
// (wrapped) when neither manifest.json nor the legacy .leader-epoch file
// exists; the caller is expected to fall back to a segment scan.
func readManifest(dir string) (*Manifest, error) {
	path := filepath.Join(dir, manifestFilename)
	data, err := os.ReadFile(path)
	if err == nil {
		var m Manifest
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, fmt.Errorf("manifest: parse %s: %w", path, err)
		}
		return &m, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}

	// Migration from the legacy single-field .leader-epoch file. We keep the
	// epoch and let the caller fill in HWM / logStartOffset from a segment scan.
	legacyPath := filepath.Join(dir, legacyEpochFilename)
	legacyData, lerr := os.ReadFile(legacyPath)
	if lerr != nil {
		if errors.Is(lerr, fs.ErrNotExist) {
			return nil, fs.ErrNotExist
		}
		return nil, lerr
	}
	if len(legacyData) < 8 {
		return nil, fmt.Errorf("manifest: legacy .leader-epoch too short: %d bytes", len(legacyData))
	}
	return &Manifest{
		Epoch: int64(binary.BigEndian.Uint64(legacyData[0:8])),
	}, nil
}

// writeManifest atomically writes the manifest via tmp + rename in the same
// directory (NFSv4 guarantees same-directory rename atomicity). The tmp file
// is fsync'd before rename so a crash mid-write leaves no torn JSON.
func writeManifest(dir string, m *Manifest) error {
	if err := os.MkdirAll(dir, 0o775); err != nil {
		return err
	}

	data, err := json.Marshal(m)
	if err != nil {
		return err
	}

	tmpPath := filepath.Join(dir, manifestFilename+".tmp")
	finalPath := filepath.Join(dir, manifestFilename)

	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}

	// Best-effort: drop the legacy single-field file once the manifest is
	// authoritative. Failures here are not fatal — the migration path in
	// readManifest prefers manifest.json over .leader-epoch anyway.
	_ = os.Remove(filepath.Join(dir, legacyEpochFilename))
	return nil
}
