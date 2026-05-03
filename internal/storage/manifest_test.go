package storage

import (
	"encoding/binary"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

func TestManifestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	in := &Manifest{Epoch: 7, HighWatermark: 1234, LogStartOffset: 100}
	if err := writeManifest(dir, in); err != nil {
		t.Fatalf("writeManifest: %v", err)
	}

	out, err := readManifest(dir)
	if err != nil {
		t.Fatalf("readManifest: %v", err)
	}
	if *out != *in {
		t.Errorf("round trip: got %+v want %+v", out, in)
	}
}

func TestManifestMissingReturnsErrNotExist(t *testing.T) {
	dir := t.TempDir()
	_, err := readManifest(dir)
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("expected fs.ErrNotExist, got %v", err)
	}
}

func TestManifestMigrationFromLegacyLeaderEpoch(t *testing.T) {
	dir := t.TempDir()

	// Lay down the legacy 8-byte big-endian epoch file.
	legacy := make([]byte, 8)
	binary.BigEndian.PutUint64(legacy, 42)
	if err := os.WriteFile(filepath.Join(dir, legacyEpochFilename), legacy, 0644); err != nil {
		t.Fatal(err)
	}

	m, err := readManifest(dir)
	if err != nil {
		t.Fatalf("readManifest: %v", err)
	}
	if m.Epoch != 42 {
		t.Errorf("legacy epoch: got %d want 42", m.Epoch)
	}
	if m.HighWatermark != 0 || m.LogStartOffset != 0 {
		t.Errorf("legacy migration should leave HWM/logStartOffset zero, got %+v", m)
	}
}

func TestManifestWriteRemovesLegacyFile(t *testing.T) {
	dir := t.TempDir()
	legacy := make([]byte, 8)
	binary.BigEndian.PutUint64(legacy, 5)
	if err := os.WriteFile(filepath.Join(dir, legacyEpochFilename), legacy, 0644); err != nil {
		t.Fatal(err)
	}

	if err := writeManifest(dir, &Manifest{Epoch: 5, HighWatermark: 100}); err != nil {
		t.Fatalf("writeManifest: %v", err)
	}

	if _, err := os.Stat(filepath.Join(dir, legacyEpochFilename)); !os.IsNotExist(err) {
		t.Errorf("legacy .leader-epoch should have been removed; stat err=%v", err)
	}
}

func TestManifestAtomicNoOrphanTmp(t *testing.T) {
	dir := t.TempDir()
	if err := writeManifest(dir, &Manifest{Epoch: 1, HighWatermark: 50}); err != nil {
		t.Fatalf("writeManifest: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, manifestFilename+".tmp")); !os.IsNotExist(err) {
		t.Errorf("tmp file should not exist after successful write; err=%v", err)
	}
}

func TestManifestParseError(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, manifestFilename), []byte("not json"), 0644); err != nil {
		t.Fatal(err)
	}
	_, err := readManifest(dir)
	if err == nil {
		t.Fatal("expected parse error for non-JSON manifest, got nil")
	}
	// Should NOT be ErrNotExist — caller distinguishes "missing" (fall back to scan)
	// from "corrupt" (probably a real bug to surface).
	if errors.Is(err, fs.ErrNotExist) {
		t.Errorf("parse error should not be fs.ErrNotExist; got %v", err)
	}
}

func TestManifestOverwrite(t *testing.T) {
	dir := t.TempDir()
	if err := writeManifest(dir, &Manifest{Epoch: 1, HighWatermark: 10}); err != nil {
		t.Fatal(err)
	}
	if err := writeManifest(dir, &Manifest{Epoch: 2, HighWatermark: 20, LogStartOffset: 5}); err != nil {
		t.Fatal(err)
	}
	m, err := readManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if m.Epoch != 2 || m.HighWatermark != 20 || m.LogStartOffset != 5 {
		t.Errorf("overwrite: got %+v want {2 20 5}", m)
	}
}
