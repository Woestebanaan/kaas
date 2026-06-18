package storage

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/woestebanaan/skafka/tests/testutil/recordbatch"
)

// TestTakeOverHealsPhantomHWM is the gh #138 regression guard.
//
// Recreates the "phantom HWM" disk state observed live: no closed
// segments, an empty active segment whose baseOffset > 0, and a
// manifest claiming logStartOffset=0, highWatermark=baseOffset. Before
// the fix, takeover would lock that state in (HWM=baseOffset,
// logStart=0) and the partition would report millions of records that
// don't exist. After the fix, takeover detects the inconsistency and
// advances logStart so HWM == logStart and HWM - logStart == 0
// records.
func TestTakeOverHealsPhantomHWM(t *testing.T) {
	dataDir := t.TempDir()
	cfg := DefaultConfig()
	cfg.FlushIntervalMessages = 0
	e, err := NewDiskStorageEngine(dataDir, &neverLeaderLeases{}, cfg)
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	if err := e.CreatePartition("phantom", 0); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Hand-craft the phantom state on disk:
	//   - empty active segment at baseOffset=1_500_000 (the value the
	//     active had when records were last present, before an
	//     interrupted purge).
	//   - manifest claiming logStartOffset=0, highWatermark=1_500_000.
	partDir := filepath.Join(dataDir, "phantom", "0")
	// Wipe whatever CreatePartition wrote — we want a deliberately
	// inconsistent layout.
	entries, err := os.ReadDir(partDir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	for _, ent := range entries {
		_ = os.Remove(filepath.Join(partDir, ent.Name()))
	}
	const phantomHWM = int64(1_500_000)
	phantomLog := filepath.Join(partDir, "00000005-00000000000001500000.log")
	phantomIdx := filepath.Join(partDir, "00000005-00000000000001500000.index")
	if err := os.WriteFile(phantomLog, nil, 0o644); err != nil {
		t.Fatalf("write phantom log: %v", err)
	}
	if err := os.WriteFile(phantomIdx, nil, 0o644); err != nil {
		t.Fatalf("write phantom idx: %v", err)
	}
	manifest := map[string]any{
		"epoch":          16,
		"highWatermark":  phantomHWM,
		"logStartOffset": int64(0),
	}
	manifestBytes, _ := json.Marshal(manifest)
	if err := os.WriteFile(filepath.Join(partDir, "manifest.json"), manifestBytes, 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	// Re-open the engine so it picks up the hand-crafted state.
	e2, err := NewDiskStorageEngine(dataDir, &neverLeaderLeases{}, cfg)
	if err != nil {
		t.Fatalf("engine reopen: %v", err)
	}
	if _, err := e2.TakeOver(context.Background(), "phantom", 0, 17); err != nil {
		t.Fatalf("takeover: %v", err)
	}

	// Post-takeover invariants.
	hwm, err := e2.HighWatermark("phantom", 0)
	if err != nil {
		t.Fatalf("HighWatermark: %v", err)
	}
	logStart, err := e2.LogStartOffset("phantom", 0)
	if err != nil {
		t.Fatalf("LogStartOffset: %v", err)
	}
	if hwm != phantomHWM {
		t.Errorf("HWM=%d, want %d (preserved as the empty active's baseOffset)", hwm, phantomHWM)
	}
	if logStart != phantomHWM {
		t.Errorf("logStart=%d, want %d — phantom HWM heal didn't advance logStart", logStart, phantomHWM)
	}
	if hwm-logStart != 0 {
		t.Errorf("HWM-logStart=%d, want 0 (partition should appear empty after heal)", hwm-logStart)
	}
}

// TestTakeOverHealsPhantomHWMWithStaleClosedSegments is the gh #123
// State B regression. The exact on-disk layout observed live:
//
//	00000000-00000000000000000000.log    (0 bytes) — stale epoch-0 closed segment
//	00000000-00000000000000000000.index  (0 bytes)
//	00000005-00000000000001483826.log    (0 bytes) — empty active at next epoch's base
//	00000005-00000000000001483826.index  (0 bytes)
//	manifest.json: {"epoch":16,"highWatermark":1483826,"logStartOffset":0}
//
// Before the gh #123 fix the existing heal's `len(ps.segments) == 0`
// check returned false (there IS a closed segment, just an empty one),
// so the partition stayed phantom-HWM. After the fix the detector
// looks at on-disk bytes (all zero → heal fires) AND the heal drops
// the stale 0-byte closed segment files from disk.
func TestTakeOverHealsPhantomHWMWithStaleClosedSegments(t *testing.T) {
	dataDir := t.TempDir()
	cfg := DefaultConfig()
	cfg.FlushIntervalMessages = 0
	e, err := NewDiskStorageEngine(dataDir, &neverLeaderLeases{}, cfg)
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	if err := e.CreatePartition("phantom-b", 0); err != nil {
		t.Fatalf("create: %v", err)
	}

	partDir := filepath.Join(dataDir, "phantom-b", "0")
	// Wipe CreatePartition's output — we want a deliberately
	// inconsistent layout that matches gh #123 State B.
	entries, _ := os.ReadDir(partDir)
	for _, ent := range entries {
		_ = os.Remove(filepath.Join(partDir, ent.Name()))
	}

	const phantomHWM = int64(1_483_826)
	// Stale epoch-0 closed segment (0 bytes).
	staleLog := filepath.Join(partDir, "00000000-00000000000000000000.log")
	staleIdx := filepath.Join(partDir, "00000000-00000000000000000000.index")
	if err := os.WriteFile(staleLog, nil, 0o644); err != nil {
		t.Fatalf("write stale log: %v", err)
	}
	if err := os.WriteFile(staleIdx, nil, 0o644); err != nil {
		t.Fatalf("write stale idx: %v", err)
	}
	// Empty active at next epoch's baseOffset (0 bytes).
	activeLog := filepath.Join(partDir, "00000005-00000000000001483826.log")
	activeIdx := filepath.Join(partDir, "00000005-00000000000001483826.index")
	if err := os.WriteFile(activeLog, nil, 0o644); err != nil {
		t.Fatalf("write active log: %v", err)
	}
	if err := os.WriteFile(activeIdx, nil, 0o644); err != nil {
		t.Fatalf("write active idx: %v", err)
	}
	// Manifest reflects the partial-purge state: logStart=0 but HWM=1.48M.
	manifest := map[string]any{
		"epoch":          16,
		"highWatermark":  phantomHWM,
		"logStartOffset": int64(0),
	}
	manifestBytes, _ := json.Marshal(manifest)
	if err := os.WriteFile(filepath.Join(partDir, "manifest.json"), manifestBytes, 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	// Re-open and take over.
	e2, err := NewDiskStorageEngine(dataDir, &neverLeaderLeases{}, cfg)
	if err != nil {
		t.Fatalf("engine reopen: %v", err)
	}
	if _, err := e2.TakeOver(context.Background(), "phantom-b", 0, 17); err != nil {
		t.Fatalf("takeover: %v", err)
	}

	// Heal invariants.
	hwm, _ := e2.HighWatermark("phantom-b", 0)
	logStart, _ := e2.LogStartOffset("phantom-b", 0)
	if hwm != phantomHWM {
		t.Errorf("HWM=%d, want %d", hwm, phantomHWM)
	}
	if logStart != phantomHWM {
		t.Errorf("logStart=%d, want %d — heal didn't advance past stale closed seg", logStart, phantomHWM)
	}
	if hwm-logStart != 0 {
		t.Errorf("HWM-logStart=%d, want 0 (partition empty after heal)", hwm-logStart)
	}

	// Stale closed-segment files should be unlinked.
	if _, err := os.Stat(staleLog); !os.IsNotExist(err) {
		t.Errorf("stale closed-segment log still on disk: %v (want NotExist)", err)
	}
	if _, err := os.Stat(staleIdx); !os.IsNotExist(err) {
		t.Errorf("stale closed-segment index still on disk: %v (want NotExist)", err)
	}
}

// TestTakeOverPreservesGenuineLogStartWhenSegmentsExist confirms the
// gh #138 detector doesn't false-trigger when there are real closed
// segments — that's the legitimate "logStart < HWM, records do exist
// on disk" case which must NOT be healed.
func TestTakeOverPreservesGenuineLogStartWhenSegmentsExist(t *testing.T) {
	dataDir := t.TempDir()
	cfg := DefaultConfig()
	cfg.SegmentBytes = 4 * 1024 // small so we get a closed segment
	cfg.FlushIntervalMessages = 0
	e, err := NewDiskStorageEngine(dataDir, &neverLeaderLeases{}, cfg)
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	if err := e.CreatePartition("real", 0); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := e.TakeOver(context.Background(), "real", 0, 1); err != nil {
		t.Fatalf("initial takeover: %v", err)
	}

	// Append enough records to force at least one segment roll.
	value := make([]byte, 500)
	for i := 0; i < 30; i++ {
		batch := simpleBatch(int64(i), value)
		if _, err := e.Append(context.Background(), "real", 0, 1, -1, batch); err != nil {
			t.Fatalf("append i=%d: %v", i, err)
		}
	}
	hwmBefore, _ := e.HighWatermark("real", 0)
	logStartBefore, _ := e.LogStartOffset("real", 0)

	// Close cleanly before reopen — matches the broker shutdown path
	// and releases file handles so the second engine can mutate.
	if err := e.ClosePartition("real", 0); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Re-open and take over again — exercise the detection path.
	e2, err := NewDiskStorageEngine(dataDir, &neverLeaderLeases{}, cfg)
	if err != nil {
		t.Fatalf("engine reopen: %v", err)
	}
	if _, err := e2.TakeOver(context.Background(), "real", 0, 2); err != nil {
		t.Fatalf("takeover after reopen: %v", err)
	}

	hwmAfter, _ := e2.HighWatermark("real", 0)
	logStartAfter, _ := e2.LogStartOffset("real", 0)
	if hwmAfter != hwmBefore {
		t.Errorf("HWM=%d before, %d after — should be preserved", hwmBefore, hwmAfter)
	}
	if logStartAfter != logStartBefore {
		t.Errorf("logStart=%d before, %d after — must NOT be advanced when real segments exist (false-positive heal)",
			logStartBefore, logStartAfter)
	}
}

// simpleBatch produces a minimal valid RecordBatch with one record.
func simpleBatch(_ int64, value []byte) []byte {
	return recordbatch.Encode(nil, &recordbatch.RecordBatch{
		BaseOffset: 0, LastOffsetDelta: 0,
		BaseTimestamp: 1700000000000,
		MaxTimestamp:  1700000000000,
		ProducerID:    -1, ProducerEpoch: -1, BaseSequence: -1,
		Records: []recordbatch.Record{
			{OffsetDelta: 0, Key: nil, Value: value},
		},
	})
}
