package storage

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/woestebanaan/skafka/tests/testutil/recordbatch"
)

// TestOpenPartitionTrustsManifest guards the v0.1.31 fast-path: when a
// fresh manifest is on disk, openPartition uses its HighWatermark and
// SKIPS the full log scan. This is what keeps broker startup O(1) per
// partition on shared NFS — the symptom of regressing this would be
// startup time scaling with log size again, the same CrashLoopBackoff
// the v0.1.27/28/29 rollouts hit on the kperf-bench dataset.
//
// We force the case where the manifest disagrees with the log (manifest
// HWM smaller than what the log actually contains) and assert the
// engine reports the manifest's value. recoverSegment on takeover
// later picks up the additional log content; that's correctness — but
// startup itself must trust the manifest.
func TestOpenPartitionTrustsManifest(t *testing.T) {
	dir := t.TempDir()
	leases := &neverLeaderLeases{}

	cfg := DefaultConfig()
	e, err := NewDiskStorageEngine(dir, leases, cfg)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	if err := e.CreatePartition("t", 0); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := e.TakeOver(context.Background(), "t", 0, 1); err != nil {
		t.Fatalf("takeover: %v", err)
	}

	// Append 5 batches; HWM ends at 5.
	for i := 0; i < 5; i++ {
		batch := &recordbatch.RecordBatch{
			BaseOffset:      int64(i),
			LastOffsetDelta: 0,
			ProducerID:      -1,
			ProducerEpoch:   -1,
			BaseSequence:    -1,
			Records:         []recordbatch.Record{{OffsetDelta: 0, Value: []byte("x")}},
		}
		raw := recordbatch.Encode(nil, batch)
		if _, err := e.Append(context.Background(), "t", 0, 1, raw); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if hwm, _ := e.HighWatermark("t", 0); hwm != 5 {
		t.Fatalf("expected hwm=5 after 5 appends, got %d", hwm)
	}

	// Close the in-memory state and surgically rewrite the manifest
	// to claim a *smaller* HWM than the log actually contains. This
	// is the exact post-crash race we want to characterise: log
	// fsynced through offset 5, but manifest only reflects 3.
	if err := e.ClosePartition("t", 0); err != nil {
		t.Fatalf("close: %v", err)
	}
	pdir := filepath.Join(dir, "t", "0")
	manifest, err := readManifest(pdir)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	manifest.HighWatermark = 3
	if err := writeManifest(pdir, manifest); err != nil {
		t.Fatalf("write tampered manifest: %v", err)
	}

	// Reopen. The fast path must trust the manifest — we expect
	// HWM=3, NOT 5 (which would require a full scan).
	e2, err := NewDiskStorageEngine(dir, &neverLeaderLeases{}, cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	hwm, err := e2.HighWatermark("t", 0)
	if err != nil {
		t.Fatalf("hwm after reopen: %v", err)
	}
	if hwm != 3 {
		t.Errorf("expected hwm=3 from manifest fast path, got %d (would mean a scan ran)", hwm)
	}

	// Now drive the recovery path: a takeover at a higher epoch
	// triggers recoverSegment + rebuildIndex, which CRC-walks the log
	// and lifts the HWM back to its real value.
	hwm, err = e2.TakeOver(context.Background(), "t", 0, 99)
	if err != nil {
		t.Fatalf("takeover: %v", err)
	}
	if hwm != 5 {
		t.Errorf("expected post-takeover hwm=5 (recoverSegment caught up), got %d", hwm)
	}
}

// TestOpenPartitionFallsBackToScanWhenNoManifest exercises the cold
// path: a partition directory with log+index files but no manifest.
// openPartition must run the full scan and recover HWM correctly,
// matching what the manifest would have told us.
func TestOpenPartitionFallsBackToScanWhenNoManifest(t *testing.T) {
	dir := t.TempDir()
	leases := &neverLeaderLeases{}

	cfg := DefaultConfig()
	e, err := NewDiskStorageEngine(dir, leases, cfg)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	if err := e.CreatePartition("t", 0); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := e.TakeOver(context.Background(), "t", 0, 1); err != nil {
		t.Fatalf("takeover: %v", err)
	}
	for i := 0; i < 4; i++ {
		batch := &recordbatch.RecordBatch{
			BaseOffset: int64(i), LastOffsetDelta: 0,
			ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1,
			Records: []recordbatch.Record{{OffsetDelta: 0, Value: []byte("y")}},
		}
		if _, err := e.Append(context.Background(), "t", 0, 1, recordbatch.Encode(nil, batch)); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := e.ClosePartition("t", 0); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Delete the manifest so reopen takes the fallback path.
	if err := os.Remove(filepath.Join(dir, "t", "0", "manifest.json")); err != nil {
		t.Fatalf("remove manifest: %v", err)
	}

	e2, err := NewDiskStorageEngine(dir, &neverLeaderLeases{}, cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	hwm, err := e2.HighWatermark("t", 0)
	if err != nil {
		t.Fatalf("hwm: %v", err)
	}
	if hwm != 4 {
		t.Errorf("expected hwm=4 from log scan fallback, got %d", hwm)
	}
}

// silence unused import nag if the binary import set ever drifts.
var _ = binary.BigEndian
