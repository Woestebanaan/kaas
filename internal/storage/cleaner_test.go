package storage

import (
	"os"
	"path/filepath"
	"testing"
)

// makeClosedSegment writes a synthetic closed-segment file of `size` bytes
// at `dir/<epoch>-<base>.log` and returns the corresponding segmentMeta.
// The cleaner's size-based loop only needs the on-disk file size; it
// never reads the bytes themselves.
func makeClosedSegment(t *testing.T, dir string, base int64, size int64) segmentMeta {
	t.Helper()
	logPath := segmentLogPath(dir, base, 0)
	if err := os.WriteFile(logPath, make([]byte, size), 0o644); err != nil {
		t.Fatal(err)
	}
	return segmentMeta{
		baseOffset: base,
		logPath:    logPath,
		indexPath:  segmentIndexPath(dir, base, 0),
	}
}

// TestRetentionCleaner_SizeBasedDeletion guards gh #47: when a partition's
// total closed-segment size exceeds the limit, oldest segments are deleted
// until total ≤ limit. The active segment is never touched, and at least
// one closed segment is always preserved so reads near HWM don't fall off
// a cliff.
func TestRetentionCleaner_SizeBasedDeletion(t *testing.T) {
	dir := t.TempDir()
	leases := &neverLeaderLeases{}
	e, err := NewDiskStorageEngine(dir, leases, DefaultConfig())
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	pdir := filepath.Join(dir, "t", "0")
	if err := os.MkdirAll(pdir, 0o755); err != nil {
		t.Fatal(err)
	}

	// 5 closed segments, 10 MB each = 50 MB total. Limit at 25 MB; expect
	// the cleaner to drop the 3 oldest, leaving the 2 newest closed + 1
	// active.
	segs := []segmentMeta{
		makeClosedSegment(t, pdir, 0, 10<<20),
		makeClosedSegment(t, pdir, 1000, 10<<20),
		makeClosedSegment(t, pdir, 2000, 10<<20),
		makeClosedSegment(t, pdir, 3000, 10<<20),
		makeClosedSegment(t, pdir, 4000, 10<<20),
	}
	active, err := createSegment(pdir, 5000, 0)
	if err != nil {
		t.Fatal(err)
	}
	ps := &partitionState{
		dir:                    pdir,
		segments:               segs,
		active:                 active,
		retentionBytesOverride: 25 << 20, // 25 MB
	}
	e.partitions[e.partKey("t", 0)] = ps

	cleaner := NewRetentionCleaner(e, leases, 0)
	cleaner.cleanPartition(PartitionID{Topic: "t", Partition: 0})

	if got := len(ps.segments); got != 2 {
		t.Errorf("len(ps.segments) = %d, want 2 (3 oldest should be dropped)", got)
	}
	if ps.segments[0].baseOffset != 3000 {
		t.Errorf("oldest remaining baseOffset = %d, want 3000", ps.segments[0].baseOffset)
	}
	if ps.active == nil {
		t.Error("active segment must not be touched by retention")
	}
}

// TestRetentionCleaner_SizeRespectsPerTopicOverride confirms that
// retentionBytesOverride takes precedence over engine.cfg.RetentionBytes.
// This is the path where the operator's WriteTopicConfig drives behavior.
func TestRetentionCleaner_SizeRespectsPerTopicOverride(t *testing.T) {
	dir := t.TempDir()
	leases := &neverLeaderLeases{}
	cfg := DefaultConfig()
	cfg.RetentionBytes = 1 << 30 // 1 GB engine default — should be ignored
	e, err := NewDiskStorageEngine(dir, leases, cfg)
	if err != nil {
		t.Fatal(err)
	}

	pdir := filepath.Join(dir, "t", "0")
	_ = os.MkdirAll(pdir, 0o755)
	ps := &partitionState{
		dir: pdir,
		segments: []segmentMeta{
			makeClosedSegment(t, pdir, 0, 10<<20),
			makeClosedSegment(t, pdir, 1000, 10<<20),
			makeClosedSegment(t, pdir, 2000, 10<<20),
		},
		retentionBytesOverride: 25 << 20, // 25 MB — drops 1 of 3 (30 MB → 20 MB)
	}
	active, _ := createSegment(pdir, 3000, 0)
	ps.active = active
	e.partitions[e.partKey("t", 0)] = ps

	cleaner := NewRetentionCleaner(e, leases, 0)
	cleaner.cleanPartition(PartitionID{Topic: "t", Partition: 0})

	if got := len(ps.segments); got != 2 {
		t.Errorf("len(ps.segments) = %d, want 2", got)
	}
}

// TestRetentionCleaner_NoLimitNoDeletes confirms that when both engine
// default and override are 0, the size loop is a no-op even with massive
// closed segments.
func TestRetentionCleaner_NoLimitNoDeletes(t *testing.T) {
	dir := t.TempDir()
	leases := &neverLeaderLeases{}
	e, err := NewDiskStorageEngine(dir, leases, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	pdir := filepath.Join(dir, "t", "0")
	_ = os.MkdirAll(pdir, 0o755)
	ps := &partitionState{
		dir: pdir,
		segments: []segmentMeta{
			makeClosedSegment(t, pdir, 0, 100<<20),
			makeClosedSegment(t, pdir, 1000, 100<<20),
		},
	}
	active, _ := createSegment(pdir, 2000, 0)
	ps.active = active
	e.partitions[e.partKey("t", 0)] = ps

	cleaner := NewRetentionCleaner(e, leases, 0)
	cleaner.cleanPartition(PartitionID{Topic: "t", Partition: 0})

	if got := len(ps.segments); got != 2 {
		t.Errorf("len(ps.segments) = %d, want 2 (no limit set)", got)
	}
}

// TestReadTopicConfig_RoundTrip writes a per-topic config and reads it
// back. Asserts the operator → broker rendezvous file works end-to-end.
func TestReadTopicConfig_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	want := int64(10 << 30)
	if err := WriteTopicConfig(dir, &TopicConfigFile{RetentionBytes: &want}); err != nil {
		t.Fatal(err)
	}
	got, err := ReadTopicConfig(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.RetentionBytes == nil || *got.RetentionBytes != want {
		t.Errorf("round-trip mismatch: got=%+v want=%d", got, want)
	}
}
