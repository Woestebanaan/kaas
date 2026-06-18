package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestRetentionCleaner_TimeBasedDeletion pins gh #46: a closed segment
// whose maxTimestamp is older than (now - cfg.RetentionMs) is deleted;
// a segment whose maxTimestamp falls inside the retention window is
// kept. Boundary check guards both directions — the cleaner must not
// be too aggressive (keep fresh data) nor too conservative (let stale
// data accumulate).
func TestRetentionCleaner_TimeBasedDeletion(t *testing.T) {
	dir := t.TempDir()
	leases := &neverLeaderLeases{}
	cfg := DefaultConfig()
	// Retention window: 1 hour. Segments older than 1h ago are evicted.
	cfg.RetentionMs = int64(time.Hour / time.Millisecond)
	cfg.RetentionBytes = 0 // disable size path so we test time in isolation
	e, err := NewDiskStorageEngine(dir, leases, cfg)
	if err != nil {
		t.Fatalf("engine: %v", err)
	}

	pdir := filepath.Join(dir, "t", "0")
	if err := os.MkdirAll(pdir, 0o755); err != nil {
		t.Fatal(err)
	}

	nowMs := time.Now().UnixMilli()
	old := makeClosedSegment(t, pdir, 0, 1024)
	// 2 hours ago — well past the 1h cutoff.
	old.maxTimestamp = nowMs - 2*int64(time.Hour/time.Millisecond)
	fresh := makeClosedSegment(t, pdir, 1000, 1024)
	// 30 minutes ago — inside the 1h window.
	fresh.maxTimestamp = nowMs - 30*int64(time.Minute/time.Millisecond)

	ps := &partitionState{
		dir:      pdir,
		segments: []segmentMeta{old, fresh},
	}
	active, err := createSegment(pdir, 2000, 0)
	if err != nil {
		t.Fatal(err)
	}
	ps.active = active
	e.partitions[e.partKey("t", 0)] = ps

	cleaner := NewRetentionCleaner(e, leases, 0)
	cleaner.cleanPartition(PartitionID{Topic: "t", Partition: 0})

	if len(ps.segments) != 1 {
		t.Fatalf("after time retention, segments=%d, want 1 (old dropped, fresh kept)", len(ps.segments))
	}
	if ps.segments[0].baseOffset != 1000 {
		t.Errorf("survivor baseOffset=%d, want 1000 (the fresh segment)", ps.segments[0].baseOffset)
	}
	if ps.active == nil {
		t.Error("active segment must not be touched by time retention")
	}
}

// TestRetentionCleaner_TimeBasedKeepsActive — even an absurdly small
// RetentionMs must not affect the active segment. Apache's semantic
// is "retention applies to closed segments only"; otherwise an idle
// topic with no produces would lose its tail forever.
func TestRetentionCleaner_TimeBasedKeepsActive(t *testing.T) {
	dir := t.TempDir()
	leases := &neverLeaderLeases{}
	cfg := DefaultConfig()
	cfg.RetentionMs = 1 // 1ms — anything not actively being written should be dropped
	cfg.RetentionBytes = 0
	e, err := NewDiskStorageEngine(dir, leases, cfg)
	if err != nil {
		t.Fatal(err)
	}

	pdir := filepath.Join(dir, "t", "0")
	if err := os.MkdirAll(pdir, 0o755); err != nil {
		t.Fatal(err)
	}
	ps := &partitionState{dir: pdir}
	active, _ := createSegment(pdir, 0, 0)
	ps.active = active
	e.partitions[e.partKey("t", 0)] = ps

	cleaner := NewRetentionCleaner(e, leases, 0)
	cleaner.cleanPartition(PartitionID{Topic: "t", Partition: 0})

	if ps.active == nil {
		t.Error("active segment must survive even with RetentionMs=1")
	}
}

// TestRetentionCleaner_TimeBasedZeroDisabled — RetentionMs=0 means
// "no time-based retention". Combined with RetentionBytes=0 the
// cleaner must be a no-op (mirrors Apache's `retention.ms=-1`
// semantic; skafka's config uses 0 as the "disabled" sentinel).
func TestRetentionCleaner_TimeBasedZeroDisabled(t *testing.T) {
	dir := t.TempDir()
	leases := &neverLeaderLeases{}
	cfg := DefaultConfig()
	cfg.RetentionMs = 0
	cfg.RetentionBytes = 0
	e, err := NewDiskStorageEngine(dir, leases, cfg)
	if err != nil {
		t.Fatal(err)
	}

	pdir := filepath.Join(dir, "t", "0")
	_ = os.MkdirAll(pdir, 0o755)

	old := makeClosedSegment(t, pdir, 0, 1024)
	old.maxTimestamp = 1 // year 1970 — would normally be deleted
	ps := &partitionState{
		dir:      pdir,
		segments: []segmentMeta{old},
	}
	active, _ := createSegment(pdir, 1000, 0)
	ps.active = active
	e.partitions[e.partKey("t", 0)] = ps

	cleaner := NewRetentionCleaner(e, leases, 0)
	cleaner.cleanPartition(PartitionID{Topic: "t", Partition: 0})

	if len(ps.segments) != 1 {
		t.Errorf("RetentionMs=0 deleted segments=%d; cleaner should be a no-op", 1-len(ps.segments))
	}
}
