package storage

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/woestebanaan/skafka/tests/testutil/recordbatch"
)

// TestDeleteRecordsAdvancesLogStart pins the DeleteRecords API contract:
// advancing the log start offset hides earlier records from Fetch
// (LogStartOffset goes up), drops fully-covered closed segments from
// disk, and persists the new state in the manifest.
func TestDeleteRecordsAdvancesLogStart(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	// Tiny segment cap so we get multiple closed segments to delete.
	cfg.SegmentBytes = 4 * 1024
	cfg.FlushIntervalMessages = 1

	e, err := NewDiskStorageEngine(dir, &neverLeaderLeases{}, cfg)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	if err := e.CreatePartition("t", 0); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := e.TakeOver(context.Background(), "t", 0, 1); err != nil {
		t.Fatalf("takeover: %v", err)
	}

	// Append 200 batches with 256 B values → multiple segment rolls.
	for i := 0; i < 200; i++ {
		batch := &recordbatch.RecordBatch{
			BaseOffset: 0, LastOffsetDelta: 0,
			ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1,
			Records: []recordbatch.Record{{
				OffsetDelta: 0,
				Value:       make([]byte, 256),
			}},
		}
		if _, err := e.Append(context.Background(), "t", 0, 1, recordbatch.Encode(nil, batch)); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	hwm, _ := e.HighWatermark("t", 0)
	if hwm != 200 {
		t.Fatalf("HWM=%d after 200 appends, want 200", hwm)
	}
	logStart, _ := e.LogStartOffset("t", 0)
	if logStart != 0 {
		t.Fatalf("LogStartOffset=%d before delete, want 0", logStart)
	}

	// Purge everything before offset 100. Closed segments fully below
	// offset 100 should be physically removed.
	pdir := filepath.Join(dir, "t", "0")
	segsBefore, _ := listSegments(pdir)

	newLogStart, err := e.DeleteRecords("t", 0, 100)
	if err != nil {
		t.Fatalf("DeleteRecords: %v", err)
	}
	if newLogStart != 100 {
		t.Errorf("newLogStart=%d, want 100", newLogStart)
	}

	logStart, _ = e.LogStartOffset("t", 0)
	if logStart != 100 {
		t.Errorf("LogStartOffset post-DeleteRecords=%d, want 100", logStart)
	}

	segsAfter, _ := listSegments(pdir)
	if len(segsAfter) >= len(segsBefore) {
		t.Errorf("expected fewer segments after DeleteRecords, got %d before / %d after",
			len(segsBefore), len(segsAfter))
	}

	// Manifest must reflect the new logStart so a restart picks it up.
	m, err := readManifest(pdir)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if m.LogStartOffset != 100 {
		t.Errorf("manifest.LogStartOffset=%d, want 100", m.LogStartOffset)
	}
}

// TestDeleteRecords_HighWatermarkSentinel verifies offset=-1 means
// "purge everything currently visible" — same semantics as Apache
// Kafka and what Kafbat sends when the user clicks "Purge messages"
// without specifying an offset.
func TestDeleteRecords_HighWatermarkSentinel(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.FlushIntervalMessages = 1

	e, err := NewDiskStorageEngine(dir, &neverLeaderLeases{}, cfg)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	if err := e.CreatePartition("t", 0); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := e.TakeOver(context.Background(), "t", 0, 1); err != nil {
		t.Fatalf("takeover: %v", err)
	}
	for i := 0; i < 5; i++ {
		batch := &recordbatch.RecordBatch{
			BaseOffset: 0, LastOffsetDelta: 0,
			ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1,
			Records:    []recordbatch.Record{{OffsetDelta: 0, Value: []byte("z")}},
		}
		if _, err := e.Append(context.Background(), "t", 0, 1, recordbatch.Encode(nil, batch)); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	newLogStart, err := e.DeleteRecords("t", 0, -1)
	if err != nil {
		t.Fatalf("DeleteRecords(-1): %v", err)
	}
	if newLogStart != 5 {
		t.Errorf("newLogStart=%d after sentinel purge, want 5 (== HWM)", newLogStart)
	}
}

// TestDeleteRecords_CompletePurgeReclaimsActive guards the disk-
// reclamation invariant: a "purge all" (offset=-1, logStart catches
// up to HWM) must drop the bytes physically — not just hide them.
// Before this fix, Kafbat's "Purge messages" left up to SegmentBytes
// on disk per partition: invisible to consumers but still counted
// against the PVC. We force-roll the active segment and unlink its
// files when the purge covers the entire partition.
func TestDeleteRecords_CompletePurgeReclaimsActive(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.FlushIntervalMessages = 1

	e, err := NewDiskStorageEngine(dir, &neverLeaderLeases{}, cfg)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	if err := e.CreatePartition("t", 0); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := e.TakeOver(context.Background(), "t", 0, 1); err != nil {
		t.Fatalf("takeover: %v", err)
	}

	// Append a few batches with non-trivial payloads so the active
	// segment actually consumes bytes on disk.
	for i := 0; i < 10; i++ {
		batch := &recordbatch.RecordBatch{
			BaseOffset: 0, LastOffsetDelta: 0,
			ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1,
			Records: []recordbatch.Record{{
				OffsetDelta: 0,
				Value:       make([]byte, 4096),
			}},
		}
		if _, err := e.Append(context.Background(), "t", 0, 1, recordbatch.Encode(nil, batch)); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	sizeBefore := e.PartitionSize("t", 0)
	if sizeBefore < 40*1024 {
		t.Fatalf("PartitionSize before purge = %d, expected ≥ ~40 KiB of payload", sizeBefore)
	}

	if _, err := e.DeleteRecords("t", 0, -1); err != nil {
		t.Fatalf("DeleteRecords(-1): %v", err)
	}

	sizeAfter := e.PartitionSize("t", 0)
	// Active segment got rolled and the old one unlinked; only the
	// fresh empty segment remains. Allow a small floor for the empty
	// segment's metadata files (manifest, empty .log/.index).
	if sizeAfter > 8*1024 {
		t.Errorf("PartitionSize after complete purge = %d, expected ~empty (≤ 8 KiB); active segment was not reclaimed", sizeAfter)
	}
	t.Logf("PartitionSize: %d → %d after purge", sizeBefore, sizeAfter)

	// Topic must still be writable: post-purge HWM resets perception
	// — new appends start at the new active's baseOffset.
	batch := &recordbatch.RecordBatch{
		BaseOffset: 0, LastOffsetDelta: 0,
		ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1,
		Records: []recordbatch.Record{{OffsetDelta: 0, Value: []byte("post-purge")}},
	}
	if _, err := e.Append(context.Background(), "t", 0, 1, recordbatch.Encode(nil, batch)); err != nil {
		t.Errorf("append after purge: %v", err)
	}
}

// TestDeleteRecords_OutOfRange guards the error path for offsets past
// HWM — an admin tool that asks to truncate beyond what exists must
// see ErrOffsetOutOfRange, not silent acceptance.
func TestDeleteRecords_OutOfRange(t *testing.T) {
	dir := t.TempDir()
	e, err := NewDiskStorageEngine(dir, &neverLeaderLeases{}, DefaultConfig())
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	if err := e.CreatePartition("t", 0); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := e.TakeOver(context.Background(), "t", 0, 1); err != nil {
		t.Fatalf("takeover: %v", err)
	}

	if _, err := e.DeleteRecords("t", 0, 100); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Errorf("DeleteRecords past HWM: err=%v, want ErrOffsetOutOfRange", err)
	}
}
