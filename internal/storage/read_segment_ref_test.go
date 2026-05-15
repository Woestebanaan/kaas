package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/woestebanaan/skafka/tests/testutil/recordbatch"
)

// TestReadSegmentRefHappyPath: after a few Appends, asking for a slice
// starting at the segment baseOffset returns a non-nil file ref + the
// expected length, AND the bytes at (file, offset, length) match what
// Read would return for the same range.
func TestReadSegmentRefHappyPath(t *testing.T) {
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

	// Three small batches.
	for i := 0; i < 3; i++ {
		b := recordbatch.Encode(nil, &recordbatch.RecordBatch{
			BaseOffset: int64(i), LastOffsetDelta: 0,
			ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1,
			Records: []recordbatch.Record{{OffsetDelta: 0, Value: []byte("payload")}},
		})
		if _, err := e.Append(context.Background(), "t", 0, 1, -1, b); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	file, off, length, cleanup, ok, err := e.ReadSegmentRef("t", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("ReadSegmentRef: %v", err)
	}
	if !ok {
		t.Fatalf("ok=false on happy path; want true")
	}
	if cleanup != nil {
		t.Errorf("cleanup=non-nil for active-segment hit; want nil")
		t.Cleanup(cleanup)
	}
	if file == nil {
		t.Fatalf("file=nil; want non-nil")
	}
	if off != 0 {
		t.Errorf("offset=%d; want 0 (start of segment)", off)
	}
	if length <= 0 {
		t.Errorf("length=%d; want > 0", length)
	}

	// Cross-check: the bytes referenced match what Read returns.
	got := make([]byte, length)
	if _, err := file.ReadAt(got, off); err != nil {
		t.Fatalf("file.ReadAt: %v", err)
	}
	via, err := e.Read(context.Background(), "t", 0, 0, length)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(via) != len(got) {
		t.Errorf("Read returned %d bytes, ReadSegmentRef pointed at %d", len(via), len(got))
	}
	for i := 0; i < min(len(via), len(got)); i++ {
		if via[i] != got[i] {
			t.Errorf("byte %d differs: Read=%d, splice-ref=%d", i, via[i], got[i])
			break
		}
	}
}

// TestReadSegmentRefBeyondHWM returns ok=false (nothing to read).
func TestReadSegmentRefBeyondHWM(t *testing.T) {
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

	_, _, _, _, ok, err := e.ReadSegmentRef("t", 0, 100, 1<<20)
	if err != nil {
		t.Fatalf("ReadSegmentRef: %v", err)
	}
	if ok {
		t.Errorf("ok=true for offset past HWM; want false")
	}
}

// TestReadSegmentRefClosedSegment: force a segment roll, then ask for
// a slice starting in the (now-closed) older segment. The splice path
// must lazy-open the closed file and return cleanup != nil. Same
// byte-identity check as the happy-path test — Read and the splice ref
// must point at the same bytes.
func TestReadSegmentRefClosedSegment(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.SegmentBytes = 4 * 1024 // small so a few batches force a roll
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
	// ClosePartition before TempDir cleanup so the per-partition
	// committer goroutine stops and any rolled-but-not-yet-finalized
	// segment writes drain. Relinquish only closes the active fd —
	// not enough; t.TempDir's cleanup would intermittently fail with
	// "directory not empty" because the committer or finalize
	// goroutines still held files in the partition dir.
	t.Cleanup(func() {
		_ = e.ClosePartition("t", 0)
		// Drain the unsupervised roll-finalize goroutine: it can
		// re-create manifest.json in the partition dir AFTER
		// ClosePartition's RemoveAll. Best-effort retry loop
		// (typically settles in <50 ms; cap at 1 s).
		partDir := filepath.Join(dir, "t", "0")
		for i := 0; i < 20; i++ {
			_ = os.RemoveAll(partDir)
			if _, err := os.Stat(partDir); os.IsNotExist(err) {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	})

	// Append enough batches to force a segment roll past the 4 KiB
	// ceiling. Each value is 512 bytes so ~10 batches roll the segment.
	payload := make([]byte, 512)
	for i := 0; i < 50; i++ {
		b := recordbatch.Encode(nil, &recordbatch.RecordBatch{
			BaseOffset: int64(i), LastOffsetDelta: 0,
			ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1,
			Records: []recordbatch.Record{{OffsetDelta: 0, Value: payload}},
		})
		if _, err := e.Append(context.Background(), "t", 0, 1, -1, b); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	// Ask for offset 0 — that lives in the FIRST closed segment after rolling.
	file, off, length, cleanup, ok, err := e.ReadSegmentRef("t", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("ReadSegmentRef: %v", err)
	}
	if !ok {
		t.Fatalf("ok=false on closed-segment splice; want true")
	}
	if cleanup == nil {
		t.Fatalf("cleanup=nil for closed-segment splice; want non-nil so caller can release the fd")
	}
	t.Cleanup(cleanup)
	if file == nil {
		t.Fatalf("file=nil; want non-nil")
	}
	if length <= 0 {
		t.Errorf("length=%d; want > 0", length)
	}

	// Byte-identity check: the splice file's bytes at [off, off+length)
	// match what Read returns for the same offset range.
	got := make([]byte, length)
	if _, err := file.ReadAt(got, off); err != nil {
		t.Fatalf("file.ReadAt: %v", err)
	}
	via, err := e.Read(context.Background(), "t", 0, 0, length)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(via) != len(got) {
		t.Errorf("Read returned %d bytes, ReadSegmentRef pointed at %d", len(via), len(got))
	}
	for i := 0; i < min(len(via), len(got)); i++ {
		if via[i] != got[i] {
			t.Errorf("byte %d differs: Read=%d, splice-ref=%d", i, via[i], got[i])
			break
		}
	}
}

// TestReadSegmentRefUnknownPartition returns an error and ok=false.
func TestReadSegmentRefUnknownPartition(t *testing.T) {
	dir := t.TempDir()
	e, err := NewDiskStorageEngine(dir, &neverLeaderLeases{}, DefaultConfig())
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	_, _, _, _, ok, err := e.ReadSegmentRef("nope", 0, 0, 1<<20)
	if err == nil {
		t.Errorf("ReadSegmentRef on unknown partition returned nil error; want ErrUnknownPartition")
	}
	if ok {
		t.Errorf("ok=true on unknown partition; want false")
	}
}
