package storage

import (
	"context"
	"testing"

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

	file, off, length, ok, err := e.ReadSegmentRef("t", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("ReadSegmentRef: %v", err)
	}
	if !ok {
		t.Fatalf("ok=false on happy path; want true")
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

	_, _, _, ok, err := e.ReadSegmentRef("t", 0, 100, 1<<20)
	if err != nil {
		t.Fatalf("ReadSegmentRef: %v", err)
	}
	if ok {
		t.Errorf("ok=true for offset past HWM; want false")
	}
}

// TestReadSegmentRefUnknownPartition returns an error and ok=false.
func TestReadSegmentRefUnknownPartition(t *testing.T) {
	dir := t.TempDir()
	e, err := NewDiskStorageEngine(dir, &neverLeaderLeases{}, DefaultConfig())
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	_, _, _, ok, err := e.ReadSegmentRef("nope", 0, 0, 1<<20)
	if err == nil {
		t.Errorf("ReadSegmentRef on unknown partition returned nil error; want ErrUnknownPartition")
	}
	if ok {
		t.Errorf("ok=true on unknown partition; want false")
	}
}
