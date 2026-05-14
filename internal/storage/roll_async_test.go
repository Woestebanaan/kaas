package storage

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/woestebanaan/skafka/tests/testutil/recordbatch"
)

// TestRollDoesNotStallConcurrentAppends pins gh #82's segment-roll
// async finalize: rollSegment should keep ps.mu only long enough for
// the swap (one log fsync + one createSegment). The deferred finalize
// (index fsync + close + manifest) runs in a goroutine and must not
// block subsequent Appends.
//
// Trigger a roll via a small SegmentBytes, fire concurrent Appends
// across the boundary, and verify they all complete and the engine
// is consistent across rolls.
func TestRollDoesNotStallConcurrentAppends(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	// Tiny segments so roll fires after just a few records.
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

	const n = 200
	mkBatch := func() []byte {
		return recordbatch.Encode(nil, &recordbatch.RecordBatch{
			BaseOffset: 0, LastOffsetDelta: 0,
			ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1,
			Records: []recordbatch.Record{{
				OffsetDelta: 0,
				// Body sized so a few batches per segment, multiple
				// rolls during the test.
				Value: make([]byte, 256),
			}},
		})
	}

	var wg sync.WaitGroup
	wg.Add(n)
	start := time.Now()
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			if _, err := e.Append(context.Background(), "t", 0, 1, -1, mkBatch()); err != nil {
				t.Errorf("append: %v", err)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	t.Logf("200 concurrent appends across N segment rolls: %v", elapsed)

	// HWM advanced by exactly n records.
	if hwm, _ := e.HighWatermark("t", 0); hwm != int64(n) {
		t.Errorf("HWM=%d, want %d", hwm, n)
	}

	// At least one roll happened (otherwise the test isn't exercising
	// the path it claims to). Multiple closed segment files on disk
	// is the disk-side proof.
	pdir := filepath.Join(dir, "t", "0")
	segs, err := listSegments(pdir)
	if err != nil {
		t.Fatalf("listSegments: %v", err)
	}
	if len(segs) < 2 {
		t.Errorf("expected ≥2 segments after roll, got %d (%v)", len(segs), segs)
	}

	// Give async finalize goroutines time to drain so the test cleanup
	// doesn't race them. Not strictly required for correctness, but
	// keeps t.TempDir() teardown clean.
	time.Sleep(50 * time.Millisecond)
}
