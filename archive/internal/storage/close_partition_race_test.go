package storage

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/woestebanaan/skafka/tests/testutil/recordbatch"
)

// TestClosePartitionWaitsForRollFinalize is the gh #136 regression.
//
// Pre-fix: rollSegment spawned an unsupervised goroutine that called
// finalizeAfterRoll + persistManifestLocked. If ClosePartition (or
// DeletePartition / the reaper) ran while that goroutine was in
// flight, the manifest write would re-create the partition dir AFTER
// the teardown had already removed it. Surfaced as flaky test
// cleanup ("directory not empty") and a real-world race during
// operator topic delete + recreate.
//
// Post-fix: ClosePartition Waits on ps.rollFinalize before returning.
// This test forces a segment roll, immediately calls ClosePartition,
// then RemoveAll's the dir, and asserts the dir stays gone — no
// finalize re-creation behind our back.
func TestClosePartitionWaitsForRollFinalize(t *testing.T) {
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

	// Force at least one roll. Each batch ~600 bytes (512 payload +
	// header overhead); SegmentBytes=4 KiB rolls every ~7 batches.
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

	// Close immediately after the last Append. Pre-fix, the most
	// recent rollSegment's finalize goroutine can still be running at
	// this exact moment.
	if err := e.ClosePartition("t", 0); err != nil {
		t.Fatalf("ClosePartition: %v", err)
	}

	// Caller-side RemoveAll, then sleep to give any errant finalize a
	// chance to misbehave. Post-fix, ps.rollFinalize.Wait() inside
	// ClosePartition has already drained the goroutine — nothing can
	// re-create the dir here.
	partDir := dir + "/t/0"
	if err := os.RemoveAll(partDir); err != nil {
		t.Fatalf("RemoveAll: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if _, err := os.Stat(partDir); !os.IsNotExist(err) {
		t.Fatalf("partition dir reappeared after Close+RemoveAll (gh #136 race): %v", err)
	}
}
