package storage

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/woestebanaan/skafka/tests/testutil/recordbatch"
)

// TestSegmentRoll_BySize pins gh #49: appending past
// cfg.SegmentBytes triggers a fresh active segment. We expect
// exactly one closed segment after the roll and the active segment
// to carry the trailing data.
func TestSegmentRoll_BySize(t *testing.T) {
	dir := t.TempDir()
	// Storage no longer consults IsLeader on Append (gh #75 cleanup);
	// any lease stub works here.
	leases := &neverLeaderLeases{}
	cfg := DefaultConfig()
	cfg.SegmentBytes = 8 * 1024 // 8 KiB — small enough to roll after a few batches

	e, err := NewDiskStorageEngine(dir, leases, cfg)
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	if err := e.CreatePartition("t", 0); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := e.TakeOver(context.Background(), "t", 0, 1); err != nil {
		t.Fatalf("takeover: %v", err)
	}

	// Append ~32 KiB worth of records, in batches well under the
	// segment threshold so we get multiple rolls (not just one).
	ctx := context.Background()
	const batchPayload = 1024 // ~1 KiB per record, plus batch overhead
	value := []byte(strings.Repeat("x", batchPayload))
	for i := 0; i < 32; i++ {
		batch := recordbatch.Encode(nil, &recordbatch.RecordBatch{
			BaseOffset:      int64(i),
			LastOffsetDelta: 0,
			ProducerID:      -1,
			ProducerEpoch:   -1,
			BaseSequence:    -1,
			Records: []recordbatch.Record{
				{OffsetDelta: 0, Value: value},
			},
		})
		if _, err := e.Append(ctx, "t", 0, 0, -1, batch); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	ps, ok := e.partitions[e.partKey("t", 0)]
	if !ok {
		t.Fatal("partition state missing after appends")
	}
	ps.mu.Lock()
	closedCount := len(ps.segments)
	activeBase := ps.active.baseOffset
	ps.mu.Unlock()

	if closedCount == 0 {
		t.Errorf("expected at least one closed segment after 32 KiB / SegmentBytes=8 KiB; got 0 (segment roll didn't fire)")
	}
	if activeBase == 0 {
		t.Errorf("active.baseOffset=0 — the roll didn't open a fresh segment at the next batch's offset")
	}

	// Sanity: the segment dir should physically have multiple .log files.
	matches, err := filepath.Glob(filepath.Join(dir, "t", "0", "*.log"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) < 2 {
		t.Errorf("disk shows %d .log files, want >=2 after segment roll", len(matches))
	}
}
