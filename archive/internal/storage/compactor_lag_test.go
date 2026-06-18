package storage

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/woestebanaan/skafka/tests/testutil/recordbatch"
)

// TestCompactPartitionRespectsMinCompactionLagMs pins gh #116 part 1:
// a segment whose maxTimestamp is within the lag window must NOT be
// compacted. With the lag window covering every segment, the compactor
// is a no-op; with the lag at 0, every segment is eligible (existing
// behaviour, covered by the dedupe test).
func TestCompactPartitionRespectsMinCompactionLagMs(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.SegmentBytes = 4 * 1024
	cfg.FlushIntervalMessages = 0

	e, err := NewDiskStorageEngine(dir, &neverLeaderLeases{}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.CreatePartition("ktbl", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := e.TakeOver(context.Background(), "ktbl", 0, 1); err != nil {
		t.Fatal(err)
	}

	// Append 30 records spanning multiple closed segments so the
	// compactor has something to chew on. All records timestamped
	// "now" so the lag gate applies uniformly.
	nowMs := time.Now().UnixMilli()
	for i := 0; i < 30; i++ {
		key := []byte{byte('k'), byte('0' + (i % 3))}
		value := make([]byte, 500)
		value[0] = byte(i)
		batch := recordbatch.Encode(nil, &recordbatch.RecordBatch{
			BaseOffset: 0, LastOffsetDelta: 0,
			BaseTimestamp: nowMs,
			MaxTimestamp:  nowMs,
			ProducerID:    -1, ProducerEpoch: -1, BaseSequence: -1,
			Records: []recordbatch.Record{
				{OffsetDelta: 0, Key: key, Value: value},
			},
		})
		if _, err := e.Append(context.Background(), "ktbl", 0, 1, -1, batch); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	ps, ok := e.getPartition("ktbl", 0)
	if !ok {
		t.Fatal("partition missing")
	}

	// First pass: lag = 1 hour. Every closed segment is within the
	// window — compactor must skip them all.
	ps.mu.Lock()
	ps.minCompactionLagMsOverride = int64(time.Hour / time.Millisecond)
	closedBefore := len(ps.segments)
	ps.mu.Unlock()
	kept, dropped, err := e.compactPartition(ps)
	if err != nil {
		t.Fatal(err)
	}
	if kept != 0 || dropped != 0 {
		t.Errorf("with lag=1h all-fresh: kept=%d dropped=%d, want 0/0 (no segment eligible)", kept, dropped)
	}
	ps.mu.Lock()
	closedAfter := len(ps.segments)
	ps.mu.Unlock()
	if closedAfter != closedBefore {
		t.Errorf("closed-segment count changed: before=%d after=%d (lag gate must skip everything)", closedBefore, closedAfter)
	}

	// Second pass: lag = 0 (disabled). Compaction runs as before.
	ps.mu.Lock()
	ps.minCompactionLagMsOverride = 0
	ps.mu.Unlock()
	kept2, _, err := e.compactPartition(ps)
	if err != nil {
		t.Fatal(err)
	}
	if kept2 == 0 {
		t.Errorf("with lag=0: kept=%d, want >0 (compaction must run when lag is disabled)", kept2)
	}
}

// TestCompactPartitionDropsExpiredTombstones pins gh #116 part 2:
// a tombstone (value=nil) older than delete.retention.ms is dropped
// even when it's the "latest" for its key. With deleteRetentionMs=0
// (disabled), tombstones survive.
func TestCompactPartitionDropsExpiredTombstones(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.SegmentBytes = 1024 // small so 3 batches roll
	cfg.FlushIntervalMessages = 0

	e, err := NewDiskStorageEngine(dir, &neverLeaderLeases{}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.CreatePartition("ktbl", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := e.TakeOver(context.Background(), "ktbl", 0, 1); err != nil {
		t.Fatal(err)
	}

	// Three batches:
	// 1. key=k1 value=v1  ts=oldOld (older than delete.retention.ms)
	// 2. key=k1 value=nil ts=oldOld (tombstone, old)
	// 3. key=k2 value=v2  ts=now    (filler to force a roll past
	//                                the tombstone)
	oldMs := int64(100)
	pad := make([]byte, 500)
	batches := []*recordbatch.RecordBatch{
		{BaseOffset: 0, BaseTimestamp: oldMs, MaxTimestamp: oldMs, ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1,
			Records: []recordbatch.Record{{OffsetDelta: 0, Key: []byte("k1"), Value: append([]byte{1}, pad...)}}},
		{BaseOffset: 0, BaseTimestamp: oldMs, MaxTimestamp: oldMs, ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1,
			Records: []recordbatch.Record{{OffsetDelta: 0, Key: []byte("k1"), Value: nil}}}, // tombstone
		{BaseOffset: 0, BaseTimestamp: oldMs, MaxTimestamp: oldMs, ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1,
			Records: []recordbatch.Record{{OffsetDelta: 0, Key: []byte("k2"), Value: append([]byte{2}, pad...)}}},
	}
	for _, b := range batches {
		enc := recordbatch.Encode(nil, b)
		if _, err := e.Append(context.Background(), "ktbl", 0, 1, -1, enc); err != nil {
			t.Fatal(err)
		}
	}
	// Force a roll so all three batches end up as closed segments.
	enc := recordbatch.Encode(nil, &recordbatch.RecordBatch{
		BaseOffset: 0, BaseTimestamp: oldMs, MaxTimestamp: oldMs,
		ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1,
		Records: []recordbatch.Record{{OffsetDelta: 0, Key: []byte("filler"), Value: pad}},
	})
	if _, err := e.Append(context.Background(), "ktbl", 0, 1, -1, enc); err != nil {
		t.Fatal(err)
	}

	ps, ok := e.getPartition("ktbl", 0)
	if !ok {
		t.Fatal("partition missing")
	}
	ps.mu.Lock()
	// Set delete.retention.ms = 50ms; the tombstone's baseTimestamp
	// is 100ms epoch, so cutoff = now-50ms ≈ huge → tombstone < cutoff,
	// hence expired.
	ps.deleteRetentionMsOverride = 50
	ps.mu.Unlock()

	if _, _, err := e.compactPartition(ps); err != nil {
		t.Fatalf("compactPartition: %v", err)
	}

	// After compaction, walk the surviving segment(s) to confirm
	// no record for k1 exists (both the original and the tombstone
	// got dropped — the tombstone via delete.retention.ms, the
	// original via being superseded).
	ps.mu.Lock()
	survivors := append([]segmentMeta(nil), ps.segments...)
	ps.mu.Unlock()
	sawK1 := false
	for _, seg := range survivors {
		_ = filepath.Base(seg.logPath)
		_ = walkSegmentRecords(seg.logPath, func(_ int64, key []byte, _ bool) error {
			if string(key) == "k1" {
				sawK1 = true
			}
			return nil
		})
	}
	if sawK1 {
		t.Error("expected k1 records to be gone (original + tombstone), but at least one survived")
	}
}
