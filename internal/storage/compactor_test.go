package storage

import (
	"context"
	"encoding/binary"
	"io"
	"os"
	"testing"

	"github.com/woestebanaan/skafka/tests/testutil/recordbatch"
)

// TestCompactPartitionDedupesByKey is the gh #48 happy-path
// behaviour: write distinct-key records spread across multiple
// closed segments, force a compaction, verify only the latest
// record per key survives and absolute offsets are preserved.
func TestCompactPartitionDedupesByKey(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	// Tiny segment limit so the produce loop rolls multiple
	// closed segments — compaction's interesting case.
	cfg.SegmentBytes = 4 * 1024
	cfg.FlushIntervalMessages = 0

	e, err := NewDiskStorageEngine(dir, &neverLeaderLeases{}, cfg)
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	if err := e.CreatePartition("ktbl", 0); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := e.TakeOver(context.Background(), "ktbl", 0, 1); err != nil {
		t.Fatalf("takeover: %v", err)
	}

	keys := []string{"k1", "k2", "k3"}
	// Pad each value to ~500 bytes so 30 single-record batches
	// exceed the 4 KiB segment limit and trigger multiple rolls.
	// First byte of the value encodes the iteration index (0..29)
	// — readKeyValuesFromSegments asserts on it.
	for i := 0; i < 30; i++ {
		key := keys[i%3]
		value := make([]byte, 500)
		value[0] = byte(i)
		batch := recordbatch.Encode(nil, &recordbatch.RecordBatch{
			BaseOffset: 0, LastOffsetDelta: 0,
			BaseTimestamp: 1700000000000 + int64(i),
			MaxTimestamp:  1700000000000 + int64(i),
			ProducerID:    -1, ProducerEpoch: -1, BaseSequence: -1,
			Records: []recordbatch.Record{
				{OffsetDelta: 0, Key: []byte(key), Value: value},
			},
		})
		if _, err := e.Append(context.Background(), "ktbl", 0, 1, batch); err != nil {
			t.Fatalf("append i=%d: %v", i, err)
		}
	}
	hwmBefore, _ := e.HighWatermark("ktbl", 0)
	if hwmBefore != 30 {
		t.Fatalf("HWM before compact=%d, want 30", hwmBefore)
	}

	ps, ok := e.getPartition("ktbl", 0)
	if !ok {
		t.Fatal("partition state missing post-takeover")
	}
	// Pre-compact: snapshot the closed-segments slice and compute
	// the expected post-compaction state. The active segment is
	// off-limits to compaction (Apache rule) so winners are the
	// LATEST per-key value in CLOSED segments only — the active
	// segment's records survive verbatim.
	ps.mu.Lock()
	closedSegsBefore := append([]segmentMeta(nil), ps.segments...)
	ps.mu.Unlock()
	if len(closedSegsBefore) < 2 {
		t.Fatalf("only %d closed segment(s) after 30 appends; engine cfg drift?", len(closedSegsBefore))
	}
	expectedWinners := readKeyValuesFromSegments(t, closedSegsBefore)
	expectedClosedRecords := countRecordsInSegments(t, closedSegsBefore)

	kept, dropped, err := e.compactPartition(ps)
	if err != nil {
		t.Fatalf("compactPartition: %v", err)
	}
	t.Logf("compacted: kept=%d dropped=%d closedBefore=%d closedRecords=%d expectedWinners=%v",
		kept, dropped, len(closedSegsBefore), expectedClosedRecords, expectedWinners)

	// Compaction collapses the closed segments to one record per
	// distinct key. Active records aren't counted here because the
	// compactor doesn't touch them.
	if kept != len(expectedWinners) {
		t.Errorf("kept=%d, want %d (one per distinct closed-segment key)", kept, len(expectedWinners))
	}
	if dropped != expectedClosedRecords-len(expectedWinners) {
		t.Errorf("dropped=%d, want %d (closed total - winners)", dropped, expectedClosedRecords-len(expectedWinners))
	}

	// Post-compact: walk the new closed segments and confirm we
	// see exactly the expected winners.
	ps.mu.Lock()
	segsAfter := append([]segmentMeta(nil), ps.segments...)
	ps.mu.Unlock()
	gotWinners := readKeyValuesFromSegments(t, segsAfter)
	if len(gotWinners) != len(expectedWinners) {
		t.Errorf("post-compact distinct keys=%d, want %d", len(gotWinners), len(expectedWinners))
	}
	for k, v := range expectedWinners {
		if got, ok := gotWinners[k]; !ok || got != v {
			t.Errorf("key %q: post-compact value=%d (ok=%v), want %d", k, got, ok, v)
		}
	}

	hwmAfter, _ := e.HighWatermark("ktbl", 0)
	if hwmAfter != hwmBefore {
		t.Errorf("HWM changed from %d to %d (compaction must not move HWM)", hwmBefore, hwmAfter)
	}
}

// countRecordsInSegments walks segs and returns the total record
// count across all batches. Test helper for compaction assertions
// where the closed-segment count varies with engine config.
func countRecordsInSegments(t *testing.T, segs []segmentMeta) int {
	t.Helper()
	total := 0
	for _, seg := range segs {
		if err := walkSegmentRecords(seg.logPath, func(_ int64, _ []byte, _ bool) error {
			total++
			return nil
		}); err != nil {
			t.Fatalf("walk %s: %v", seg.logPath, err)
		}
	}
	return total
}

// TestCompactPartitionPreservesNullKeyedRecords: records without
// keys can't be deduped (no key to dedupe by). Apache keeps them
// verbatim; skafka must too.
func TestCompactPartitionPreservesNullKeyedRecords(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.SegmentBytes = 4 * 1024
	cfg.FlushIntervalMessages = 0
	e, _ := NewDiskStorageEngine(dir, &neverLeaderLeases{}, cfg)
	_ = e.CreatePartition("mixed", 0)
	if _, err := e.TakeOver(context.Background(), "mixed", 0, 1); err != nil {
		t.Fatalf("takeover: %v", err)
	}

	for i := 0; i < 20; i++ {
		var key []byte
		if i%2 == 0 {
			key = []byte("the-only-key")
		}
		value := make([]byte, 500)
		value[0] = byte(i)
		batch := recordbatch.Encode(nil, &recordbatch.RecordBatch{
			BaseOffset: 0, LastOffsetDelta: 0,
			ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1,
			Records: []recordbatch.Record{{OffsetDelta: 0, Key: key, Value: value}},
		})
		if _, err := e.Append(context.Background(), "mixed", 0, 1, batch); err != nil {
			t.Fatalf("append i=%d: %v", i, err)
		}
	}

	ps, _ := e.getPartition("mixed", 0)
	if len(ps.segments) < 2 {
		t.Skipf("only %d closed segments; test needs >=2", len(ps.segments))
	}

	// Same dynamic-expectation pattern as the dedupes test:
	// active segment's records won't be in `kept` because
	// compaction doesn't touch them.
	ps.mu.Lock()
	closedBefore := append([]segmentMeta(nil), ps.segments...)
	ps.mu.Unlock()
	expectedKept := 0
	expectedKeysSeen := map[string]bool{}
	for _, seg := range closedBefore {
		if err := walkSegmentRecords(seg.logPath, func(_ int64, key []byte, _ bool) error {
			if key == nil {
				expectedKept++ // every null-keyed record survives
			} else {
				expectedKeysSeen[string(key)] = true
			}
			return nil
		}); err != nil {
			t.Fatalf("walk %s: %v", seg.logPath, err)
		}
	}
	expectedKept += len(expectedKeysSeen) // one survivor per distinct key

	kept, _, err := e.compactPartition(ps)
	if err != nil {
		t.Fatalf("compactPartition: %v", err)
	}
	if kept != expectedKept {
		t.Errorf("kept=%d, want %d (null-keyed preserved + 1 per distinct key in closed segments)", kept, expectedKept)
	}
}

// TestCompactPartitionEmptyPartitionNoOp: a partition with no
// closed segments (only the active segment) must not error.
func TestCompactPartitionEmptyPartitionNoOp(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.FlushIntervalMessages = 0
	e, _ := NewDiskStorageEngine(dir, &neverLeaderLeases{}, cfg)
	_ = e.CreatePartition("fresh", 0)
	if _, err := e.TakeOver(context.Background(), "fresh", 0, 1); err != nil {
		t.Fatalf("takeover: %v", err)
	}
	ps, _ := e.getPartition("fresh", 0)
	kept, dropped, err := e.compactPartition(ps)
	if err != nil {
		t.Errorf("empty partition: err=%v, want nil", err)
	}
	if kept != 0 || dropped != 0 {
		t.Errorf("empty partition: kept=%d dropped=%d, want 0,0", kept, dropped)
	}
}

// readKeyValuesFromSegments scans the segment files post-compaction
// and returns map[key]firstByteOfValue. Reads each batch's
// 12-byte prefix + body, then iterateRecords for keys, then a
// small inline parse to recover the value. Production code keeps
// values opaque, so this helper belongs only in tests.
func readKeyValuesFromSegments(t *testing.T, segs []segmentMeta) map[string]byte {
	t.Helper()
	out := map[string]byte{}
	for _, seg := range segs {
		f, err := os.Open(seg.logPath)
		if err != nil {
			t.Fatalf("open %s: %v", seg.logPath, err)
		}
		for {
			var prefix [12]byte
			_, rerr := io.ReadFull(f, prefix[:])
			if rerr == io.EOF {
				break
			}
			if rerr != nil {
				f.Close()
				t.Fatalf("read prefix in %s: %v", seg.logPath, rerr)
			}
			batchLength := int32(binary.BigEndian.Uint32(prefix[8:12]))
			raw := make([]byte, 12+int(batchLength))
			copy(raw[:12], prefix[:])
			if _, rerr := io.ReadFull(f, raw[12:]); rerr != nil {
				f.Close()
				t.Fatalf("read body in %s: %v", seg.logPath, rerr)
			}
			_, _, ierr := iterateRecords(raw, func(rec compactedRecord) error {
				if rec.Key == nil {
					return nil
				}
				val := extractValueByte(rec.Body)
				out[string(rec.Key)] = val
				return nil
			})
			if ierr != nil {
				f.Close()
				t.Fatalf("iterate batch in %s: %v", seg.logPath, ierr)
			}
		}
		f.Close()
	}
	return out
}

// extractValueByte reads a record body and returns the first byte
// of its value (or 0 if the value is empty/null). Mirrors
// parseRecordBody but pulls one extra field. Test-only.
func extractValueByte(body []byte) byte {
	pos := 1 // attrs
	_, n := binary.Varint(body[pos:])
	pos += n // tsDelta
	_, n = binary.Varint(body[pos:])
	pos += n // offsetDelta
	keyLen, n := binary.Varint(body[pos:])
	pos += n
	if keyLen >= 0 {
		pos += int(keyLen)
	}
	valLen, n := binary.Varint(body[pos:])
	pos += n
	if valLen <= 0 || pos >= len(body) {
		return 0
	}
	return body[pos]
}
