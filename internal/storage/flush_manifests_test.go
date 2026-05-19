package storage

import (
	"context"
	"strings"
	"testing"

	"github.com/woestebanaan/skafka/tests/testutil/recordbatch"
)

// TestFlushManifests_PersistsCurrentHWM pins gh #139: after Append
// updates the in-memory HighWatermark but BEFORE any segment roll
// or Relinquish would normally write the manifest, FlushManifests
// must persist the current HWM to disk. The next openPartition
// (simulating broker restart) must then read that HWM, not 0.
//
// Without this, the lazy-manifest design (manifest written only on
// roll / cleaner / takeover / Relinquish) leaves the manifest stale
// across SIGTERM bounces. The new broker reads HWM=0 → kafka-get-
// offsets reports 0 → operators see "all data lost" even though
// the log file on disk still has the records.
func TestFlushManifests_PersistsCurrentHWM(t *testing.T) {
	dir := t.TempDir()
	leases := &neverLeaderLeases{}
	cfg := DefaultConfig()
	cfg.SegmentBytes = 1 << 30 // huge — guarantee no roll fires

	e1, err := NewDiskStorageEngine(dir, leases, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := e1.CreatePartition("t", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := e1.TakeOver(context.Background(), "t", 0, 1); err != nil {
		t.Fatal(err)
	}

	// Append N records. No roll → manifest is never auto-rewritten.
	const N = 50
	for i := 0; i < N; i++ {
		batch := recordbatch.Encode(nil, &recordbatch.RecordBatch{
			BaseOffset:      int64(i),
			LastOffsetDelta: 0,
			ProducerID:      -1,
			ProducerEpoch:   -1,
			BaseSequence:    -1,
			Records: []recordbatch.Record{
				{OffsetDelta: 0, Value: []byte(strings.Repeat("v", 16))},
			},
		})
		if _, err := e1.Append(context.Background(), "t", 0, 0, -1, batch); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	hwmBefore, _ := e1.HighWatermark("t", 0)
	if hwmBefore != int64(N) {
		t.Fatalf("in-memory HWM=%d, want %d", hwmBefore, N)
	}

	// Read the manifest BEFORE FlushManifests — it should still be
	// at 0 (or some older value), proving the lazy-manifest design
	// hasn't written through.
	m1, err := readManifest(partitionDir(dir, "t", 0))
	if err != nil {
		t.Fatalf("readManifest pre-flush: %v", err)
	}
	if m1.HighWatermark >= int64(N) {
		t.Logf("note: pre-flush manifest already at HWM=%d (segment-roll or other path persisted it)", m1.HighWatermark)
	}

	// Now flush. After this, the manifest must reflect N.
	if err := e1.FlushManifests(); err != nil {
		t.Fatalf("FlushManifests: %v", err)
	}

	m2, err := readManifest(partitionDir(dir, "t", 0))
	if err != nil {
		t.Fatalf("readManifest post-flush: %v", err)
	}
	if m2.HighWatermark != int64(N) {
		t.Errorf("post-flush manifest HWM=%d, want %d", m2.HighWatermark, N)
	}

	// Simulate broker restart: open a fresh engine on the same dir.
	// Without the FlushManifests fix, this would read HWM=0.
	e2, err := NewDiskStorageEngine(dir, leases, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := e2.CreatePartition("t", 0); err != nil {
		t.Fatal(err)
	}
	hwmAfter, err := e2.TakeOver(context.Background(), "t", 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if hwmAfter != int64(N) {
		t.Errorf("after restart, takeover HWM=%d, want %d (the gh #139 symptom)", hwmAfter, N)
	}
}

// partitionDir mirrors the storage engine's filesystem layout.
func partitionDir(dataDir, topic string, partition int32) string {
	return dataDir + "/" + topic + "/" + intToStr(int(partition))
}
