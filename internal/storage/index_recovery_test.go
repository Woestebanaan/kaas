package storage

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/woestebanaan/skafka/tests/testutil/recordbatch"
)

// TestOpenPartitionToleratesEmptyIndex guards gh #81: the index file is
// no longer fsynced per Produce, so on broker restart the active
// segment's index may be missing tail entries (or be empty entirely if
// nothing made it to disk). openPartition must succeed regardless.
// Existing index entries on disk always point at valid log positions
// because they were only written after the corresponding batch was
// fsynced; missing tail entries just leave the index sparse. Fetch
// linear-scans forward from the nearest indexed offset.
//
// We simulate the worst case by truncating the index file to zero on
// disk after appends, then re-opening the engine. Open must succeed
// without scanning the entire log on the hot startup path — that kept
// large partitions from coming back inside the kubelet's liveness
// probe deadline (skafka-2 CrashLoopBackoff after the v0.1.27 rollout).
func TestOpenPartitionToleratesEmptyIndex(t *testing.T) {
	dir := t.TempDir()
	leases := &neverLeaderLeases{}

	// Use a tiny IndexIntervalBytes so a handful of small batches
	// generate multiple index entries, making the absence of any one of
	// them detectable.
	cfg := DefaultConfig()
	cfg.IndexIntervalBytes = 64

	e, err := NewDiskStorageEngine(dir, leases, cfg)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	if err := e.CreatePartition("t", 0); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := e.TakeOver(context.Background(), "t", 0, 1); err != nil {
		t.Fatalf("takeover: %v", err)
	}

	// Append enough batches that the index would have several entries
	// at the chosen 64-byte interval.
	for i := 0; i < 20; i++ {
		batch := &recordbatch.RecordBatch{
			BaseOffset:      int64(i),
			LastOffsetDelta: 0,
			ProducerID:      -1,
			ProducerEpoch:   -1,
			BaseSequence:    -1,
			Records: []recordbatch.Record{{
				OffsetDelta: 0,
				Value:       []byte("payload-bigger-than-twenty-bytes"),
			}},
		}
		raw := recordbatch.Encode(nil, batch)
		if _, err := e.Append(context.Background(), "t", 0, 1, raw); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	// Locate the active segment's .index file via the partition dir
	// listing — this is the on-disk state that openPartition will read.
	entries, err := os.ReadDir(filepath.Join(dir, "t", "0"))
	if err != nil {
		t.Fatalf("readdir partition: %v", err)
	}
	var indexPath string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".index") {
			indexPath = filepath.Join(dir, "t", "0", e.Name())
			break
		}
	}
	if indexPath == "" {
		t.Fatal("no .index file found for partition")
	}

	indexBefore, err := os.Stat(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	if indexBefore.Size() == 0 {
		t.Fatal("expected non-empty index after appends; cannot exercise rebuild path")
	}

	// Close the partition handles cleanly and simulate the post-crash
	// state where the OS-cached index writes were lost: truncate the
	// index file to zero bytes. The log is untouched (it WAS fsynced).
	if err := e.ClosePartition("t", 0); err != nil {
		t.Fatalf("close partition: %v", err)
	}
	if err := os.Truncate(indexPath, 0); err != nil {
		t.Fatalf("truncate index: %v", err)
	}

	// Re-open. openPartition must rebuild the index from the durable log.
	if _, err := NewDiskStorageEngine(dir, &neverLeaderLeases{}, cfg); err != nil {
		t.Fatalf("reopen engine: %v", err)
	}

	indexAfter, err := os.Stat(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	// Index size on reopen is irrelevant to correctness — what matters
	// is that the engine opened without error. The append path will
	// repopulate index entries as new batches arrive. Just guard that
	// nothing exotic happened on disk (size remains a multiple of 8).
	if indexAfter.Size()%8 != 0 {
		t.Errorf("index size %d is not a multiple of 8 (8B per entry)",
			indexAfter.Size())
	}
}
