package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/woestebanaan/skafka/tests/testutil/recordbatch"
)

// TestProduceDoesNotWriteManifest pins gh #80: flushLocked must NOT
// touch manifest.json on the Produce hot path. This is the single
// biggest per-Produce NFS-RPC saving in the storage rework — every
// hot-path manifest write was costing ~4 RPCs (CREATE/WRITE/RENAME/dir
// COMMIT for the atomic tmp+rename), bigger than the log fsync itself
// on wifi-attached NFS.
//
// Strategy: snapshot manifest mtime after takeover (which DOES persist),
// run a burst of Produces, and assert mtime didn't change. If a future
// refactor accidentally re-introduces a hot-path manifest write, this
// test fails immediately.
func TestProduceDoesNotWriteManifest(t *testing.T) {
	dir := t.TempDir()
	leases := &neverLeaderLeases{}
	cfg := DefaultConfig()

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

	manifestPath := filepath.Join(dir, "t", "0", "manifest.json")
	before, err := os.Stat(manifestPath)
	if err != nil {
		t.Fatalf("stat manifest: %v", err)
	}

	// Produce a burst.
	for i := 0; i < 50; i++ {
		batch := &recordbatch.RecordBatch{
			BaseOffset: int64(i), LastOffsetDelta: 0,
			ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1,
			Records: []recordbatch.Record{{OffsetDelta: 0, Value: []byte("x")}},
		}
		if _, err := e.Append(context.Background(), "t", 0, 1, recordbatch.Encode(nil, batch)); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	after, err := os.Stat(manifestPath)
	if err != nil {
		t.Fatalf("stat manifest after appends: %v", err)
	}

	// The post-takeover manifest write set both mtime and ctime; a
	// hot-path re-write would update mtime again. We compare mtime
	// strictly because the atomic tmp+rename creates a new inode each
	// time, so even non-content changes show up.
	if !after.ModTime().Equal(before.ModTime()) || after.Size() != before.Size() {
		t.Errorf("manifest changed during a Produce burst (gh #80 regression): "+
			"before mtime=%v size=%d, after mtime=%v size=%d",
			before.ModTime(), before.Size(), after.ModTime(), after.Size())
	}
}
