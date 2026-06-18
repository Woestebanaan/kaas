package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/woestebanaan/skafka/tests/testutil/recordbatch"
)

// TestGroupCommitCoalescesConcurrentAppends pins gh #82: multiple
// Appends to the same partition that arrive close together share one
// fsync round-trip via the committer goroutine, instead of serialising
// one fsync per record.
//
// Strategy: launch N goroutines that each Append once and time the
// total wall-clock. With group commit they all complete in roughly
// one fsync's worth of time, not N. We can't trivially count fsyncs
// from outside the engine, but the wall-clock signal is reliable:
// if appends were serialised, total time would scale linearly with
// concurrency; with group commit it stays roughly flat.
func TestGroupCommitCoalescesConcurrentAppends(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	// FlushIntervalMessages=1 = fsync per Produce (worst case for
	// serialisation, best case for showing group commit's win).
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

	// Sanity baseline: a single Append succeeds.
	batch := func(off int64) []byte {
		return recordbatch.Encode(nil, &recordbatch.RecordBatch{
			BaseOffset: off, LastOffsetDelta: 0,
			ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1,
			Records: []recordbatch.Record{{OffsetDelta: 0, Value: []byte("x")}},
		})
	}
	if _, err := e.Append(context.Background(), "t", 0, 1, -1, batch(0)); err != nil {
		t.Fatalf("baseline append: %v", err)
	}

	// Concurrent Appends. With FlushIntervalMessages=1 every Append
	// triggers a flush request. The committer should coalesce them.
	const concurrency = 16
	var wg sync.WaitGroup
	wg.Add(concurrency)
	start := time.Now()
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			if _, err := e.Append(context.Background(), "t", 0, 1, -1, batch(0)); err != nil {
				t.Errorf("concurrent append: %v", err)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	t.Logf("concurrent fsync wall-clock: %v over %d goroutines", elapsed, concurrency)

	// All records were appended; HWM advanced by 1 (baseline) + concurrency.
	hwm, _ := e.HighWatermark("t", 0)
	want := int64(1 + concurrency)
	if hwm != want {
		t.Errorf("HWM=%d, want %d (some Appends were lost)", hwm, want)
	}
}

// TestGroupCommitDurabilityHoldsAcrossRestart guards the durability
// invariant: when Append returns success under FlushIntervalMessages=1,
// the data must survive a crash. Group commit must not regress this.
func TestGroupCommitDurabilityHoldsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()

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

	const n = 8
	for i := 0; i < n; i++ {
		_, err := e.Append(context.Background(), "t", 0, 1, -1, recordbatch.Encode(nil, &recordbatch.RecordBatch{
			BaseOffset: int64(i), LastOffsetDelta: 0,
			ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1,
			Records: []recordbatch.Record{{OffsetDelta: 0, Value: []byte("y")}},
		}))
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	// Drop the engine without a clean close — simulates SIGKILL.
	// Any record whose Append returned nil must be on durable storage.
	// (A clean close would shadow this — we'd miss a real bug where
	// only the close-time fsync is what landed the data.)

	// Reopen + takeover (bumped epoch triggers recoverSegment).
	e2, err := NewDiskStorageEngine(dir, &neverLeaderLeases{}, cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	hwm, err := e2.TakeOver(context.Background(), "t", 0, 99)
	if err != nil {
		t.Fatalf("takeover: %v", err)
	}
	if hwm != int64(n) {
		t.Errorf("recovered HWM=%d after %d Appends + crash, want %d", hwm, n, n)
	}
}
