package storage

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// TestProducerSnapshotRoundTrip pins the JSON-on-disk encoding of
// the dedupe window. Keeping it stable matters because operators
// will hand-inspect this file the first time a producer fence
// produces a confusing error in the field.
func TestProducerSnapshotRoundTrip(t *testing.T) {
	dir := t.TempDir()
	original := map[int64]*producerEntry{
		111: {epoch: 0, recent: []recentBatch{
			{firstSeq: 0, lastSeq: 4, baseOffset: 0},
			{firstSeq: 5, lastSeq: 9, baseOffset: 5},
		}},
		222: {epoch: 3, recent: []recentBatch{
			{firstSeq: 0, lastSeq: 0, baseOffset: 100},
		}},
		// Producer with no entries (just an epoch) — exercise the
		// omitempty path on the on-disk schema.
		333: {epoch: 7},
	}
	if err := writeProducerSnapshot(dir, original); err != nil {
		t.Fatalf("write: %v", err)
	}

	got, err := readProducerSnapshot(dir)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	// Map equality is awkward with pointer values. Compare per-PID.
	if len(got) != len(original) {
		t.Fatalf("decoded %d producers, want %d", len(got), len(original))
	}
	for pid, want := range original {
		gotE, ok := got[pid]
		if !ok {
			t.Fatalf("PID %d missing from decode", pid)
		}
		if gotE.epoch != want.epoch {
			t.Errorf("PID %d epoch=%d, want %d", pid, gotE.epoch, want.epoch)
		}
		// nil vs empty slice are equivalent for our semantics —
		// JSON's omitempty drops the field on encode and the
		// decoder produces an empty slice; either is "no batches
		// in the dedupe window".
		if len(gotE.recent) != len(want.recent) {
			t.Errorf("PID %d recent len=%d, want %d", pid, len(gotE.recent), len(want.recent))
			continue
		}
		if len(want.recent) > 0 && !reflect.DeepEqual(gotE.recent, want.recent) {
			t.Errorf("PID %d recent=%+v, want %+v", pid, gotE.recent, want.recent)
		}
	}
}

// TestProducerSnapshotMissingReturnsNotExist guards the open path's
// "no snapshot yet" branch. If the file is missing we return
// fs.ErrNotExist (the engine treats it as "fresh state").
func TestProducerSnapshotMissingReturnsNotExist(t *testing.T) {
	dir := t.TempDir()
	_, err := readProducerSnapshot(dir)
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("err=%v, want fs.ErrNotExist", err)
	}
}

// TestProducerSnapshotFutureVersionStartsFresh: a snapshot written
// by a future schema version returns (nil, nil) — engine treats
// that as "start with empty state". Strictly better than refusing
// to open the partition or panicking on unknown fields.
func TestProducerSnapshotFutureVersionStartsFresh(t *testing.T) {
	dir := t.TempDir()
	// Hand-craft a file with version=99.
	bad := []byte(`{"version":99,"entries":[{"producer_id":1,"epoch":0}]}`)
	path := filepath.Join(dir, producerSnapshotFilename)
	if err := writeFile(path, bad); err != nil {
		t.Fatalf("seed: %v", err)
	}
	got, err := readProducerSnapshot(dir)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got != nil {
		t.Errorf("future-version snapshot returned %v, want nil", got)
	}
}

// TestProducerSnapshotMalformedReturnsError: a corrupt JSON file
// surfaces the parse error so an operator can investigate. We do
// NOT silently start fresh on corruption — that would hide a
// systemic bug (e.g. truncated writes, fs corruption).
func TestProducerSnapshotMalformedReturnsError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, producerSnapshotFilename)
	if err := writeFile(path, []byte("{not json")); err != nil {
		t.Fatalf("seed: %v", err)
	}
	_, err := readProducerSnapshot(dir)
	if err == nil {
		t.Error("malformed snapshot should return an error")
	}
	if errors.Is(err, fs.ErrNotExist) {
		t.Errorf("malformed should NOT collapse to ErrNotExist: %v", err)
	}
}

// TestEngineProducerStateSurvivesReopen exercises the full B2
// promise end-to-end: open partition, idempotent producer writes
// some batches, close engine, re-open, retry the most recent
// batch. The re-open must restore the dedupe window so the retry
// returns the original baseOffset (not OUT_OF_ORDER, which a
// fresh-state broker would emit).
//
// This is the live-cluster scenario stage B1 alone fails: any
// pod restart in the middle of a producer's session breaks all
// in-flight idempotent retries.
func TestEngineProducerStateSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.FlushIntervalMessages = 0

	e1, err := NewDiskStorageEngine(dir, &neverLeaderLeases{}, cfg)
	if err != nil {
		t.Fatalf("engine 1: %v", err)
	}
	if err := e1.CreatePartition("t", 0); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := e1.TakeOver(context.Background(), "t", 0, 1); err != nil {
		t.Fatalf("takeover 1: %v", err)
	}

	// Producer writes two batches at PID=42, epoch=0.
	first, err := e1.Append(context.Background(), "t", 0, 1, idempotentBatch(42, 0, 0, 5))
	if err != nil {
		t.Fatalf("seed 1: %v", err)
	}
	second, err := e1.Append(context.Background(), "t", 0, 1, idempotentBatch(42, 0, 5, 5))
	if err != nil {
		t.Fatalf("seed 2: %v", err)
	}

	// Force the snapshot to disk via Relinquish (graceful handoff
	// path that B2 hooks). DiskStorageEngine has no general
	// "flush" — Relinquish is the closest broker-level analogue.
	if err := e1.Relinquish("t", 0); err != nil {
		t.Fatalf("relinquish: %v", err)
	}

	// Fresh engine on the same data dir — simulates restart /
	// pod-replacement / leader-takeover.
	e2, err := NewDiskStorageEngine(dir, &neverLeaderLeases{}, cfg)
	if err != nil {
		t.Fatalf("engine 2: %v", err)
	}
	if _, err := e2.TakeOver(context.Background(), "t", 0, 1); err != nil {
		t.Fatalf("takeover 2: %v", err)
	}

	// Producer's in-flight retry of the SECOND batch must dedupe
	// to the original baseOffset, not be treated as out-of-order.
	retry, err := e2.Append(context.Background(), "t", 0, 1, idempotentBatch(42, 0, 5, 5))
	if err != nil {
		t.Fatalf("post-restart retry: %v (B2 snapshot did not restore dedupe window)", err)
	}
	if retry != second {
		t.Errorf("post-restart retry baseOffset=%d, want %d (dedupe of original)", retry, second)
	}
	if retry == first {
		t.Error("retry deduped to FIRST batch — wrong window slot")
	}
}

// TestEngineProducerStateRestoresAcrossDifferentEpoch: the
// snapshot must record the producer's epoch so a zombie at the
// older epoch is fenced (errCode 47) even after restart. Without
// epoch persistence, a stale producer's writes would appear as
// "first batch from this PID" and corrupt the log.
func TestEngineProducerStateRestoresAcrossDifferentEpoch(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.FlushIntervalMessages = 0

	e1, err := NewDiskStorageEngine(dir, &neverLeaderLeases{}, cfg)
	if err != nil {
		t.Fatalf("engine 1: %v", err)
	}
	if err := e1.CreatePartition("t", 0); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := e1.TakeOver(context.Background(), "t", 0, 1); err != nil {
		t.Fatalf("takeover 1: %v", err)
	}

	// Establish PID=99 at epoch=5 with a batch landed.
	if _, err := e1.Append(context.Background(), "t", 0, 1, idempotentBatch(99, 5, 0, 3)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := e1.Relinquish("t", 0); err != nil {
		t.Fatalf("relinquish: %v", err)
	}

	e2, err := NewDiskStorageEngine(dir, &neverLeaderLeases{}, cfg)
	if err != nil {
		t.Fatalf("engine 2: %v", err)
	}
	if _, err := e2.TakeOver(context.Background(), "t", 0, 1); err != nil {
		t.Fatalf("takeover 2: %v", err)
	}

	// Zombie producer at older epoch=4 attempts to write — must
	// get InvalidProducerEpoch (47), not appear as a fresh PID.
	_, err = e2.Append(context.Background(), "t", 0, 1, idempotentBatch(99, 4, 3, 3))
	if !errors.Is(err, ErrInvalidProducerEpoch) {
		t.Errorf("post-restart zombie err=%v, want ErrInvalidProducerEpoch (B2 did not persist epoch)", err)
	}
}

func writeFile(path string, data []byte) error {
	return os.WriteFile(path, data, 0644)
}
