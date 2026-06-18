package storage

import (
	"context"
	"testing"
	"time"

	"github.com/woestebanaan/skafka/tests/testutil/recordbatch"
)

// TestOffsetForTimestamp_NoSegments — empty partition returns
// (-1, -1) per the Apache wire contract: "no matching record".
func TestOffsetForTimestamp_NoSegments(t *testing.T) {
	e, _ := NewDiskStorageEngine(t.TempDir(), &neverLeaderLeases{}, DefaultConfig())
	if err := e.CreatePartition("t", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := e.TakeOver(context.Background(), "t", 0, 1); err != nil {
		t.Fatal(err)
	}
	off, ts, err := e.OffsetForTimestamp("t", 0, time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if off != -1 || ts != -1 {
		t.Errorf("got (%d, %d), want (-1, -1) for empty partition", off, ts)
	}
}

// TestOffsetForTimestamp_HitsActiveSegment — when records are
// written via Append, OffsetForTimestamp(now-ish) returns the
// active segment's baseOffset (0 for a fresh partition).
func TestOffsetForTimestamp_HitsActiveSegment(t *testing.T) {
	e, _ := NewDiskStorageEngine(t.TempDir(), &neverLeaderLeases{}, DefaultConfig())
	if err := e.CreatePartition("t", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := e.TakeOver(context.Background(), "t", 0, 1); err != nil {
		t.Fatal(err)
	}

	beforeProduce := time.Now().UnixMilli()
	batch := recordbatch.Encode(nil, &recordbatch.RecordBatch{
		BaseOffset:      0,
		LastOffsetDelta: 0,
		ProducerID:      -1,
		ProducerEpoch:   -1,
		BaseSequence:    -1,
		MaxTimestamp:    time.Now().UnixMilli(),
		Records: []recordbatch.Record{
			{OffsetDelta: 0, Value: []byte("hi")},
		},
	})
	if _, err := e.Append(context.Background(), "t", 0, 0, -1, batch); err != nil {
		t.Fatal(err)
	}

	// Request at beforeProduce — every record was written at or after
	// that timestamp, so the active segment satisfies.
	off, ts, err := e.OffsetForTimestamp("t", 0, beforeProduce)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if off != 0 {
		t.Errorf("got offset=%d, want 0 (baseOffset of the active segment)", off)
	}
	if ts <= 0 {
		t.Errorf("got ts=%d, want a real maxTimestamp", ts)
	}
}

// TestOffsetForTimestamp_FutureRequestReturnsNoMatch — a request
// for a timestamp past all records returns (-1, -1). Java
// consumers using offsetsForTimes() interpret this as "topic has
// no record at or after T", which is correct.
func TestOffsetForTimestamp_FutureRequestReturnsNoMatch(t *testing.T) {
	e, _ := NewDiskStorageEngine(t.TempDir(), &neverLeaderLeases{}, DefaultConfig())
	if err := e.CreatePartition("t", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := e.TakeOver(context.Background(), "t", 0, 1); err != nil {
		t.Fatal(err)
	}
	batch := recordbatch.Encode(nil, &recordbatch.RecordBatch{
		BaseOffset:      0,
		LastOffsetDelta: 0,
		ProducerID:      -1,
		ProducerEpoch:   -1,
		BaseSequence:    -1,
		MaxTimestamp:    100,
		Records: []recordbatch.Record{
			{OffsetDelta: 0, Value: []byte("x")},
		},
	})
	if _, err := e.Append(context.Background(), "t", 0, 0, -1, batch); err != nil {
		t.Fatal(err)
	}
	off, ts, err := e.OffsetForTimestamp("t", 0, time.Now().UnixMilli()+1<<32)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if off != -1 || ts != -1 {
		t.Errorf("got (%d, %d), want (-1, -1) for far-future timestamp", off, ts)
	}
}

// TestOffsetForTimestamp_UnknownPartition pins the wire contract:
// asking about a partition this broker doesn't host returns
// ErrUnknownPartition (the handler maps this to
// UNKNOWN_TOPIC_OR_PARTITION on the wire).
func TestOffsetForTimestamp_UnknownPartition(t *testing.T) {
	e, _ := NewDiskStorageEngine(t.TempDir(), &neverLeaderLeases{}, DefaultConfig())
	_, _, err := e.OffsetForTimestamp("ghost", 0, 0)
	if err == nil {
		t.Error("expected error for unknown partition, got nil")
	}
}
