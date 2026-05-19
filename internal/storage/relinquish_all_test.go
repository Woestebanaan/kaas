package storage

import (
	"context"
	"strings"
	"testing"

	"github.com/woestebanaan/skafka/tests/testutil/recordbatch"
)

// TestRelinquishAll_PersistsAndClosesEveryPartition pins gh #61's
// scale-down drain contract: every partition this broker leads must
// have its manifest flushed AND its file descriptors closed before
// the broker exits.
//
// Mirrors the production shutdown path: write some records into
// each of several partitions, call RelinquishAll, then reopen the
// engine and confirm every partition's recovered HWM matches what
// was written.
func TestRelinquishAll_PersistsAndClosesEveryPartition(t *testing.T) {
	dir := t.TempDir()
	leases := &neverLeaderLeases{}
	cfg := DefaultConfig()
	cfg.SegmentBytes = 1 << 30 // huge — no roll

	e1, err := NewDiskStorageEngine(dir, leases, cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Three topics × two partitions = six leases to drain.
	const recordsPerPartition = 7
	owners := []struct {
		topic string
		part  int32
	}{
		{"a", 0},
		{"a", 1},
		{"b", 0},
		{"b", 1},
		{"c", 0},
		{"c", 1},
	}
	for _, o := range owners {
		if err := e1.CreatePartition(o.topic, o.part); err != nil {
			t.Fatalf("create %s/%d: %v", o.topic, o.part, err)
		}
		if _, err := e1.TakeOver(context.Background(), o.topic, o.part, 1); err != nil {
			t.Fatalf("takeover %s/%d: %v", o.topic, o.part, err)
		}
		for i := 0; i < recordsPerPartition; i++ {
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
			if _, err := e1.Append(context.Background(), o.topic, o.part, 0, -1, batch); err != nil {
				t.Fatalf("append %s/%d: %v", o.topic, o.part, err)
			}
		}
	}

	// Drain.
	if err := e1.RelinquishAll(); err != nil {
		t.Fatalf("RelinquishAll: %v", err)
	}

	// Re-open the same dir as a fresh engine (simulating the next
	// broker pod taking over). Every partition's HWM after takeover
	// must equal recordsPerPartition.
	e2, err := NewDiskStorageEngine(dir, leases, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range owners {
		if err := e2.CreatePartition(o.topic, o.part); err != nil {
			t.Fatal(err)
		}
		hwm, err := e2.TakeOver(context.Background(), o.topic, o.part, 2)
		if err != nil {
			t.Fatalf("takeover2 %s/%d: %v", o.topic, o.part, err)
		}
		if hwm != int64(recordsPerPartition) {
			t.Errorf("%s/%d: HWM after restart=%d, want %d (drain didn't persist correctly)",
				o.topic, o.part, hwm, recordsPerPartition)
		}
	}
}

// TestRelinquishAll_NoPanicOnEmptyEngine guards the call against an
// engine that owns nothing — happens in the dev-mode broker that
// shut down before any partition was opened.
func TestRelinquishAll_NoPanicOnEmptyEngine(t *testing.T) {
	e, err := NewDiskStorageEngine(t.TempDir(), &neverLeaderLeases{}, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := e.RelinquishAll(); err != nil {
		t.Errorf("empty-engine RelinquishAll: %v, want nil", err)
	}
}

// TestSplitPartKey_Roundtrip pins the encoding contract the
// internal RelinquishAll loop depends on: partKey produces a
// "topic/partition" form that splitPartKey reverses cleanly,
// including for topic names that contain slashes.
func TestSplitPartKey_Roundtrip(t *testing.T) {
	cases := []struct {
		topic string
		part  int32
	}{
		{"plain", 0},
		{"plain", 17},
		{"my.namespaced.topic", 3},
		{"with-hyphen", 9},
		// Edge case: Kafka topic names don't contain '/' per spec
		// but partKey uses '/' as separator; verify a name that
		// looks slash-heavy doesn't break the round trip.
		{"a/b/c", 4},
	}
	e := &DiskStorageEngine{}
	for _, tc := range cases {
		k := e.partKey(tc.topic, tc.part)
		gotTopic, gotPart := splitPartKey(k)
		if gotTopic != tc.topic || gotPart != tc.part {
			t.Errorf("splitPartKey(%q) = (%q, %d), want (%q, %d)",
				k, gotTopic, gotPart, tc.topic, tc.part)
		}
	}
}
