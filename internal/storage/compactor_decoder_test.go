package storage

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/woestebanaan/skafka/tests/testutil/recordbatch"
)

// TestIterateRecordsExtractsKeysAndOffsets exercises the gh #48
// Phase 2 decode contract: every record in a v2 batch surfaces
// with its key, offsetDelta and tombstone-flag. Builds the batch
// via the canonical tests/testutil/recordbatch encoder so the
// wire format matches what Apache-compatible clients produce — if
// the decoder ever drifts from the encoder, this test fails.
func TestIterateRecordsExtractsKeysAndOffsets(t *testing.T) {
	batch := &recordbatch.RecordBatch{
		BaseOffset:      100,
		LastOffsetDelta: 3,
		BaseTimestamp:   1700000000000,
		MaxTimestamp:    1700000003000,
		ProducerID:      -1,
		ProducerEpoch:   -1,
		BaseSequence:    -1,
		Records: []recordbatch.Record{
			{OffsetDelta: 0, Key: []byte("alpha"), Value: []byte("v0")},
			{OffsetDelta: 1, Key: []byte("beta"), Value: []byte("v1")},
			{OffsetDelta: 2, Key: []byte("gamma"), Value: nil}, // tombstone
			{OffsetDelta: 3, Key: nil, Value: []byte("v3")},     // null-keyed
		},
	}
	raw := recordbatch.Encode(nil, batch)

	var got []compactedRecord
	baseOffset, baseTimestamp, err := iterateRecords(raw, func(r compactedRecord) error {
		got = append(got, r)
		return nil
	})
	if err != nil {
		t.Fatalf("iterateRecords: %v", err)
	}
	if baseOffset != 100 {
		t.Errorf("baseOffset=%d, want 100", baseOffset)
	}
	if baseTimestamp != 1700000000000 {
		t.Errorf("baseTimestamp=%d, want 1700000000000", baseTimestamp)
	}
	if len(got) != 4 {
		t.Fatalf("decoded %d records, want 4", len(got))
	}

	want := []struct {
		offsetDelta int32
		key         []byte
		tombstone   bool
	}{
		{0, []byte("alpha"), false},
		{1, []byte("beta"), false},
		{2, []byte("gamma"), true},
		{3, nil, false},
	}
	for i, w := range want {
		if got[i].OffsetDelta != w.offsetDelta {
			t.Errorf("rec[%d] offsetDelta=%d, want %d", i, got[i].OffsetDelta, w.offsetDelta)
		}
		if !bytes.Equal(got[i].Key, w.key) {
			t.Errorf("rec[%d] key=%q, want %q", i, got[i].Key, w.key)
		}
		if got[i].IsTombstone != w.tombstone {
			t.Errorf("rec[%d] tombstone=%v, want %v", i, got[i].IsTombstone, w.tombstone)
		}
	}
	if got[3].HasKey() {
		t.Errorf("null-keyed record HasKey()=true; should be false (compactor uses this to skip dedup)")
	}
}

// TestReemitRecordChangesOnlyOffsetDelta pins the rewrite contract:
// re-emit with a new offsetDelta produces an identical record body
// EXCEPT for the offsetDelta varint. Catches a regression where
// the rewriter accidentally truncates / corrupts attrs / tsDelta /
// key / value / headers.
//
// Verification path: encode a batch with offsetDelta=5, decode it
// to recover the original record body, reemit with offsetDelta=0,
// then decode the reemitted body again and assert offsetDelta
// flipped while key/tombstone-flag stayed.
func TestReemitRecordChangesOnlyOffsetDelta(t *testing.T) {
	original := &recordbatch.RecordBatch{
		BaseOffset:      0,
		LastOffsetDelta: 5,
		BaseTimestamp:   1700000000000, MaxTimestamp: 1700000000000,
		ProducerID:    -1, ProducerEpoch: -1, BaseSequence: -1,
		Records: []recordbatch.Record{
			{OffsetDelta: 5, Key: []byte("k"), Value: []byte("hello world")},
		},
	}
	raw := recordbatch.Encode(nil, original)

	var src compactedRecord
	if _, _, err := iterateRecords(raw, func(r compactedRecord) error {
		src = r
		return nil
	}); err != nil {
		t.Fatalf("iterate source: %v", err)
	}
	if src.OffsetDelta != 5 {
		t.Fatalf("source offsetDelta=%d, want 5 (test fixture)", src.OffsetDelta)
	}

	// Re-emit with offsetDelta=0. Output is length-prefixed body.
	dst, err := reemitRecord(nil, src.Body, 0)
	if err != nil {
		t.Fatalf("reemit: %v", err)
	}

	// Strip the length prefix and parse the body directly. We
	// don't need to round-trip through a full batch — parseRecordBody
	// is the exact code path iterateRecords would use, so its
	// output is the canonical "what the compactor sees".
	bodyLen, n := binary.Varint(dst)
	if n <= 0 {
		t.Fatalf("reemit produced malformed length prefix")
	}
	if int(bodyLen) != len(dst)-n {
		t.Errorf("reemit length-prefix=%d but body bytes=%d", bodyLen, len(dst)-n)
	}
	rt, err := parseRecordBody(dst[n:])
	if err != nil {
		t.Fatalf("parseRecordBody on reemit: %v", err)
	}
	if rt.OffsetDelta != 0 {
		t.Errorf("rewritten offsetDelta=%d, want 0", rt.OffsetDelta)
	}
	if !bytes.Equal(rt.Key, []byte("k")) {
		t.Errorf("key changed: %q (rewriter corrupted surrounding bytes)", rt.Key)
	}
	if rt.IsTombstone {
		t.Error("tombstone-flag flipped after reemit (value len byte got mis-walked)")
	}
}

// TestReemitTombstonePreservesNullValue: a record with value=nil
// (delete-key marker) must remain a tombstone after reemit. Our
// rewrite only touches offsetDelta; value-len varint stays the
// special -1 sentinel.
func TestReemitTombstonePreservesNullValue(t *testing.T) {
	original := &recordbatch.RecordBatch{
		BaseOffset: 0, LastOffsetDelta: 0,
		ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1,
		Records: []recordbatch.Record{
			{OffsetDelta: 7, Key: []byte("expired-key"), Value: nil},
		},
	}
	raw := recordbatch.Encode(nil, original)

	var src compactedRecord
	if _, _, err := iterateRecords(raw, func(r compactedRecord) error {
		src = r
		return nil
	}); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	if !src.IsTombstone {
		t.Fatal("source record was not detected as tombstone — fixture wrong")
	}

	dst, err := reemitRecord(nil, src.Body, 0)
	if err != nil {
		t.Fatalf("reemit: %v", err)
	}
	_, n := binary.Varint(dst)
	rt, err := parseRecordBody(dst[n:])
	if err != nil {
		t.Fatalf("parseRecordBody: %v", err)
	}
	if !rt.IsTombstone {
		t.Errorf("rewritten record's tombstone flag lost (value-len varint corrupted)")
	}
}

// TestIterateRecordsRejectsUnsupportedMagic: skafka only writes
// magic=2 batches, but a corrupt log or a future v3 schema could
// produce magic=3. Compaction must refuse rather than silently
// mis-decode.
func TestIterateRecordsRejectsUnsupportedMagic(t *testing.T) {
	raw := recordbatch.Encode(nil, &recordbatch.RecordBatch{
		BaseOffset: 0, LastOffsetDelta: 0,
		ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1,
		Records: []recordbatch.Record{{OffsetDelta: 0, Value: []byte("x")}},
	})
	raw[16] = 3 // bump magic past v2
	_, _, err := iterateRecords(raw, func(compactedRecord) error { return nil })
	if err == nil {
		t.Error("iterateRecords accepted magic=3; should refuse non-v2 batches")
	}
}

// TestIterateRecordsRejectsTruncatedBatch: the batch claims more
// records than the bytes contain. Compaction must error rather
// than panic on out-of-bounds slice access.
func TestIterateRecordsRejectsTruncatedBatch(t *testing.T) {
	raw := recordbatch.Encode(nil, &recordbatch.RecordBatch{
		BaseOffset: 0, LastOffsetDelta: 0,
		ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1,
		Records: []recordbatch.Record{{OffsetDelta: 0, Value: []byte("x")}},
	})
	truncated := raw[:len(raw)-2]
	_, _, err := iterateRecords(truncated, func(compactedRecord) error { return nil })
	if err == nil {
		t.Error("iterateRecords accepted truncated batch; should error")
	}
}

// TestIterateRecordsTooShortRejected: a batch shorter than the
// header itself must error before any out-of-bounds read.
func TestIterateRecordsTooShortRejected(t *testing.T) {
	_, _, err := iterateRecords(make([]byte, 30), func(compactedRecord) error { return nil })
	if err == nil {
		t.Error("iterateRecords accepted 30-byte input; should reject < 65")
	}
}
