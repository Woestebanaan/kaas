package codec

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// makeBatch builds the bytes of one v2 RecordBatch with the requested
// numRecords. The records payload itself doesn't have to parse — the
// walker only reads the header — so we pad with zeros.
func makeBatch(baseOffset int64, numRecords int32, payloadLen int) []byte {
	const headerLen = batchHeaderSize
	batchLen := int32(headerLen - batchPrefixSize + payloadLen) // batchLength field value
	buf := make([]byte, batchPrefixSize+int(batchLen))
	binary.BigEndian.PutUint64(buf[0:], uint64(baseOffset))
	binary.BigEndian.PutUint32(buf[8:], uint32(batchLen))
	// partitionLeaderEpoch (12..16), magic (16), crc (17..21), attrs
	// (21..23), lastOffsetDelta (23..27), baseTimestamp (27..35),
	// maxTimestamp (35..43), producerId (43..51), producerEpoch (51..53),
	// baseSequence (53..57) — zeros are fine for the count walker.
	binary.BigEndian.PutUint32(buf[batchNumRecordsOffset:], uint32(numRecords))
	return buf
}

func TestCountRecordsInBatchesEmpty(t *testing.T) {
	if got := CountRecordsInBatches(nil); got != 0 {
		t.Errorf("nil: got %d, want 0", got)
	}
	if got := CountRecordsInBatches([]byte{}); got != 0 {
		t.Errorf("empty: got %d, want 0", got)
	}
}

func TestCountRecordsInBatchesSingle(t *testing.T) {
	b := makeBatch(0, 5, 16)
	if got := CountRecordsInBatches(b); got != 5 {
		t.Errorf("single batch of 5: got %d, want 5", got)
	}
}

func TestCountRecordsInBatchesMultiple(t *testing.T) {
	// Three back-to-back batches with 5, 10, 3 records → 18 total.
	// The pre-fix recordCountFromBatch only read the first header,
	// so it would have returned 5.
	var combined []byte
	combined = append(combined, makeBatch(0, 5, 20)...)
	combined = append(combined, makeBatch(5, 10, 80)...)
	combined = append(combined, makeBatch(15, 3, 9)...)
	if got := CountRecordsInBatches(combined); got != 18 {
		t.Errorf("3 batches (5+10+3): got %d, want 18", got)
	}
}

func TestCountRecordsInBatchesTruncatedTail(t *testing.T) {
	// Two complete batches + a truncated third — count only the
	// complete ones (Kafka spec lets the tail of a Fetch response
	// be truncated).
	var combined []byte
	combined = append(combined, makeBatch(0, 4, 20)...)
	combined = append(combined, makeBatch(4, 6, 40)...)
	tail := makeBatch(10, 7, 50)
	combined = append(combined, tail[:len(tail)-5]...) // chop 5 bytes off
	if got := CountRecordsInBatches(combined); got != 10 {
		t.Errorf("2 complete + 1 truncated: got %d, want 10", got)
	}
}

func TestCountRecordsInBatchesGarbageBatchLen(t *testing.T) {
	// A batch followed by 61 zero bytes that look like a header
	// with batchLength=0 — walker must stop, not infinite-loop.
	good := makeBatch(0, 3, 16)
	junk := make([]byte, batchHeaderSize)
	combined := append([]byte(nil), good...)
	combined = append(combined, junk...)
	if got := CountRecordsInBatches(combined); got != 3 {
		t.Errorf("good + garbage: got %d, want 3", got)
	}
}

func TestCountRecordsInBatchesAtRoundtrip(t *testing.T) {
	var combined []byte
	combined = append(combined, makeBatch(0, 7, 30)...)
	combined = append(combined, makeBatch(7, 2, 12)...)
	combined = append(combined, makeBatch(9, 11, 60)...)

	want := int64(20)
	if got := CountRecordsInBatches(combined); got != want {
		t.Fatalf("byte-slice walker: got %d, want %d", got, want)
	}
	got, err := CountRecordsInBatchesAt(bytes.NewReader(combined), 0, len(combined))
	if err != nil {
		t.Fatalf("CountRecordsInBatchesAt: %v", err)
	}
	if got != want {
		t.Errorf("ReaderAt walker: got %d, want %d", got, want)
	}
}

func TestCountRecordsInBatchesAtPartialLength(t *testing.T) {
	// 3 batches; pass a `length` covering only the first two.
	first := makeBatch(0, 4, 20)
	second := makeBatch(4, 5, 30)
	third := makeBatch(9, 99, 40)
	all := append(append(append([]byte(nil), first...), second...), third...)
	cutoff := len(first) + len(second)
	got, err := CountRecordsInBatchesAt(bytes.NewReader(all), 0, cutoff)
	if err != nil {
		t.Fatalf("CountRecordsInBatchesAt: %v", err)
	}
	if got != 9 {
		t.Errorf("first-two-only via length cap: got %d, want 9", got)
	}
}
