package codec

import (
	"encoding/binary"
	"errors"
	"io"
)

// Layout of a v2 RecordBatch header (61 bytes):
//   [baseOffset:8][batchLength:4][partitionLeaderEpoch:4][magic:1]
//   [crc:4][attrs:2][lastOffsetDelta:4][baseTimestamp:8][maxTimestamp:8]
//   [producerId:8][producerEpoch:2][baseSequence:4][numRecords:4]
//
// Wire length of a batch = 12 (baseOffset + batchLength) + value of batchLength.
const (
	batchHeaderSize       = 61
	batchLengthOffset     = 8  // batchLength int32 within the batch
	batchPrefixSize       = 12 // baseOffset (8) + batchLength (4)
	batchNumRecordsOffset = 57 // numRecords int32 within the batch
	// Smallest valid batchLength value: header tail (49 bytes from
	// partitionLeaderEpoch through numRecords) when the records list
	// is empty. A smaller value means the bytes aren't a v2 batch.
	minBatchLengthField = 49
)

// CountRecordsInBatches walks a contiguous stream of v2 RecordBatches and
// returns the total numRecords across all complete batches. A truncated
// final batch at the tail is ignored — Kafka clients tolerate truncation
// at the end of a Fetch response by spec.
func CountRecordsInBatches(b []byte) int64 {
	var total int64
	pos := 0
	for pos+batchHeaderSize <= len(b) {
		batchLen := int32(binary.BigEndian.Uint32(b[pos+batchLengthOffset:]))
		if batchLen < minBatchLengthField {
			return total
		}
		wireLen := batchPrefixSize + int(batchLen)
		if pos+wireLen > len(b) {
			return total
		}
		records := int32(binary.BigEndian.Uint32(b[pos+batchNumRecordsOffset:]))
		if records > 0 {
			total += int64(records)
		}
		pos += wireLen
	}
	return total
}

// CountRecordsInBatchesAt walks v2 RecordBatches starting at byte
// position `pos` in `r`, covering up to `length` bytes. Reads only the
// 61-byte header of each batch — records payload is left on disk, so
// the sendfile(2) splice path is not undone by this counting pass.
func CountRecordsInBatchesAt(r io.ReaderAt, pos int64, length int) (int64, error) {
	var total int64
	end := pos + int64(length)
	hdr := make([]byte, batchHeaderSize)
	for pos+int64(batchHeaderSize) <= end {
		n, err := r.ReadAt(hdr, pos)
		if errors.Is(err, io.EOF) && n == batchHeaderSize {
			err = nil
		}
		if err != nil {
			return total, err
		}
		batchLen := int32(binary.BigEndian.Uint32(hdr[batchLengthOffset:]))
		if batchLen < minBatchLengthField {
			return total, nil
		}
		wireLen := batchPrefixSize + int(batchLen)
		if pos+int64(wireLen) > end {
			return total, nil
		}
		records := int32(binary.BigEndian.Uint32(hdr[batchNumRecordsOffset:]))
		if records > 0 {
			total += int64(records)
		}
		pos += int64(wireLen)
	}
	return total, nil
}
