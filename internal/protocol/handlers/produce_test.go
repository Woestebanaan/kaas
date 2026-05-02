package handlers

import (
	"encoding/binary"
	"testing"

	"github.com/woestebanaan/skafka/tests/testutil/recordbatch"
)

func validBatch(t *testing.T, baseOffset int64, numRecords int) []byte {
	t.Helper()
	batch := &recordbatch.RecordBatch{
		BaseOffset:      baseOffset,
		LastOffsetDelta: int32(numRecords - 1),
		ProducerID:      -1,
		ProducerEpoch:   -1,
		BaseSequence:    -1,
	}
	for i := 0; i < numRecords; i++ {
		batch.Records = append(batch.Records, recordbatch.Record{
			OffsetDelta: int32(i),
			Value:       []byte{byte(i)},
		})
	}
	return recordbatch.Encode(nil, batch)
}

func TestValidateProduceBatches_Empty(t *testing.T) {
	if !validateProduceBatches(nil) {
		t.Fatal("nil records should validate")
	}
	if !validateProduceBatches([]byte{}) {
		t.Fatal("empty records should validate")
	}
}

func TestValidateProduceBatches_OneValidBatch(t *testing.T) {
	if !validateProduceBatches(validBatch(t, 0, 5)) {
		t.Fatal("valid batch rejected")
	}
}

func TestValidateProduceBatches_TwoValidBatchesConcatenated(t *testing.T) {
	combined := append(validBatch(t, 0, 3), validBatch(t, 3, 2)...)
	if !validateProduceBatches(combined) {
		t.Fatal("two concatenated valid batches rejected")
	}
}

func TestValidateProduceBatches_TruncatedHeader(t *testing.T) {
	if validateProduceBatches([]byte{0, 0, 0, 0}) {
		t.Fatal("truncated header should fail")
	}
}

func TestValidateProduceBatches_TruncatedBody(t *testing.T) {
	b := validBatch(t, 0, 1)
	if validateProduceBatches(b[:len(b)-5]) {
		t.Fatal("truncated body should fail")
	}
}

func TestValidateProduceBatches_BadMagic(t *testing.T) {
	b := validBatch(t, 0, 1)
	b[16] = 1 // magic byte
	if validateProduceBatches(b) {
		t.Fatal("magic=1 should fail")
	}
}

func TestValidateProduceBatches_CorruptedCRCPayload(t *testing.T) {
	b := validBatch(t, 0, 1)
	b[len(b)-1] ^= 0xFF // corrupt the records area; CRC is unchanged
	if validateProduceBatches(b) {
		t.Fatal("corrupted CRC payload should fail")
	}
}

func TestValidateProduceBatches_FlippedCRC(t *testing.T) {
	b := validBatch(t, 0, 1)
	binary.BigEndian.PutUint32(b[17:21], 0xDEADBEEF)
	if validateProduceBatches(b) {
		t.Fatal("wrong stored CRC should fail")
	}
}

func TestValidateProduceBatches_BatchLengthBelowMinimum(t *testing.T) {
	b := validBatch(t, 0, 1)
	binary.BigEndian.PutUint32(b[8:12], 10) // claim batchLength=10 (below 49 minimum)
	if validateProduceBatches(b) {
		t.Fatal("batchLength<49 should fail")
	}
}
