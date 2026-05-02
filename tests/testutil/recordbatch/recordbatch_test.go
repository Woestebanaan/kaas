package recordbatch

import (
	"testing"

	"github.com/woestebanaan/skafka/internal/protocol/codec"
)

func TestRecordBatchRoundTrip(t *testing.T) {
	batch := &RecordBatch{
		BaseOffset:           100,
		PartitionLeaderEpoch: 3,
		Attributes:           0,
		LastOffsetDelta:      1,
		BaseTimestamp:        1700000000000,
		MaxTimestamp:         1700000000001,
		ProducerID:           -1,
		ProducerEpoch:        -1,
		BaseSequence:         -1,
		Records: []Record{
			{
				Attributes:     0,
				TimestampDelta: 0,
				OffsetDelta:    0,
				Key:            []byte("key-1"),
				Value:          []byte("value-1"),
				Headers:        nil,
			},
			{
				Attributes:     0,
				TimestampDelta: 1,
				OffsetDelta:    1,
				Key:            nil,
				Value:          []byte("value-2"),
				Headers: []RecordHeader{
					{Key: "h1", Value: []byte("hv1")},
				},
			},
		},
	}

	encoded := Encode(nil, batch)
	r := codec.NewReader(encoded)
	got, err := Decode(r)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if got.BaseOffset != batch.BaseOffset {
		t.Errorf("BaseOffset: got %d want %d", got.BaseOffset, batch.BaseOffset)
	}
	if got.PartitionLeaderEpoch != batch.PartitionLeaderEpoch {
		t.Errorf("PartitionLeaderEpoch: got %d want %d", got.PartitionLeaderEpoch, batch.PartitionLeaderEpoch)
	}
	if got.Attributes != batch.Attributes {
		t.Errorf("Attributes: got %d want %d", got.Attributes, batch.Attributes)
	}
	if got.BaseTimestamp != batch.BaseTimestamp {
		t.Errorf("BaseTimestamp: got %d want %d", got.BaseTimestamp, batch.BaseTimestamp)
	}
	if got.ProducerID != batch.ProducerID {
		t.Errorf("ProducerID: got %d want %d", got.ProducerID, batch.ProducerID)
	}
	if len(got.Records) != len(batch.Records) {
		t.Fatalf("Records count: got %d want %d", len(got.Records), len(batch.Records))
	}

	r0 := got.Records[0]
	if string(r0.Key) != "key-1" || string(r0.Value) != "value-1" {
		t.Errorf("Record[0]: key=%q value=%q", r0.Key, r0.Value)
	}
	if r0.Key == nil {
		t.Error("Record[0]: key should not be nil")
	}

	r1 := got.Records[1]
	if r1.Key != nil {
		t.Errorf("Record[1]: key should be nil, got %q", r1.Key)
	}
	if string(r1.Value) != "value-2" {
		t.Errorf("Record[1]: value=%q", r1.Value)
	}
	if len(r1.Headers) != 1 || r1.Headers[0].Key != "h1" || string(r1.Headers[0].Value) != "hv1" {
		t.Errorf("Record[1]: headers=%v", r1.Headers)
	}
}

func TestRecordBatchCRCValidation(t *testing.T) {
	batch := &RecordBatch{
		BaseOffset:    0,
		ProducerID:    -1,
		ProducerEpoch: -1,
		BaseSequence:  -1,
		Records:       []Record{{Value: []byte("hello")}},
	}
	encoded := Encode(nil, batch)

	// Corrupt a byte in the CRC payload area (after baseOffset+batchLength+ple+magic+crc = 8+4+4+1+4 = 21 bytes)
	encoded[22] ^= 0xFF

	r := codec.NewReader(encoded)
	if _, err := Decode(r); err == nil {
		t.Error("Decode: expected CRC error for corrupted batch, got nil")
	}
}

func TestRecordBatchWrongMagic(t *testing.T) {
	batch := &RecordBatch{
		BaseOffset:    0,
		ProducerID:    -1,
		ProducerEpoch: -1,
		BaseSequence:  -1,
		Records:       []Record{{Value: []byte("x")}},
	}
	encoded := Encode(nil, batch)

	// Magic byte is at offset 8(baseOffset)+4(batchLength)+4(ple) = 16
	encoded[16] = 1

	r := codec.NewReader(encoded)
	if _, err := Decode(r); err == nil {
		t.Error("Decode: expected error for magic=1, got nil")
	}
}

func TestRecordBatchAttributeFlags(t *testing.T) {
	batch := &RecordBatch{
		Attributes:    (1 << 4) | (1 << 5), // isTransactional + isControlBatch
		ProducerID:    -1,
		ProducerEpoch: -1,
		BaseSequence:  -1,
		Records:       []Record{},
	}
	encoded := Encode(nil, batch)
	r := codec.NewReader(encoded)
	got, err := Decode(r)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !got.IsTransactional() {
		t.Error("IsTransactional() should be true")
	}
	if !got.IsControlBatch() {
		t.Error("IsControlBatch() should be true")
	}
}

func TestRecordBatchEmptyRecords(t *testing.T) {
	batch := &RecordBatch{
		BaseOffset:    42,
		ProducerID:    -1,
		ProducerEpoch: -1,
		BaseSequence:  -1,
		Records:       []Record{},
	}
	encoded := Encode(nil, batch)
	r := codec.NewReader(encoded)
	got, err := Decode(r)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(got.Records) != 0 {
		t.Errorf("expected 0 records, got %d", len(got.Records))
	}
	if got.BaseOffset != 42 {
		t.Errorf("BaseOffset: got %d want 42", got.BaseOffset)
	}
}

func TestRecordNullKeyAndValue(t *testing.T) {
	batch := &RecordBatch{
		ProducerID:    -1,
		ProducerEpoch: -1,
		BaseSequence:  -1,
		Records: []Record{
			{Key: nil, Value: nil},
		},
	}
	encoded := Encode(nil, batch)
	r := codec.NewReader(encoded)
	got, err := Decode(r)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Records[0].Key != nil || got.Records[0].Value != nil {
		t.Error("null key/value should decode as nil")
	}
}
