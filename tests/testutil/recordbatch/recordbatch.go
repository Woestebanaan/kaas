// Package recordbatch provides decoded RecordBatch / Record types and
// encode/decode helpers for tests only. The production codec (internal/protocol/codec)
// deliberately treats RecordBatch payloads as opaque bytes — see v3.3 plan
// constraint #22 — so the decoded representation must not live there. Test
// fixtures and CRC/byte-opacity assertions import this package instead.
package recordbatch

import (
	"encoding/binary"
	"fmt"

	"github.com/woestebanaan/skafka/internal/protocol/codec"
)

// RecordHeader is a key/value pair attached to a Record.
type RecordHeader struct {
	Key   string
	Value []byte
}

// Record is a single message within a RecordBatch.
type Record struct {
	Attributes     int8
	TimestampDelta int64
	OffsetDelta    int32
	Key            []byte
	Value          []byte
	Headers        []RecordHeader
}

// RecordBatch is the top-level unit of storage in the Kafka log (magic=2).
type RecordBatch struct {
	BaseOffset           int64
	PartitionLeaderEpoch int32
	// Attributes bits:
	//   0-2: compression (0=none,1=gzip,2=snappy,3=lz4,4=zstd)
	//   3:   timestampType (0=create,1=log append)
	//   4:   isTransactional
	//   5:   isControlBatch
	Attributes      int16
	LastOffsetDelta int32
	BaseTimestamp   int64
	MaxTimestamp    int64
	ProducerID      int64
	ProducerEpoch   int16
	BaseSequence    int32
	Records         []Record
}

// IsTransactional reports whether the transactional bit is set.
func (b *RecordBatch) IsTransactional() bool { return b.Attributes&(1<<4) != 0 }

// IsControlBatch reports whether the control batch bit is set (COMMIT/ABORT markers).
func (b *RecordBatch) IsControlBatch() bool { return b.Attributes&(1<<5) != 0 }

// Encode serialises a RecordBatch into the Kafka wire format (magic=2)
// and appends it to dst. The CRC32C field is computed and written automatically.
func Encode(dst []byte, b *RecordBatch) []byte {
	var records []byte
	for _, rec := range b.Records {
		records = appendRecord(records, &rec)
	}

	// CRC covers from attributes to end of records (NOT including magic).
	// Wire order: ple, magic, crc, [crcPayload = attrs onwards].
	var crcPayload []byte
	crcPayload = binary.BigEndian.AppendUint16(crcPayload, uint16(b.Attributes))
	crcPayload = binary.BigEndian.AppendUint32(crcPayload, uint32(b.LastOffsetDelta))
	crcPayload = binary.BigEndian.AppendUint64(crcPayload, uint64(b.BaseTimestamp))
	crcPayload = binary.BigEndian.AppendUint64(crcPayload, uint64(b.MaxTimestamp))
	crcPayload = binary.BigEndian.AppendUint64(crcPayload, uint64(b.ProducerID))
	crcPayload = binary.BigEndian.AppendUint16(crcPayload, uint16(b.ProducerEpoch))
	crcPayload = binary.BigEndian.AppendUint32(crcPayload, uint32(b.BaseSequence))
	crcPayload = binary.BigEndian.AppendUint32(crcPayload, uint32(len(b.Records)))
	crcPayload = append(crcPayload, records...)

	crc := codec.ComputeCRC(crcPayload)

	// batchLength = everything after the batchLength field:
	// 4 (ple) + 1 (magic) + 4 (crc) + len(crcPayload)
	batchLength := int32(4 + 1 + 4 + len(crcPayload))

	dst = binary.BigEndian.AppendUint64(dst, uint64(b.BaseOffset))
	dst = binary.BigEndian.AppendUint32(dst, uint32(batchLength))
	dst = binary.BigEndian.AppendUint32(dst, uint32(b.PartitionLeaderEpoch))
	dst = append(dst, 2) // magic = 2
	dst = binary.BigEndian.AppendUint32(dst, crc)
	dst = append(dst, crcPayload...)
	return dst
}

// Decode reads one RecordBatch from r, validates the CRC, and returns it.
func Decode(r *codec.Reader) (*RecordBatch, error) {
	baseOffset, err := r.ReadInt64()
	if err != nil {
		return nil, err
	}
	batchLength, err := r.ReadInt32()
	if err != nil {
		return nil, err
	}
	body, err := r.ReadRaw(int(batchLength))
	if err != nil {
		return nil, err
	}
	sub := codec.NewReader(body)

	ple, err := sub.ReadInt32()
	if err != nil {
		return nil, err
	}
	magic, err := sub.ReadInt8()
	if err != nil {
		return nil, err
	}
	if magic != 2 {
		return nil, fmt.Errorf("recordbatch: unsupported RecordBatch magic %d (want 2)", magic)
	}
	storedCRC, err := sub.ReadInt32()
	if err != nil {
		return nil, err
	}
	// CRC covers everything after the crc field: body[9:] (skip ple+magic+crc = 4+1+4 bytes).
	if err := codec.ValidateCRC(body[9:], uint32(storedCRC)); err != nil {
		return nil, err
	}

	attrs, err := sub.ReadInt16()
	if err != nil {
		return nil, err
	}
	lastOffsetDelta, err := sub.ReadInt32()
	if err != nil {
		return nil, err
	}
	baseTimestamp, err := sub.ReadInt64()
	if err != nil {
		return nil, err
	}
	maxTimestamp, err := sub.ReadInt64()
	if err != nil {
		return nil, err
	}
	producerID, err := sub.ReadInt64()
	if err != nil {
		return nil, err
	}
	producerEpoch, err := sub.ReadInt16()
	if err != nil {
		return nil, err
	}
	baseSequence, err := sub.ReadInt32()
	if err != nil {
		return nil, err
	}
	numRecords, err := sub.ReadInt32()
	if err != nil {
		return nil, err
	}

	records := make([]Record, 0, numRecords)
	for i := int32(0); i < numRecords; i++ {
		rec, err := decodeRecord(sub)
		if err != nil {
			return nil, fmt.Errorf("recordbatch: record %d: %w", i, err)
		}
		records = append(records, rec)
	}

	return &RecordBatch{
		BaseOffset:           baseOffset,
		PartitionLeaderEpoch: ple,
		Attributes:           attrs,
		LastOffsetDelta:      lastOffsetDelta,
		BaseTimestamp:        baseTimestamp,
		MaxTimestamp:         maxTimestamp,
		ProducerID:           producerID,
		ProducerEpoch:        producerEpoch,
		BaseSequence:         baseSequence,
		Records:              records,
	}, nil
}

func appendRecord(dst []byte, rec *Record) []byte {
	var body []byte
	body = append(body, byte(rec.Attributes))
	body = binary.AppendVarint(body, rec.TimestampDelta)
	body = binary.AppendVarint(body, int64(rec.OffsetDelta))
	if rec.Key == nil {
		body = binary.AppendVarint(body, -1)
	} else {
		body = binary.AppendVarint(body, int64(len(rec.Key)))
		body = append(body, rec.Key...)
	}
	if rec.Value == nil {
		body = binary.AppendVarint(body, -1)
	} else {
		body = binary.AppendVarint(body, int64(len(rec.Value)))
		body = append(body, rec.Value...)
	}
	body = binary.AppendUvarint(body, uint64(len(rec.Headers)))
	for _, h := range rec.Headers {
		body = binary.AppendVarint(body, int64(len(h.Key)))
		body = append(body, h.Key...)
		if h.Value == nil {
			body = binary.AppendVarint(body, -1)
		} else {
			body = binary.AppendVarint(body, int64(len(h.Value)))
			body = append(body, h.Value...)
		}
	}

	dst = binary.AppendVarint(dst, int64(len(body)))
	dst = append(dst, body...)
	return dst
}

func decodeRecord(r *codec.Reader) (Record, error) {
	length, err := r.ReadVarint()
	if err != nil {
		return Record{}, err
	}
	if length < 0 {
		return Record{}, fmt.Errorf("recordbatch: negative record length %d", length)
	}
	raw, err := r.ReadRaw(int(length))
	if err != nil {
		return Record{}, err
	}
	sub := codec.NewReader(raw)

	attrs, err := sub.ReadInt8()
	if err != nil {
		return Record{}, err
	}
	tsDelta, err := sub.ReadVarint()
	if err != nil {
		return Record{}, err
	}
	offDelta, err := sub.ReadVarint()
	if err != nil {
		return Record{}, err
	}

	keyLen, err := sub.ReadVarint()
	if err != nil {
		return Record{}, err
	}
	var key []byte
	if keyLen >= 0 {
		key, err = sub.ReadRaw(int(keyLen))
		if err != nil {
			return Record{}, err
		}
	}

	valLen, err := sub.ReadVarint()
	if err != nil {
		return Record{}, err
	}
	var value []byte
	if valLen >= 0 {
		value, err = sub.ReadRaw(int(valLen))
		if err != nil {
			return Record{}, err
		}
	}

	numHeaders, err := sub.ReadUvarint()
	if err != nil {
		return Record{}, err
	}
	headers := make([]RecordHeader, 0, numHeaders)
	for i := uint64(0); i < numHeaders; i++ {
		hkLen, err := sub.ReadVarint()
		if err != nil {
			return Record{}, err
		}
		if hkLen < 0 {
			return Record{}, fmt.Errorf("recordbatch: negative header key length %d", hkLen)
		}
		hkBytes, err := sub.ReadRaw(int(hkLen))
		if err != nil {
			return Record{}, err
		}
		hk := string(hkBytes)

		hvLen, err := sub.ReadVarint()
		if err != nil {
			return Record{}, err
		}
		var hv []byte
		if hvLen >= 0 {
			hv, err = sub.ReadRaw(int(hvLen))
			if err != nil {
				return Record{}, err
			}
		}
		headers = append(headers, RecordHeader{Key: hk, Value: hv})
	}

	return Record{
		Attributes:     attrs,
		TimestampDelta: tsDelta,
		OffsetDelta:    int32(offDelta),
		Key:            key,
		Value:          value,
		Headers:        headers,
	}, nil
}
