package handlers

import (
	"encoding/binary"
	"hash/crc32"
)

// encodeControlBatch produces the on-wire bytes for a single-record
// transactional COMMIT or ABORT marker batch — Kafka log format v2
// with attributes bit 4 (isTransactional) + bit 5 (isControl) set.
// gh #114.
//
// Apache reference: ControlRecordType.{ABORT,COMMIT}, encoded in
// the record key as int16 version + int16 type. Value is an
// EndTxnMarker (int16 version + int32 coordinatorEpoch = 6 bytes).
//
//	commit=true  → key = [00 00 00 01], type = COMMIT
//	commit=false → key = [00 00 00 00], type = ABORT
//
// The producer state (PID, epoch, baseSequence=-1, lastOffsetDelta=0)
// matches Apache's TransactionMarker — control batches are
// idempotence-exempt (sequence -1 means "do not dedupe"), so the
// storage-side classifyIdempotence accepts them without consuming a
// sequence slot.
func encodeControlBatch(producerID int64, producerEpoch int16, commit bool, coordinatorEpoch int32) []byte {
	const (
		magic = 2

		// Attributes bits: 4=isTransactional, 5=isControl.
		attrIsTransactional int16 = 1 << 4
		attrIsControl       int16 = 1 << 5

	)
	baseSequenceControl := int32(-1)
	attributes := attrIsTransactional | attrIsControl

	// Record key: int16 version + int16 type (network byte order).
	var controlType int16 = 0 // ABORT
	if commit {
		controlType = 1 // COMMIT
	}
	key := make([]byte, 4)
	binary.BigEndian.PutUint16(key[0:2], 0)                  // version
	binary.BigEndian.PutUint16(key[2:4], uint16(controlType)) // type

	// Record value: EndTxnMarker = int16 version + int32 coordEpoch.
	val := make([]byte, 6)
	binary.BigEndian.PutUint16(val[0:2], 0) // EndTxnMarker schema version
	binary.BigEndian.PutUint32(val[2:6], uint32(coordinatorEpoch))

	// Encode the single record (varint-prefixed length + body).
	recordBody := encodeControlRecord(attributes, key, val)

	// CRC payload = attributes onwards.
	var crcPayload []byte
	crcPayload = binary.BigEndian.AppendUint16(crcPayload, uint16(attributes))
	crcPayload = binary.BigEndian.AppendUint32(crcPayload, 0)              // lastOffsetDelta=0 (single record)
	crcPayload = binary.BigEndian.AppendUint64(crcPayload, 0)              // baseTimestamp=0
	crcPayload = binary.BigEndian.AppendUint64(crcPayload, 0)              // maxTimestamp=0
	crcPayload = binary.BigEndian.AppendUint64(crcPayload, uint64(producerID))
	crcPayload = binary.BigEndian.AppendUint16(crcPayload, uint16(producerEpoch))
	var seqRaw [4]byte
	binary.BigEndian.PutUint32(seqRaw[:], uint32(baseSequenceControl))
	crcPayload = append(crcPayload, seqRaw[:]...)
	crcPayload = binary.BigEndian.AppendUint32(crcPayload, 1) // recordCount=1
	crcPayload = append(crcPayload, recordBody...)

	crc := crc32.Checksum(crcPayload, crc32.MakeTable(crc32.Castagnoli))

	batchLength := int32(4 + 1 + 4 + len(crcPayload)) // ple + magic + crc + payload

	var batch []byte
	batch = binary.BigEndian.AppendUint64(batch, 0) // baseOffset (the broker rewrites this on Append)
	batch = binary.BigEndian.AppendUint32(batch, uint32(batchLength))
	batch = binary.BigEndian.AppendUint32(batch, 0) // partitionLeaderEpoch
	batch = append(batch, magic)
	batch = binary.BigEndian.AppendUint32(batch, crc)
	batch = append(batch, crcPayload...)
	return batch
}

// encodeControlRecord wires up a single record in v2 record format
// (varint-prefixed). attributes is the batch attribute, NOT the
// record attribute (which is always 0 for control records).
func encodeControlRecord(_ int16, key, val []byte) []byte {
	// Record body (without leading varint length):
	//   attributes (int8) = 0
	//   timestampDelta    (varlong) = 0
	//   offsetDelta       (varint)  = 0
	//   keyLength         (varint)  = len(key)
	//   keyBytes
	//   valueLength       (varint)  = len(value)
	//   valueBytes
	//   headersCount      (varint)  = 0
	body := []byte{0} // attributes
	body = appendVarLong(body, 0)
	body = appendVarInt(body, 0)
	body = appendVarInt(body, int32(len(key)))
	body = append(body, key...)
	body = appendVarInt(body, int32(len(val)))
	body = append(body, val...)
	body = appendVarInt(body, 0) // headers count

	// Prepend zigzag-varint length.
	prefixed := appendVarInt(nil, int32(len(body)))
	prefixed = append(prefixed, body...)
	return prefixed
}

func appendVarInt(dst []byte, v int32) []byte {
	zz := uint32(v<<1) ^ uint32(v>>31)
	return appendUvar(dst, uint64(zz))
}

func appendVarLong(dst []byte, v int64) []byte {
	zz := uint64(v<<1) ^ uint64(v>>63)
	return appendUvar(dst, zz)
}

func appendUvar(dst []byte, v uint64) []byte {
	for v >= 0x80 {
		dst = append(dst, byte(v)|0x80)
		v >>= 7
	}
	return append(dst, byte(v))
}
