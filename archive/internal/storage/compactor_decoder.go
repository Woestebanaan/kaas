package storage

// Log-compaction record decoder/encoder. THIS IS THE ONE LEGITIMATE
// EXCEPTION to skafka's byte-opacity invariant — every other code
// path in the broker treats RecordBatch payloads as opaque bytes
// per the v3.3 plan. Compaction by definition cannot work without
// reading individual record keys (to dedupe by key) and re-emitting
// records with new offsets (because the post-compaction batch's
// baseOffset shifts).
//
// To keep observability of the broader byte-opacity invariant
// useful — i.e. so a tripwire alert "we decoded a record outside
// compaction" still means a real bug — this file does NOT call
// observability.BumpCodecRecordDecode / BumpCodecBatchReencode.
// Instead it bumps a separate compactor-record metric (see
// internal/observability/compaction.go follow-up if we ever add
// one). For now no metric is emitted; the cleaner's slog.Info
// "compacted partition X: kept Y of Z records" in phase 3 is the
// observable signal.
//
// Apache Kafka's LogCleaner is the closest reference; the layout
// below matches the v2 RecordBatch records area:
//
//   per record: varint length-prefix
//     attrs(int8) tsDelta(varint) offDelta(varint)
//     keyLen(varint) keyBytes valueLen(varint) valueBytes
//     headerCount(uvarint) [headers...]
//
// Records start at byte 61 of the batch (numRecords field at
// byte 57, then 4 bytes for the count). See
// parseBatchProducerInfo in idempotence.go for the rest of the
// header offsets — that file is the canonical commentary.

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// recordsAreaOffset is the byte offset within a v2 RecordBatch
// where the records area begins. Equals
// 61 = headerEnd + numRecords-field-width = 57 + 4.
const recordsAreaOffset = 61

// compactedRecord is one record's view for compaction. raw is the
// original record body (excluding the outer length prefix) so we
// can re-emit unchanged keys/values/headers without touching them
// — only offsetDelta needs rewriting on emit. KeyHash is a
// content-addressable identity for the key (collision-resistant
// is enough; we use FNV-1a 64-bit because the existing
// internal/broker/group_hash.go uses the same family).
type compactedRecord struct {
	OffsetDelta int32  // relative to the source batch's baseOffset
	Key         []byte // nil = no key (cannot be deduped)
	IsTombstone bool   // value is null (Kafka delete-key marker)
	Body        []byte // original record body, length-prefix stripped
}

// HasKey reports whether this record participates in compaction.
// Apache treats null-keyed records as ineligible — they're kept
// verbatim because there's nothing to dedupe by.
func (r compactedRecord) HasKey() bool { return r.Key != nil }

// iterateRecords decodes every record in a single v2 RecordBatch
// (raw is the FULL batch including the 8-byte baseOffset prefix).
// fn is called per record with the (key, isTombstone, recordBody)
// triple. Body is a slice into raw — caller must copy if they
// retain it past the next call.
//
// Returns the (baseOffset, baseTimestamp) pair from the header so
// callers can reconstruct absolute offsets and preserve timestamps
// when re-emitting.
func iterateRecords(raw []byte, fn func(rec compactedRecord) error) (baseOffset int64, baseTimestamp int64, err error) {
	if len(raw) < recordsAreaOffset+4 {
		return 0, 0, fmt.Errorf("compactor: batch too short for records area: %d bytes", len(raw))
	}
	if raw[16] != 2 {
		return 0, 0, fmt.Errorf("compactor: unsupported magic %d (only v2 batches compactable)", raw[16])
	}
	baseOffset = int64(binary.BigEndian.Uint64(raw[0:8]))
	baseTimestamp = int64(binary.BigEndian.Uint64(raw[27:35]))
	numRecords := int32(binary.BigEndian.Uint32(raw[57:61]))

	pos := recordsAreaOffset
	for i := int32(0); i < numRecords; i++ {
		// Read the per-record varint length prefix.
		recLen, n := binary.Varint(raw[pos:])
		if n <= 0 {
			return 0, 0, fmt.Errorf("compactor: record %d: malformed length varint", i)
		}
		bodyStart := pos + n
		bodyEnd := bodyStart + int(recLen)
		if bodyEnd > len(raw) {
			return 0, 0, fmt.Errorf("compactor: record %d: body extends past batch (end=%d, len=%d)", i, bodyEnd, len(raw))
		}
		body := raw[bodyStart:bodyEnd]
		rec, perr := parseRecordBody(body)
		if perr != nil {
			return 0, 0, fmt.Errorf("compactor: record %d: %w", i, perr)
		}
		if err := fn(rec); err != nil {
			return 0, 0, err
		}
		pos = bodyEnd
	}
	return baseOffset, baseTimestamp, nil
}

// parseRecordBody decodes a single record body (the bytes between
// the length prefix and the next record). Skips fields it doesn't
// need and only retains key + tombstone-flag + the raw body.
func parseRecordBody(body []byte) (compactedRecord, error) {
	if len(body) < 1 {
		return compactedRecord{}, errors.New("body empty")
	}
	pos := 1 // attrs (int8) — skipped
	// tsDelta (varint)
	_, n := binary.Varint(body[pos:])
	if n <= 0 {
		return compactedRecord{}, errors.New("malformed tsDelta")
	}
	pos += n
	// offsetDelta (varint)
	offDelta, n := binary.Varint(body[pos:])
	if n <= 0 {
		return compactedRecord{}, errors.New("malformed offsetDelta")
	}
	pos += n
	// keyLen (varint)
	keyLen, n := binary.Varint(body[pos:])
	if n <= 0 {
		return compactedRecord{}, errors.New("malformed keyLen")
	}
	pos += n
	var key []byte
	if keyLen >= 0 {
		if pos+int(keyLen) > len(body) {
			return compactedRecord{}, errors.New("key extends past body")
		}
		key = body[pos : pos+int(keyLen)]
		pos += int(keyLen)
	}
	// valueLen (varint)
	valLen, n := binary.Varint(body[pos:])
	if n <= 0 {
		return compactedRecord{}, errors.New("malformed valueLen")
	}
	// We don't need to read value bytes — just whether the value
	// is null (tombstone) and where it ends so the next record
	// starts at the right place. iterateRecords uses the outer
	// length prefix for that, so we can stop here.
	return compactedRecord{
		OffsetDelta: int32(offDelta),
		Key:         key,
		IsTombstone: valLen == -1,
		Body:        body,
	}, nil
}

// reemitRecord re-encodes a record body with a new offsetDelta.
// All other fields (attrs, tsDelta, key, value, headers) are
// preserved verbatim. dst is appended to and returned (caller-
// extends pattern matches binary.AppendVarint et al.).
//
// Required because compaction consolidates surviving records into
// fewer batches; their original offsetDelta no longer matches the
// new baseOffset. Re-encoding only the offsetDelta varint is
// cheaper than re-encoding the full body but still requires
// touching the bytes.
func reemitRecord(dst []byte, body []byte, newOffsetDelta int32) ([]byte, error) {
	if len(body) < 1 {
		return nil, errors.New("body empty")
	}
	// Walk to the offsetDelta varint, then splice in the new value.
	pos := 1 // attrs
	_, n := binary.Varint(body[pos:])
	if n <= 0 {
		return nil, errors.New("malformed tsDelta")
	}
	pos += n
	// offsetDelta starts at pos. We need its width to know where
	// the rest of the body begins.
	_, n = binary.Varint(body[pos:])
	if n <= 0 {
		return nil, errors.New("malformed offsetDelta")
	}
	offDeltaEnd := pos + n

	// Encode the new offsetDelta into a temp buffer. Varints
	// vary in width (1-10 bytes), so the new body's length may
	// differ from the original — we must compute the length-
	// prefix accordingly.
	var newOffDelta [binary.MaxVarintLen64]byte
	newWidth := binary.PutVarint(newOffDelta[:], int64(newOffsetDelta))

	// Build the new body:
	//   [0..pos-1] = original up to offsetDelta start (attrs + tsDelta)
	//   [new offsetDelta varint]
	//   [original offDeltaEnd..end]
	newBodyLen := pos + newWidth + (len(body) - offDeltaEnd)

	// Outer length prefix + body.
	dst = binary.AppendVarint(dst, int64(newBodyLen))
	dst = append(dst, body[:pos]...)
	dst = append(dst, newOffDelta[:newWidth]...)
	dst = append(dst, body[offDeltaEnd:]...)
	return dst, nil
}
