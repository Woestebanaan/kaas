package handlers

import (
	"encoding/binary"
	"fmt"

	"github.com/woestebanaan/skafka/internal/protocol"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

// writeFetchResponseWithSplices encodes the Fetch response onto the
// splicer in the right wire order, calling Splice() in place of the
// per-partition records-bytes write. Encoding mirrors
// api.EncodeFetchResponse for flexible v12+ (the only versions
// HandleSplicing claims) but interleaves splices for the records
// section instead of letting the codec.Writer emit a []byte copy.
//
// Wire shape on the way out (all length-prefixed by the standard
// int32 frame length, computed up-front by encoding the header bytes
// into a temp codec.Writer):
//
//	[int32 frame_length]
//	[int32 correlation_id][1B tagged_fields=0]   // response prefix
//	[int32 throttle_time_ms][int16 err][int32 session_id]
//	[uvarint topics_count+1]
//	  for each topic:
//	    [compact_string topic_name]
//	    [uvarint partitions_count+1]
//	    for each partition:
//	      [int32 idx][int16 err][int64 hwm][int64 lso][int64 lso2]
//	      [uvarint aborted_txns_count+1]       (always 0+1 here)
//	      [int32 preferred_read_replica]
//	      [uvarint records_len+1][SPLICE records_len bytes from disk]
//	      [1B tagged_fields=0]
//	    [1B tagged_fields=0 for topic]
//	[1B tagged_fields=0 for response]
//
// The function builds two byte slices per partition (pre-records and
// post-records header bytes), splices the records in between, then
// emits the trailing topic + response tagged fields.
func writeFetchResponseWithSplices(hdr protocol.RequestHeader, version int16, resp *api.FetchResponse, slices []fetchPartitionSlice, splicer protocol.Splicer) error {
	if version < 12 {
		return fmt.Errorf("fetch splice: version %d not supported (need >= 12)", version)
	}

	// Build the FULL response header + per-partition prefix bytes into
	// a codec.Writer first. We need the total length to write the
	// outermost int32 frame length prefix before any other byte goes
	// out — splice has to know its absolute offset.
	body := codec.NewWriter()

	// Response prefix: correlation_id + tagged_fields (flexible header).
	// Fetch v12+ always uses RESPONSE_HEADER_V1, so we emit the empty
	// tagged-fields byte after the correlation ID.
	body.WriteInt32(hdr.CorrelationID)
	body.WriteEmptyTaggedFields()

	body.WriteInt32(resp.ThrottleTimeMs)
	body.WriteInt16(resp.ErrorCode)
	body.WriteInt32(resp.SessionID)

	// Topics array: uvarint(count+1) followed by entries.
	body.WriteUvarint(uint64(len(resp.Responses)) + 1)

	// Walk the response in lock-step with the slices table to interleave
	// header bytes and splices.
	sliceIdx := 0
	for _, t := range resp.Responses {
		body.WriteCompactString(t.Name)

		// Partitions array: uvarint(count+1) followed by entries.
		body.WriteUvarint(uint64(len(t.Partitions)) + 1)

		for _, p := range t.Partitions {
			body.WriteInt32(p.PartitionIndex)
			body.WriteInt16(p.ErrorCode)
			body.WriteInt64(p.HighWatermark)
			body.WriteInt64(p.LastStableOffset)
			body.WriteInt64(p.LogStartOffset)
			body.WriteUvarint(0 + 1) // aborted_transactions: empty compact array
			body.WriteInt32(p.PreferredReadReplica)

			// Records: emit the length-prefix here. The actual bytes
			// will be spliced in the per-partition emit loop below.
			// For error responses (no records), records_len == 0 →
			// uvarint(0+1) = single byte.
			recLen := 0
			if sliceIdx < len(slices) {
				recLen = slices[sliceIdx].length
			}
			body.WriteUvarint(uint64(recLen) + 1)

			// Mark the per-partition split point: bytes written into
			// `body` SO FAR will go onto the wire before the splice;
			// bytes that follow this point (tagged fields, the next
			// partition's header, etc.) go AFTER. We don't actually
			// split the buffer here — we just queue the splice in a
			// list, and run the alternation at the bottom of this
			// function once `body` is fully built.
			sliceIdx++

			body.WriteEmptyTaggedFields() // per-partition tagged fields
		}

		body.WriteEmptyTaggedFields() // per-topic tagged fields
	}

	body.WriteEmptyTaggedFields() // response-level tagged fields

	// At this point `body` contains the FULL response if every records
	// section were 0 bytes long. To produce the actual wire output we
	// need to "explode" it at each per-partition records boundary and
	// substitute the splice. This is structurally simpler done as a
	// SECOND pass that re-encodes header + walks slices, emitting
	// (bytes, splice, bytes, splice, ...) directly to the splicer.
	// The first pass exists solely to compute the total length.

	totalBytes := len(body.Bytes())
	for _, s := range slices {
		// body already accounts for records_len in the length prefix,
		// but the records bytes themselves were NOT written into body
		// (records_len = 0 there). Add them now to the wire total.
		totalBytes += s.length
	}

	// Emit the int32 frame-length prefix: int32 is the number of bytes
	// that follow (response body, not counting this prefix).
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(totalBytes))
	if _, err := splicer.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("fetch splice: writing frame length: %w", err)
	}

	// Second pass: re-encode and emit byte chunks interleaved with
	// splices. Cheaper than holding two codec.Writers parallel; same
	// codec calls give the same byte output.
	emit := codec.NewWriter()
	emit.WriteInt32(hdr.CorrelationID)
	emit.WriteEmptyTaggedFields()
	emit.WriteInt32(resp.ThrottleTimeMs)
	emit.WriteInt16(resp.ErrorCode)
	emit.WriteInt32(resp.SessionID)
	emit.WriteUvarint(uint64(len(resp.Responses)) + 1)

	sliceIdx = 0
	for _, t := range resp.Responses {
		emit.WriteCompactString(t.Name)
		emit.WriteUvarint(uint64(len(t.Partitions)) + 1)

		for _, p := range t.Partitions {
			emit.WriteInt32(p.PartitionIndex)
			emit.WriteInt16(p.ErrorCode)
			emit.WriteInt64(p.HighWatermark)
			emit.WriteInt64(p.LastStableOffset)
			emit.WriteInt64(p.LogStartOffset)
			emit.WriteUvarint(0 + 1)
			emit.WriteInt32(p.PreferredReadReplica)

			recLen := 0
			if sliceIdx < len(slices) {
				recLen = slices[sliceIdx].length
			}
			emit.WriteUvarint(uint64(recLen) + 1)

			// Flush header bytes accumulated so far, then splice the
			// records, then reset the writer for the next partition's
			// trailing tagged fields + next-partition header.
			if _, err := splicer.Write(emit.Bytes()); err != nil {
				return fmt.Errorf("fetch splice: write partition header: %w", err)
			}
			emit.Reset()

			if sliceIdx < len(slices) && slices[sliceIdx].file != nil && slices[sliceIdx].length > 0 {
				if err := splicer.Splice(slices[sliceIdx].file, slices[sliceIdx].offset, slices[sliceIdx].length); err != nil {
					return fmt.Errorf("fetch splice: splice partition %d: %w", p.PartitionIndex, err)
				}
			}
			sliceIdx++

			emit.WriteEmptyTaggedFields() // per-partition tagged fields
		}
		emit.WriteEmptyTaggedFields() // per-topic tagged fields
	}
	emit.WriteEmptyTaggedFields() // response-level tagged fields

	if _, err := splicer.Write(emit.Bytes()); err != nil {
		return fmt.Errorf("fetch splice: write trailing: %w", err)
	}
	if err := splicer.Flush(); err != nil {
		return fmt.Errorf("fetch splice: flush: %w", err)
	}
	return nil
}
