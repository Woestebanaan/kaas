package storage

import (
	"encoding/binary"
	"fmt"
)

// producerEntry tracks per-(producerID) state for a single partition's
// idempotence guarantees. Apache Kafka maintains a sliding window of
// the producer's last `producerSnapshotCacheSize` (5) batches so a
// retry of any in-flight batch can be deduped — Java's idempotent
// producer caps max.in.flight.requests.per.connection at 5 to keep
// this window small. We mirror that exact capacity.
type producerEntry struct {
	epoch  int16
	recent []recentBatch // ordered oldest-first; len <= producerWindowSize
}

// recentBatch is one cache slot in the per-producer window.
// firstSeq..lastSeq describes the contiguous range of record-level
// sequence numbers in the batch (Java tracks both because retries
// match on the *first* seq, but logging/metrics want the last seq
// too). baseOffset is what we returned to the producer so we can
// hand back the same value if the batch is replayed.
type recentBatch struct {
	firstSeq, lastSeq int32
	baseOffset        int64
}

const producerWindowSize = 5

// idempotenceAction is the decision the storage layer reaches before
// it touches the log.
type idempotenceAction int

const (
	// idemNotIdempotent: producerID == -1; the current behaviour from
	// pre-#12 days. Caller proceeds without idempotence checks.
	idemNotIdempotent idempotenceAction = iota
	// idemAccept: the batch is a fresh, in-sequence record. Caller
	// appends as normal and calls recordIdempotenceOutcome afterwards.
	idemAccept
	// idemDuplicate: an exact in-window retry. Caller skips the
	// append and returns the cached baseOffset with errCode=0.
	idemDuplicate
	// idemOutOfOrder: gap or first-batch-sequence != 0. Maps to
	// Kafka error 45 (ErrOutOfOrderSequence).
	idemOutOfOrder
	// idemInvalidEpoch: batch epoch is older than what we have on
	// file. Maps to Kafka error 47 (ErrInvalidProducerEpoch).
	idemInvalidEpoch
)

// batchProducerInfo carries everything we extract from a v2 record
// batch header for the idempotence check. lastSeq is firstSeq +
// lastOffsetDelta — i.e., the sequence of the LAST record in the
// batch. Stored separately so the dedupe path can match either by
// firstSeq (the canonical "is this batch a retry?" check) or by
// lastSeq (useful for logs / future window-extension logic).
type batchProducerInfo struct {
	producerID int64
	epoch      int16
	firstSeq   int32
	lastSeq    int32
}

// parseBatchProducerInfo extracts the producer fields from a raw v2
// RecordBatch. Layout (from the Apache Kafka spec; same offsets the
// engine uses for parseBatchOffsets):
//
//	[0:8]   baseOffset
//	[8:12]  batchLength
//	[12:16] partitionLeaderEpoch
//	[16]    magic
//	[17:21] crc
//	[21:23] attrs
//	[23:27] lastOffsetDelta
//	[27:35] baseTimestamp
//	[35:43] maxTimestamp
//	[43:51] producerID
//	[51:53] producerEpoch
//	[53:57] baseSequence
//	[57:61] numRecords
//
// lastOffsetDelta is reused as the per-record sequence delta — Apache
// Kafka treats them as identical because every record in a v2 batch
// is sequence-numbered in lock-step with its offset.
func parseBatchProducerInfo(raw []byte) (batchProducerInfo, error) {
	const headerEnd = 57
	if len(raw) < headerEnd {
		return batchProducerInfo{}, fmt.Errorf("batch too short for producer info: %d bytes", len(raw))
	}
	lastOffsetDelta := int32(binary.BigEndian.Uint32(raw[23:27]))
	pid := int64(binary.BigEndian.Uint64(raw[43:51]))
	epoch := int16(binary.BigEndian.Uint16(raw[51:53]))
	baseSeq := int32(binary.BigEndian.Uint32(raw[53:57]))
	info := batchProducerInfo{producerID: pid, epoch: epoch, firstSeq: baseSeq}
	// lastSeq = firstSeq + lastOffsetDelta. For PID == -1 the values
	// are arbitrary and ignored downstream, so we don't validate.
	info.lastSeq = baseSeq + lastOffsetDelta
	return info, nil
}

// classifyIdempotence is the pure decision function: given the
// per-partition map of producer state and the new batch's producer
// info, return what to do and (for dedupe) the cached baseOffset to
// echo back. The caller (Append) holds ps.mu while this runs.
func classifyIdempotence(states map[int64]*producerEntry, info batchProducerInfo) (idempotenceAction, int64) {
	if info.producerID < 0 {
		return idemNotIdempotent, 0
	}
	entry, ok := states[info.producerID]
	if !ok || info.epoch > entry.epoch {
		// First batch ever from this PID, or a fresh-epoch reset
		// (KIP-360 PID renewal). The first batch's sequence MUST
		// start at 0 — anything else is a gap.
		if info.firstSeq != 0 {
			return idemOutOfOrder, 0
		}
		return idemAccept, 0
	}
	if info.epoch < entry.epoch {
		return idemInvalidEpoch, 0
	}
	// Same epoch: dedupe against the recent window first, then check
	// for exact next-sequence to accept.
	for _, rb := range entry.recent {
		if rb.firstSeq == info.firstSeq && rb.lastSeq == info.lastSeq {
			return idemDuplicate, rb.baseOffset
		}
	}
	if len(entry.recent) == 0 {
		// Same-epoch entry with empty window can only happen during
		// snapshot restore where state was preserved but the window
		// wasn't (B2). Treat first-seq=0 as accept; otherwise gap.
		if info.firstSeq != 0 {
			return idemOutOfOrder, 0
		}
		return idemAccept, 0
	}
	last := entry.recent[len(entry.recent)-1]
	if info.firstSeq == last.lastSeq+1 {
		return idemAccept, 0
	}
	return idemOutOfOrder, 0
}

// recordIdempotenceOutcome advances the per-PID window after the
// caller's Append has succeeded. NOT called for dedupe (state is
// already correct) or rejection paths.
func recordIdempotenceOutcome(states map[int64]*producerEntry, info batchProducerInfo, baseOffset int64) {
	entry, ok := states[info.producerID]
	if !ok || info.epoch > entry.epoch {
		entry = &producerEntry{epoch: info.epoch}
		states[info.producerID] = entry
	}
	rb := recentBatch{firstSeq: info.firstSeq, lastSeq: info.lastSeq, baseOffset: baseOffset}
	entry.recent = append(entry.recent, rb)
	if len(entry.recent) > producerWindowSize {
		entry.recent = entry.recent[len(entry.recent)-producerWindowSize:]
	}
}
