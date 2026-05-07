package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/woestebanaan/skafka/internal/protocol/codec"
)

// compactPartition runs gh #48 log compaction on a single partition's
// CLOSED segments. The active segment is never touched — same rule
// Apache Kafka follows. Caller is responsible for checking that the
// partition's cleanup.policy actually involves compaction; this
// function does the work unconditionally.
//
// Algorithm (mirrors LogCleaner.scala — KAFKA's reference impl):
//
//  1. Snapshot the closed-segments slice under ps.mu so we don't
//     race a concurrent segment roll. We don't hold ps.mu during
//     I/O — that would block every Append for the duration.
//  2. Pass 1: scan all closed segments, build map[string]int64
//     (key → highest absolute offset seen). This is the "OffsetMap"
//     in Apache parlance; we use a flat map because skafka's typical
//     compacted topic (Streams changelog, __consumer_offsets-style)
//     fits well under the partition's RAM budget.
//  3. Pass 2: walk closed segments in offset order. For each record:
//        - PID==-1 records with non-nil keys: keep iff this is the
//          highest-offset occurrence (record.absOffset == map[key]).
//        - null-keyed records: keep verbatim (Apache's rule — they
//          can't be deduped because there's no key to match on).
//        - tombstones (value==nil): keep iff this is the highest-
//          offset occurrence; otherwise drop. delete.retention.ms
//          eviction of stale tombstones is a follow-up.
//     Emit the kept records into a new batch per source batch
//     (preserves transactional metadata + the producer-id fence).
//     Each kept record's offsetDelta is rewritten so
//     absOffset = newBaseOffset + newOffsetDelta.
//  4. Atomic swap: write the new compacted segment to a tmp file,
//     fsync, rename onto a fresh epoch-prefixed path. Under ps.mu,
//     replace the closed-segments slice's compacted entries with
//     a single segmentMeta pointing at the new file. Old segment
//     files are unlinked AFTER ps.mu is released.
//
// Returns (recordsKept, recordsDropped, err). On error the partition
// is left untouched — the cleaner's next pass will retry.
func (e *DiskStorageEngine) compactPartition(ps *partitionState) (kept, dropped int, err error) {
	ps.mu.Lock()
	if len(ps.segments) == 0 {
		ps.mu.Unlock()
		return 0, 0, nil
	}
	// Defensive copy: holding the slice header without a copy would
	// race the segment-roll path, which mutates ps.segments under
	// the same lock.
	closedSegs := make([]segmentMeta, len(ps.segments))
	copy(closedSegs, ps.segments)
	dir := ps.dir
	epoch := ps.epoch
	ps.mu.Unlock()

	// Pass 1: build the key → highest-offset map. Walk segments in
	// order so a later occurrence in a later segment overwrites
	// the map entry — which is exactly the Kafka semantics
	// "compaction keeps the latest value".
	offsetMap := map[string]int64{}
	for _, seg := range closedSegs {
		if err := walkSegmentRecords(seg.logPath, func(absOffset int64, key []byte, _ bool) error {
			if key == nil {
				return nil // null-keyed records can't be deduped
			}
			offsetMap[string(key)] = absOffset
			return nil
		}); err != nil {
			return 0, 0, fmt.Errorf("compactor pass 1 (%s): %w", seg.logPath, err)
		}
	}

	// Pass 2: rewrite. Build the compacted output as one segment file
	// with one batch per source batch (preserving transactional
	// metadata + producer-id state across compaction).
	//
	// New baseOffset = oldest closed segment's baseOffset. Records
	// keep their ORIGINAL absolute offsets — consumers tracking
	// offsets must still find their records, so compaction produces
	// a sparse log (gaps where deduped records used to be), not a
	// renumbered one.
	newBaseOffset := closedSegs[0].baseOffset
	tmpPath := filepath.Join(dir, fmt.Sprintf("compact-%020d.log.tmp", newBaseOffset))
	finalPath := segmentLogPath(dir, newBaseOffset, epoch)

	out, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return 0, 0, fmt.Errorf("compactor: open tmp %s: %w", tmpPath, err)
	}
	// On any error after this point, clean up the tmp file.
	defer func() {
		if err != nil {
			out.Close()
			_ = os.Remove(tmpPath)
		}
	}()

	for _, seg := range closedSegs {
		k, d, werr := rewriteSegment(seg.logPath, out, offsetMap)
		if werr != nil {
			err = fmt.Errorf("compactor pass 2 (%s): %w", seg.logPath, werr)
			return 0, 0, err
		}
		kept += k
		dropped += d
	}

	if err = out.Sync(); err != nil {
		return 0, 0, fmt.Errorf("compactor: sync tmp: %w", err)
	}
	if err = out.Close(); err != nil {
		return 0, 0, fmt.Errorf("compactor: close tmp: %w", err)
	}
	if err = os.Rename(tmpPath, finalPath); err != nil {
		return 0, 0, fmt.Errorf("compactor: rename %s → %s: %w", tmpPath, finalPath, err)
	}

	// Swap segment list under ps.mu. Replace the compacted entries
	// (the entire closed-segments range we processed) with a single
	// new segmentMeta. The active segment is untouched.
	ps.mu.Lock()
	// Verify ps.segments hasn't been replaced under us by takeover/
	// recovery (the old slice header could be stale). We check by
	// comparing the first segment's logPath — if it differs, the
	// world moved on, abandon the rewrite.
	if len(ps.segments) == 0 || ps.segments[0].logPath != closedSegs[0].logPath {
		ps.mu.Unlock()
		_ = os.Remove(finalPath)
		return 0, 0, fmt.Errorf("compactor: ps.segments changed during compaction; aborting")
	}
	// Remove the entries we compacted. Closed segments at the head
	// of ps.segments are the ones we processed (sorted by baseOffset);
	// any segments rolled in DURING compaction would be at the tail.
	keep := ps.segments[len(closedSegs):]
	newSeg := segmentMeta{
		baseOffset: newBaseOffset,
		logPath:    finalPath,
		indexPath:  segmentIndexPath(dir, newBaseOffset, epoch),
	}
	ps.segments = append([]segmentMeta{newSeg}, keep...)
	ps.mu.Unlock()

	// Unlink old files AFTER ps.mu release. Best-effort — a leftover
	// file from a previous broker isn't fatal; a future cleaner pass
	// or operator-side cleanup script handles it.
	for _, seg := range closedSegs {
		_ = os.Remove(seg.logPath)
		_ = os.Remove(seg.indexPath)
		_ = os.Remove(strings.TrimSuffix(seg.logPath, ".log") + ".timeindex")
	}

	slog.Info("compactor: compacted partition",
		"dir", dir, "segments_in", len(closedSegs), "records_kept", kept, "records_dropped", dropped)
	return kept, dropped, nil
}

// walkSegmentRecords streams every record in every batch of the
// segment log file at path, calling fn(absOffset, key, isTombstone)
// per record. Used by Pass 1 to build the offset map.
//
// The file is read in batch-sized chunks. We read the 12-byte
// "baseOffset + batchLength" prefix, allocate a buffer for the full
// batch (12 + batchLength bytes), then hand the batch to
// iterateRecords. Memory peak per call is one batch — bounded by
// the producer's batch.size (default 16 KB).
func walkSegmentRecords(path string, fn func(absOffset int64, key []byte, isTombstone bool) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	for {
		var prefix [12]byte
		_, err := io.ReadFull(f, prefix[:])
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read prefix: %w", err)
		}
		batchBaseOffset := int64(binary.BigEndian.Uint64(prefix[0:8]))
		batchLength := int32(binary.BigEndian.Uint32(prefix[8:12]))
		raw := make([]byte, 12+int(batchLength))
		copy(raw[:12], prefix[:])
		if _, err := io.ReadFull(f, raw[12:]); err != nil {
			return fmt.Errorf("read body @ baseOffset=%d: %w", batchBaseOffset, err)
		}
		var fnErr error
		_, _, perr := iterateRecords(raw, func(rec compactedRecord) error {
			absOffset := batchBaseOffset + int64(rec.OffsetDelta)
			if err := fn(absOffset, rec.Key, rec.IsTombstone); err != nil {
				fnErr = err
				return err
			}
			return nil
		})
		if perr != nil {
			return fmt.Errorf("iterate batch @ baseOffset=%d: %w", batchBaseOffset, perr)
		}
		if fnErr != nil {
			return fnErr
		}
	}
}

// rewriteSegment reads every batch from inputPath, drops superseded
// records, and writes one new batch per surviving source batch into
// out. Returns the kept/dropped record counts.
func rewriteSegment(inputPath string, out *os.File, offsetMap map[string]int64) (kept, dropped int, err error) {
	f, err := os.Open(inputPath)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	for {
		var prefix [12]byte
		_, perr := io.ReadFull(f, prefix[:])
		if perr == io.EOF {
			return kept, dropped, nil
		}
		if perr != nil {
			return kept, dropped, fmt.Errorf("read prefix: %w", perr)
		}
		batchLength := int32(binary.BigEndian.Uint32(prefix[8:12]))
		raw := make([]byte, 12+int(batchLength))
		copy(raw[:12], prefix[:])
		if _, perr := io.ReadFull(f, raw[12:]); perr != nil {
			return kept, dropped, fmt.Errorf("read body: %w", perr)
		}
		newBatch, k, d, berr := compactBatch(raw, offsetMap)
		if berr != nil {
			return kept, dropped, fmt.Errorf("compact batch: %w", berr)
		}
		kept += k
		dropped += d
		if newBatch == nil {
			continue // batch entirely superseded
		}
		if _, perr := out.Write(newBatch); perr != nil {
			return kept, dropped, fmt.Errorf("write compacted batch: %w", perr)
		}
	}
}

// compactBatch reduces a single source batch to its surviving
// records and re-emits a new v2 RecordBatch with a fresh CRC. nil
// return means every record was superseded — the caller skips the
// write entirely. kept/dropped are the per-batch counts.
//
// The new batch's baseOffset matches the FIRST kept record's
// absolute offset; the source batch's other header fields (PLE,
// attrs, baseTimestamp, maxTimestamp, producerID/epoch/seq) are
// preserved verbatim. lastOffsetDelta gets recomputed from the
// kept records' offsetDeltas.
func compactBatch(raw []byte, offsetMap map[string]int64) (newBatch []byte, kept, dropped int, err error) {
	if len(raw) < recordsAreaOffset+4 {
		return nil, 0, 0, fmt.Errorf("batch too short: %d", len(raw))
	}
	srcBaseOffset := int64(binary.BigEndian.Uint64(raw[0:8]))

	// Walk records, deciding kept/dropped + collecting their
	// (absoluteOffset, body) pairs.
	type keptRecord struct {
		absOffset int64
		body      []byte
	}
	var keptRecords []keptRecord
	if _, _, perr := iterateRecords(raw, func(rec compactedRecord) error {
		absOffset := srcBaseOffset + int64(rec.OffsetDelta)
		keep := false
		if rec.Key == nil {
			keep = true // null-keyed records preserved verbatim
		} else {
			latest, ok := offsetMap[string(rec.Key)]
			keep = ok && absOffset == latest
		}
		if keep {
			keptRecords = append(keptRecords, keptRecord{absOffset: absOffset, body: rec.Body})
			kept++
		} else {
			dropped++
		}
		return nil
	}); perr != nil {
		return nil, kept, dropped, perr
	}

	if len(keptRecords) == 0 {
		return nil, kept, dropped, nil
	}

	// New batch baseOffset = first kept record's absolute offset.
	newBaseOffset := keptRecords[0].absOffset

	// Re-emit each kept record with offsetDelta = absOffset - newBaseOffset.
	var recordsArea []byte
	for _, kr := range keptRecords {
		newDelta := int32(kr.absOffset - newBaseOffset)
		recordsArea, err = reemitRecord(recordsArea, kr.body, newDelta)
		if err != nil {
			return nil, kept, dropped, fmt.Errorf("reemit @ abs=%d: %w", kr.absOffset, err)
		}
	}

	// Build the batch. Header fields below mirror the layout
	// commentary in compactor_decoder.go and the source batch.
	newLastOffsetDelta := int32(keptRecords[len(keptRecords)-1].absOffset - newBaseOffset)

	var crcPayload []byte
	crcPayload = append(crcPayload, raw[21:23]...)                // attrs
	crcPayload = binary.BigEndian.AppendUint32(crcPayload, uint32(newLastOffsetDelta))
	crcPayload = append(crcPayload, raw[27:35]...)                // baseTimestamp
	crcPayload = append(crcPayload, raw[35:43]...)                // maxTimestamp
	crcPayload = append(crcPayload, raw[43:51]...)                // producerID
	crcPayload = append(crcPayload, raw[51:53]...)                // producerEpoch
	crcPayload = append(crcPayload, raw[53:57]...)                // baseSequence
	crcPayload = binary.BigEndian.AppendUint32(crcPayload, uint32(len(keptRecords)))
	crcPayload = append(crcPayload, recordsArea...)

	crc := codec.ComputeCRC(crcPayload)

	// batchLength = ple(4) + magic(1) + crc(4) + len(crcPayload)
	batchLength := int32(4 + 1 + 4 + len(crcPayload))

	out := make([]byte, 0, 12+int(batchLength))
	out = binary.BigEndian.AppendUint64(out, uint64(newBaseOffset))
	out = binary.BigEndian.AppendUint32(out, uint32(batchLength))
	out = append(out, raw[12:16]...) // partitionLeaderEpoch (preserve)
	out = append(out, byte(2))       // magic
	out = binary.BigEndian.AppendUint32(out, crc)
	out = append(out, crcPayload...)
	return out, kept, dropped, nil
}

// varintAt parses one zigzag varint at buf[pos]. Used by tests +
// inline rewriting; encoding/binary.Varint returns the decoded
// value plus byte width so it would suffice on its own — this
// wrapper exists so tests have a stable named symbol to shim.
func varintAt(buf []byte, pos int) (int64, int) {
	if pos >= len(buf) {
		return 0, 0
	}
	return binary.Varint(buf[pos:])
}

// _ "we always sort segments by baseOffset" guard against a future
// change to listSegments — if that returns unsorted, the offsetMap
// pass would build the wrong winners. This compile-time assertion
// just documents the dependency; the actual sort happens in
// listSegments today.
var _ = sort.SliceStable
