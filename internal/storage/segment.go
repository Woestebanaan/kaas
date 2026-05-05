package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/woestebanaan/skafka/internal/protocol/codec"
)

// segmentMeta describes a closed (read-only) segment.
type segmentMeta struct {
	baseOffset   int64
	maxTimestamp int64 // highest maxTimestamp in this segment; 0 if unknown
	logPath      string
	indexPath    string
}

// activeSegment holds the open file handles for the current write segment.
//
// logPath / indexPath snapshot the on-disk paths used at creation time so
// the engine can list them in segmentMeta during a roll without having to
// re-derive the format (which depends on whether this segment was created
// before or after the v3.3 epoch-prefix migration).
type activeSegment struct {
	baseOffset        int64
	epoch             int64
	logFile           *os.File
	indexFile         *os.File
	logPath           string
	indexPath         string
	logSize           int64
	lastOffset        int64
	lastIndexedLogPos int64 // log position at which the last index entry was written
}

// segmentLogPath returns the .log file path for a segment.
//
// Format: {epoch:08x}-{base_offset:020d}.log
//
// Epoch-prefixed naming is the v3 single-writer-by-construction story:
// a partitioned ex-leader and a fresh leader at a higher epoch never
// target the same path, so concurrent writes during takeover are
// physically harmless. Legacy unprefixed segments (`{base_offset:020d}.log`)
// are still parsed by listSegments for migration; createSegment always
// emits the new format.
func segmentLogPath(dir string, baseOffset int64, epoch int64) string {
	return filepath.Join(dir, fmt.Sprintf("%08x-%020d.log", uint32(epoch), baseOffset))
}

// segmentIndexPath returns the .index file path matching segmentLogPath.
func segmentIndexPath(dir string, baseOffset int64, epoch int64) string {
	return filepath.Join(dir, fmt.Sprintf("%08x-%020d.index", uint32(epoch), baseOffset))
}

// legacySegmentLogPath returns the pre-Phase-4 unprefixed format, kept
// for the migration test fixtures. listSegments parses files matching
// either layout.
func legacySegmentLogPath(dir string, baseOffset int64) string {
	return filepath.Join(dir, fmt.Sprintf("%020d.log", baseOffset))
}

// createSegment creates a fresh segment starting at baseOffset under the
// given leader epoch. The epoch shows up in the filename so a stale
// ex-leader's writes can never collide with a new leader's segment file.
func createSegment(dir string, baseOffset, epoch int64) (*activeSegment, error) {
	logPath := segmentLogPath(dir, baseOffset, epoch)
	indexPath := segmentIndexPath(dir, baseOffset, epoch)

	lf, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}
	idxf, err := os.OpenFile(indexPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		_ = lf.Close()
		return nil, err
	}
	return &activeSegment{
		baseOffset: baseOffset,
		epoch:      epoch,
		logFile:    lf,
		indexFile:  idxf,
		logPath:    logPath,
		indexPath:  indexPath,
	}, nil
}

// openActiveSegment opens an existing segment's log+index files and reads
// their on-disk sizes. It does NOT scan the log to recompute the high
// watermark — that's the caller's choice (manifest fast path vs full
// scan fallback). Cheap: a couple of file opens + Seek-to-end. Used on
// broker startup so partition open is O(1) per partition rather than
// O(log_size); recoverSegment + rebuildIndex run later in
// takeoverInternal when this broker is about to become leader.
func openActiveSegment(meta segmentMeta) (*activeSegment, error) {
	lf, err := os.OpenFile(meta.logPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	idxf, err := os.OpenFile(meta.indexPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		_ = lf.Close()
		return nil, err
	}

	logSize, err := lf.Seek(0, io.SeekEnd)
	if err != nil {
		_ = lf.Close()
		_ = idxf.Close()
		return nil, err
	}
	idxSize, err := idxf.Seek(0, io.SeekEnd)
	if err != nil {
		_ = lf.Close()
		_ = idxf.Close()
		return nil, err
	}

	return &activeSegment{
		baseOffset:        meta.baseOffset,
		logFile:           lf,
		indexFile:         idxf,
		logPath:           meta.logPath,
		indexPath:         meta.indexPath,
		logSize:           logSize,
		lastIndexedLogPos: idxSize / 8 * 4096, // rough estimate; exact value not critical for appends
	}, nil
}

// openActiveSegmentFromDisk opens an existing segment and scans it to
// recover state. Returns the segment and the recovered high watermark.
// Used on the cold path when no manifest is present (legacy / fresh
// deployment) — startup-time callers prefer openActiveSegment + the
// manifest's HighWatermark to avoid the full-log scan.
func openActiveSegmentFromDisk(meta segmentMeta) (*activeSegment, int64, error) {
	seg, err := openActiveSegment(meta)
	if err != nil {
		return nil, 0, err
	}
	hwm, err := scanHighWatermark(seg.logFile, meta.baseOffset)
	if err != nil {
		_ = seg.logFile.Close()
		_ = seg.indexFile.Close()
		return nil, 0, err
	}
	if hwm > meta.baseOffset {
		seg.lastOffset = hwm - 1
	}
	return seg, hwm, nil
}

// scanHighWatermarkBufSize controls the bufio reader window scanning the
// active segment at open. 4 MiB lets one NFS READ RPC carry many batches
// of work; without this, the broker issued one tiny RPC per batch header
// and per batch body, turning startup into thousands of round-trips per
// partition. (The crash that motivated this: 16 partitions × ~800 MB
// each on shared NFS took >60s to scan, exceeding kubelet's startupProbe
// budget.)
const scanHighWatermarkBufSize = 4 * 1024 * 1024

// scanHighWatermark scans a log file to find the high watermark (next
// offset to write). The scan stops at the first malformed/truncated
// batch — that's the post-crash partial-write boundary; the returned
// hwm reflects only fully-persisted batches. Bufio-wrapped sequential
// read so big logs on NFS open in the order of MB/s, not RPCs/s.
func scanHighWatermark(f *os.File, segmentBaseOffset int64) (int64, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return segmentBaseOffset, err
	}

	br := bufio.NewReaderSize(f, scanHighWatermarkBufSize)
	hwm := segmentBaseOffset
	header := make([]byte, 12)
	// We only need the first 16 bytes of the body (attrs at [9:11],
	// lastOffsetDelta at [11:15]); the rest gets discarded.
	bodyHead := make([]byte, 16)
	for {
		if _, err := io.ReadFull(br, header); err != nil {
			break
		}
		batchLength := int32(binary.BigEndian.Uint32(header[8:12]))
		if batchLength < int32(len(bodyHead)) {
			break
		}
		if _, err := io.ReadFull(br, bodyHead); err != nil {
			break
		}
		// Skip the rest of the body — bufio.Discard issues bulk reads
		// from the buffered window, no per-byte syscalls.
		if _, err := br.Discard(int(batchLength) - len(bodyHead)); err != nil {
			break
		}
		baseOffset := int64(binary.BigEndian.Uint64(header[0:8]))
		lastOffsetDelta := int32(binary.BigEndian.Uint32(bodyHead[11:15]))
		hwm = baseOffset + int64(lastOffsetDelta) + 1
	}
	return hwm, nil
}

// appendBatch writes raw RecordBatch bytes to the log file and conditionally writes an index entry.
func (s *activeSegment) appendBatch(raw []byte, indexIntervalBytes int64) error {
	baseOffset, lastOffsetDelta, err := parseBatchOffsets(raw)
	if err != nil {
		return err
	}

	if _, err := s.logFile.WriteAt(raw, s.logSize); err != nil {
		return err
	}

	// Write index entry if we've accumulated enough log bytes since the last entry.
	if s.logSize-s.lastIndexedLogPos >= indexIntervalBytes {
		relOffset := int32(baseOffset - s.baseOffset)
		pos := int32(s.logSize)
		entry := make([]byte, 8)
		binary.BigEndian.PutUint32(entry[0:4], uint32(relOffset))
		binary.BigEndian.PutUint32(entry[4:8], uint32(pos))
		if _, err := s.indexFile.Write(entry); err != nil {
			return err
		}
		s.lastIndexedLogPos = s.logSize
	}

	s.logSize += int64(len(raw))
	s.lastOffset = baseOffset + int64(lastOffsetDelta)
	return nil
}

// roll closes the current segment and returns a new segment starting at
// newBaseOffset under the given leader epoch. Used by callers that want
// the full pre-#82 semantics (synchronous fsync of both files) — kept
// for tests and explicit sync paths.
func (s *activeSegment) roll(dir string, newBaseOffset, epoch int64) (*activeSegment, error) {
	if err := s.logFile.Sync(); err != nil {
		return nil, err
	}
	if err := s.indexFile.Sync(); err != nil {
		return nil, err
	}
	return createSegment(dir, newBaseOffset, epoch)
}

// rollFast is the segment-roll critical-path (gh #82): it fsyncs the
// log (so the trigger-batch is durable), creates the new segment, and
// returns. The OLD segment's index fsync, file close, and the manifest
// write are deferred to finalizeAfterRoll, which runs in a goroutine
// outside ps.mu — turning a multi-second NFS stall into a fast pointer
// swap.
func (s *activeSegment) rollFast(dir string, newBaseOffset, epoch int64) (*activeSegment, error) {
	if err := s.logFile.Sync(); err != nil {
		return nil, err
	}
	return createSegment(dir, newBaseOffset, epoch)
}

// finalizeAfterRoll runs the deferred close-out work for a segment that
// rollFast left dangling. Safe to call from a goroutine — the segment
// is no longer ps.active so no concurrent appends touch it. Index fsync
// is best-effort (a stale/short index just means Fetch falls back to a
// linear scan from the nearest indexed offset; #81's recovery covers
// the lossy-tail case).
func (s *activeSegment) finalizeAfterRoll() error {
	if s.indexFile != nil {
		_ = s.indexFile.Sync()
	}
	return s.close()
}

// close flushes and closes the segment files.
func (s *activeSegment) close() error {
	var lerr, ierr error
	if s.logFile != nil {
		_ = s.logFile.Sync()
		lerr = s.logFile.Close()
	}
	if s.indexFile != nil {
		_ = s.indexFile.Sync()
		ierr = s.indexFile.Close()
	}
	if lerr != nil {
		return lerr
	}
	return ierr
}

// readBatches reads RecordBatch bytes from a .log file. It seeks to approxPos (from the
// index), then scans forward returning batches at or after startOffset, up to maxBytes.
func readBatches(logPath string, approxPos int64, startOffset int64, maxBytes int) ([]byte, error) {
	f, err := os.Open(logPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	if approxPos > 0 {
		if _, err := f.Seek(approxPos, io.SeekStart); err != nil {
			return nil, err
		}
	}

	var out []byte
	header := make([]byte, 12)
	for len(out) < maxBytes {
		if _, err := io.ReadFull(f, header); err != nil {
			break
		}
		batchLength := int32(binary.BigEndian.Uint32(header[8:12]))
		if batchLength <= 0 {
			break
		}
		batch := make([]byte, 12+int(batchLength))
		copy(batch, header)
		if _, err := io.ReadFull(f, batch[12:]); err != nil {
			break
		}

		baseOffset := int64(binary.BigEndian.Uint64(batch[0:8]))
		lastOffsetDelta := int32(binary.BigEndian.Uint32(batch[23:27]))
		batchLastOffset := baseOffset + int64(lastOffsetDelta)

		if batchLastOffset < startOffset {
			continue
		}
		out = append(out, batch...)
	}
	return out, nil
}

// searchIndex binary-searches the index file for the largest position whose relative
// offset is <= (targetOffset - segmentBaseOffset). Returns 0 if the index is empty.
func searchIndex(indexPath string, segmentBaseOffset int64, targetOffset int64) (int64, error) {
	data, err := os.ReadFile(indexPath)
	if err != nil {
		return 0, nil
	}

	n := len(data) / 8
	if n == 0 {
		return 0, nil
	}

	targetRel := int32(targetOffset - segmentBaseOffset)
	if targetRel < 0 {
		return 0, nil
	}

	lo, hi := 0, n-1
	result := int64(0)
	for lo <= hi {
		mid := (lo + hi) / 2
		relOffset := int32(binary.BigEndian.Uint32(data[mid*8 : mid*8+4]))
		pos := int64(binary.BigEndian.Uint32(data[mid*8+4 : mid*8+8]))
		if relOffset <= targetRel {
			result = pos
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return result, nil
}

// listSegments returns all segments in dir sorted by base offset, as segmentMeta.
//
// Two filename formats are accepted:
//
//   - {epoch:08x}-{base_offset:020d}.log — v3.3 epoch-prefixed (canonical).
//   - {base_offset:020d}.log              — legacy v2.6, kept for migration.
//
// segmentMeta.indexPath is derived from the .log path's stem so the dual-
// format parsing only happens once.
func listSegments(dir string) ([]segmentMeta, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var segs []segmentMeta
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".log") {
			continue
		}
		if strings.HasSuffix(e.Name(), ".log.sealed") {
			// Sealed-by-takeover marker; not a readable segment.
			continue
		}
		stem := strings.TrimSuffix(e.Name(), ".log")
		baseOffset, ok := parseSegmentStem(stem)
		if !ok {
			continue
		}
		segs = append(segs, segmentMeta{
			baseOffset: baseOffset,
			logPath:    filepath.Join(dir, e.Name()),
			indexPath:  filepath.Join(dir, stem+".index"),
		})
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].baseOffset < segs[j].baseOffset })
	return segs, nil
}

// parseSegmentStem extracts the baseOffset from either a legacy stem
// (`00000000000000000123`) or an epoch-prefixed stem (`00000005-00000000000000000123`).
func parseSegmentStem(stem string) (int64, bool) {
	if dash := strings.IndexByte(stem, '-'); dash >= 0 {
		// Epoch-prefixed format. Validate the epoch is a hex number; we
		// don't currently use it on the read path (epoch is stored in
		// the manifest), but a non-hex prefix means this isn't our file.
		if _, err := strconv.ParseUint(stem[:dash], 16, 32); err != nil {
			return 0, false
		}
		bo, err := strconv.ParseInt(stem[dash+1:], 10, 64)
		if err != nil {
			return 0, false
		}
		return bo, true
	}
	bo, err := strconv.ParseInt(stem, 10, 64)
	if err != nil {
		return 0, false
	}
	return bo, true
}

// segmentMaxTimestamp scans a .log file and returns the highest maxTimestamp seen.
func segmentMaxTimestamp(logPath string) (int64, error) {
	f, err := os.Open(logPath)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	var maxTS int64
	header := make([]byte, 12)
	for {
		if _, err := io.ReadFull(f, header); err != nil {
			break
		}
		batchLength := int32(binary.BigEndian.Uint32(header[8:12]))
		if batchLength <= 0 {
			break
		}
		// maxTimestamp is at body offset [23:31] (body = everything after the 12-byte header)
		// Within the full batch at offset [35:43].
		body := make([]byte, int(batchLength))
		if _, err := io.ReadFull(f, body); err != nil {
			break
		}
		// full batch: header(12) + body
		// maxTimestamp offset in full batch = 35
		// In body = 35 - 12 = 23
		if len(body) >= 31 {
			ts := int64(binary.BigEndian.Uint64(body[23:31]))
			if ts > maxTS {
				maxTS = ts
			}
		}
	}
	return maxTS, nil
}

// recoverSegment scans the active segment forward, finds the last completely written and
// CRC-valid RecordBatch, and truncates any partial data after it. Returns the new highWatermark.
func recoverSegment(seg *activeSegment) (int64, error) {
	if _, err := seg.logFile.Seek(0, io.SeekStart); err != nil {
		return seg.baseOffset, err
	}

	var validEnd int64
	hwm := seg.baseOffset
	header := make([]byte, 12)
	for {
		pos, err := seg.logFile.Seek(0, io.SeekCurrent)
		if err != nil {
			break
		}
		if _, err := io.ReadFull(seg.logFile, header); err != nil {
			break
		}
		batchLength := int32(binary.BigEndian.Uint32(header[8:12]))
		if batchLength <= 0 {
			break
		}
		body := make([]byte, int(batchLength))
		if _, err := io.ReadFull(seg.logFile, body); err != nil {
			break
		}
		// body layout: [ple:4][magic:1][crc:4][crcPayload...]
		// CRC covers body[9:]
		if len(body) < 9 {
			break
		}
		storedCRC := binary.BigEndian.Uint32(body[5:9])
		if err := codec.ValidateCRC(body[9:], storedCRC); err != nil {
			break
		}

		baseOffset := int64(binary.BigEndian.Uint64(header[0:8]))
		// body[9:11]=attrs, body[11:15]=lastOffsetDelta
		lastOffsetDelta := int32(binary.BigEndian.Uint32(body[11:15]))
		validEnd = pos + 12 + int64(batchLength)
		hwm = baseOffset + int64(lastOffsetDelta) + 1
	}

	if err := seg.logFile.Truncate(validEnd); err != nil {
		return 0, err
	}
	if _, err := seg.logFile.Seek(validEnd, io.SeekStart); err != nil {
		return 0, err
	}
	seg.logSize = validEnd
	return hwm, nil
}

// rebuildIndex rewrites the .index file by scanning the .log file from the beginning.
func rebuildIndex(seg *activeSegment, indexIntervalBytes int64) error {
	if err := seg.indexFile.Truncate(0); err != nil {
		return err
	}
	if _, err := seg.indexFile.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err := seg.logFile.Seek(0, io.SeekStart); err != nil {
		return err
	}

	var lastIndexedPos int64
	var logPos int64
	header := make([]byte, 12)
	for {
		if _, err := io.ReadFull(seg.logFile, header); err != nil {
			break
		}
		batchLength := int32(binary.BigEndian.Uint32(header[8:12]))
		if batchLength <= 0 {
			break
		}
		baseOffset := int64(binary.BigEndian.Uint64(header[0:8]))

		if logPos-lastIndexedPos >= indexIntervalBytes {
			relOffset := int32(baseOffset - seg.baseOffset)
			entry := make([]byte, 8)
			binary.BigEndian.PutUint32(entry[0:4], uint32(relOffset))
			binary.BigEndian.PutUint32(entry[4:8], uint32(logPos))
			if _, err := seg.indexFile.Write(entry); err != nil {
				return err
			}
			lastIndexedPos = logPos
		}

		if _, err := seg.logFile.Seek(int64(batchLength), io.SeekCurrent); err != nil {
			break
		}
		logPos += 12 + int64(batchLength)
	}
	seg.lastIndexedLogPos = lastIndexedPos
	return seg.indexFile.Sync()
}

