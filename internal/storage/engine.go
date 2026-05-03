package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/woestebanaan/skafka/internal/lease"
	"github.com/woestebanaan/skafka/internal/observability"
)

var (
	ErrNotLeader     = errors.New("storage: not leader for partition")
	ErrEpochMismatch = errors.New("storage: epoch behind current partition leader")
)

// StorageEngine is the interface for reading and writing topic partition data.
// Append and Read operate on raw RecordBatch bytes as sent by / returned to
// Kafka clients — never on a decoded record set. epoch is the v3 leader-epoch
// fence supplied by the BrokerCoordinator; Append returns ErrEpochMismatch
// when the caller's epoch is behind the partition's current epoch. Phase 1
// plumbs the parameter; the fence is wired together with the BrokerCoordinator
// in Phase 4. Pass 0 to skip the fence under v2.6 callers.
type StorageEngine interface {
	Append(ctx context.Context, topic string, partition int32, epoch uint32, batchBytes []byte) (baseOffset int64, err error)
	Read(ctx context.Context, topic string, partition int32, startOffset int64, maxBytes int) ([]byte, error)
	HighWatermark(topic string, partition int32) (int64, error)
	LogStartOffset(topic string, partition int32) (int64, error)
	CreatePartition(topic string, partition int32) error
	DeletePartition(topic string, partition int32) error
	// PartitionSize returns the total bytes occupied by the partition's segment
	// files (logs + indexes). Returns 0 for unknown partitions rather than an
	// error so callers can iterate TopicSource without filtering.
	PartitionSize(topic string, partition int32) int64
	// DataDir returns the broker's data directory (advertised as the "log dir"
	// in DescribeLogDirs).
	DataDir() string
	// TakeOver claims write ownership of a partition under the given epoch.
	// Acquires whatever local locks the implementation uses, recovers any
	// partial writes from a previous leader, and returns the recovered high
	// watermark so the caller can report it back to the controller in the
	// next heartbeat. Part of the v3 coordination contract; v2.6 callers
	// (per-partition Lease callbacks) may continue to use TakeoverPartition.
	TakeOver(ctx context.Context, topic string, partition int32, epoch uint32) (recoveryOffset int64, err error)
	// Relinquish releases write ownership of a partition. Part of the v3
	// coordination contract.
	Relinquish(topic string, partition int32) error
}

// Config holds tunable parameters for DiskStorageEngine.
type Config struct {
	SegmentBytes       int64 // roll to a new segment at this size (default 1 GB)
	IndexIntervalBytes int64 // write an index entry every N bytes of log data (default 4096)
	RetentionMs        int64 // delete segments older than this (default 7 days)
	// FlushIntervalMessages caps how many records can sit in the OS write-back
	// cache before the engine forces an fsync(2). Skafka is RF=1, so the
	// storage layer is the entire durability story — the default of 1 trades
	// ~1ms/batch for honest acks=all semantics. Operators on slow NFS or with
	// replication-equivalent guarantees from below can relax this; 0 disables
	// message-driven flushing entirely (sync only at segment roll).
	FlushIntervalMessages int64
}

func DefaultConfig() Config {
	return Config{
		SegmentBytes:          1 << 30,
		IndexIntervalBytes:    4096,
		RetentionMs:           7 * 24 * int64(time.Hour/time.Millisecond),
		FlushIntervalMessages: 1,
	}
}

// PartitionID identifies a topic partition.
type PartitionID struct {
	Topic     string
	Partition int32
}

// DiskStorageEngine implements StorageEngine using Kafka-compatible log segment files.
//
// As of Phase 4, single-writer enforcement is no longer flock-based: the
// BrokerCoordinator owns the ownership decision (it consults the
// authoritative assignment.json), and segment filenames embed the leader
// epoch (`{epoch:08x}-{base_offset:020d}.log`) so two leaders at different
// epochs physically can't target the same file. The leases field stays
// for v2.6 callers (TakeoverPartition is still wired into older paths)
// but new v3 code goes through TakeOver.
type DiskStorageEngine struct {
	dataDir string
	leases  lease.LeaseManager
	cfg     Config

	mu         sync.RWMutex
	partitions map[string]*partitionState
}

type partitionState struct {
	mu       sync.Mutex
	dir      string
	active   *activeSegment
	segments []segmentMeta // closed segments, sorted by baseOffset
	logStart int64
	highWater int64
	epoch    int64 // current leader epoch, persisted in manifest.json
	// pendingFlushRecords counts records appended since the last fsync.
	// flushLocked checks against Config.FlushIntervalMessages and resets to 0.
	pendingFlushRecords int64
}

// persistManifestLocked writes manifest.json from the partition's current
// in-memory state. Caller must hold ps.mu.
func (ps *partitionState) persistManifestLocked() error {
	return writeManifest(ps.dir, &Manifest{
		Epoch:          ps.epoch,
		HighWatermark:  ps.highWater,
		LogStartOffset: ps.logStart,
	})
}

// flushLocked fsyncs the active segment's log + index files and writes the
// manifest, then resets the pending-record counter. Caller must hold ps.mu.
// A flush failure surfaces to the caller — the alternative is silently
// returning a successful Append for data that may not be durable, which is
// the bug the flush policy exists to prevent.
func (ps *partitionState) flushLocked() error {
	if ps.active == nil {
		return nil
	}
	start := time.Now()
	if ps.active.logFile != nil {
		if err := ps.active.logFile.Sync(); err != nil {
			return fmt.Errorf("storage: fsync log: %w", err)
		}
	}
	if ps.active.indexFile != nil {
		if err := ps.active.indexFile.Sync(); err != nil {
			return fmt.Errorf("storage: fsync index: %w", err)
		}
	}
	if err := ps.persistManifestLocked(); err != nil {
		return err
	}
	// FsyncLatency covers both log + index Sync + manifest write — the
	// user-observable durability cost. Caller (Append) already accounts
	// for total request time via WriteLatency in produce.go.
	observability.Global().FsyncLatency.Record(context.Background(), time.Since(start).Seconds())
	ps.pendingFlushRecords = 0
	return nil
}

// NewDiskStorageEngine opens (or creates) the data directory and loads all existing partitions.
func NewDiskStorageEngine(dataDir string, leases lease.LeaseManager, cfg Config) (*DiskStorageEngine, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}
	e := &DiskStorageEngine{
		dataDir:    dataDir,
		leases:     leases,
		cfg:        cfg,
		partitions: make(map[string]*partitionState),
	}
	return e, e.loadExisting()
}

// loadExisting walks the data directory and opens all topic/partition subdirectories.
func (e *DiskStorageEngine) loadExisting() error {
	topicDirs, err := os.ReadDir(e.dataDir)
	if err != nil {
		return err
	}
	for _, te := range topicDirs {
		if !te.IsDir() || te.Name() == "__cluster" {
			continue
		}
		partDirs, err := os.ReadDir(filepath.Join(e.dataDir, te.Name()))
		if err != nil {
			continue
		}
		for _, pe := range partDirs {
			if !pe.IsDir() {
				continue
			}
			n, err := strconv.Atoi(pe.Name())
			if err != nil {
				continue
			}
			if err := e.openPartition(te.Name(), int32(n)); err != nil {
				return fmt.Errorf("load %s/%d: %w", te.Name(), n, err)
			}
		}
	}
	return nil
}

func (e *DiskStorageEngine) partKey(topic string, partition int32) string {
	return topic + "/" + strconv.Itoa(int(partition))
}

func (e *DiskStorageEngine) getPartition(topic string, partition int32) (*partitionState, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	p, ok := e.partitions[e.partKey(topic, partition)]
	return p, ok
}

// CreatePartition creates the partition directory and registers it. Idempotent.
func (e *DiskStorageEngine) CreatePartition(topic string, partition int32) error {
	e.mu.Lock()
	key := e.partKey(topic, partition)
	if _, ok := e.partitions[key]; ok {
		e.mu.Unlock()
		return nil
	}
	e.mu.Unlock()

	dir := filepath.Join(e.dataDir, topic, strconv.Itoa(int(partition)))
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	return e.openPartition(topic, partition)
}

// openPartition reads segment files on disk and sets up in-memory state.
//
// State recovery order: (1) manifest.json — authoritative for {epoch, hwm,
// logStartOffset} when present; (2) segment scan — used to derive a fresh
// HWM and logStartOffset when the manifest is missing; (3) legacy
// .leader-epoch — read once for epoch only, then the manifest is written
// over the top so subsequent opens are fast.
func (e *DiskStorageEngine) openPartition(topic string, partition int32) error {
	dir := filepath.Join(e.dataDir, topic, strconv.Itoa(int(partition)))
	segs, err := listSegments(dir)
	if err != nil {
		return err
	}

	manifest, manifestErr := readManifest(dir)
	if manifestErr != nil && !errors.Is(manifestErr, fs.ErrNotExist) {
		return fmt.Errorf("storage: read manifest %s: %w", dir, manifestErr)
	}

	// Epoch for fresh segments comes from the manifest if present (post-
	// takeover restart) or 0 if this is a brand-new partition that has
	// never gone through TakeOver. The first TakeOver after Start will
	// write a higher epoch into the manifest and roll a fresh segment.
	var seedEpoch int64
	if manifest != nil {
		seedEpoch = manifest.Epoch
	}

	var ps *partitionState
	if len(segs) == 0 {
		seg, err := createSegment(dir, 0, seedEpoch)
		if err != nil {
			return err
		}
		ps = &partitionState{dir: dir, active: seg, epoch: seedEpoch}
	} else {
		last := segs[len(segs)-1]
		active, scannedHWM, err := openActiveSegmentFromDisk(last)
		if err != nil {
			return fmt.Errorf("open active segment base=%d: %w", last.baseOffset, err)
		}
		closed := segs[:len(segs)-1]
		scannedLogStart := last.baseOffset
		if len(closed) > 0 {
			scannedLogStart = closed[0].baseOffset
		}
		ps = &partitionState{
			dir:       dir,
			active:    active,
			segments:  closed,
			logStart:  scannedLogStart,
			highWater: scannedHWM,
		}
	}

	if manifest != nil {
		ps.epoch = manifest.Epoch
		// Manifest HWM/logStartOffset are authoritative when present, but
		// reconcile against the segment scan: if the manifest claims an HWM
		// past the highest scanned offset (e.g. truncated active segment), the
		// scan wins because the data is what's actually on disk.
		if manifest.HighWatermark > 0 && manifest.HighWatermark <= ps.highWater {
			ps.highWater = manifest.HighWatermark
		}
		if manifest.LogStartOffset > ps.logStart {
			ps.logStart = manifest.LogStartOffset
		}
	}

	// First open under v3.3 storage: persist a manifest so subsequent opens
	// don't pay a full segment scan. Best-effort — a write failure here is
	// not fatal because openPartition() will fall back to scanning again.
	if manifest == nil || manifest.HighWatermark != ps.highWater || manifest.LogStartOffset != ps.logStart {
		_ = writeManifest(dir, &Manifest{
			Epoch:          ps.epoch,
			HighWatermark:  ps.highWater,
			LogStartOffset: ps.logStart,
		})
	}

	e.mu.Lock()
	e.partitions[e.partKey(topic, partition)] = ps
	e.mu.Unlock()
	return nil
}

// DeletePartition closes and removes the partition directory.
func (e *DiskStorageEngine) DeletePartition(topic string, partition int32) error {
	e.mu.Lock()
	key := e.partKey(topic, partition)
	ps, ok := e.partitions[key]
	if ok {
		delete(e.partitions, key)
	}
	e.mu.Unlock()

	if ps != nil {
		ps.mu.Lock()
		if ps.active != nil {
			_ = ps.active.close()
		}
		ps.mu.Unlock()
	}
	return os.RemoveAll(filepath.Join(e.dataDir, topic, strconv.Itoa(int(partition))))
}

// Append writes a raw RecordBatch to the partition log.
// Both the Kubernetes Lease and the filesystem lock must be held by the caller.
//
// epoch is the v3 leader-epoch fence supplied by the BrokerCoordinator. When
// both caller and stored epoch are non-zero and disagree, Append returns
// ErrEpochMismatch — the data-plane half of the v3.3 epoch fence pairs with
// assignment_watch.go's file-validation half. epoch==0 is the v2.6
// compatibility sentinel ("no fence configured"), as is ps.epoch==0 (no
// TakeOver has run since the partition was opened).
func (e *DiskStorageEngine) Append(_ context.Context, topic string, partition int32, epoch uint32, rawBatch []byte) (int64, error) {
	if len(rawBatch) == 0 {
		hwm, _ := e.HighWatermark(topic, partition)
		return hwm, nil
	}

	if !e.leases.IsLeader(topic, partition) {
		return -1, ErrNotLeader
	}

	ps, ok := e.getPartition(topic, partition)
	if !ok {
		return -1, fmt.Errorf("storage: unknown partition %s/%d", topic, partition)
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()

	// Epoch fence: caller's epoch must match the partition's stored epoch
	// when both sides are non-zero. Strict equality matches the plan
	// pseudocode in §"Append flow" — a caller running ahead of TakeOver
	// is just as wrong as one running behind.
	if epoch != 0 && ps.epoch != 0 && uint32(ps.epoch) != epoch {
		return -1, ErrEpochMismatch
	}

	// Brokers own offsets. Producers ship baseOffset=0 (the wire convention);
	// rewrite to the partition's high watermark before persisting so reads
	// see strictly-increasing offsets across batches. CRC32C covers
	// attrs..records (body[9:]), not baseOffset, so the 8-byte overwrite is safe.
	binary.BigEndian.PutUint64(rawBatch[0:8], uint64(ps.highWater))

	baseOffset, lastOffsetDelta, err := parseBatchOffsets(rawBatch)
	if err != nil {
		return -1, fmt.Errorf("storage: %w", err)
	}

	if err := ps.active.appendBatch(rawBatch, e.cfg.IndexIntervalBytes); err != nil {
		return -1, err
	}
	ps.highWater = baseOffset + int64(lastOffsetDelta) + 1
	ps.pendingFlushRecords += int64(lastOffsetDelta) + 1

	if ps.active.logSize >= e.cfg.SegmentBytes {
		if err := e.rollSegment(ps); err != nil {
			return -1, err
		}
		// rollSegment fsyncs the previous segment via roll() and persists
		// the manifest, so the pending counter is fully discharged.
		ps.pendingFlushRecords = 0
	} else if e.cfg.FlushIntervalMessages > 0 && ps.pendingFlushRecords >= e.cfg.FlushIntervalMessages {
		if err := ps.flushLocked(); err != nil {
			return -1, err
		}
	}

	return baseOffset, nil
}

// rollSegment closes the active segment and opens a new one.
// Must be called with ps.mu held.
func (e *DiskStorageEngine) rollSegment(ps *partitionState) error {
	closed := segmentMeta{
		baseOffset: ps.active.baseOffset,
		logPath:    ps.active.logPath,
		indexPath:  ps.active.indexPath,
	}
	// Capture maxTimestamp from the last batch in the segment.
	if ts, err := segmentMaxTimestamp(closed.logPath); err == nil {
		closed.maxTimestamp = ts
	}

	newSeg, err := ps.active.roll(ps.dir, ps.highWater, ps.epoch)
	if err != nil {
		return err
	}
	_ = ps.active.close()
	ps.segments = append(ps.segments, closed)
	ps.active = newSeg

	// Roll is a natural manifest checkpoint: the previous segment is now
	// closed and the HWM is at a stable boundary. A failure here is not
	// fatal — the next TakeOver / open will rebuild from a segment scan.
	_ = ps.persistManifestLocked()
	return nil
}

// Read returns a concatenation of raw RecordBatch bytes starting at or after startOffset,
// spanning segment boundaries up to maxBytes.
func (e *DiskStorageEngine) Read(_ context.Context, topic string, partition int32, startOffset int64, maxBytes int) ([]byte, error) {
	ps, ok := e.getPartition(topic, partition)
	if !ok {
		return nil, fmt.Errorf("storage: unknown partition %s/%d", topic, partition)
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()

	if startOffset >= ps.highWater {
		return nil, nil
	}

	// Build an ordered list of all segments (closed then active).
	all := make([]segmentMeta, 0, len(ps.segments)+1)
	all = append(all, ps.segments...)
	all = append(all, segmentMeta{
		baseOffset: ps.active.baseOffset,
		logPath:    ps.active.logPath,
		indexPath:  ps.active.indexPath,
	})

	// Find the first segment that could contain startOffset.
	startIdx := 0
	for i := len(all) - 1; i >= 0; i-- {
		if all[i].baseOffset <= startOffset {
			startIdx = i
			break
		}
	}

	var out []byte
	for i := startIdx; i < len(all) && len(out) < maxBytes; i++ {
		seg := all[i]
		reqOffset := startOffset
		approxPos := int64(0)
		if i == startIdx {
			approxPos, _ = searchIndex(seg.indexPath, seg.baseOffset, startOffset)
		} else {
			reqOffset = seg.baseOffset
		}
		batch, err := readBatches(seg.logPath, approxPos, reqOffset, maxBytes-len(out))
		if err != nil {
			return nil, err
		}
		out = append(out, batch...)
	}
	return out, nil
}

func (e *DiskStorageEngine) HighWatermark(topic string, partition int32) (int64, error) {
	ps, ok := e.getPartition(topic, partition)
	if !ok {
		return 0, fmt.Errorf("storage: unknown partition %s/%d", topic, partition)
	}
	ps.mu.Lock()
	hwm := ps.highWater
	ps.mu.Unlock()
	return hwm, nil
}

// PartitionSize sums the sizes of all files in the partition directory
// (log + index + timeindex + leader-epoch + lock). Used by DescribeLogDirs.
func (e *DiskStorageEngine) PartitionSize(topic string, partition int32) int64 {
	ps, ok := e.getPartition(topic, partition)
	if !ok {
		return 0
	}
	ps.mu.Lock()
	dir := ps.dir
	ps.mu.Unlock()

	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	var total int64
	for _, ent := range entries {
		info, err := ent.Info()
		if err != nil {
			continue
		}
		total += info.Size()
	}
	return total
}

// DataDir returns the broker's root data directory.
func (e *DiskStorageEngine) DataDir() string { return e.dataDir }

func (e *DiskStorageEngine) LogStartOffset(topic string, partition int32) (int64, error) {
	ps, ok := e.getPartition(topic, partition)
	if !ok {
		return 0, fmt.Errorf("storage: unknown partition %s/%d", topic, partition)
	}
	ps.mu.Lock()
	ls := ps.logStart
	ps.mu.Unlock()
	return ls, nil
}

// RelinquishPartition is now a no-op kept for v2.6 caller compatibility.
// Single-writer enforcement moved to BrokerCoordinator.Owns + epoch-prefixed
// segment filenames in Phase 4; flock is no longer the safety boundary.
func (e *DiskStorageEngine) RelinquishPartition(_ string, _ int32) {}

// TakeoverPartition is called when this broker becomes leader. It validates
// the log (truncating any partial writes from the previous leader), writes
// the new epoch, and marks the partition writable.
//
// Retained for the v2.6 per-partition Lease callback wiring in cmd/skafka.
// New code should prefer TakeOver, which returns the recovered high watermark
// for reporting back to the v3 controller.
func (e *DiskStorageEngine) TakeoverPartition(topic string, partition int32, newEpoch int64) error {
	_, err := e.takeoverInternal(topic, partition, newEpoch)
	return err
}

// takeoverInternal is the shared implementation behind TakeoverPartition (v2.6
// callers) and TakeOver (v3 callers). Returns the recovered high watermark.
func (e *DiskStorageEngine) takeoverInternal(topic string, partition int32, newEpoch int64) (int64, error) {
	ps, ok := e.getPartition(topic, partition)
	if !ok {
		return 0, fmt.Errorf("storage: unknown partition %s/%d", topic, partition)
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()

	diskEpoch := ps.epoch
	if m, err := readManifest(ps.dir); err == nil {
		diskEpoch = m.Epoch
	} else if !errors.Is(err, fs.ErrNotExist) {
		return 0, fmt.Errorf("storage: read manifest: %w", err)
	}

	if diskEpoch < newEpoch {
		hwm, err := recoverSegment(ps.active)
		if err != nil {
			return 0, fmt.Errorf("storage: recover segment: %w", err)
		}
		if err := rebuildIndex(ps.active, e.cfg.IndexIntervalBytes); err != nil {
			return 0, fmt.Errorf("storage: rebuild index: %w", err)
		}
		ps.highWater = hwm
	}

	ps.epoch = newEpoch
	if err := ps.persistManifestLocked(); err != nil {
		return 0, fmt.Errorf("storage: write manifest: %w", err)
	}
	return ps.highWater, nil
}

// TakeOver implements the v3 StorageEngine contract. It claims write ownership
// of the partition under the given epoch, recovers any partial writes, and
// returns the recovered high watermark.
func (e *DiskStorageEngine) TakeOver(_ context.Context, topic string, partition int32, epoch uint32) (int64, error) {
	return e.takeoverInternal(topic, partition, int64(epoch))
}

// Relinquish implements the v3 StorageEngine contract. The partition becomes
// read-only on this broker. With flock removed in Phase 4, this is a no-op
// at the storage layer — the BrokerCoordinator.Owns check on the produce
// hot path is the only enforcement point.
func (e *DiskStorageEngine) Relinquish(_ string, _ int32) error {
	return nil
}

// AllPartitions returns all known partitions — used by the retention cleaner.
func (e *DiskStorageEngine) AllPartitions() []PartitionID {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]PartitionID, 0, len(e.partitions))
	for k := range e.partitions {
		parts := strings.SplitN(k, "/", 2)
		if len(parts) != 2 {
			continue
		}
		n, _ := strconv.Atoi(parts[1])
		out = append(out, PartitionID{Topic: parts[0], Partition: int32(n)})
	}
	return out
}

// deleteSegment removes a closed segment from the partition state and disk.
// Must be called with ps.mu held.
func (ps *partitionState) deleteSegment(idx int) {
	seg := ps.segments[idx]
	_ = os.Remove(seg.logPath)
	_ = os.Remove(seg.indexPath)
	_ = os.Remove(strings.TrimSuffix(seg.logPath, ".log") + ".timeindex")
	ps.segments = append(ps.segments[:idx], ps.segments[idx+1:]...)
	if len(ps.segments) > 0 {
		ps.logStart = ps.segments[0].baseOffset
	} else if ps.active != nil {
		ps.logStart = ps.active.baseOffset
	}
}

// parseBatchOffsets extracts baseOffset and lastOffsetDelta from raw RecordBatch bytes.
func parseBatchOffsets(raw []byte) (baseOffset int64, lastOffsetDelta int32, err error) {
	if len(raw) < 27 {
		return 0, 0, fmt.Errorf("batch too short: %d bytes", len(raw))
	}
	baseOffset = int64(binary.BigEndian.Uint64(raw[0:8]))
	lastOffsetDelta = int32(binary.BigEndian.Uint32(raw[23:27]))
	return
}

