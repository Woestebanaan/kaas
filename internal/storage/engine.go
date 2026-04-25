package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/woestebanaan/skafka/internal/lease"
	"github.com/woestebanaan/skafka/internal/lock"
)

var (
	ErrNotLeader   = errors.New("storage: not leader for partition")
	ErrLockNotHeld = errors.New("storage: filesystem lock not held")
)

// StorageEngine is the interface for reading and writing topic partition data.
// Append and Read operate on raw RecordBatch bytes as sent by / returned to Kafka clients.
type StorageEngine interface {
	Append(ctx context.Context, topic string, partition int32, rawBatch []byte) (baseOffset int64, err error)
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
}

// Config holds tunable parameters for DiskStorageEngine.
type Config struct {
	SegmentBytes       int64 // roll to a new segment at this size (default 1 GB)
	IndexIntervalBytes int64 // write an index entry every N bytes of log data (default 4096)
	RetentionMs        int64 // delete segments older than this (default 7 days)
}

func DefaultConfig() Config {
	return Config{
		SegmentBytes:       1 << 30,
		IndexIntervalBytes: 4096,
		RetentionMs:        7 * 24 * int64(time.Hour/time.Millisecond),
	}
}

// PartitionID identifies a topic partition.
type PartitionID struct {
	Topic     string
	Partition int32
}

// DiskStorageEngine implements StorageEngine using Kafka-compatible log segment files.
type DiskStorageEngine struct {
	dataDir string
	leases  lease.LeaseManager
	locks   lock.PartitionLock
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
}

// NewDiskStorageEngine opens (or creates) the data directory and loads all existing partitions.
func NewDiskStorageEngine(dataDir string, leases lease.LeaseManager, locks lock.PartitionLock, cfg Config) (*DiskStorageEngine, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}
	e := &DiskStorageEngine{
		dataDir:    dataDir,
		leases:     leases,
		locks:      locks,
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
func (e *DiskStorageEngine) openPartition(topic string, partition int32) error {
	dir := filepath.Join(e.dataDir, topic, strconv.Itoa(int(partition)))
	segs, err := listSegments(dir)
	if err != nil {
		return err
	}

	var ps *partitionState
	if len(segs) == 0 {
		seg, err := createSegment(dir, 0)
		if err != nil {
			return err
		}
		ps = &partitionState{dir: dir, active: seg}
	} else {
		last := segs[len(segs)-1]
		active, hwm, err := openActiveSegmentFromDisk(last)
		if err != nil {
			return fmt.Errorf("open active segment base=%d: %w", last.baseOffset, err)
		}
		closed := segs[:len(segs)-1]
		logStart := last.baseOffset
		if len(closed) > 0 {
			logStart = closed[0].baseOffset
		}
		ps = &partitionState{
			dir:       dir,
			active:    active,
			segments:  closed,
			logStart:  logStart,
			highWater: hwm,
		}
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
func (e *DiskStorageEngine) Append(_ context.Context, topic string, partition int32, rawBatch []byte) (int64, error) {
	if len(rawBatch) == 0 {
		hwm, _ := e.HighWatermark(topic, partition)
		return hwm, nil
	}

	if !e.leases.IsLeader(topic, partition) {
		return -1, ErrNotLeader
	}
	if !e.locks.IsLocked(topic, partition) {
		return -1, ErrLockNotHeld
	}

	ps, ok := e.getPartition(topic, partition)
	if !ok {
		return -1, fmt.Errorf("storage: unknown partition %s/%d", topic, partition)
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()

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

	if ps.active.logSize >= e.cfg.SegmentBytes {
		if err := e.rollSegment(ps); err != nil {
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
		logPath:    segmentLogPath(ps.dir, ps.active.baseOffset),
		indexPath:  segmentIndexPath(ps.dir, ps.active.baseOffset),
	}
	// Capture maxTimestamp from the last batch in the segment.
	if ts, err := segmentMaxTimestamp(closed.logPath); err == nil {
		closed.maxTimestamp = ts
	}

	newSeg, err := ps.active.roll(ps.dir, ps.highWater)
	if err != nil {
		return err
	}
	_ = ps.active.close()
	ps.segments = append(ps.segments, closed)
	ps.active = newSeg
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
		logPath:    segmentLogPath(ps.dir, ps.active.baseOffset),
		indexPath:  segmentIndexPath(ps.dir, ps.active.baseOffset),
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

// RelinquishPartition releases the filesystem lock for a partition. Called when this
// broker loses the Kubernetes Lease so no further writes are accepted.
func (e *DiskStorageEngine) RelinquishPartition(topic string, partition int32) {
	_ = e.locks.Unlock(topic, partition)
}

// TakeoverPartition is called when this broker becomes leader. It acquires the
// filesystem lock, validates the log (truncating any partial writes from the
// previous leader), writes the new epoch, and marks the partition writable.
func (e *DiskStorageEngine) TakeoverPartition(topic string, partition int32, newEpoch int64) error {
	if err := e.locks.Lock(topic, partition); err != nil {
		return fmt.Errorf("storage: acquire fs lock: %w", err)
	}

	ps, ok := e.getPartition(topic, partition)
	if !ok {
		return fmt.Errorf("storage: unknown partition %s/%d", topic, partition)
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()

	diskEpoch, err := readLeaderEpoch(ps.dir)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("storage: read leader epoch: %w", err)
	}

	if diskEpoch < newEpoch {
		hwm, err := recoverSegment(ps.active)
		if err != nil {
			return fmt.Errorf("storage: recover segment: %w", err)
		}
		if err := rebuildIndex(ps.active, e.cfg.IndexIntervalBytes); err != nil {
			return fmt.Errorf("storage: rebuild index: %w", err)
		}
		ps.highWater = hwm
	}

	return writeLeaderEpoch(ps.dir, newEpoch)
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

