package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/woestebanaan/skafka/internal/lease"
	"github.com/woestebanaan/skafka/internal/observability"
)

var (
	ErrNotLeader     = errors.New("storage: not leader for partition")
	ErrEpochMismatch    = errors.New("storage: epoch behind current partition leader")
	ErrOffsetOutOfRange = errors.New("storage: offset out of range")

	// ErrOutOfOrderSequence pairs with Kafka error 45
	// (OUT_OF_ORDER_SEQUENCE_NUMBER): an idempotent producer's batch
	// arrived with a baseSequence that does not pick up where its
	// previous batch left off. Producers treat this as fatal —
	// retrying would only widen the gap — and surface it as a
	// KafkaException to the application.
	ErrOutOfOrderSequence = errors.New("storage: out-of-order sequence number")
	// ErrInvalidProducerEpoch pairs with Kafka error 47: the batch's
	// producerEpoch is older than what we have on file for this
	// producerID. Apache Kafka emits this when a zombie producer
	// (one that was fenced by a newer InitProducerId on the same
	// transactional ID) tries to write — for skafka stage B it also
	// fires when a non-transactional producer's epoch goes
	// backwards, which should never happen on a healthy connection
	// but is the correct response if it does.
	ErrInvalidProducerEpoch = errors.New("storage: invalid producer epoch")

	// ErrStorageStalled fires when the partition committer's fsync
	// exceeds Config.FsyncMaxLatency (gh #95). The underlying RWX
	// backend is unreachable / unresponsive — typically a crashed NFS
	// server. Without the watchdog, fsync sits in an uninterruptible
	// kernel syscall for as long as the backend takes to recover, and
	// every appender on the partition queues silently behind ps.mu
	// while the broker still passes /healthz. Surfacing the timeout
	// as a real error makes the Produce handler fail fast with
	// REQUEST_TIMED_OUT, lets /healthz flag storage_stalled, and
	// gives the Java client's idempotent retry loop something
	// recoverable to chew on instead of dropping the connection.
	ErrStorageStalled = errors.New("storage: fsync exceeded FsyncMaxLatency")

	// ErrUnknownPartition fires when Append/Read is called for a
	// partition this broker hasn't opened (TakeOver never ran for it).
	// Pre-gh #132 this was an unstructured fmt.Errorf, so handler-side
	// errors.Is() couldn't classify it and the produce-errors metric
	// dumped these into 'unknown'. Promoted to a sentinel so the
	// dashboard can call out "wrong-broker traffic" by its proper name.
	ErrUnknownPartition = errors.New("storage: unknown partition")
	// ErrPartitionClosing fires when Append blocks waiting on a flush
	// and the partition is relinquished underneath it. Same motivation
	// as ErrUnknownPartition — classify cleanly instead of collapsing
	// to 'unknown'.
	ErrPartitionClosing = errors.New("storage: partition closing")
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
	// DeleteRecords advances the partition's log start offset to (at
	// least) targetOffset, making earlier records invisible to Fetch.
	// targetOffset == -1 is a sentinel for "current high watermark"
	// (purge all). Returns the new log start offset, or
	// ErrOffsetOutOfRange if targetOffset is past HWM.
	DeleteRecords(topic string, partition int32, targetOffset int64) (newLogStart int64, err error)
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
	RetentionBytes     int64 // delete oldest segments when partition total exceeds this (0 = unlimited)
	// FlushIntervalMessages caps how many records can sit in the OS write-back
	// cache before the engine forces an fsync(2). Skafka is RF=1, so the
	// storage layer is the entire durability story — the default of 1 trades
	// ~1ms/batch for honest acks=all semantics. Operators on slow NFS or with
	// replication-equivalent guarantees from below can relax this; 0 disables
	// message-driven flushing entirely (sync only at segment roll).
	FlushIntervalMessages int64

	// FsyncMaxLatency bounds how long the committer waits for one
	// logFile.Sync() before concluding the storage backend has stalled
	// (gh #95). When the deadline fires, committerLoop sets
	// flushErr=ErrStorageStalled, broadcasts the flushCond so queued
	// appenders fail fast, and continues — the next Append cycle gets
	// a fresh attempt. The orphaned Sync goroutine sticks around until
	// the kernel returns; on a healthy backend that's milliseconds, on
	// a hung NFS it can be minutes, but it doesn't block any other
	// committer or appender.
	//
	// Default 30 s — short enough to surface inside the Java
	// idempotent producer's request.timeout.ms (typically 30-120 s),
	// long enough to ride out a normal NFS server restart. 0 disables
	// the watchdog and restores pre-#95 behaviour (Sync may block
	// indefinitely).
	FsyncMaxLatency time.Duration
}

func DefaultConfig() Config {
	return Config{
		SegmentBytes:          1 << 30,
		IndexIntervalBytes:    4096,
		RetentionMs:           7 * 24 * int64(time.Hour/time.Millisecond),
		FlushIntervalMessages: 1,
		FsyncMaxLatency:       30 * time.Second,
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

	// reaper drains the slow phase of ClosePartition off the request
	// path. gh #119. Nil in tests / dev mode unless WithReaper is set;
	// when nil, ClosePartition falls back to the synchronous close.
	reaper *PartitionReaper
}

type partitionState struct {
	mu       sync.Mutex
	dir      string
	active   *activeSegment
	segments []segmentMeta // closed segments, sorted by baseOffset
	logStart int64
	highWater int64
	// gh #134: lock-free atomic mirrors of the above. Written under
	// ps.mu alongside the canonical fields; readable without taking
	// ps.mu. Used by HighWatermark() / LogStartOffset() so the OTel
	// gauge callback (which iterates all owned partitions) never
	// blocks on the storage mutex. Pre-gh #134 a stuck NAS fsync
	// held ps.mu for the watchdog deadline, the gauge callback
	// stalled, the PeriodicReader's Export deadline elapsed, and
	// ALL skafka metrics vanished from Prometheus until the stall
	// cleared. Canonical reads inside Append/Read/etc. still use
	// the int64 fields under the lock — atomics are the read-only
	// observation channel.
	highWaterAtomic atomic.Int64
	logStartAtomic  atomic.Int64
	epoch    int64 // current leader epoch, persisted in manifest.json
	// pendingFlushRecords counts records appended since the last
	// flush request. Reset when a flush is requested (not when it
	// completes), so we don't enqueue redundant signals.
	pendingFlushRecords int64
	// retentionBytesOverride is loaded from /data/<topic>/.config.json on
	// partition open. 0 means "no override" — the cleaner falls back to
	// engine.cfg.RetentionBytes (which itself is 0 = unlimited by default).
	// Per-topic operator-driven retention plumbing (gh #47).
	retentionBytesOverride int64

	// Group-commit state (gh #82). The committer goroutine fsyncs the
	// active segment outside ps.mu, so concurrent Appends to the same
	// partition share one fsync round-trip instead of serialising one
	// per record. requestedFlushSeq increments on every flush request
	// (under ps.mu); completedFlushSeq advances when the corresponding
	// fsync returns. Append blocks on flushCond until its mySeq is
	// covered. flushErr surfaces an fsync failure (sticky — the broker
	// is in a bad state and operator intervention is expected).
	flushReqCh        chan struct{}
	flushCond         *sync.Cond
	requestedFlushSeq int64
	completedFlushSeq int64
	flushErr          error
	committerDone     chan struct{}
	closing           bool

	// gh #132 item 1 v2: sequence-numbered durability tracking.
	// nextWriteSeq is incremented (under ps.mu) every time Append
	// reserves a write position. After pwrite returns, the Append
	// calls markWriteComplete which advances completedWriteSeq via
	// a contiguous watermark — that's what lets the committer wait
	// for "all writes <= flushBarrier are durable in the page cache"
	// even though pwrite happens outside the partition mutex.
	//
	// flushBarriers[N] stores the writeSeq watermark captured at the
	// moment the Nth flush was requested. The committer pulls the
	// barrier for the seq it's flushing, waits for completedWriteSeq
	// to reach it, then calls fsync(2). After fsync returns, every
	// writeSeq <= that barrier is on durable storage.
	//
	// reservedHighWater is the offset that the NEXT Append will
	// reserve; highWater (above) is the offset of the LAST byte that
	// has actually been written. They drift apart while pwrites are
	// in flight. Read paths consult highWater; idempotence /
	// offset-assignment paths read reservedHighWater (next batch's
	// baseOffset) under the lock. pendingEndOffsets[seq] records the
	// end-offset of writeSeq=seq so markWriteComplete can advance
	// the visible highWater along with completedWriteSeq.
	nextWriteSeq       uint64
	completedWriteSeq  uint64
	pendingCompletions map[uint64]struct{}
	flushBarriers      map[int64]uint64
	reservedHighWater  int64
	pendingEndOffsets  map[uint64]int64

	// fsyncMaxLatency mirrors Config.FsyncMaxLatency at the moment
	// startCommitter ran. Per-partition rather than per-engine so the
	// committer's tight loop doesn't have to chase the engine pointer
	// through ps.mu (#95).
	fsyncMaxLatency time.Duration

	// stalled is true while the most recent fsync timed out and
	// hasn't recovered. healthz aggregates this over all partitions
	// to surface a cluster-level "storage backend hung" signal.
	stalled bool

	// syncOverride is a test-only hook that replaces the default
	// logFile.Sync() in committerLoop. Production callers leave it
	// nil and the committer falls through to the real syscall.
	// Used by gh #95 timeout tests to simulate an NFS hang without
	// needing an actual hung filesystem.
	syncOverride func() error

	// producerStates tracks per-(producerID) idempotence state for
	// Apache Kafka's idempotent-producer guarantees (gh #12 stage B):
	// dedupe of in-window retries, OUT_OF_ORDER_SEQUENCE_NUMBER for
	// gaps, INVALID_PRODUCER_EPOCH for stale-epoch writes. The map is
	// in-memory only in stage B1; persistence on segment roll is B2.
	// Nil until the first idempotent batch lands so non-idempotent
	// partitions don't pay the allocation cost.
	producerStates map[int64]*producerEntry
}

// markWriteComplete advances the contiguous-completion watermark
// completedWriteSeq when writeSeq W's pwrite has returned. If W is the
// next-expected seq, we extend the watermark and consume any further
// contiguous completions that were stashed out-of-order. Otherwise we
// stash W in pendingCompletions and let a later in-order completion
// pick it up. Must be called with ps.mu held.
//
// gh #132 item 1 v2: lets the committer wait for "all writes <=
// barrier are durable in the page cache" even though pwrite happens
// outside ps.mu and may complete out of reservation order.
//
// Returns the offset the visible highWater should be advanced to —
// the largest end-offset across all seqs that just became contiguously
// complete. The caller updates ps.highWater + the atomic mirror. The
// pendingEndOffsets entries for the newly-completed seqs are removed.
func (ps *partitionState) markWriteComplete(W uint64) (visibleHighWater int64) {
	if W == ps.completedWriteSeq+1 {
		ps.completedWriteSeq = W
		if eo, ok := ps.pendingEndOffsets[W]; ok {
			visibleHighWater = eo
			delete(ps.pendingEndOffsets, W)
		}
		for {
			next := ps.completedWriteSeq + 1
			if _, ok := ps.pendingCompletions[next]; !ok {
				break
			}
			delete(ps.pendingCompletions, next)
			ps.completedWriteSeq = next
			if eo, ok := ps.pendingEndOffsets[next]; ok {
				if eo > visibleHighWater {
					visibleHighWater = eo
				}
				delete(ps.pendingEndOffsets, next)
			}
		}
		return visibleHighWater
	}
	if ps.pendingCompletions == nil {
		ps.pendingCompletions = make(map[uint64]struct{})
	}
	ps.pendingCompletions[W] = struct{}{}
	return 0
}

// startCommitter spawns the per-partition group-commit goroutine. Must
// be called exactly once per partitionState before it's exposed to
// Append callers. The goroutine exits when flushReqCh is closed or
// closing is set.
func (ps *partitionState) startCommitter() {
	ps.flushReqCh = make(chan struct{}, 1)
	ps.flushCond = sync.NewCond(&ps.mu)
	ps.committerDone = make(chan struct{})
	go ps.committerLoop()
}

// committerLoop drains flushReqCh, runs one fsync per signal, and
// broadcasts completion. Multiple Appends that signaled before the
// fsync started share the same fsync — that's the point.
func (ps *partitionState) committerLoop() {
	defer close(ps.committerDone)
	for {
		_, ok := <-ps.flushReqCh
		if !ok {
			// Channel closed (shutdown). Run one final flush to
			// drain any pending Appends, then exit.
			ps.drainAndExit()
			return
		}

		ps.mu.Lock()
		if ps.closing {
			ps.mu.Unlock()
			ps.drainAndExit()
			return
		}
		// If a prior cycle already covered everything, nothing to do.
		if ps.requestedFlushSeq <= ps.completedFlushSeq {
			ps.mu.Unlock()
			continue
		}
		seqAtStart := ps.requestedFlushSeq
		// gh #132 item 1 v2: wait for every pwrite that was inflight at
		// the moment of flush request to land in the page cache before
		// we issue fsync. flushBarriers[seqAtStart] is the writeSeq
		// watermark captured under the lock by Append when it requested
		// the flush. After completedWriteSeq reaches that barrier, the
		// fsync we're about to issue is guaranteed to cover those bytes.
		barrier, hasBarrier := ps.flushBarriers[seqAtStart]
		for hasBarrier && ps.completedWriteSeq < barrier && !ps.closing && ps.flushErr == nil {
			ps.flushCond.Wait()
		}
		if hasBarrier {
			delete(ps.flushBarriers, seqAtStart)
		}
		if ps.closing {
			ps.mu.Unlock()
			ps.drainAndExit()
			return
		}
		// Snapshot the logFile pointer (not just the segment) so that
		// a concurrent Relinquish — which sets ps.active.logFile = nil
		// after closing — doesn't cause us to deref nil between this
		// snapshot and the Sync call. If the file was closed under
		// us we get EBADF on Sync; that propagates as flushErr to the
		// in-flight waiters and gets cleared on the next successful
		// cycle (committer's "no error → leave flushErr alone" branch
		// handles this since the next signal comes after a TakeOver
		// re-opened the handle).
		var logFile *os.File
		if ps.active != nil {
			logFile = ps.active.logFile
		}
		ps.mu.Unlock()

		var err error
		if logFile != nil {
			start := time.Now()
			syncFn := func() error { return logFile.Sync() }
			if ps.syncOverride != nil {
				syncFn = ps.syncOverride
			}
			err = syncWithDeadline(syncFn, ps.fsyncMaxLatency)
			observability.Global().FsyncLatency.Record(context.Background(), time.Since(start).Seconds())
		}

		ps.mu.Lock()
		// flushErr is set when this cycle errored, cleared on a clean
		// cycle. Lets a transient EBADF from a Relinquish race recover
		// once the next leadership takes over and signals fresh.
		ps.flushErr = err
		// Track storage_stalled as a sticky-but-recoverable flag
		// (gh #95). Sets on ErrStorageStalled, clears on the first
		// successful cycle after recovery. healthz unions across
		// partitions. Log on the rising and falling edges so
		// operators have a broker-side breadcrumb to correlate with
		// producer-side REQUEST_TIMED_OUT — silent stalls were the
		// exact UX gap that motivated #95.
		nowStalled := errors.Is(err, ErrStorageStalled)
		switch {
		case nowStalled && !ps.stalled:
			slog.Warn("storage: fsync watchdog timeout — backend stalled",
				"partition_dir", ps.dir,
				"deadline", ps.fsyncMaxLatency.String(),
				"seq", seqAtStart)
		case !nowStalled && ps.stalled:
			slog.Info("storage: fsync recovered",
				"partition_dir", ps.dir,
				"seq", seqAtStart)
		}
		ps.stalled = nowStalled
		ps.completedFlushSeq = seqAtStart
		ps.flushCond.Broadcast()
		ps.mu.Unlock()
	}
}

// syncWithDeadline runs syncFn in a child goroutine and waits up to
// max for it to return (gh #95). When the deadline fires, returns
// ErrStorageStalled and lets the syncFn goroutine continue running
// to completion in the background. The orphan eventually exits when
// the underlying syscall returns; on a hung NFS that can be minutes
// (the kernel can't preempt an in-flight RPC) but doesn't block the
// next committer cycle from issuing a fresh attempt.
//
// max <= 0 disables the watchdog: syncFn is called inline. Matches
// the documented "0 disables" semantics on Config.FsyncMaxLatency.
func syncWithDeadline(syncFn func() error, max time.Duration) error {
	if max <= 0 {
		return syncFn()
	}
	done := make(chan error, 1)
	go func() { done <- syncFn() }()
	select {
	case err := <-done:
		return err
	case <-time.After(max):
		return ErrStorageStalled
	}
}

// drainAndExit runs a final fsync (best-effort) and wakes any pending
// Appends. Called once when the committer is told to stop.
func (ps *partitionState) drainAndExit() {
	ps.mu.Lock()
	seg := ps.active
	seqAtStart := ps.requestedFlushSeq
	ps.mu.Unlock()

	var err error
	if seg != nil && seg.logFile != nil {
		err = seg.logFile.Sync()
	}

	ps.mu.Lock()
	if err != nil && ps.flushErr == nil {
		ps.flushErr = err
	}
	ps.completedFlushSeq = seqAtStart
	ps.flushCond.Broadcast()
	ps.mu.Unlock()
}

// stopCommitter signals the committer to exit and waits for it. Called
// from ClosePartition / DeletePartition. Pending Appends waiting on the
// flush either complete (data fsynced) or return the broker-shutdown
// path's "partition closing" error if they raced past the closing flag.
func (ps *partitionState) stopCommitter() {
	if ps.flushReqCh == nil {
		return
	}
	ps.mu.Lock()
	if ps.closing {
		ps.mu.Unlock()
		<-ps.committerDone
		return
	}
	ps.closing = true
	ps.flushCond.Broadcast()
	ps.mu.Unlock()
	close(ps.flushReqCh)
	<-ps.committerDone
}

// persistManifestLocked writes manifest.json from the partition's current
// in-memory state. Caller must hold ps.mu.
//
// Stage B2 of gh #12 also persists producerStates to a sibling
// producer-state.snapshot file so a restart / leader takeover
// preserves the idempotent-producer dedupe window. Failure to
// write the snapshot is best-effort: the manifest is still the
// source of truth for HWM/logStart, and a missing snapshot just
// means new idempotent producers start with a fresh window
// (i.e. the stage-B1 behaviour).
func (ps *partitionState) persistManifestLocked() error {
	if len(ps.producerStates) > 0 {
		_ = writeProducerSnapshot(ps.dir, ps.producerStates)
	}
	return writeManifest(ps.dir, &Manifest{
		Epoch:          ps.epoch,
		HighWatermark:  ps.highWater,
		LogStartOffset: ps.logStart,
	})
}

// NewDiskStorageEngine opens (or creates) the data directory and loads all existing partitions.
func NewDiskStorageEngine(dataDir string, leases lease.LeaseManager, cfg Config) (*DiskStorageEngine, error) {
	if err := os.MkdirAll(dataDir, 0o775); err != nil {
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
	if err := os.MkdirAll(dir, 0o775); err != nil {
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
		var active *activeSegment
		var hwm int64
		// Fast path: trust the manifest. The manifest is rewritten on
		// every flushLocked, so its HWM matches what's on disk in
		// steady state. The only mismatch case — log fsynced but
		// manifest write failed/skipped before crash — is caught at
		// takeover time: takeoverInternal calls recoverSegment which
		// CRC-walks the log and truncates partial writes, then
		// rebuildIndex repopulates the index. Until takeover this
		// broker isn't serving Fetch for the partition anyway, so a
		// briefly-stale HWM has no observable effect.
		//
		// Cold path (manifest absent / zero HWM, e.g. legacy data, or
		// the very first open after a fresh chart install): fall back
		// to the full log scan. That path is bounded by SegmentBytes
		// and rare — manifests are written eagerly.
		if manifest != nil {
			active, err = openActiveSegment(last)
			if err != nil {
				return fmt.Errorf("open active segment base=%d: %w", last.baseOffset, err)
			}
			hwm = manifest.HighWatermark
			if hwm > last.baseOffset {
				active.lastOffset = hwm - 1
			}
		} else {
			active, hwm, err = openActiveSegmentFromDisk(last)
			if err != nil {
				return fmt.Errorf("open active segment base=%d: %w", last.baseOffset, err)
			}
		}
		// gh #81: no index rebuild on startup. Existing entries point
		// at valid batch boundaries; missing tail entries leave the
		// index sparse but correct. takeoverInternal calls rebuildIndex
		// when this broker is about to become leader, and it's only at
		// that point we need a complete index for Fetch seeks.
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
			highWater: hwm,
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
	// gh #134: seed the atomic mirrors with the values we just settled
	// on. Subsequent reads via HighWatermark()/LogStartOffset() will
	// pick them up lock-free.
	ps.highWaterAtomic.Store(ps.highWater)
	ps.logStartAtomic.Store(ps.logStart)

	// Stage B2 of gh #12: restore the idempotent-producer dedupe
	// window. Missing or unreadable snapshot is non-fatal — fall
	// through to fresh state, which is the stage-B1 behaviour.
	// snapshot file → producerStates: the next idempotent batch
	// dedupes correctly across this restart.
	if states, err := readProducerSnapshot(dir); err == nil && states != nil {
		ps.producerStates = states
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

	// Per-topic config from /data/<topic>/.config.json (operator-written).
	// Currently only retentionBytes is wired through to the cleaner; other
	// fields are accepted but ignored (gh #47 follow-ups).
	topicDir := filepath.Join(e.dataDir, topic)
	if cfg, err := ReadTopicConfig(topicDir); err == nil && cfg != nil {
		if cfg.RetentionBytes != nil {
			ps.retentionBytesOverride = *cfg.RetentionBytes
		}
	}

	// Group-commit committer goroutine. Spawned exactly once per
	// partitionState before it's exposed to Append callers (gh #82).
	// FsyncMaxLatency snapshot stamps the watchdog deadline (gh #95);
	// per-partition rather than per-engine so the committer hot path
	// doesn't have to chase the engine pointer.
	ps.fsyncMaxLatency = e.cfg.FsyncMaxLatency
	ps.startCommitter()

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
		ps.stopCommitter()
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
func (e *DiskStorageEngine) Append(ctx context.Context, topic string, partition int32, epoch uint32, rawBatch []byte) (int64, error) {
	if len(rawBatch) == 0 {
		hwm, _ := e.HighWatermark(topic, partition)
		return hwm, nil
	}

	// Ownership check is the caller's responsibility (ProduceHandler
	// gates on coord.Owns + heartbeat freshness in the v3 path; the
	// v2.6 single-broker path uses LocalLeaseManager which always says
	// yes). The engine's truth is "is the partition open?" — i.e., did
	// TakeOver run for it. Without that, getPartition fails and we
	// surface "unknown partition" rather than re-checking the legacy
	// per-partition Lease (which gh #75 stopped acquiring).
	ps, ok := e.getPartition(topic, partition)
	if !ok {
		return -1, fmt.Errorf("%w: %s/%d", ErrUnknownPartition, topic, partition)
	}

	// gh #132 item 1: parse the batch BEFORE taking ps.mu. These are
	// pure functions over rawBatch bytes — no partition state involved
	// — so there's no reason to hold the lock through them. On the
	// produce hot path this shaves a few µs off every lock acquisition
	// (parseBatchProducerInfo walks the batch header for the PID/epoch/
	// sequence fields, parseBatchOffsets is a fixed-offset read). The
	// big lock-duration win (moving WriteAt out) is parked as a
	// follow-up because it needs sequence-numbered durability tracking
	// to interact correctly with the group-commit fsync barrier.
	prodInfo, err := parseBatchProducerInfo(rawBatch)
	if err != nil {
		return -1, fmt.Errorf("storage: %w", err)
	}

	// gh #132 item 1 v2: phase 1 (under ps.mu) — reserve write position
	// and bookkeeping. Phase 2 (outside ps.mu) — issue pwrite. Phase 3
	// (under ps.mu) — mark complete via the seq watermark + optionally
	// signal the committer. Multiple concurrent Appends to the same
	// partition can pwrite in parallel because pwrite is positional.
	ps.mu.Lock()

	// Epoch fence: caller's epoch must match the partition's stored epoch
	// when both sides are non-zero. Strict equality matches the plan
	// pseudocode in §"Append flow" — a caller running ahead of TakeOver
	// is just as wrong as one running behind.
	if epoch != 0 && ps.epoch != 0 && uint32(ps.epoch) != epoch {
		ps.mu.Unlock()
		return -1, ErrEpochMismatch
	}

	// Don't fail fast on a sticky ps.flushErr here — new Appends after
	// a failed cycle deserve their own attempt (the flushErr clears on
	// the next flush-trigger anyway, matching gh #95's recovery
	// contract). flushErr is only consulted in the flush-wait below
	// and during segment-roll-drain.

	// gh #12 stage B: idempotence check before we touch the log.
	// Runs on the partition mutex so dedupe + offset assignment +
	// state advance are atomic with concurrent Appends. PID == -1
	// (the wire sentinel for non-idempotent producers) skips this
	// branch entirely.
	if ps.producerStates == nil && prodInfo.producerID >= 0 {
		ps.producerStates = make(map[int64]*producerEntry)
	}
	switch action, savedOffset := classifyIdempotence(ps.producerStates, prodInfo); action {
	case idemDuplicate:
		ps.mu.Unlock()
		return savedOffset, nil
	case idemOutOfOrder:
		ps.mu.Unlock()
		return -1, ErrOutOfOrderSequence
	case idemInvalidEpoch:
		ps.mu.Unlock()
		return -1, ErrInvalidProducerEpoch
	case idemAccept, idemNotIdempotent:
		// fall through
	}

	// If reserving this batch would push the active segment past the
	// configured size, roll FIRST. The roll needs to drain inflight
	// pwrites to the old segment before swapping in the new one (so
	// the old segment's fsync inside rollFast sees all the bytes).
	if ps.active.logSize+int64(len(rawBatch)) >= e.cfg.SegmentBytes {
		for ps.completedWriteSeq < ps.nextWriteSeq && !ps.closing && ps.flushErr == nil {
			ps.flushCond.Wait()
		}
		if ps.flushErr != nil {
			err := ps.flushErr
			ps.mu.Unlock()
			return -1, err
		}
		if ps.closing {
			ps.mu.Unlock()
			return -1, ErrPartitionClosing
		}
		if err := e.rollSegment(ctx, ps); err != nil {
			ps.mu.Unlock()
			return -1, err
		}
		ps.pendingFlushRecords = 0
	}

	// Brokers own offsets. Producers ship baseOffset=0 (the wire convention);
	// rewrite to the partition's high watermark before persisting so reads
	// see strictly-increasing offsets across batches. CRC32C covers
	// attrs..records (body[9:]), not baseOffset, so the 8-byte overwrite is safe.
	//
	// Reservation uses reservedHighWater (advances under the lock at
	// reservation time). The VISIBLE highWater — which Read consults —
	// only advances in phase 3 after pwrite returns, so a consumer at
	// highWater never tries to read bytes that haven't landed in the
	// page cache yet. First Append on a freshly-opened partition picks
	// up the recovered highWater into reservedHighWater.
	if ps.reservedHighWater < ps.highWater {
		ps.reservedHighWater = ps.highWater
	}
	baseOffset := ps.reservedHighWater
	binary.BigEndian.PutUint64(rawBatch[0:8], uint64(baseOffset))

	_, lastOffsetDelta, err := parseBatchOffsets(rawBatch)
	if err != nil {
		ps.mu.Unlock()
		return -1, fmt.Errorf("storage: %w", err)
	}

	endOffset := baseOffset + int64(lastOffsetDelta) + 1
	ps.reservedHighWater = endOffset

	seg := ps.active
	writePos := seg.logSize
	seg.logSize += int64(len(rawBatch))
	seg.lastOffset = baseOffset + int64(lastOffsetDelta)
	// Incremental maxTimestamp track (gh #132 part 1) — read the
	// batch's maxTimestamp from rawBatch[35:43] directly so we don't
	// need to call segmentMaxTimestamp on segment roll.
	if len(rawBatch) >= 43 {
		if ts := int64(binary.BigEndian.Uint64(rawBatch[35:43])); ts > seg.maxTimestamp {
			seg.maxTimestamp = ts
		}
	}

	// Index entry decision is made (and the entry write happens) under
	// the lock — sparse (every IndexIntervalBytes), tiny (8 B), so it
	// doesn't extend the critical section meaningfully.
	var indexEntry [8]byte
	var indexNeeded bool
	if seg.logSize-seg.lastIndexedLogPos >= e.cfg.IndexIntervalBytes {
		relOffset := int32(baseOffset - seg.baseOffset)
		binary.BigEndian.PutUint32(indexEntry[0:4], uint32(relOffset))
		binary.BigEndian.PutUint32(indexEntry[4:8], uint32(writePos))
		seg.lastIndexedLogPos = seg.logSize
		indexNeeded = true
	}

	if prodInfo.producerID >= 0 {
		recordIdempotenceOutcome(ps.producerStates, prodInfo, baseOffset)
	}
	ps.pendingFlushRecords += int64(lastOffsetDelta) + 1

	// Reserve a write seq for the durability barrier and stash the
	// end-offset of this batch so phase 3 can advance the visible
	// highWater contiguously once the pwrite returns.
	ps.nextWriteSeq++
	myWriteSeq := ps.nextWriteSeq
	if ps.pendingEndOffsets == nil {
		ps.pendingEndOffsets = make(map[uint64]int64)
	}
	ps.pendingEndOffsets[myWriteSeq] = endOffset

	// Snapshot the file handles so a concurrent Relinquish (which
	// sets active.logFile = nil) can't race us into a nil deref.
	logFile := seg.logFile
	indexFile := seg.indexFile

	// Decide flush trigger under the lock — captures the barrier.
	var triggeredFlushSeq int64
	if e.cfg.FlushIntervalMessages > 0 && ps.pendingFlushRecords >= e.cfg.FlushIntervalMessages {
		// Clear any stale error from a previous cycle (gh #95). flushErr
		// is "sticky" so concurrent appenders that piggybacked onto a
		// failed cycle all see the same error — but a NEW request that
		// arrives after the failed cycle completed should not fail
		// immediately on a leftover sentinel; it deserves its own
		// fresh attempt.
		ps.flushErr = nil
		ps.requestedFlushSeq++
		triggeredFlushSeq = ps.requestedFlushSeq
		ps.pendingFlushRecords = 0
		// Record the writeSeq barrier this flush has to wait for.
		// Anything reserved up to nextWriteSeq must be durable in the
		// page cache before the fsync runs.
		if ps.flushBarriers == nil {
			ps.flushBarriers = make(map[int64]uint64)
		}
		ps.flushBarriers[triggeredFlushSeq] = ps.nextWriteSeq
	}
	ps.mu.Unlock()

	// Phase 2: outside the lock. Issue the actual pwrite. Multiple
	// concurrent Appends on the same partition reach this point with
	// different writePos values — pwrite is positional, the kernel
	// serialises the page-cache writes internally, and there's no
	// userspace contention.
	var writeErr error
	if logFile != nil {
		if _, err := logFile.WriteAt(rawBatch, writePos); err != nil {
			writeErr = err
		}
	} else {
		writeErr = fmt.Errorf("storage: log file closed during append")
	}
	if writeErr == nil && indexNeeded && indexFile != nil {
		// Index file writes are small + sparse; let the kernel order
		// them. Failures here are non-fatal (the index is rebuildable
		// from the log on takeover) but still mark the partition's
		// flushErr so the next caller can decide what to do.
		if _, err := indexFile.Write(indexEntry[:]); err != nil {
			writeErr = err
		}
	}

	// Phase 3: re-acquire lock briefly to mark our pwrite complete and
	// notify the committer + any in-progress flush waiters.
	ps.mu.Lock()
	if writeErr != nil {
		if ps.flushErr == nil {
			ps.flushErr = writeErr
		}
		// Still mark our seq complete so the watermark advances and
		// the committer / segment-roll waiters don't deadlock on us.
		// Drop the pendingEndOffset entry so it doesn't pollute the
		// visible highWater advance.
		delete(ps.pendingEndOffsets, myWriteSeq)
		ps.markWriteComplete(myWriteSeq)
		ps.flushCond.Broadcast()
		ps.mu.Unlock()
		return -1, writeErr
	}
	if newHW := ps.markWriteComplete(myWriteSeq); newHW > ps.highWater {
		ps.highWater = newHW
		ps.highWaterAtomic.Store(newHW) // gh #134: mirror for lock-free reads
	}
	ps.flushCond.Broadcast()

	if triggeredFlushSeq > 0 {
		// We triggered a flush. Signal the committer (non-blocking;
		// committer's next iteration picks up requestedFlushSeq) and
		// wait for the fsync to cover our seq.
		select {
		case ps.flushReqCh <- struct{}{}:
		default:
		}
		for ps.completedFlushSeq < triggeredFlushSeq && !ps.closing && ps.flushErr == nil {
			ps.flushCond.Wait()
		}
		if ps.flushErr != nil {
			err := ps.flushErr
			ps.mu.Unlock()
			return -1, err
		}
		if ps.closing && ps.completedFlushSeq < triggeredFlushSeq {
			ps.mu.Unlock()
			return -1, ErrPartitionClosing
		}
	}
	ps.mu.Unlock()

	return baseOffset, nil
}

// rollSegment closes the active segment and opens a new one.
// Must be called with ps.mu held.
//
// rollSegment splits the work into a fast critical-path (under ps.mu)
// and an async finalize step (gh #82). The lock is held only for:
//   - log fsync of the old segment (so the trigger-batch is durable)
//   - createSegment for the new active
//   - the in-memory swap (segments/active pointer)
//
// The deferred work runs in a goroutine after ps.mu is released:
//   - fsync the old index (best-effort; missing tail entries are fine
//     because Fetch falls back to a linear scan — #81)
//   - close the old segment's file handles
//   - persist the manifest with the new logStartOffset
//
// On NFS the previous synchronous version held ps.mu for 5-30s during
// segment roll; this brings it down to ~one fsync + two file CREATEs.
func (e *DiskStorageEngine) rollSegment(ctx context.Context, ps *partitionState) error {
	ctx, span := observability.Tracer().Start(ctx, "storage.segment_roll",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String("partition_dir", ps.dir),
			attribute.Int64("epoch", ps.epoch),
			attribute.Int64("highwater", ps.highWater),
		),
	)
	defer span.End()

	closed := segmentMeta{
		baseOffset: ps.active.baseOffset,
		logPath:    ps.active.logPath,
		indexPath:  ps.active.indexPath,
		// gh #132: use the running maxTimestamp from activeSegment instead
		// of re-scanning the closed log. The scan held ps.mu for seconds
		// on a 1 GiB segment and was the dominant p99 spike on the
		// matched-substrate Strimzi bench. The cleaner still falls back
		// to segmentMaxTimestamp on segments restored from disk (where
		// the running value isn't available).
		maxTimestamp: ps.active.maxTimestamp,
	}

	oldActive := ps.active
	newSeg, err := oldActive.rollFast(ps.dir, ps.highWater, ps.epoch)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "rollFast")
		return err
	}
	ps.segments = append(ps.segments, closed)
	ps.active = newSeg
	span.SetAttributes(
		attribute.Int64("closed.base_offset", closed.baseOffset),
		attribute.Int64("active.base_offset", newSeg.baseOffset),
	)

	// Async finalize: index fsync, close old, manifest checkpoint.
	// Detached goroutine outlives the parent ctx, so finalize starts
	// its own root span (linked to the parent so traces correlate)
	// rather than chaining a child off a span that's about to End().
	parentLink := trace.LinkFromContext(ctx)
	go func(seg *activeSegment, dir string, link trace.Link) {
		_, fSpan := observability.Tracer().Start(context.Background(),
			"storage.segment_finalize",
			trace.WithSpanKind(trace.SpanKindInternal),
			trace.WithLinks(link),
			trace.WithAttributes(
				attribute.String("partition_dir", dir),
			),
		)
		defer fSpan.End()
		if err := seg.finalizeAfterRoll(); err != nil {
			fSpan.RecordError(err)
			fSpan.SetStatus(codes.Error, "finalizeAfterRoll")
		}
		ps.mu.Lock()
		_ = ps.persistManifestLocked()
		ps.mu.Unlock()
	}(oldActive, ps.dir, parentLink)

	return nil
}

// Read returns a concatenation of raw RecordBatch bytes starting at or after startOffset,
// spanning segment boundaries up to maxBytes.
func (e *DiskStorageEngine) Read(_ context.Context, topic string, partition int32, startOffset int64, maxBytes int) ([]byte, error) {
	ps, ok := e.getPartition(topic, partition)
	if !ok {
		return nil, fmt.Errorf("%w: %s/%d", ErrUnknownPartition, topic, partition)
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

// DeleteRecords advances the partition's log start offset to (at least)
// targetOffset. -1 is a sentinel for "current high watermark" (purge
// everything currently visible). Closed segments whose entire range
// falls below the new logStart are removed from disk synchronously;
// the active segment is left alone (Read filters by logStart).
//
// Returns the new low-watermark (== logStart) or ErrOffsetOutOfRange
// when targetOffset is past HWM. Targets at or below the current
// logStart are a no-op (the LowWatermark in the response is just the
// current logStart, no error).
func (e *DiskStorageEngine) DeleteRecords(topic string, partition int32, targetOffset int64) (int64, error) {
	ps, ok := e.getPartition(topic, partition)
	if !ok {
		return 0, fmt.Errorf("storage: unknown partition %s/%d", topic, partition)
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()

	target := targetOffset
	if target == -1 {
		target = ps.highWater
	}
	if target > ps.highWater {
		return ps.logStart, ErrOffsetOutOfRange
	}
	// target <= logStart means the caller is asking to truncate to a
	// point we've already truncated past. Don't move logStart, but we
	// still fall through to the reclamation check below: a previous
	// purge may have stranded the active segment on disk if it ran
	// against a broker that didn't have the active-segment-reclaim
	// path (pre-v0.1.37). A second "Purge messages" click then
	// finishes the job. Idempotent — when there's nothing stranded,
	// the reclamation block is a no-op.
	if target > ps.logStart {
		ps.logStart = target
		ps.logStartAtomic.Store(ps.logStart) // gh #134: mirror for lock-free reads
	}

	// Drop closed segments entirely below the new logStart. A segment
	// is "entirely below" when the next segment's baseOffset (or, for
	// the last closed one, the active segment's baseOffset) is <=
	// logStart. We can't use ps.deleteSegment here because it
	// overwrites ps.logStart with the next segment's baseOffset —
	// fine for the cleaner's natural-retention path, wrong for
	// DeleteRecords where logStart can land mid-segment.
	for len(ps.segments) > 0 {
		first := ps.segments[0]
		var nextBase int64
		if len(ps.segments) > 1 {
			nextBase = ps.segments[1].baseOffset
		} else {
			nextBase = ps.active.baseOffset
		}
		if nextBase > ps.logStart {
			break
		}
		_ = os.Remove(first.logPath)
		_ = os.Remove(first.indexPath)
		_ = os.Remove(strings.TrimSuffix(first.logPath, ".log") + ".timeindex")
		ps.segments = ps.segments[1:]
	}

	// Reclaim the active segment too when the entire partition has
	// been purged (logStart caught up to HWM). Otherwise a "purge all"
	// from Kafbat leaves up to SegmentBytes of physical bytes on disk
	// per partition — invisible to consumers but counting against the
	// PVC. We force-roll the active synchronously, finalise it, and
	// drop the now-orphaned files. Skipped if the active is empty
	// (no bytes to reclaim) or if there are still visible records in
	// the active (partial purge).
	if ps.active != nil && ps.active.logSize > 0 && ps.logStart >= ps.highWater {
		oldLogPath := ps.active.logPath
		oldIndexPath := ps.active.indexPath
		oldActive := ps.active
		newSeg, err := oldActive.rollFast(ps.dir, ps.highWater, ps.epoch)
		if err == nil {
			ps.active = newSeg
			// finalize synchronously so file handles are closed
			// before we unlink (NFS silly-rename otherwise leaves
			// .nfsXXXX files dangling).
			_ = oldActive.finalizeAfterRoll()
			_ = os.Remove(oldLogPath)
			_ = os.Remove(oldIndexPath)
			_ = os.Remove(strings.TrimSuffix(oldLogPath, ".log") + ".timeindex")
		}
	}

	// Persist the manifest so a restart picks up the new logStart
	// without rediscovering already-deleted segments.
	if err := ps.persistManifestLocked(); err != nil {
		return ps.logStart, fmt.Errorf("persist manifest: %w", err)
	}
	return ps.logStart, nil
}

func (e *DiskStorageEngine) HighWatermark(topic string, partition int32) (int64, error) {
	ps, ok := e.getPartition(topic, partition)
	if !ok {
		return 0, fmt.Errorf("%w: %s/%d", ErrUnknownPartition, topic, partition)
	}
	// gh #134: lock-free read via atomic mirror. Pre-gh #134 this took
	// ps.mu, which blocked the OTel gauge callback whenever an Append
	// was stuck on a stalled NAS fsync — propagating the storage stall
	// up into the metrics pipeline and silencing ALL skafka metrics.
	return ps.highWaterAtomic.Load(), nil
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
		return 0, fmt.Errorf("%w: %s/%d", ErrUnknownPartition, topic, partition)
	}
	// gh #134: lock-free read via atomic mirror. Same motivation as
	// HighWatermark — the fetch hot path and gauge callbacks both
	// call this; neither should block on the storage mutex.
	return ps.logStartAtomic.Load(), nil
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
	_, err := e.takeoverInternal(context.Background(), topic, partition, newEpoch)
	return err
}

// takeoverInternal is the shared implementation behind TakeoverPartition (v2.6
// callers) and TakeOver (v3 callers). Returns the recovered high watermark.
func (e *DiskStorageEngine) takeoverInternal(ctx context.Context, topic string, partition int32, newEpoch int64) (int64, error) {
	ctx, span := observability.Tracer().Start(ctx, "storage.takeover",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String("topic", topic),
			attribute.Int("partition", int(partition)),
			attribute.Int64("epoch.new", newEpoch),
		),
	)
	defer span.End()
	_ = ctx

	ps, ok := e.getPartition(topic, partition)
	if !ok {
		err := fmt.Errorf("storage: unknown partition %s/%d", topic, partition)
		span.RecordError(err)
		span.SetStatus(codes.Error, "unknown partition")
		return 0, err
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()

	// Open file handles if this is a follower → leader transition.
	// openPartition leaves handles closed so non-leaders don't hold
	// fds that block the leader's segment-roll/DeleteRecords-driven
	// os.Remove on NFS (gh #76 follow-up). Idempotent if already open.
	if ps.active != nil {
		if err := ps.active.openHandles(); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "openHandles")
			return 0, fmt.Errorf("storage: open handles: %w", err)
		}
	}

	diskEpoch := ps.epoch
	if m, err := readManifest(ps.dir); err == nil {
		diskEpoch = m.Epoch
	} else if !errors.Is(err, fs.ErrNotExist) {
		span.RecordError(err)
		span.SetStatus(codes.Error, "readManifest")
		return 0, fmt.Errorf("storage: read manifest: %w", err)
	}
	span.SetAttributes(attribute.Int64("epoch.disk", diskEpoch))

	if diskEpoch < newEpoch {
		hwm, err := recoverSegment(ps.active)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "recoverSegment")
			return 0, fmt.Errorf("storage: recover segment: %w", err)
		}
		if err := rebuildIndex(ps.active, e.cfg.IndexIntervalBytes); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "rebuildIndex")
			return 0, fmt.Errorf("storage: rebuild index: %w", err)
		}
		ps.highWater = hwm
		ps.highWaterAtomic.Store(hwm) // gh #134: mirror for lock-free reads
		span.SetAttributes(attribute.Bool("recovered", true))

		// gh #138: heal the "phantom HWM" state. Symptom: a previous
		// retention-clean / DeleteRecords advanced past every record
		// the partition held, but the manifest persist that should
		// have followed was interrupted (NAS stall, broker SIGKILL).
		// We end up with:
		//   - no closed segments
		//   - active segment file at baseOffset=X but logSize=0
		//   - manifest claims logStartOffset < X, highWatermark = X
		// On disk there are zero records, but Fetch(end_offset) -
		// Fetch(start_offset) suggests millions exist. Heal by
		// advancing logStart to match the empty active's baseOffset
		// — that's the offset the next produce would land at anyway,
		// and it makes HWM - logStart == 0 (the actual record count).
		if len(ps.segments) == 0 && ps.active != nil && ps.active.logSize == 0 && ps.highWater > ps.logStart {
			slog.Warn("storage: takeover detected phantom-HWM state (active segment empty, manifest logStart < HWM with no closed segments) — healing by advancing logStart to HWM",
				"topic", topic,
				"partition", partition,
				"prev_logStart", ps.logStart,
				"hwm", ps.highWater,
				"active_baseOffset", ps.active.baseOffset)
			ps.logStart = ps.highWater
			ps.logStartAtomic.Store(ps.logStart)
			span.SetAttributes(attribute.Bool("phantom_hwm_healed", true))
		}
	}

	ps.epoch = newEpoch
	if err := ps.persistManifestLocked(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "persistManifest")
		return 0, fmt.Errorf("storage: write manifest: %w", err)
	}
	span.SetAttributes(attribute.Int64("highwater", ps.highWater))
	return ps.highWater, nil
}

// TakeOver implements the v3 StorageEngine contract. It claims write ownership
// of the partition under the given epoch, recovers any partial writes, and
// returns the recovered high watermark.
func (e *DiskStorageEngine) TakeOver(ctx context.Context, topic string, partition int32, epoch uint32) (int64, error) {
	return e.takeoverInternal(ctx, topic, partition, int64(epoch))
}

// Relinquish implements the v3 StorageEngine contract. The partition
// becomes read-only on this broker — and, more importantly, the broker
// drops its file descriptors on the active segment so that when the
// new leader rolls or DeleteRecords-unlinks the segment file, NFS can
// actually free the bytes (instead of silly-renaming the file because
// followers still hold it open). The BrokerCoordinator.Owns check on
// the produce hot path remains the authoritative leadership gate.
func (e *DiskStorageEngine) Relinquish(topic string, partition int32) error {
	ps, ok := e.getPartition(topic, partition)
	if !ok {
		return nil
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if ps.active == nil {
		return nil
	}
	// Stage B2 of gh #12: persist the producer-state snapshot on
	// graceful handoff so the next leader picks up the dedupe
	// window. Best-effort; segment-roll snapshots and the open-time
	// write also cover this surface.
	_ = ps.persistManifestLocked()
	return ps.active.closeHandles()
}

// ClosePartition closes the partition's open log + index file handles
// and forgets it from the in-memory map. Used by the broker on
// KafkaTopic deletion: NFS silly-renames open files into .nfsXXXX
// entries that EBUSY the operator's unlinkat on the parent dir
// (gh #76). Closing the handles before the operator's finalizer
// reconciles lets NFS drop the silly-renames so the directory can be
// removed cleanly.
//
// Idempotent — re-calling on an already-closed partition is a no-op.
// Errors from the underlying segment close are surfaced; the broker
// logs them but doesn't retry, since the operator's finalizer will
// retry the unlink on its own reconcile cadence.
func (e *DiskStorageEngine) ClosePartition(topic string, partition int32) error {
	// Phase 1 (gh #119): detach from the in-memory map. This is the
	// instant, hot-path-friendly part. After this returns, Produce/
	// Fetch see UNKNOWN_TOPIC_OR_PARTITION immediately.
	e.mu.Lock()
	key := e.partKey(topic, partition)
	ps, ok := e.partitions[key]
	if !ok {
		e.mu.Unlock()
		return nil
	}
	delete(e.partitions, key)
	e.mu.Unlock()

	partDir := filepath.Join(e.dataDir, topic, fmt.Sprintf("%d", partition))

	// Phase 2 (gh #119): if a reaper is wired, hand off the slow
	// work (stop-committer + close-handles + os.RemoveAll) to a
	// rate-limited background goroutine. Otherwise fall back to the
	// pre-#119 synchronous path so tests and dev mode keep their
	// existing semantics.
	if e.reaper != nil {
		slog.Info("storage: partition close requested (handing off to reaper)",
			"topic", topic, "partition", partition, "dir", partDir)
		return e.reaper.Enqueue(topic, partition, ps, partDir)
	}

	// Synchronous fallback (no reaper wired).
	ps.stopCommitter()
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if ps.active == nil {
		return nil
	}
	err := ps.active.close()
	ps.active = nil
	return err
}

// WithReaper attaches the background partition reaper. The caller is
// responsible for starting the reaper's Run goroutine and Stop'ing
// it on shutdown. Wired by `broker.New` in production; tests can
// skip this to keep ClosePartition synchronous. gh #119.
func (e *DiskStorageEngine) WithReaper(r *PartitionReaper) *DiskStorageEngine {
	e.reaper = r
	return e
}

// Reaper returns the attached reaper (or nil). Used by /readyz to
// peek queue depth for the gh #118 backpressure surface.
func (e *DiskStorageEngine) Reaper() *PartitionReaper { return e.reaper }

// AllPartitions returns all known partitions — used by the retention cleaner.
// FenceProducerEpoch advances every partition's view of (PID, epoch)
// to AT LEAST `epoch` for the given producerID, and clears the
// per-PID dedupe window. After this returns, any subsequent
// Append carrying batch.epoch < epoch returns ErrInvalidProducerEpoch
// regardless of which partition it targets.
//
// gh #30: closes the cross-partition gap left by gh #12 stage B.
// B's classifyIdempotence advances the partition's per-PID epoch
// only when a new-epoch batch lands on THAT partition. A producer
// that bumps its epoch via InitProducerId (gh #22) but hasn't yet
// written to all its partitions leaves a window where in-flight
// zombie batches on those partitions are wrongly accepted.
// Calling this from the InitProducerId rejoin path closes the
// window proactively.
//
// In-memory only — the next persistManifestLocked (segment roll
// or Relinquish) flushes the bumped epoch to producer-state.snapshot.
// A broker restart between the bump and the next persist accepts
// a zombie batch under the OLD epoch (it sees the persisted
// snapshot's old state). The window is narrow in practice; tighter
// closure would require synchronously snapshotting every partition
// per InitProducerId call, which costs O(partitions) writes.
func (e *DiskStorageEngine) FenceProducerEpoch(pid int64, epoch int16) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, ps := range e.partitions {
		ps.mu.Lock()
		entry, ok := ps.producerStates[pid]
		if ok && entry.epoch < epoch {
			entry.epoch = epoch
			entry.recent = nil
		}
		ps.mu.Unlock()
	}
}

// AnyStalled reports whether at least one partition's most recent
// committer fsync exceeded FsyncMaxLatency (gh #95). Healthz aggregates
// this into a cluster-level signal so operators see "storage backend
// hung" before queued appenders accumulate to the point that the broker
// looks externally idle. Cleared per-partition by the next clean fsync.
func (e *DiskStorageEngine) AnyStalled() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, ps := range e.partitions {
		ps.mu.Lock()
		stalled := ps.stalled
		ps.mu.Unlock()
		if stalled {
			return true
		}
	}
	return false
}

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
	ps.logStartAtomic.Store(ps.logStart) // gh #134: mirror for lock-free reads
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

