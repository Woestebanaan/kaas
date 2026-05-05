package storage

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/woestebanaan/skafka/internal/lease"
)

// RetentionCleaner deletes log segments that exceed the configured retention period.
// Only the partition leader runs the cleaner for each partition.
type RetentionCleaner struct {
	engine  *DiskStorageEngine
	leases  lease.LeaseManager
	interval time.Duration
}

// NewRetentionCleaner creates a cleaner that runs every interval (default 5 minutes).
func NewRetentionCleaner(engine *DiskStorageEngine, leases lease.LeaseManager, interval time.Duration) *RetentionCleaner {
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	return &RetentionCleaner{engine: engine, leases: leases, interval: interval}
}

// Run starts the retention loop, blocking until ctx is cancelled.
func (c *RetentionCleaner) Run(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.runOnce()
		case <-ctx.Done():
			return
		}
	}
}

func (c *RetentionCleaner) runOnce() {
	for _, p := range c.engine.AllPartitions() {
		if !c.leases.IsLeader(p.Topic, p.Partition) {
			continue
		}
		c.cleanPartition(p)
	}
}

func (c *RetentionCleaner) cleanPartition(p PartitionID) {
	ps, ok := c.engine.getPartition(p.Topic, p.Partition)
	if !ok {
		return
	}

	cutoffMs := time.Now().UnixMilli() - c.engine.cfg.RetentionMs

	ps.mu.Lock()
	defer ps.mu.Unlock()

	// --- time-based retention ---
	deletedByTime := 0
	for len(ps.segments) > 0 {
		seg := ps.segments[0]

		// Load maxTimestamp from disk if not cached.
		if seg.maxTimestamp == 0 {
			ts, err := segmentMaxTimestamp(seg.logPath)
			if err == nil {
				ps.segments[0].maxTimestamp = ts
				seg.maxTimestamp = ts
			}
		}

		if seg.maxTimestamp == 0 || seg.maxTimestamp >= cutoffMs {
			break
		}

		slog.Info("retention cleaner: deleting segment (time)",
			"topic", p.Topic, "partition", p.Partition,
			"baseOffset", seg.baseOffset,
			"maxTimestamp", seg.maxTimestamp)
		ps.deleteSegment(0)
		deletedByTime++
	}

	// --- size-based retention (gh #47) ---
	// Active segment is intentionally never deleted; only closed segments.
	// Per-partition override (loaded from /data/<topic>/.config.json) wins
	// over the engine-wide RetentionBytes default.
	limit := ps.retentionBytesOverride
	if limit == 0 {
		limit = c.engine.cfg.RetentionBytes
	}
	deletedBySize := 0
	if limit > 0 {
		total := totalClosedSize(ps.segments)
		// Don't delete every closed segment to satisfy a tight limit —
		// keep at least the most recent closed one so reads near HWM
		// don't fall off a cliff.
		for total > limit && len(ps.segments) > 1 {
			seg := ps.segments[0]
			sz := segmentSize(seg)
			slog.Info("retention cleaner: deleting segment (size)",
				"topic", p.Topic, "partition", p.Partition,
				"baseOffset", seg.baseOffset,
				"segmentSize", sz,
				"totalBefore", total, "limit", limit)
			total -= sz
			ps.deleteSegment(0)
			deletedBySize++
		}
	}

	if deletedByTime > 0 || deletedBySize > 0 {
		slog.Info("retention cleaner: cleaned partition",
			"topic", p.Topic, "partition", p.Partition,
			"deletedByTime", deletedByTime, "deletedBySize", deletedBySize)
		// Persist the new logStartOffset so a broker restart picks
		// up the cleaner's progress instead of "rediscovering" the
		// already-deleted segments via a directory listing. The
		// hot-path Produce no longer writes the manifest (gh #80),
		// so this is the only path that captures cleaner-driven
		// logStart advances. Best-effort — a write failure just
		// means the next clean cycle re-deletes (idempotent).
		if err := ps.persistManifestLocked(); err != nil {
			slog.Warn("retention cleaner: persist manifest failed",
				"topic", p.Topic, "partition", p.Partition, "err", err)
		}
	}
}

// segmentSize returns the closed segment's on-disk log file size.
// Falls back to 0 on a stat failure — the cleaner treats missing-stat
// as "I can't bound this" and just doesn't delete it.
func segmentSize(seg segmentMeta) int64 {
	if fi, err := os.Stat(seg.logPath); err == nil {
		return fi.Size()
	}
	return 0
}

func totalClosedSize(segs []segmentMeta) int64 {
	var total int64
	for _, s := range segs {
		total += segmentSize(s)
	}
	return total
}
