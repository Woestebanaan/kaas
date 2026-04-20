package storage

import (
	"context"
	"log/slog"
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

	deleted := 0
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

		slog.Info("retention cleaner: deleting segment",
			"topic", p.Topic, "partition", p.Partition,
			"baseOffset", seg.baseOffset,
			"maxTimestamp", seg.maxTimestamp)
		ps.deleteSegment(0)
		deleted++
	}

	if deleted > 0 {
		slog.Info("retention cleaner: cleaned partition",
			"topic", p.Topic, "partition", p.Partition, "segmentsDeleted", deleted)
	}
}
