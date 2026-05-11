package storage

import (
	"context"
	"log/slog"
	"os"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/woestebanaan/skafka/internal/lease"
	"github.com/woestebanaan/skafka/internal/observability"
)

// CleanupPolicySource is the gh #48 hook the cleaner uses to learn
// each topic's cleanup.policy. Production wires
// *broker.TopicRegistry through a thin adapter — keeping the
// dependency at this small interface avoids importing
// internal/broker from internal/storage (which would form a
// cycle: broker imports storage already).
//
// Empty string means "default", which the cleaner treats as
// cleanup.policy=delete (retention-only). Unknown strings also
// fall through to the default — fail-safe: never silently start
// compacting based on a misspelled policy.
type CleanupPolicySource interface {
	CleanupPolicy(topic string) string
}

// RetentionCleaner runs both retention (delete-by-age and
// delete-by-size, gh #47) and log compaction (gh #48). Which path
// each partition takes is decided by its cleanup.policy:
//
//   ""              → retention only (the default)
//   "delete"        → retention only
//   "compact"       → compaction only
//   "compact,delete" → both passes (Apache supports this combo;
//                      Streams uses it for changelog topics
//                      under EOS)
//
// Only the partition leader runs the cleaner for each partition;
// followers (in skafka's RF=1 model, that's the broker that lost
// the assignment) skip via the lease check.
type RetentionCleaner struct {
	engine    *DiskStorageEngine
	leases    lease.LeaseManager
	interval  time.Duration
	policySrc CleanupPolicySource
}

// NewRetentionCleaner creates a cleaner that runs every interval (default 5 minutes).
func NewRetentionCleaner(engine *DiskStorageEngine, leases lease.LeaseManager, interval time.Duration) *RetentionCleaner {
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	return &RetentionCleaner{engine: engine, leases: leases, interval: interval}
}

// WithPolicySource wires the gh #48 cleanup.policy lookup. Without
// it, every partition is treated as cleanup.policy=delete (the
// pre-#48 behaviour) — that's the safe default for tests and
// dev mode.
func (c *RetentionCleaner) WithPolicySource(src CleanupPolicySource) *RetentionCleaner {
	c.policySrc = src
	return c
}

// policyIsCompact / policyIsDelete: tiny dispatch helpers. Live in
// storage rather than re-importing broker.CleanupPolicy because
// the dependency direction is broker → storage.
func policyIsCompact(p string) bool { return p == "compact" || p == "compact,delete" }
func policyIsDelete(p string) bool {
	return p == "" || p == "delete" || p == "compact,delete"
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

	policy := ""
	if c.policySrc != nil {
		policy = c.policySrc.CleanupPolicy(p.Topic)
	}

	mx := observability.Global()
	cleanStart := time.Now()
	cleanResult := "ok"
	defer func() {
		mx.CleanerDuration.Record(context.Background(), time.Since(cleanStart).Seconds(),
			metric.WithAttributes(attribute.String("topic", p.Topic)))
		mx.CleanerRuns.Add(context.Background(), 1, metric.WithAttributes(attribute.String("result", cleanResult)))
	}()

	// gh #48: if the topic's policy involves compaction, run it
	// first. Compaction operates on closed segments and produces
	// a single replacement segment — the retention pass that
	// follows still applies its time/size rules to whatever
	// remains. compactPartition takes ps.mu briefly at the head
	// and tail; we don't hold it during compaction's I/O.
	if policyIsCompact(policy) {
		if _, _, cerr := c.engine.compactPartition(ps); cerr != nil {
			slog.Warn("compactor: partition pass failed (retention pass will still run; compactor retries on next cycle)",
				"topic", p.Topic, "partition", p.Partition, "err", cerr)
			cleanResult = "error"
		}
	}

	if !policyIsDelete(policy) {
		// Pure compaction (no retention) — done.
		return
	}

	cutoffMs := time.Now().UnixMilli() - c.engine.cfg.RetentionMs

	ps.mu.Lock()
	defer ps.mu.Unlock()

	// --- time-based retention ---
	deletedByTime := 0
	bytesByTime := int64(0)
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

		sz := segmentSize(seg)
		slog.Info("retention cleaner: deleting segment (time)",
			"topic", p.Topic, "partition", p.Partition,
			"baseOffset", seg.baseOffset,
			"maxTimestamp", seg.maxTimestamp,
			"sizeBytes", sz)
		ps.deleteSegment(0)
		deletedByTime++
		bytesByTime += sz
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
	bytesBySize := int64(0)
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
			bytesBySize += sz
		}
	}

	if deletedByTime > 0 {
		mx.CleanerSegmentsDeleted.Add(context.Background(), int64(deletedByTime),
			metric.WithAttributes(attribute.String("reason", "time")))
		mx.CleanerBytesReclaimed.Add(context.Background(), bytesByTime,
			metric.WithAttributes(attribute.String("reason", "time")))
	}
	if deletedBySize > 0 {
		mx.CleanerSegmentsDeleted.Add(context.Background(), int64(deletedBySize),
			metric.WithAttributes(attribute.String("reason", "size")))
		mx.CleanerBytesReclaimed.Add(context.Background(), bytesBySize,
			metric.WithAttributes(attribute.String("reason", "size")))
	}

	if deletedByTime > 0 || deletedBySize > 0 {
		slog.Info("retention cleaner: cleaned partition",
			"topic", p.Topic, "partition", p.Partition,
			"deletedByTime", deletedByTime, "deletedBySize", deletedBySize,
			"bytesReclaimed", bytesByTime+bytesBySize)
		// Persist the new logStartOffset so a broker restart picks
		// up the cleaner's progress instead of "rediscovering" the
		// already-deleted segments via a directory listing. The
		// hot-path Produce no longer writes the manifest (gh #80),
		// so this is the only path that captures cleaner-driven
		// logStart advances. Best-effort — a write failure just
		// means the next clean cycle re-deletes (idempotent).
		if err := ps.persistManifestLocked(); err != nil {
			slog.Warn("retention cleaner: persisting manifest after deletes failed (cleaner re-runs on next cycle; harmless retry)",
				"topic", p.Topic, "partition", p.Partition,
				"deletedByTime", deletedByTime, "deletedBySize", deletedBySize,
				"err", err)
			cleanResult = "error"
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
