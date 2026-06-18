package storage

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.opentelemetry.io/otel/metric"
	"golang.org/x/time/rate"
)

// PartitionReaper does the slow phase of `ClosePartition` — closing
// the active segment's file handles and `os.RemoveAll`-ing the
// partition directory — on a single background goroutine with a
// token-bucket rate limiter. gh #119.
//
// Two-phase deletion split:
//
//   - `engine.ClosePartition` (phase 1, instant): removes the partition
//     from `engine.partitions` so Produce/Fetch see UNKNOWN_TOPIC_OR_PARTITION
//     immediately, then enqueues the slow work here.
//   - PartitionReaper.Run (phase 2, background): drains the queue at a
//     bounded rate, releasing NFS metadata pressure for Produce traffic.
//
// Self-heal: queue is in-memory only. If the broker crashes mid-reap,
// the startup `controllers.SweepTopics` path (which walks /data/ vs
// the KafkaTopic CR list) re-enqueues any orphan partitions. The
// KafkaTopic CR is the single source of truth.
//
// Safety: before each reap, the reaper rechecks whether the topic's
// CR has come back (someone re-created a topic with the same name
// during the reap window). If so, the reap is aborted — the
// partition's data is left intact. The TopicExists callback is wired
// to the in-memory TopicRegistry which the topic-watcher already
// maintains.
type PartitionReaper struct {
	cfg ReaperConfig

	queue        chan reapJob
	rateLimiter  *rate.Limiter
	topicExists  func(topic string) bool // CR-existence recheck before reap

	// metrics is the optional OTel instrument bundle. Wired from
	// observability bootstrap so the reaper goroutine doesn't import
	// the observability package directly. Nil → metrics disabled (tests).
	metrics *ReaperMetrics

	once sync.Once
	wg   sync.WaitGroup
	stop chan struct{}
}

// ReaperMetrics is the gh #119 instrument bundle the reaper emits.
// Lives here (not in observability) so the reaper goroutine can
// reach it without an import cycle. Wired by
// observability.NewReaperMetrics() in production.
type ReaperMetrics struct {
	Enqueued  metric.Int64Counter   // total partitions enqueued for reap
	Completed metric.Int64Counter   // successful reaps
	Aborted   metric.Int64Counter   // CR-recheck aborts (topic reappeared)
	Retried   metric.Int64Counter   // transient-error retries
	GivenUp   metric.Int64Counter   // exhausted MaxRetries — rely on next startup sweep
	Duration  metric.Float64Histogram // reap-work wall-clock per partition
}

// WithMetrics attaches an OTel instrument bundle. Nil-safe: passing
// nil disables emission (tests).
func (r *PartitionReaper) WithMetrics(m *ReaperMetrics) {
	r.metrics = m
}

// ReaperConfig holds the tunables. Zero values pick sensible defaults.
type ReaperConfig struct {
	// RatePerSec caps how many partitions are reaped per second.
	// Default 5 — completes a 70-partition cascade in ~14 seconds
	// of *background* work while leaving plenty of NFS bandwidth
	// for active Produce traffic. Override via SKAFKA_DELETION_RATE_PER_SEC.
	RatePerSec float64

	// QueueDepth is the buffered-channel capacity. Default 1024.
	// Exceeding it makes Enqueue block briefly; that's acceptable
	// because phase 1 already detached the partition from the
	// request hot path.
	QueueDepth int

	// TopicExists is the CR re-check hook. Defaults to "always false"
	// (reap proceeds unconditionally) when not wired; production
	// passes the in-memory TopicRegistry's Has(topic) method.
	TopicExists func(topic string) bool

	// MaxRetries caps the per-job retry budget on transient I/O
	// errors (NFS hiccups). Default 3. Each retry waits
	// `RetryBackoff * attempt` before the next try.
	MaxRetries int

	// RetryBackoff is the per-attempt sleep multiplier (linear).
	// Default 2s.
	RetryBackoff time.Duration
}

// reapJob is the in-flight unit of work for the reaper. Caller is
// responsible for ensuring `ps` was already removed from the
// engine's partitions map (so Produce/Fetch can't reach it) before
// enqueueing.
type reapJob struct {
	topic     string
	partition int32
	ps        *partitionState
	partDir   string // /data/<topic>/<partition>; reaped on success
	attempts  int    // exponential-backoff retry counter
}

// NewPartitionReaper builds a reaper with the supplied config. Start
// the worker by calling Run with a context.
func NewPartitionReaper(cfg ReaperConfig) *PartitionReaper {
	if cfg.RatePerSec <= 0 {
		cfg.RatePerSec = 5
	}
	if cfg.QueueDepth <= 0 {
		cfg.QueueDepth = 1024
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 3
	}
	if cfg.RetryBackoff <= 0 {
		cfg.RetryBackoff = 2 * time.Second
	}
	if cfg.TopicExists == nil {
		cfg.TopicExists = func(string) bool { return false }
	}
	return &PartitionReaper{
		cfg:         cfg,
		queue:       make(chan reapJob, cfg.QueueDepth),
		rateLimiter: rate.NewLimiter(rate.Limit(cfg.RatePerSec), 1),
		topicExists: cfg.TopicExists,
		stop:        make(chan struct{}),
	}
}

// Enqueue schedules a partition for background reap. Non-blocking
// up to QueueDepth; over capacity it blocks briefly (waiting for
// the worker to drain). Returns nil on success, an error only if
// the reaper has been stopped.
//
// The caller has already removed the partition from
// `engine.partitions` — Enqueue takes ownership of the *partitionState
// pointer for the rest of its lifecycle (close handles, free).
func (r *PartitionReaper) Enqueue(topic string, partition int32, ps *partitionState, partDir string) error {
	select {
	case <-r.stop:
		return fmt.Errorf("reaper stopped; not enqueueing %s/%d", topic, partition)
	default:
	}
	select {
	case r.queue <- reapJob{topic: topic, partition: partition, ps: ps, partDir: partDir}:
		// Observability (gh #119): one INFO line per enqueue.
		// During a mass-delete cascade the log shows the queue
		// growing then draining as the worker processes each entry.
		slog.Info("reaper: enqueued partition for reap",
			"topic", topic, "partition", partition,
			"queue_depth", len(r.queue))
		if r.metrics != nil {
			r.metrics.Enqueued.Add(context.Background(), 1)
		}
		return nil
	case <-r.stop:
		return fmt.Errorf("reaper stopped while enqueueing %s/%d", topic, partition)
	}
}

// Run is the worker loop. Returns when ctx is cancelled or Stop is
// called. Idempotent: calling Stop after a context cancellation is
// safe (and vice versa).
func (r *PartitionReaper) Run(ctx context.Context) {
	r.wg.Add(1)
	defer r.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stop:
			return
		case job := <-r.queue:
			// Block until we have a token. This is what bounds the
			// NFS metadata pressure deletion can generate.
			if err := r.rateLimiter.Wait(ctx); err != nil {
				return // ctx cancelled
			}
			r.reapOne(ctx, job)
		}
	}
}

// Stop signals the worker to exit. Idempotent. Blocks until the
// worker has returned. Outstanding queue entries are dropped — the
// startup SweepTopics path picks them up after the next boot
// (self-heal).
func (r *PartitionReaper) Stop() {
	r.once.Do(func() { close(r.stop) })
	r.wg.Wait()
}

// reapOne processes a single job. Errors are logged and trigger a
// bounded retry; after MaxRetries the job is dropped and recovered
// on the next startup sweep. CR-recheck before any I/O.
func (r *PartitionReaper) reapOne(ctx context.Context, job reapJob) {
	// Safety: did the topic come back during the reap window? A
	// recreate-with-same-name would put a valid KafkaTopic CR in
	// the registry; reaping under that condition would silently
	// delete the new topic's data.
	if r.topicExists(job.topic) {
		slog.Warn("reaper: topic reappeared during reap; aborting",
			"topic", job.topic, "partition", job.partition,
			"queue_depth", len(r.queue))
		if r.metrics != nil {
			r.metrics.Aborted.Add(ctx, 1)
		}
		return
	}

	slog.Info("reaper: starting reap",
		"topic", job.topic, "partition", job.partition,
		"queue_depth", len(r.queue), "attempts", job.attempts)

	start := time.Now()
	err := r.reapWork(job)
	elapsed := time.Since(start)
	if r.metrics != nil {
		r.metrics.Duration.Record(ctx, elapsed.Seconds())
	}

	if err == nil {
		slog.Info("reaper: reap complete",
			"topic", job.topic, "partition", job.partition,
			"duration_ms", elapsed.Milliseconds(),
			"queue_depth", len(r.queue))
		if r.metrics != nil {
			r.metrics.Completed.Add(ctx, 1)
		}
		return
	}

	job.attempts++
	if job.attempts >= r.cfg.MaxRetries {
		slog.Error("reaper: giving up after MaxRetries; relying on next startup SweepTopics",
			"topic", job.topic, "partition", job.partition,
			"attempts", job.attempts, "err", err)
		if r.metrics != nil {
			r.metrics.GivenUp.Add(ctx, 1)
		}
		return
	}
	backoff := r.cfg.RetryBackoff * time.Duration(job.attempts)
	slog.Warn("reaper: partition reap hit a transient error (NFS stalling, peer broker still holding a stale fd, or directory not yet empty); will retry after backoff. After MaxRetries the reaper gives up and the directory persists until next startup sweep",
		"topic", job.topic, "partition", job.partition,
		"attempts", job.attempts, "max_retries", r.cfg.MaxRetries,
		"backoff", backoff, "err", err)
	if r.metrics != nil {
		r.metrics.Retried.Add(ctx, 1)
	}

	// Re-enqueue after backoff via a goroutine so we don't
	// block the worker's main loop.
	go func() {
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return
		}
		_ = r.Enqueue(job.topic, job.partition, job.ps, job.partDir)
	}()
}

// reapWork does the actual close + remove. Returns nil on full
// success (or `os.RemoveAll` on a non-existent dir, which is also
// success).
func (r *PartitionReaper) reapWork(job reapJob) error {
	ps := job.ps
	if ps != nil {
		// Stop the committer goroutine. It runs a final drainAndExit
		// fsync — that's the slow step but it's already running in
		// the committer's own goroutine, not the request hot path.
		ps.stopCommitter()

		// gh #136: drain in-flight rollSegment finalize goroutines so
		// none of them re-create manifest.json AFTER the os.RemoveAll
		// at the bottom of this function. Without this Wait, a
		// recently-rolled segment's finalize can sneak in and write
		// manifest.json into the dir the reaper just tore down.
		ps.rollFinalize.Wait()

		ps.mu.Lock()
		if ps.active != nil {
			if err := ps.active.close(); err != nil {
				ps.mu.Unlock()
				return fmt.Errorf("close active segment: %w", err)
			}
			ps.active = nil
		}
		// Closed segments (ps.segments) are segmentMeta records only —
		// per the gh #76 lazy-open follow-up, only the active segment
		// holds open file descriptors. Nothing to close here.
		ps.mu.Unlock()
	}

	if job.partDir == "" {
		// No partition dir handed in — caller wanted only the
		// handle-close half. Done.
		return nil
	}

	// os.RemoveAll on a non-existent dir returns nil; idempotent
	// against a peer reaper or operator-side cleanup having already
	// removed it.
	if err := os.RemoveAll(job.partDir); err != nil {
		return fmt.Errorf("remove %s: %w", job.partDir, err)
	}

	// Best-effort: if the topic directory is empty, remove it too.
	// Ignore errors — a future partition under the same topic name
	// will recreate it.
	topicDir := filepath.Dir(job.partDir)
	if entries, err := os.ReadDir(topicDir); err == nil && len(entries) == 0 {
		_ = os.Remove(topicDir)
	}

	return nil
}

// QueueDepth reports the current backlog. Used by /readyz (gh #118)
// to decide whether the broker should advertise itself as ready.
func (r *PartitionReaper) QueueDepth() int {
	return len(r.queue)
}

// WithTopicExists wires the CR-existence recheck hook AFTER the
// reaper has been constructed. Used by broker.WireReaperCRCheck
// once the TopicRegistry is populated by the topic-watcher.
// Mutating after Run has started is safe — the callback is
// re-read on every reap.
func (r *PartitionReaper) WithTopicExists(fn func(topic string) bool) {
	if fn == nil {
		return
	}
	r.topicExists = fn
}
