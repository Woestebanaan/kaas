package storage

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestReaperRateLimits pins the gh #119 token-bucket contract: 20
// enqueued jobs at rate=10/sec take ~2 seconds of wall clock. The
// test asserts the slope, not an exact value — Go's `rate.Limiter`
// is allowed minor jitter under load.
func TestReaperRateLimits(t *testing.T) {
	r := NewPartitionReaper(ReaperConfig{RatePerSec: 10, QueueDepth: 100})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create 20 dummy partition dirs in a temp tree.
	root := t.TempDir()
	for i := 0; i < 20; i++ {
		dir := filepath.Join(root, "topic", "0")
		_ = os.MkdirAll(dir, 0o775)
		if err := r.Enqueue("topic", int32(i), nil, dir); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	start := time.Now()
	go r.Run(ctx)
	for {
		if r.QueueDepth() == 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("queue did not drain within 5s")
		case <-time.After(50 * time.Millisecond):
		}
	}
	elapsed := time.Since(start)
	r.Stop()

	// 20 jobs at 10/sec ≈ 2 seconds. Allow ±50%.
	if elapsed < 1*time.Second || elapsed > 4*time.Second {
		t.Errorf("queue drained in %s — expected ~2s for 20 jobs at 10/sec", elapsed)
	}
}

// TestReaperAbortsWhenTopicReturns guards the gh #119 safety check:
// if the CR-existence callback says the topic is back (recreate-
// with-same-name during the reap window), the reap is aborted and
// the partition dir is left intact.
func TestReaperAbortsWhenTopicReturns(t *testing.T) {
	root := t.TempDir()
	topicDir := filepath.Join(root, "ghost-topic")
	partDir := filepath.Join(topicDir, "0")
	if err := os.MkdirAll(partDir, 0o775); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// Put a tombstone file inside so we can detect if it survives.
	tombstone := filepath.Join(partDir, "data.log")
	if err := os.WriteFile(tombstone, []byte("important"), 0o644); err != nil {
		t.Fatalf("write tombstone: %v", err)
	}

	// CR-recheck always returns true ⇒ topic "exists" ⇒ abort.
	r := NewPartitionReaper(ReaperConfig{
		RatePerSec:  100,
		TopicExists: func(_ string) bool { return true },
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.Enqueue("ghost-topic", 0, nil, partDir); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	go r.Run(ctx)
	time.Sleep(300 * time.Millisecond)
	r.Stop()

	if _, err := os.Stat(tombstone); err != nil {
		t.Fatalf("tombstone was removed despite CR-recheck saying topic exists: %v", err)
	}
}

// TestReaperRemovesPartitionDir: happy path. CR-recheck returns false,
// reap proceeds, partition dir is gone afterwards.
func TestReaperRemovesPartitionDir(t *testing.T) {
	root := t.TempDir()
	topicDir := filepath.Join(root, "dead-topic")
	partDir := filepath.Join(topicDir, "0")
	if err := os.MkdirAll(partDir, 0o775); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	r := NewPartitionReaper(ReaperConfig{
		RatePerSec:  100,
		TopicExists: func(_ string) bool { return false },
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.Enqueue("dead-topic", 0, nil, partDir); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	go r.Run(ctx)
	// Drain.
	deadline := time.Now().Add(1 * time.Second)
	for r.QueueDepth() > 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	r.Stop()

	if _, err := os.Stat(partDir); !os.IsNotExist(err) {
		t.Fatalf("partition dir still exists: stat=%v", err)
	}
	// Topic dir should also be gone since it was the only partition.
	if _, err := os.Stat(topicDir); !os.IsNotExist(err) {
		t.Errorf("empty topic dir not cleaned up: stat=%v", err)
	}
}

// TestReaperIdempotentEnqueue: enqueueing the same (topic, partition)
// twice doesn't crash. Reaper handles both — second one becomes a
// no-op when the dir is already gone (os.RemoveAll returns nil on
// ENOENT).
func TestReaperIdempotentEnqueue(t *testing.T) {
	root := t.TempDir()
	partDir := filepath.Join(root, "t", "0")
	_ = os.MkdirAll(partDir, 0o775)

	r := NewPartitionReaper(ReaperConfig{RatePerSec: 100})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.Enqueue("t", 0, nil, partDir); err != nil {
		t.Fatalf("enqueue 1: %v", err)
	}
	if err := r.Enqueue("t", 0, nil, partDir); err != nil {
		t.Fatalf("enqueue 2: %v", err)
	}

	go r.Run(ctx)
	deadline := time.Now().Add(1 * time.Second)
	for r.QueueDepth() > 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	r.Stop()

	if _, err := os.Stat(partDir); !os.IsNotExist(err) {
		t.Errorf("partition dir still exists after double-enqueue + reap: %v", err)
	}
}

// TestReaperStopIsRaceFree: concurrent Enqueue + Stop must not panic.
func TestReaperStopIsRaceFree(t *testing.T) {
	r := NewPartitionReaper(ReaperConfig{RatePerSec: 1000, QueueDepth: 10})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)

	var wg sync.WaitGroup
	var enqueueErrors atomic.Int32
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			dir := filepath.Join(t.TempDir(), "p")
			_ = os.MkdirAll(dir, 0o775)
			if err := r.Enqueue("t", int32(idx), nil, dir); err != nil {
				enqueueErrors.Add(1)
			}
		}(i)
	}

	time.Sleep(5 * time.Millisecond)
	r.Stop()
	wg.Wait()

	// Some Enqueues may have failed (reaper stopped mid-fan-in) —
	// that's the expected drop-on-stop behaviour. The test passes
	// as long as nothing panicked.
}

// TestReaperRetryOnTransientError: when a reap fails with a
// "transient" error (e.g., NFS hiccup), the reaper re-enqueues
// with backoff and eventually succeeds. We simulate by pre-locking
// the dir via a sentinel file the test removes after the first
// failure.
//
// (Skipped — Go on Linux can't easily simulate NFS-style EIO; the
// retry logic is exercised in the dedicated integration test
// against a real broker. Kept here as documentation.)
func TestReaperRetryOnTransientError(t *testing.T) {
	t.Skip("retry path is hard to simulate without a real NFS fault injector; covered by gh #119 integration test")
}
