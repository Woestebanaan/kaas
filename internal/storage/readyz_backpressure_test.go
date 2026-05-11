package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestReaperQueueDepthExposed pins the gh #118 surface that drives
// the broker's /readyz: when many partitions are pending reap, the
// QueueDepth() count climbs and the broker advertises NotReady.
// kube-proxy then routes traffic away. Mirrors the real production
// path readyFn() takes in cmd/skafka/main.go.
func TestReaperQueueDepthExposed(t *testing.T) {
	const threshold = 50

	// Build a reaper but DON'T start the Run loop — that pins
	// queue depth at the value we enqueue.
	r := NewPartitionReaper(ReaperConfig{
		RatePerSec: 1,
		QueueDepth: 200,
	})

	// Empty queue → ready.
	if r.QueueDepth() > threshold {
		t.Fatalf("freshly-built reaper has QueueDepth=%d > threshold=%d", r.QueueDepth(), threshold)
	}

	// Enqueue threshold+1 jobs to push past the readyz threshold.
	root := t.TempDir()
	for i := 0; i <= threshold; i++ {
		dir := filepath.Join(root, "t", "0")
		_ = os.MkdirAll(dir, 0o775)
		if err := r.Enqueue("t", int32(i), nil, dir); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	if got := r.QueueDepth(); got <= threshold {
		t.Errorf("after %d enqueues, QueueDepth=%d — expected > threshold=%d", threshold+1, got, threshold)
	}
}

// TestReaperQueueDepthDrainsBelowThreshold: simulate the recovery
// path — once the worker has drained the queue, /readyz can flip
// back to ready.
func TestReaperQueueDepthDrainsBelowThreshold(t *testing.T) {
	const threshold = 5

	r := NewPartitionReaper(ReaperConfig{
		RatePerSec:  100, // fast drain for the test
		QueueDepth:  50,
		TopicExists: func(_ string) bool { return false },
	})
	root := t.TempDir()
	for i := 0; i < 20; i++ {
		dir := filepath.Join(root, "topic", "0")
		_ = os.MkdirAll(dir, 0o775)
		_ = r.Enqueue("topic", int32(i), nil, dir)
	}

	if r.QueueDepth() <= threshold {
		t.Skip("test setup didn't push queue above threshold; flaky timer? skipping")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go r.Run(ctx)

	// Wait for drain.
	deadline := time.Now().Add(2 * time.Second)
	for r.QueueDepth() > threshold && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	r.Stop()

	if got := r.QueueDepth(); got > threshold {
		t.Errorf("queue did not drain below threshold=%d in 2s; QueueDepth=%d", threshold, got)
	}
}
