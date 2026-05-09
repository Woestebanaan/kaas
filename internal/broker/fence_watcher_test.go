package broker

import (
	"sync"
	"testing"

	"github.com/woestebanaan/skafka/internal/coordinator"
)

// recordingFencer captures FenceProducerEpoch calls for assertion.
type recordingFencer struct {
	mu    sync.Mutex
	calls []fenceCall
}

type fenceCall struct {
	pid   int64
	epoch int16
}

func (r *recordingFencer) FenceProducerEpoch(pid int64, epoch int16) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, fenceCall{pid: pid, epoch: epoch})
}

func (r *recordingFencer) snapshot() []fenceCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]fenceCall, len(r.calls))
	copy(out, r.calls)
	return out
}

// TestFenceWatcherAppliesPeerFences guards the gh #108 phase 2
// headline: broker A bumps a producer's epoch and writes to its
// outbound FenceLog; broker B's FenceWatcher polls the directory,
// reads A's file, and applies the fence to B's local engine.
// This is the actual cross-broker fence broadcast.
func TestFenceWatcherAppliesPeerFences(t *testing.T) {
	dir := t.TempDir()
	// Broker A writes a fence.
	logA, _ := coordinator.NewFenceLog(dir, "skafka-0")
	if err := logA.Append(42, 7); err != nil {
		t.Fatalf("logA append: %v", err)
	}
	// Broker B watches and applies.
	rec := &recordingFencer{}
	wB := NewFenceWatcher(dir, "from-skafka-1.json", rec)
	wB.Tick()

	calls := rec.snapshot()
	if len(calls) != 1 {
		t.Fatalf("expected 1 fence call, got %d (%v)", len(calls), calls)
	}
	if calls[0].pid != 42 || calls[0].epoch != 7 {
		t.Errorf("got %+v, want {42 7}", calls[0])
	}
}

// TestFenceWatcherSkipsSelf: the watcher must NOT apply this
// broker's own outbound fences — those were already applied
// in-process when InitProducerId called local FenceProducerEpoch.
// Re-applying them is harmless (idempotent at engine layer) but
// wastes the engine-wide RLock walk on every tick. Pin the skip.
func TestFenceWatcherSkipsSelf(t *testing.T) {
	dir := t.TempDir()
	// "skafka-1" writes a fence to its OWN file.
	logSelf, _ := coordinator.NewFenceLog(dir, "skafka-1")
	if err := logSelf.Append(99, 3); err != nil {
		t.Fatalf("self append: %v", err)
	}
	rec := &recordingFencer{}
	w := NewFenceWatcher(dir, "from-skafka-1.json", rec)
	w.Tick()

	if calls := rec.snapshot(); len(calls) != 0 {
		t.Errorf("watcher applied self fence: %v", calls)
	}
}

// TestFenceWatcherDedupesAcrossTicks: the watcher's per-file
// (PID → highest-epoch-applied) cache must skip already-applied
// entries on subsequent ticks. Without dedupe, every tick would
// re-call the engine's RLock-and-walk-every-partition path
// regardless of whether anything changed.
func TestFenceWatcherDedupesAcrossTicks(t *testing.T) {
	dir := t.TempDir()
	logA, _ := coordinator.NewFenceLog(dir, "skafka-0")
	logA.Append(1, 5)

	rec := &recordingFencer{}
	w := NewFenceWatcher(dir, "from-skafka-1.json", rec)

	w.Tick()
	w.Tick()
	w.Tick()

	if calls := rec.snapshot(); len(calls) != 1 {
		t.Errorf("expected 1 fence call across 3 ticks (dedupe), got %d (%v)", len(calls), calls)
	}
}

// TestFenceWatcherRecognisesEpochBump: when broker A bumps from 5
// to 7 in its outbound file, broker B's watcher must apply the
// new (1, 7) fence on the next tick — the dedupe cache shouldn't
// hide a real upgrade.
func TestFenceWatcherRecognisesEpochBump(t *testing.T) {
	dir := t.TempDir()
	logA, _ := coordinator.NewFenceLog(dir, "skafka-0")
	logA.Append(1, 5)

	rec := &recordingFencer{}
	w := NewFenceWatcher(dir, "from-skafka-1.json", rec)
	w.Tick()

	logA.Append(1, 7)
	w.Tick()

	calls := rec.snapshot()
	if len(calls) != 2 {
		t.Fatalf("expected 2 fence calls (initial + bump), got %d (%v)", len(calls), calls)
	}
	if calls[0].epoch != 5 || calls[1].epoch != 7 {
		t.Errorf("got epochs %d,%d; want 5,7", calls[0].epoch, calls[1].epoch)
	}
}

// TestFenceWatcherMultiPeer: a 3-broker cluster — broker C's
// watcher applies fences from BOTH broker A and broker B, with
// the highest-epoch-per-PID winning when both peers fenced the
// same PID.
func TestFenceWatcherMultiPeer(t *testing.T) {
	dir := t.TempDir()
	logA, _ := coordinator.NewFenceLog(dir, "skafka-0")
	logB, _ := coordinator.NewFenceLog(dir, "skafka-1")
	logA.Append(1, 5)
	logA.Append(2, 3)
	logB.Append(2, 7) // overrides A's entry for pid=2
	logB.Append(3, 1)

	rec := &recordingFencer{}
	w := NewFenceWatcher(dir, "from-skafka-2.json", rec)
	w.Tick()

	calls := rec.snapshot()
	// 4 total entries across A and B; watcher applies them all
	// (engine-level FenceProducerEpoch is idempotent for stale
	// epochs, so we don't care if order shows pid=2 e3 then e7).
	if len(calls) != 4 {
		t.Errorf("expected 4 fence calls, got %d (%v)", len(calls), calls)
	}
}

// TestFenceWatcherMissingDirIsNoop: before the first FenceLog
// write, the producer_fences/ directory may not exist on this
// broker's view (just-mounted PVC, fresh cluster). The watcher
// must tolerate that without errors.
func TestFenceWatcherMissingDirIsNoop(t *testing.T) {
	rec := &recordingFencer{}
	w := NewFenceWatcher("/nonexistent/path", "from-skafka-0.json", rec)
	// Should not panic / error.
	w.Tick()
	if calls := rec.snapshot(); len(calls) != 0 {
		t.Errorf("watcher applied %d calls against missing dir: %v", len(calls), calls)
	}
}
