package storage

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/woestebanaan/skafka/tests/testutil/recordbatch"
)

// TestSyncWithDeadlineReturnsSyncResult: a sync that completes within
// the deadline returns whatever the sync function produced (nil for
// success, or the underlying error). Pins the happy path of the
// gh #95 watchdog helper.
func TestSyncWithDeadlineReturnsSyncResult(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		err := syncWithDeadline(func() error { return nil }, 100*time.Millisecond)
		if err != nil {
			t.Errorf("got %v, want nil", err)
		}
	})
	t.Run("error", func(t *testing.T) {
		want := errors.New("ebadf")
		err := syncWithDeadline(func() error { return want }, 100*time.Millisecond)
		if !errors.Is(err, want) {
			t.Errorf("got %v, want %v", err, want)
		}
	})
}

// TestSyncWithDeadlineTimesOutOnHang: a sync that hangs past the
// deadline returns ErrStorageStalled. The orphaned syncFn keeps
// running; the helper does not wait for it. Confirms the watchdog
// actually unblocks the caller within the configured window — the
// behaviour gh #95 needs to make the broker fail fast on NFS hangs.
func TestSyncWithDeadlineTimesOutOnHang(t *testing.T) {
	release := make(chan struct{})
	defer close(release)

	start := time.Now()
	err := syncWithDeadline(func() error {
		<-release
		return nil
	}, 30*time.Millisecond)
	elapsed := time.Since(start)

	if !errors.Is(err, ErrStorageStalled) {
		t.Errorf("err=%v, want ErrStorageStalled", err)
	}
	if elapsed > 200*time.Millisecond {
		t.Errorf("watchdog elapsed=%v, want ≤200ms (deadline 30ms + scheduling slack)", elapsed)
	}
}

// TestSyncWithDeadlineDisabledByZero: max=0 disables the watchdog
// and runs syncFn inline. Mirrors the documented "0 = restore pre-#95
// behaviour" config option.
func TestSyncWithDeadlineDisabledByZero(t *testing.T) {
	called := false
	err := syncWithDeadline(func() error {
		called = true
		return nil
	}, 0)
	if err != nil {
		t.Errorf("err=%v, want nil", err)
	}
	if !called {
		t.Errorf("syncFn not called when watchdog disabled")
	}
}

// TestCommitterFsyncStallSurfacesAsErrStorageStalled is the
// integration-level pin (gh #95): when the active segment's fsync
// hangs, queued appenders get ErrStorageStalled within the deadline
// instead of blocking forever, the partition's stalled flag flips,
// and DiskStorageEngine.AnyStalled() reports true so /healthz can
// surface the cluster-wide signal.
//
// The blocking sync is simulated via partitionState.syncOverride
// rather than a real hung filesystem — same code path, no external
// dependencies. Without the watchdog this test would deadlock until
// the test framework's own deadline kills it.
func TestCommitterFsyncStallSurfacesAsErrStorageStalled(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.FlushIntervalMessages = 1
	cfg.FsyncMaxLatency = 50 * time.Millisecond

	e, err := NewDiskStorageEngine(dir, &neverLeaderLeases{}, cfg)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	if err := e.CreatePartition("t", 0); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := e.TakeOver(context.Background(), "t", 0, 1); err != nil {
		t.Fatalf("takeover: %v", err)
	}

	// Inject a sync that blocks until the test releases it. Mirrors a
	// hung NFS round-trip — the real-world failure mode gh #95 came
	// out of (192.168.1.193 crash on 2026-05-07).
	ps, ok := e.getPartition("t", 0)
	if !ok {
		t.Fatalf("getPartition: not found")
	}
	syncCalls := atomic.Int32{}
	release := make(chan struct{})
	defer close(release)
	ps.mu.Lock()
	ps.syncOverride = func() error {
		syncCalls.Add(1)
		<-release
		return nil
	}
	ps.mu.Unlock()

	batch := recordbatch.Encode(nil, &recordbatch.RecordBatch{
		BaseOffset: 0, LastOffsetDelta: 0,
		ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1,
		Records: []recordbatch.Record{{OffsetDelta: 0, Value: []byte("x")}},
	})

	start := time.Now()
	_, err = e.Append(context.Background(), "t", 0, 1, batch)
	elapsed := time.Since(start)

	if !errors.Is(err, ErrStorageStalled) {
		t.Fatalf("Append err=%v, want ErrStorageStalled", err)
	}
	// Append must return within roughly the deadline + scheduling
	// slack — the entire point of the watchdog. Loose upper bound
	// (1s) so a slow CI runner doesn't flake.
	if elapsed > time.Second {
		t.Errorf("Append elapsed=%v, want ≤1s (deadline %v)", elapsed, cfg.FsyncMaxLatency)
	}
	if syncCalls.Load() == 0 {
		t.Errorf("syncOverride was never called — committer didn't dispatch")
	}
	if !e.AnyStalled() {
		t.Errorf("AnyStalled()=false after stall, want true (healthz signal)")
	}
}

// TestCommitterStalledClearsAfterRecovery: once the storage backend
// recovers and a clean fsync completes, the partition's stalled
// flag clears so /healthz returns to "ok" and follow-up appends
// succeed. Mirrors the operator workflow of "NFS came back, broker
// resumed" — without this the broker would carry an out-of-date
// degraded signal forever.
func TestCommitterStalledClearsAfterRecovery(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.FlushIntervalMessages = 1
	cfg.FsyncMaxLatency = 50 * time.Millisecond

	e, err := NewDiskStorageEngine(dir, &neverLeaderLeases{}, cfg)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	if err := e.CreatePartition("t", 0); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := e.TakeOver(context.Background(), "t", 0, 1); err != nil {
		t.Fatalf("takeover: %v", err)
	}

	ps, ok := e.getPartition("t", 0)
	if !ok {
		t.Fatalf("getPartition: not found")
	}
	var (
		mu       sync.Mutex
		blocking bool
		gate     = make(chan struct{})
	)
	ps.mu.Lock()
	ps.syncOverride = func() error {
		mu.Lock()
		b := blocking
		mu.Unlock()
		if b {
			<-gate
		}
		return nil
	}
	ps.mu.Unlock()

	// Phase 1: trigger a stall. Append blocks for the deadline,
	// returns ErrStorageStalled, AnyStalled() flips true.
	mu.Lock()
	blocking = true
	mu.Unlock()
	batch := func(off int64) []byte {
		return recordbatch.Encode(nil, &recordbatch.RecordBatch{
			BaseOffset: off, LastOffsetDelta: 0,
			ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1,
			Records: []recordbatch.Record{{OffsetDelta: 0, Value: []byte("x")}},
		})
	}
	if _, err := e.Append(context.Background(), "t", 0, 1, batch(0)); !errors.Is(err, ErrStorageStalled) {
		t.Fatalf("Append #1 err=%v, want ErrStorageStalled", err)
	}
	if !e.AnyStalled() {
		t.Fatalf("AnyStalled()=false after stall, want true")
	}

	// Phase 2: recover. Stop blocking, drain the orphaned committer
	// from phase 1, and submit a fresh Append. It should succeed and
	// clear the stalled flag.
	mu.Lock()
	blocking = false
	mu.Unlock()
	close(gate) // drain the orphan from phase 1's Sync call

	if _, err := e.Append(context.Background(), "t", 0, 1, batch(1)); err != nil {
		t.Fatalf("Append #2 (recovery): %v", err)
	}
	if e.AnyStalled() {
		t.Errorf("AnyStalled()=true after recovery, want false")
	}
}
