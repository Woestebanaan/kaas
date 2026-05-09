package coordinator

import (
	"path/filepath"
	"testing"
)

// TestFenceLogAppendIsIdempotent guards the per-PID highest-epoch
// dedupe: re-appending an equal-or-lower epoch must not regress
// the on-disk state.
func TestFenceLogAppendIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	log, err := NewFenceLog(dir, "skafka-1")
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := log.Append(42, 5); err != nil {
		t.Fatalf("append e5: %v", err)
	}
	if err := log.Append(42, 7); err != nil {
		t.Fatalf("append e7: %v", err)
	}
	// Lower epoch must not regress.
	if err := log.Append(42, 3); err != nil {
		t.Fatalf("append e3: %v", err)
	}
	// Equal epoch must be a no-op.
	if err := log.Append(42, 7); err != nil {
		t.Fatalf("append e7 again: %v", err)
	}

	snap := log.Snapshot()
	if got := snap[42]; got != 7 {
		t.Errorf("after 5,7,3,7 the highest epoch should be 7, got %d", got)
	}
}

// TestFenceLogPathFormat pins the on-disk file naming so
// FenceWatcher's selfFile skip logic stays in sync.
func TestFenceLogPathFormat(t *testing.T) {
	dir := t.TempDir()
	log, _ := NewFenceLog(dir, "skafka-2")
	want := filepath.Join(dir, "from-skafka-2.json")
	if log.Path() != want {
		t.Errorf("path=%q, want %q", log.Path(), want)
	}
}

// TestFenceLogPersistsAcrossReopen confirms the file is durable —
// a new FenceLog on the same dir+brokerID sees the prior state.
// Without this, every broker restart silently forgets every
// outbound fence event and peers' watchers can't reconstruct the
// cluster's highest-epoch map.
func TestFenceLogPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	log1, _ := NewFenceLog(dir, "skafka-0")
	if err := log1.Append(100, 9); err != nil {
		t.Fatalf("append: %v", err)
	}

	log2, err := NewFenceLog(dir, "skafka-0")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	snap := log2.Snapshot()
	if got := snap[100]; got != 9 {
		t.Errorf("post-reopen epoch=%d, want 9", got)
	}
}

// TestFenceLogEmptyBrokerIDRejected: a missing broker ID would
// produce "from-.json" which would collide across brokers. Fail
// at construction.
func TestFenceLogEmptyBrokerIDRejected(t *testing.T) {
	dir := t.TempDir()
	if _, err := NewFenceLog(dir, ""); err == nil {
		t.Error("empty brokerID should error")
	}
}
