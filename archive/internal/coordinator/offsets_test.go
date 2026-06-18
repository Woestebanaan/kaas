package coordinator

import (
	"testing"
)

// TestStorePendingInvisibleToFetch pins the gh #27 invariant:
// staged transactional offsets are NOT visible to regular
// OffsetFetch until CommitPending materialises them. Pre-fix
// the store had no pending layer; offsets would have leaked.
func TestStorePendingInvisibleToFetch(t *testing.T) {
	dir := t.TempDir()
	s := NewOffsetStore(dir)

	s.StorePending("cg-test", 100, map[string]int64{
		"topic-1/0": 42,
		"topic-1/1": 77,
	})

	// Regular Fetch sees -1 for both — pending is hidden.
	got := s.Fetch("cg-test", []FetchSpec{
		{Topic: "topic-1", Partitions: []int32{0, 1}},
	})
	if got["topic-1/0"] != -1 || got["topic-1/1"] != -1 {
		t.Fatalf("pending leaked into Fetch: %+v", got)
	}

	// PendingFor (test-only) returns the staged values.
	pending := s.PendingFor("cg-test", 100)
	if pending["topic-1/0"] != 42 || pending["topic-1/1"] != 77 {
		t.Fatalf("PendingFor mismatch: %+v", pending)
	}
}

// TestCommitPendingMaterialises: after CommitPending, Fetch sees
// the previously-staged offsets and the pending entry is gone.
func TestCommitPendingMaterialises(t *testing.T) {
	dir := t.TempDir()
	s := NewOffsetStore(dir)
	s.StorePending("cg", 100, map[string]int64{"t/0": 50})

	if err := s.CommitPending("cg", 100); err != nil {
		t.Fatalf("commit: %v", err)
	}

	got := s.Fetch("cg", []FetchSpec{{Topic: "t", Partitions: []int32{0}}})
	if got["t/0"] != 50 {
		t.Fatalf("Fetch after commit returned %d, want 50", got["t/0"])
	}
	if p := s.PendingFor("cg", 100); p != nil {
		t.Errorf("pending entry not cleared after commit: %+v", p)
	}
}

// TestDiscardPendingNoCommit: DiscardPending drops the staged
// offsets without materialising — gh #26 abort path.
func TestDiscardPendingNoCommit(t *testing.T) {
	dir := t.TempDir()
	s := NewOffsetStore(dir)
	s.StorePending("cg", 100, map[string]int64{"t/0": 50})

	s.DiscardPending("cg", 100)

	got := s.Fetch("cg", []FetchSpec{{Topic: "t", Partitions: []int32{0}}})
	if got["t/0"] != -1 {
		t.Errorf("offset materialised after Discard: %d", got["t/0"])
	}
	if p := s.PendingFor("cg", 100); p != nil {
		t.Errorf("pending entry not cleared after discard: %+v", p)
	}
}

// TestPendingPerProducerIsolation: two producers staging offsets
// for the same group are tracked separately (keyed by producerID).
// Aborting one must not affect the other.
func TestPendingPerProducerIsolation(t *testing.T) {
	dir := t.TempDir()
	s := NewOffsetStore(dir)
	s.StorePending("cg", 1, map[string]int64{"t/0": 10})
	s.StorePending("cg", 2, map[string]int64{"t/0": 99})

	s.DiscardPending("cg", 1)

	// Producer 2's pending entry survives.
	if got := s.PendingFor("cg", 2); got["t/0"] != 99 {
		t.Errorf("producer 2 entry lost: %+v", got)
	}
	// Producer 1's is gone.
	if got := s.PendingFor("cg", 1); got != nil {
		t.Errorf("producer 1 entry leaked after discard: %+v", got)
	}
}
