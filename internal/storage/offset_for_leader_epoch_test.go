package storage

import (
	"context"
	"testing"

	"github.com/woestebanaan/skafka/tests/testutil/recordbatch"
)

func ofleBatch() []byte {
	return recordbatch.Encode(nil, &recordbatch.RecordBatch{
		BaseOffset:      0,
		LastOffsetDelta: 0,
		Records:         []recordbatch.Record{{OffsetDelta: 0, Value: []byte("x")}},
	})
}

// TestOffsetForLeaderEpochCurrentEpochReturnsHWM exercises the
// happy-path "consumer is current" case: requested epoch matches the
// partition's current epoch, so the answer is (current, HighWatermark).
func TestOffsetForLeaderEpochCurrentEpochReturnsHWM(t *testing.T) {
	dir := t.TempDir()
	leases := &neverLeaderLeases{}
	e, err := NewDiskStorageEngine(dir, leases, DefaultConfig())
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	if err := e.CreatePartition("t", 0); err != nil {
		t.Fatalf("CreatePartition: %v", err)
	}
	if _, err := e.TakeOver(context.Background(), "t", 0, 7); err != nil {
		t.Fatalf("TakeOver: %v", err)
	}
	if _, err := e.Append(context.Background(), "t", 0, 7, -1, ofleBatch()); err != nil {
		t.Fatalf("Append: %v", err)
	}
	hwm, _ := e.HighWatermark("t", 0)

	resultEpoch, endOff, err := e.OffsetForLeaderEpoch("t", 0, 7)
	if err != nil {
		t.Fatalf("OffsetForLeaderEpoch: %v", err)
	}
	if resultEpoch != 7 {
		t.Errorf("resultEpoch=%d, want 7 (current)", resultEpoch)
	}
	if endOff != hwm {
		t.Errorf("endOffset=%d, want %d (HWM)", endOff, hwm)
	}
}

// TestOffsetForLeaderEpochFutureEpochFenced verifies the gh #101
// fenced-client guard: a consumer requesting an epoch HIGHER than the
// broker's current epoch gets ErrEpochFenced. The handler maps this
// to wire error 74 (FENCED_LEADER_EPOCH).
func TestOffsetForLeaderEpochFutureEpochFenced(t *testing.T) {
	dir := t.TempDir()
	leases := &neverLeaderLeases{}
	e, err := NewDiskStorageEngine(dir, leases, DefaultConfig())
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	if err := e.CreatePartition("t", 0); err != nil {
		t.Fatalf("CreatePartition: %v", err)
	}
	if _, err := e.TakeOver(context.Background(), "t", 0, 3); err != nil {
		t.Fatalf("TakeOver: %v", err)
	}

	_, _, err = e.OffsetForLeaderEpoch("t", 0, 99)
	if err != ErrEpochFenced {
		t.Errorf("future epoch err=%v, want ErrEpochFenced", err)
	}
}

// TestOffsetForLeaderEpochOlderEpochFindsBoundary verifies the closed-
// segment scan: given a partition whose history includes a roll at
// epoch boundary 3 → 5, a consumer asking for epoch 3 must be told
// the offset at which epoch 5 took over. Builds the segment list
// directly so the test doesn't depend on the (size-thresholded)
// roll-trigger plumbing.
func TestOffsetForLeaderEpochOlderEpochFindsBoundary(t *testing.T) {
	dir := t.TempDir()
	leases := &neverLeaderLeases{}
	e, err := NewDiskStorageEngine(dir, leases, DefaultConfig())
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	if err := e.CreatePartition("t", 0); err != nil {
		t.Fatalf("CreatePartition: %v", err)
	}
	if _, err := e.TakeOver(context.Background(), "t", 0, 5); err != nil {
		t.Fatalf("TakeOver: %v", err)
	}

	// Inject a synthetic segment history: two closed segments at
	// epoch 3 (offsets 0..49 and 50..99), one closed segment at
	// epoch 5 starting at offset 100, and the live active at epoch 5.
	// OffsetForLeaderEpoch(3) must return the epoch-5 boundary at
	// offset 100.
	ps, _ := e.getPartition("t", 0)
	ps.mu.Lock()
	ps.segments = []segmentMeta{
		{baseOffset: 0, epoch: 3},
		{baseOffset: 50, epoch: 3},
		{baseOffset: 100, epoch: 5},
	}
	ps.mu.Unlock()

	resultEpoch, endOff, err := e.OffsetForLeaderEpoch("t", 0, 3)
	if err != nil {
		t.Fatalf("OffsetForLeaderEpoch(3): %v", err)
	}
	if resultEpoch != 5 {
		t.Errorf("resultEpoch=%d, want 5 (next-up epoch)", resultEpoch)
	}
	if endOff != 100 {
		t.Errorf("endOffset=%d, want 100 (epoch-5 baseOffset)", endOff)
	}
}

// TestOffsetForLeaderEpochOlderEpochNoMatchingSegmentReturnsTooOld
// covers the degenerate case where requested < current but no segment
// or active carries a higher epoch — i.e., the retained log was
// rewritten with the same epoch as the active. The lookup returns
// ErrEpochTooOld so the handler maps it to UNKNOWN_LEADER_EPOCH (73)
// rather than silently returning a 0 offset.
func TestOffsetForLeaderEpochOlderEpochNoMatchingSegmentReturnsTooOld(t *testing.T) {
	dir := t.TempDir()
	leases := &neverLeaderLeases{}
	e, err := NewDiskStorageEngine(dir, leases, DefaultConfig())
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	if err := e.CreatePartition("t", 0); err != nil {
		t.Fatalf("CreatePartition: %v", err)
	}
	if _, err := e.TakeOver(context.Background(), "t", 0, 7); err != nil {
		t.Fatalf("TakeOver: %v", err)
	}
	// Both active and closed are at the SAME epoch as ps.epoch, and
	// none is higher than the requested 3. The walk falls through to
	// ErrEpochTooOld — there's no boundary to point at.
	ps, _ := e.getPartition("t", 0)
	ps.mu.Lock()
	ps.active.epoch = 0
	ps.segments = []segmentMeta{} // empty
	ps.mu.Unlock()

	_, _, err = e.OffsetForLeaderEpoch("t", 0, 3)
	if err != ErrEpochTooOld {
		t.Errorf("no-matching-segment err=%v, want ErrEpochTooOld", err)
	}
}

// TestOffsetForLeaderEpochUnknownPartition surfaces the not-yet-created
// partition error from storage so the handler can map it to
// UNKNOWN_TOPIC_OR_PARTITION (3).
func TestOffsetForLeaderEpochUnknownPartition(t *testing.T) {
	dir := t.TempDir()
	leases := &neverLeaderLeases{}
	e, err := NewDiskStorageEngine(dir, leases, DefaultConfig())
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	if _, _, err := e.OffsetForLeaderEpoch("does-not-exist", 0, 1); err == nil {
		t.Errorf("unknown partition err=nil, want ErrUnknownPartition")
	}
}
