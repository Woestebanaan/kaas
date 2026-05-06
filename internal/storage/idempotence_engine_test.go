package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/woestebanaan/skafka/tests/testutil/recordbatch"
)

// idempotentBatch builds a v2 RecordBatch tagged with the producer
// fields the broker checks. recordCount controls lastOffsetDelta so
// firstSeq..firstSeq+recordCount-1 is the sequence range Java/franz-go
// would send for a batch of `recordCount` records.
func idempotentBatch(pid int64, epoch int16, firstSeq int32, recordCount int) []byte {
	records := make([]recordbatch.Record, recordCount)
	for i := range records {
		records[i] = recordbatch.Record{OffsetDelta: int32(i), Value: []byte("x")}
	}
	return recordbatch.Encode(nil, &recordbatch.RecordBatch{
		BaseOffset:      0,
		LastOffsetDelta: int32(recordCount - 1),
		ProducerID:      pid,
		ProducerEpoch:   epoch,
		BaseSequence:    firstSeq,
		Records:         records,
	})
}

// newIdempotenceEngine builds an engine + opens a single partition
// for the idempotence end-to-end tests.
func newIdempotenceEngine(t *testing.T) *DiskStorageEngine {
	t.Helper()
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.FlushIntervalMessages = 0 // skip per-record fsync — these tests don't care about durability cadence
	e, err := NewDiskStorageEngine(dir, &neverLeaderLeases{}, cfg)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	if err := e.CreatePartition("t", 0); err != nil {
		t.Fatalf("create partition: %v", err)
	}
	if _, err := e.TakeOver(context.Background(), "t", 0, 1); err != nil {
		t.Fatalf("takeover: %v", err)
	}
	return e
}

// TestEngineIdempotentAppendDeduplicatesRetry walks the dominant
// retry case Java's idempotent producer hits in steady state: send
// batch [seq=0..4] → ack lost → retry [seq=0..4]. The broker MUST
// return the same baseOffset on the retry (with errCode=0) so the
// producer's internal "successfully sent" set lines up.
func TestEngineIdempotentAppendDeduplicatesRetry(t *testing.T) {
	e := newIdempotenceEngine(t)

	first, err := e.Append(context.Background(), "t", 0, 1, idempotentBatch(11, 0, 0, 5))
	if err != nil {
		t.Fatalf("first append: %v", err)
	}
	if first != 0 {
		t.Errorf("first baseOffset=%d, want 0", first)
	}

	retry, err := e.Append(context.Background(), "t", 0, 1, idempotentBatch(11, 0, 0, 5))
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if retry != first {
		t.Errorf("retry baseOffset=%d, want %d (dedupe must echo original)", retry, first)
	}

	// HighWatermark must NOT have advanced — the retry was deduped.
	hwm, _ := e.HighWatermark("t", 0)
	if hwm != 5 {
		t.Errorf("HWM=%d, want 5 (one batch of 5 records, dedupe must not double-write)", hwm)
	}
}

// TestEngineIdempotentAppendOutOfOrderReturnsErr45 asserts the
// producer-fatal error path: a gap means the broker missed a batch
// somewhere upstream and there is no way to recover.
func TestEngineIdempotentAppendOutOfOrderReturnsErr45(t *testing.T) {
	e := newIdempotenceEngine(t)

	if _, err := e.Append(context.Background(), "t", 0, 1, idempotentBatch(22, 0, 0, 5)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Skip ahead past 5..9 — broker should reject 10..14.
	_, err := e.Append(context.Background(), "t", 0, 1, idempotentBatch(22, 0, 10, 5))
	if !errors.Is(err, ErrOutOfOrderSequence) {
		t.Errorf("err=%v, want ErrOutOfOrderSequence", err)
	}
}

// TestEngineIdempotentAppendStaleEpochReturnsErr47: a zombie
// producer (one that was fenced by an epoch bump) tries to write.
// Maps to error 47.
func TestEngineIdempotentAppendStaleEpochReturnsErr47(t *testing.T) {
	e := newIdempotenceEngine(t)

	// Establish PID=33 at epoch=2.
	if _, err := e.Append(context.Background(), "t", 0, 1, idempotentBatch(33, 2, 0, 5)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Zombie at epoch=1 tries to write.
	_, err := e.Append(context.Background(), "t", 0, 1, idempotentBatch(33, 1, 5, 5))
	if !errors.Is(err, ErrInvalidProducerEpoch) {
		t.Errorf("err=%v, want ErrInvalidProducerEpoch", err)
	}
}

// TestEngineIdempotentAppendEpochBumpAccepted: the post-fence
// happy path — same PID, higher epoch, sequence resets to 0. KIP-360
// PID renewal works the same way.
func TestEngineIdempotentAppendEpochBumpAccepted(t *testing.T) {
	e := newIdempotenceEngine(t)

	if _, err := e.Append(context.Background(), "t", 0, 1, idempotentBatch(44, 0, 0, 5)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := e.Append(context.Background(), "t", 0, 1, idempotentBatch(44, 0, 5, 5)); err != nil {
		t.Fatalf("seed2: %v", err)
	}
	// Producer reinitialised — same PID, epoch++, seq back to 0.
	off, err := e.Append(context.Background(), "t", 0, 1, idempotentBatch(44, 1, 0, 5))
	if err != nil {
		t.Fatalf("epoch-bump append: %v", err)
	}
	// New batch must land on top of the existing log (offset 10), not
	// dedupe against the epoch-0 batch at offset 0.
	if off != 10 {
		t.Errorf("epoch-bump baseOffset=%d, want 10 (must be a fresh write, not dedupe)", off)
	}
}

// TestEngineNonIdempotentAppendStillWorks pins backward compatibility:
// a producer that doesn't tag its batch with a producerID (PID=-1,
// the wire sentinel) bypasses the idempotence machinery entirely.
// Two identical PID=-1 batches must both append — there's no dedupe
// on the non-idempotent path.
func TestEngineNonIdempotentAppendStillWorks(t *testing.T) {
	e := newIdempotenceEngine(t)

	first, err := e.Append(context.Background(), "t", 0, 1, idempotentBatch(-1, -1, -1, 3))
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	second, err := e.Append(context.Background(), "t", 0, 1, idempotentBatch(-1, -1, -1, 3))
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if second == first {
		t.Errorf("non-idempotent second offset=%d == first=%d (must NOT dedupe)", second, first)
	}
	hwm, _ := e.HighWatermark("t", 0)
	if hwm != 6 {
		t.Errorf("HWM=%d, want 6 (3+3 records, no dedupe)", hwm)
	}
}

// TestEngineRejoinFenceClosesZombieWindow walks the gh #22
// scenario the storage layer enables: producer A connected as
// "txn-foo" got (PID=42, epoch=0) and wrote 3 records. The
// network blipped, A retried InitProducerId, got bumped to
// (PID=42, epoch=1). Meanwhile A's old session had one batch
// in-flight under (PID=42, epoch=0). That batch arrives AFTER
// A's new session has started writing under epoch=1.
//
// Without the storage-layer fence, the zombie's batch would
// land at HWM and corrupt the log. With it, classifyIdempotence
// detects the stale epoch and returns ErrInvalidProducerEpoch
// — exactly the error #22's epoch bump exists to trigger.
func TestEngineRejoinFenceClosesZombieWindow(t *testing.T) {
	e := newIdempotenceEngine(t)

	// Session 1: PID=42, epoch=0 — write 3 records.
	if _, err := e.Append(context.Background(), "t", 0, 1, idempotentBatch(42, 0, 0, 3)); err != nil {
		t.Fatalf("session1 seed: %v", err)
	}

	// Session 2 starts (gh #22 bump): PID=42, epoch=1 — writes 5
	// records starting from seq=0 (a fresh epoch resets the
	// sequence counter on the producer side).
	if _, err := e.Append(context.Background(), "t", 0, 1, idempotentBatch(42, 1, 0, 5)); err != nil {
		t.Fatalf("session2 first batch: %v", err)
	}

	// Zombie from session 1's in-flight queue: PID=42, epoch=0,
	// seq=3 (continuing where the seed left off). MUST be fenced.
	_, err := e.Append(context.Background(), "t", 0, 1, idempotentBatch(42, 0, 3, 2))
	if !errors.Is(err, ErrInvalidProducerEpoch) {
		t.Errorf("zombie batch err=%v, want ErrInvalidProducerEpoch (gh #22 fence missed it)", err)
	}

	// Sanity: session 2 keeps working — the zombie's reject
	// didn't poison the per-PID state.
	if _, err := e.Append(context.Background(), "t", 0, 1, idempotentBatch(42, 1, 5, 3)); err != nil {
		t.Errorf("session2 continued append after fence: %v", err)
	}
}

// TestEngineIdempotentDifferentProducersDontInterfere: PID is the
// dedupe key. Two producers can both legitimately send batches with
// firstSeq=0 — they must both succeed and land at distinct offsets,
// neither dedupe against the other.
func TestEngineIdempotentDifferentProducersDontInterfere(t *testing.T) {
	e := newIdempotenceEngine(t)

	pidA, err := e.Append(context.Background(), "t", 0, 1, idempotentBatch(55, 0, 0, 5))
	if err != nil {
		t.Fatalf("A: %v", err)
	}
	pidB, err := e.Append(context.Background(), "t", 0, 1, idempotentBatch(66, 0, 0, 5))
	if err != nil {
		t.Fatalf("B: %v", err)
	}
	if pidA == pidB {
		t.Errorf("two different producers' first batches collided at offset %d", pidA)
	}
	hwm, _ := e.HighWatermark("t", 0)
	if hwm != 10 {
		t.Errorf("HWM=%d, want 10 (two 5-record batches)", hwm)
	}
}
