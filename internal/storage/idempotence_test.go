package storage

import "testing"

// classifyIdempotence is a pure decision function — no I/O, no mutex
// — so we can drive it directly through every branch without setting
// up an engine. Each test case represents one decision the broker
// must make to honour Apache Kafka 3.7's idempotent-producer
// guarantees (gh #12).

// TestClassifyNonIdempotentBypass: producer didn't call
// InitProducerId so PID == -1 (the wire sentinel). The check must
// short-circuit so non-idempotent legacy producers keep working.
func TestClassifyNonIdempotentBypass(t *testing.T) {
	states := map[int64]*producerEntry{}
	info := batchProducerInfo{producerID: -1, epoch: -1, firstSeq: -1, lastSeq: -1}
	if action, _ := classifyIdempotence(states, info); action != idemNotIdempotent {
		t.Errorf("non-idempotent batch (PID=-1) classified as %v, want idemNotIdempotent", action)
	}
}

// TestClassifyFirstBatchAcceptedAtSeqZero: a producer's very first
// batch on a partition must start at sequence 0. This is the
// post-InitProducerId, pre-any-traffic state.
func TestClassifyFirstBatchAcceptedAtSeqZero(t *testing.T) {
	states := map[int64]*producerEntry{}
	info := batchProducerInfo{producerID: 100, epoch: 0, firstSeq: 0, lastSeq: 4}
	action, savedOff := classifyIdempotence(states, info)
	if action != idemAccept {
		t.Errorf("action=%v, want idemAccept", action)
	}
	if savedOff != 0 {
		t.Errorf("savedOffset=%d, want 0 (only relevant for dedupe)", savedOff)
	}
}

// TestClassifyFirstBatchRejectedAtNonZeroSeq: a producer that
// somehow sends its first batch with baseSequence != 0 (bug,
// state corruption, or wire damage) gets OUT_OF_ORDER. Java's
// idempotent producer treats this as fatal and surfaces it as a
// KafkaException.
func TestClassifyFirstBatchRejectedAtNonZeroSeq(t *testing.T) {
	states := map[int64]*producerEntry{}
	info := batchProducerInfo{producerID: 100, epoch: 0, firstSeq: 7, lastSeq: 11}
	if action, _ := classifyIdempotence(states, info); action != idemOutOfOrder {
		t.Errorf("first batch with seq=7 classified as %v, want idemOutOfOrder", action)
	}
}

// TestClassifyContiguousAccept: the steady-state happy path —
// successive batches whose firstSeq picks up exactly where the
// previous batch's lastSeq left off.
func TestClassifyContiguousAccept(t *testing.T) {
	states := map[int64]*producerEntry{
		200: {epoch: 0, recent: []recentBatch{{firstSeq: 0, lastSeq: 4, baseOffset: 0}}},
	}
	info := batchProducerInfo{producerID: 200, epoch: 0, firstSeq: 5, lastSeq: 9}
	if action, _ := classifyIdempotence(states, info); action != idemAccept {
		t.Errorf("contiguous batch classified as %v, want idemAccept", action)
	}
}

// TestClassifyDuplicateLatest: producer didn't get a response for
// the most recent batch and retried. Broker must recognise the seq
// pair and return the same baseOffset, so the producer's
// "successfully sent" tracking lines up.
func TestClassifyDuplicateLatest(t *testing.T) {
	states := map[int64]*producerEntry{
		300: {epoch: 0, recent: []recentBatch{
			{firstSeq: 0, lastSeq: 4, baseOffset: 100},
			{firstSeq: 5, lastSeq: 9, baseOffset: 105},
		}},
	}
	info := batchProducerInfo{producerID: 300, epoch: 0, firstSeq: 5, lastSeq: 9}
	action, savedOff := classifyIdempotence(states, info)
	if action != idemDuplicate {
		t.Fatalf("duplicate batch classified as %v, want idemDuplicate", action)
	}
	if savedOff != 105 {
		t.Errorf("dedupe savedOffset=%d, want 105 (the original assignment)", savedOff)
	}
}

// TestClassifyDuplicateOldest pins the 5-batch window: even the
// oldest of the 5 in-flight batches must dedupe correctly. Java
// caps max.in.flight.requests.per.connection at 5 with idempotence
// on; if any retry of any in-flight batch fails to dedupe, the
// producer hits OUT_OF_ORDER and dies.
func TestClassifyDuplicateOldest(t *testing.T) {
	// 5-batch window, each 5 records wide.
	states := map[int64]*producerEntry{
		400: {epoch: 0, recent: []recentBatch{
			{firstSeq: 0, lastSeq: 4, baseOffset: 1000},  // <- oldest
			{firstSeq: 5, lastSeq: 9, baseOffset: 1005},
			{firstSeq: 10, lastSeq: 14, baseOffset: 1010},
			{firstSeq: 15, lastSeq: 19, baseOffset: 1015},
			{firstSeq: 20, lastSeq: 24, baseOffset: 1020}, // <- newest
		}},
	}
	info := batchProducerInfo{producerID: 400, epoch: 0, firstSeq: 0, lastSeq: 4}
	action, savedOff := classifyIdempotence(states, info)
	if action != idemDuplicate {
		t.Fatalf("oldest-window-batch retry classified as %v, want idemDuplicate", action)
	}
	if savedOff != 1000 {
		t.Errorf("oldest dedupe savedOffset=%d, want 1000", savedOff)
	}
}

// TestClassifyOutOfOrderGap: producer skips ahead — broker received
// 0..4 and the next batch arrives at seq=10. Returns 45.
func TestClassifyOutOfOrderGap(t *testing.T) {
	states := map[int64]*producerEntry{
		500: {epoch: 0, recent: []recentBatch{{firstSeq: 0, lastSeq: 4, baseOffset: 0}}},
	}
	info := batchProducerInfo{producerID: 500, epoch: 0, firstSeq: 10, lastSeq: 14}
	if action, _ := classifyIdempotence(states, info); action != idemOutOfOrder {
		t.Errorf("gap classified as %v, want idemOutOfOrder", action)
	}
}

// TestClassifyOutOfOrderBeyondWindow: a duplicate retry of a batch
// that was already pushed off the 5-batch window is unrecoverable
// — broker can't tell whether it's an old retry or a brand-new
// out-of-order batch. Defaults to OUT_OF_ORDER (producer dies); a
// future stage could distinguish DUPLICATE_SEQUENCE_NUMBER (46)
// here when we know we ARE looking at an old seq, but the spec is
// fine with 45.
func TestClassifyOutOfOrderBeyondWindow(t *testing.T) {
	states := map[int64]*producerEntry{
		600: {epoch: 0, recent: []recentBatch{
			{firstSeq: 25, lastSeq: 29, baseOffset: 25},
			{firstSeq: 30, lastSeq: 34, baseOffset: 30},
			{firstSeq: 35, lastSeq: 39, baseOffset: 35},
			{firstSeq: 40, lastSeq: 44, baseOffset: 40},
			{firstSeq: 45, lastSeq: 49, baseOffset: 45},
		}},
	}
	// firstSeq=0 is older than the oldest in-window batch (25..29).
	info := batchProducerInfo{producerID: 600, epoch: 0, firstSeq: 0, lastSeq: 4}
	if action, _ := classifyIdempotence(states, info); action != idemOutOfOrder {
		t.Errorf("beyond-window batch classified as %v, want idemOutOfOrder", action)
	}
}

// TestClassifyInvalidEpochStaleProducer: a zombie producer (one
// that was fenced by an epoch bump on the same PID) tries to write.
// Returns 47.
func TestClassifyInvalidEpochStaleProducer(t *testing.T) {
	states := map[int64]*producerEntry{
		700: {epoch: 5, recent: []recentBatch{{firstSeq: 0, lastSeq: 4, baseOffset: 0}}},
	}
	info := batchProducerInfo{producerID: 700, epoch: 4, firstSeq: 5, lastSeq: 9}
	if action, _ := classifyIdempotence(states, info); action != idemInvalidEpoch {
		t.Errorf("stale-epoch classified as %v, want idemInvalidEpoch", action)
	}
}

// TestClassifyEpochBumpResetsState: when a producer rotates its PID
// (KIP-360) or transactional epoch, the new epoch arrives with
// firstSeq=0 and must be accepted regardless of the previous
// epoch's last-seen sequence.
func TestClassifyEpochBumpResetsState(t *testing.T) {
	states := map[int64]*producerEntry{
		800: {epoch: 1, recent: []recentBatch{{firstSeq: 99, lastSeq: 102, baseOffset: 99}}},
	}
	info := batchProducerInfo{producerID: 800, epoch: 2, firstSeq: 0, lastSeq: 4}
	if action, _ := classifyIdempotence(states, info); action != idemAccept {
		t.Errorf("epoch-bump batch classified as %v, want idemAccept", action)
	}
}

// TestRecordOutcomeAdvancesWindow walks the post-Append bookkeeping
// across the 5-batch window. The 6th batch must evict the oldest
// (FIFO) — a regression that drops the newest would silently break
// the dedupe-of-latest path that Java's producer hits the most.
func TestRecordOutcomeAdvancesWindow(t *testing.T) {
	states := map[int64]*producerEntry{}
	for i := range int32(6) {
		info := batchProducerInfo{
			producerID: 900,
			epoch:      0,
			firstSeq:   i * 5,
			lastSeq:    i*5 + 4,
		}
		recordIdempotenceOutcome(states, info, int64(i*5))
	}
	entry := states[900]
	if entry == nil || len(entry.recent) != producerWindowSize {
		t.Fatalf("entry.recent len=%d, want %d", len(entry.recent), producerWindowSize)
	}
	// Oldest must be the SECOND batch (firstSeq=5), not the FIRST (firstSeq=0).
	if entry.recent[0].firstSeq != 5 {
		t.Errorf("oldest in window firstSeq=%d, want 5 (FIFO eviction)", entry.recent[0].firstSeq)
	}
	if entry.recent[len(entry.recent)-1].firstSeq != 25 {
		t.Errorf("newest in window firstSeq=%d, want 25", entry.recent[len(entry.recent)-1].firstSeq)
	}
}

// TestRecordOutcomeEpochBumpClearsHistory: a fresh epoch must
// drop the previous-epoch's window. Otherwise a retry of a
// previous-epoch batch would dedupe against state from a fenced
// generation — the OPPOSITE of what we want.
func TestRecordOutcomeEpochBumpClearsHistory(t *testing.T) {
	states := map[int64]*producerEntry{}
	recordIdempotenceOutcome(states, batchProducerInfo{producerID: 1000, epoch: 0, firstSeq: 0, lastSeq: 4}, 0)
	recordIdempotenceOutcome(states, batchProducerInfo{producerID: 1000, epoch: 1, firstSeq: 0, lastSeq: 4}, 100)

	entry := states[1000]
	if entry.epoch != 1 {
		t.Errorf("epoch=%d, want 1 after bump", entry.epoch)
	}
	if len(entry.recent) != 1 {
		t.Errorf("len(recent)=%d, want 1 (epoch bump must clear)", len(entry.recent))
	}
	if entry.recent[0].baseOffset != 100 {
		t.Errorf("recent[0].baseOffset=%d, want 100 (only the new-epoch batch)", entry.recent[0].baseOffset)
	}
}
