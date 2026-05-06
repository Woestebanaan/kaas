package handlers

import (
	"sync"
	"testing"

	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
)

// TestInitProducerIdMonotonic guards the contract gh #12 stage A
// makes to clients: every call returns a distinct producer ID, and
// IDs are monotonic so log diffs across reconnects are sortable. A
// regression that returns a constant PID would silently break
// kafka-verifiable-producer's per-message tagging once stage B
// enforces (PID, sequence) uniqueness.
func TestInitProducerIdMonotonic(t *testing.T) {
	h := NewInitProducerIdHandler()

	pid1 := callInitPID(t, h, 0)
	pid2 := callInitPID(t, h, 0)
	pid3 := callInitPID(t, h, 4)

	if pid1 == pid2 || pid2 == pid3 {
		t.Errorf("PIDs should be distinct: %d, %d, %d", pid1, pid2, pid3)
	}
	if pid2 <= pid1 || pid3 <= pid2 {
		t.Errorf("PIDs should be monotonic: %d, %d, %d", pid1, pid2, pid3)
	}
}

// TestInitProducerIdConcurrent guards uniqueness under concurrent
// callers — the atomic counter must not hand out duplicates when
// many connections race the call (the realistic startup pattern when
// a Kafka Streams app boots a topology with N stream threads).
func TestInitProducerIdConcurrent(t *testing.T) {
	h := NewInitProducerIdHandler()
	const n = 64
	pids := make([]int64, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			pids[idx] = callInitPID(t, h, 4)
		}(i)
	}
	wg.Wait()

	seen := make(map[int64]struct{}, n)
	for _, p := range pids {
		if _, dup := seen[p]; dup {
			t.Errorf("duplicate PID handed out: %d", p)
		}
		seen[p] = struct{}{}
	}
}

// TestInitProducerIdEpochZero pins epoch=0 for all freshly-allocated
// PIDs. Stage A of #12 doesn't track per-PID generations; a client
// that retries InitProducerId always gets a brand-new PID, so the
// epoch starts fresh at 0. Once stage B lands and we track stored
// state, this test will need updating to reflect epoch bumps.
func TestInitProducerIdEpochZero(t *testing.T) {
	h := NewInitProducerIdHandler()

	w := codec.NewWriter()
	w.WriteCompactNullableString("", true)
	w.WriteInt32(60_000)
	w.WriteInt64(-1)
	w.WriteInt16(-1)
	w.WriteEmptyTaggedFields()

	out, err := h.Handle(&connstate.ConnState{}, 4, w.Bytes())
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	r := codec.NewReader(out)
	_, _ = r.ReadInt32() // throttle
	errCode, _ := r.ReadInt16()
	if errCode != 0 {
		t.Errorf("errCode=%d, want 0", errCode)
	}
	_, _ = r.ReadInt64() // pid
	epoch, _ := r.ReadInt16()
	if epoch != 0 {
		t.Errorf("epoch=%d, want 0 (stage A always returns fresh epoch)", epoch)
	}
}

// fakeTxnStore is a minimal in-memory TxnStateStore for the
// handler tests. Mirrors the production store's
// (PID stays stable, epoch bumps) contract without touching disk.
type fakeTxnStore struct {
	state map[string]struct {
		pid   int64
		epoch int16
	}
}

func newFakeTxnStore() *fakeTxnStore {
	return &fakeTxnStore{state: map[string]struct {
		pid   int64
		epoch int16
	}{}}
}

func (f *fakeTxnStore) GetOrAllocate(txnID string, alloc func() int64) (int64, int16, error) {
	e, ok := f.state[txnID]
	if !ok {
		e.pid = alloc()
		e.epoch = 0
	} else {
		e.epoch++
	}
	f.state[txnID] = e
	return e.pid, e.epoch, nil
}

// TestInitProducerIdTxnIDBumpsEpochOnRejoin guards gh #22's
// handler-level contract: a request carrying the same
// transactional.id twice gets the SAME PID with epoch+1 the
// second time. Without this, the storage-layer fence at
// classifyIdempotence has nothing to fence — both sessions
// write under (PID, epoch=0) and a zombie's records appear as
// legitimate.
func TestInitProducerIdTxnIDBumpsEpochOnRejoin(t *testing.T) {
	h := NewInitProducerIdHandler().WithTxnStateStore(newFakeTxnStore())

	pid1, ep1 := callInitPIDWithTxn(t, h, "my-txn", 4)
	pid2, ep2 := callInitPIDWithTxn(t, h, "my-txn", 4)

	if pid1 != pid2 {
		t.Errorf("PIDs drifted across rejoins: %d vs %d (must be stable)", pid1, pid2)
	}
	if ep1 != 0 {
		t.Errorf("first call epoch=%d, want 0", ep1)
	}
	if ep2 != 1 {
		t.Errorf("rejoin epoch=%d, want 1 (gh #22 epoch fence)", ep2)
	}
}

// TestInitProducerIdEmptyTxnIDStillFresh: the empty
// transactional.id (the wire sentinel for non-transactional
// idempotent producers) must keep stage-A behaviour even when
// the txn store is wired. Otherwise the store would have to grow
// an entry per producer connection — unbounded.
func TestInitProducerIdEmptyTxnIDStillFresh(t *testing.T) {
	h := NewInitProducerIdHandler().WithTxnStateStore(newFakeTxnStore())

	pid1 := callInitPID(t, h, 4)
	pid2 := callInitPID(t, h, 4)

	if pid1 == pid2 {
		t.Errorf("non-txn producers got the same PID %d (must be fresh each time)", pid1)
	}
}

// TestInitProducerIdNoStoreFallsBackToFreshPID guards the warn-and-
// continue behaviour when the txn store fails to wire (disk
// missing, permission error, dev mode). Producers can still write
// — they just lose the rejoin fence. A reject here would prevent
// a broker that can't open its data dir from serving any
// transactional client at all.
func TestInitProducerIdNoStoreFallsBackToFreshPID(t *testing.T) {
	h := NewInitProducerIdHandler() // no store wired

	pid1, ep1 := callInitPIDWithTxn(t, h, "my-txn", 4)
	pid2, ep2 := callInitPIDWithTxn(t, h, "my-txn", 4)

	if pid1 == pid2 {
		t.Errorf("no-store fallback should hand out distinct PIDs each time, got %d twice", pid1)
	}
	if ep1 != 0 || ep2 != 0 {
		t.Errorf("no-store fallback epochs=(%d,%d), want (0,0)", ep1, ep2)
	}
}

// fakeFencer records every FenceProducerEpoch call so tests can
// assert exactly when the cross-partition fence fires.
type fakeFencer struct {
	calls []fakeFenceCall
}
type fakeFenceCall struct {
	pid   int64
	epoch int16
}

func (f *fakeFencer) FenceProducerEpoch(pid int64, epoch int16) {
	f.calls = append(f.calls, fakeFenceCall{pid, epoch})
}

// TestInitProducerIdFencerNotCalledOnFirstAlloc: the first
// InitProducerId for a new transactional.id has no previous
// session to fence. Calling FenceProducerEpoch with epoch=0
// would be a no-op (FenceProducerEpoch only advances), but a
// regression that fired it spuriously would still be confusing
// in logs. Pin that we skip it.
func TestInitProducerIdFencerNotCalledOnFirstAlloc(t *testing.T) {
	fencer := &fakeFencer{}
	h := NewInitProducerIdHandler().
		WithTxnStateStore(newFakeTxnStore()).
		WithFencer(fencer)

	callInitPIDWithTxn(t, h, "first-time", 4)

	if len(fencer.calls) != 0 {
		t.Errorf("first InitProducerId fired fence: %+v", fencer.calls)
	}
}

// TestInitProducerIdFencerCalledOnRejoin pins the gh #30
// callback wiring: every bump from epoch=N to N+1 produces a
// FenceProducerEpoch(pid, N+1) call so the storage layer
// rejects any in-flight epoch=N writes broker-wide.
func TestInitProducerIdFencerCalledOnRejoin(t *testing.T) {
	fencer := &fakeFencer{}
	h := NewInitProducerIdHandler().
		WithTxnStateStore(newFakeTxnStore()).
		WithFencer(fencer)

	pid1, _ := callInitPIDWithTxn(t, h, "rejoiner", 4)
	pid2, ep2 := callInitPIDWithTxn(t, h, "rejoiner", 4)

	if len(fencer.calls) != 1 {
		t.Fatalf("expected 1 fence call after one rejoin, got %d: %+v", len(fencer.calls), fencer.calls)
	}
	if fencer.calls[0].pid != pid2 || fencer.calls[0].epoch != ep2 {
		t.Errorf("fence called with (%d, %d), want (%d, %d)",
			fencer.calls[0].pid, fencer.calls[0].epoch, pid2, ep2)
	}
	// Sanity: PID didn't change between calls (this is the gh #22
	// invariant the gh #30 fence depends on — we want to fence
	// THIS pid, not a different one).
	if pid1 != pid2 {
		t.Errorf("PID changed across rejoin: %d → %d", pid1, pid2)
	}
}

// TestInitProducerIdFencerNotCalledForEmptyTxnID: the empty
// transactional.id sentinel skips the txn store entirely; with
// no store interaction, there's no rejoin signal, so the fence
// must not fire. Otherwise every non-transactional idempotent
// producer's startup would broadcast a useless fence call.
func TestInitProducerIdFencerNotCalledForEmptyTxnID(t *testing.T) {
	fencer := &fakeFencer{}
	h := NewInitProducerIdHandler().
		WithTxnStateStore(newFakeTxnStore()).
		WithFencer(fencer)

	callInitPID(t, h, 4)
	callInitPID(t, h, 4)

	if len(fencer.calls) != 0 {
		t.Errorf("empty-txn-id calls fired fence: %+v", fencer.calls)
	}
}

// callInitPIDWithTxn is callInitPID with a non-empty
// transactional.id field. v4 only — v0/v1 don't have the
// PID/epoch echo fields but they DO carry transactional.id.
func callInitPIDWithTxn(t *testing.T, h *InitProducerIdHandler, txnID string, version int16) (int64, int16) {
	t.Helper()
	w := codec.NewWriter()
	if version >= 2 {
		w.WriteCompactNullableString(txnID, txnID == "")
		w.WriteInt32(60_000)
		if version >= 3 {
			w.WriteInt64(-1)
			w.WriteInt16(-1)
		}
		w.WriteEmptyTaggedFields()
	} else {
		w.WriteNullableString(txnID, txnID == "")
		w.WriteInt32(60_000)
	}
	out, err := h.Handle(&connstate.ConnState{}, version, w.Bytes())
	if err != nil {
		t.Fatalf("Handle v%d: %v", version, err)
	}
	r := codec.NewReader(out)
	if _, err = r.ReadInt32(); err != nil { // throttle
		t.Fatal(err)
	}
	if _, err = r.ReadInt16(); err != nil { // errCode
		t.Fatal(err)
	}
	pid, err := r.ReadInt64()
	if err != nil {
		t.Fatal(err)
	}
	epoch, err := r.ReadInt16()
	if err != nil {
		t.Fatal(err)
	}
	return pid, epoch
}

// callInitPID is a v0/v4-aware helper that runs one InitProducerId
// call through the handler and returns the producer ID. v0 uses the
// legacy nullable-string + int32 timeout body; v4 adds the compact
// header, PID/epoch hint, and trailing tagged fields.
func callInitPID(t *testing.T, h *InitProducerIdHandler, version int16) int64 {
	t.Helper()
	w := codec.NewWriter()
	if version >= 2 {
		w.WriteCompactNullableString("", true)
		w.WriteInt32(60_000)
		if version >= 3 {
			w.WriteInt64(-1)
			w.WriteInt16(-1)
		}
		w.WriteEmptyTaggedFields()
	} else {
		w.WriteNullableString("", true)
		w.WriteInt32(60_000)
	}
	out, err := h.Handle(&connstate.ConnState{}, version, w.Bytes())
	if err != nil {
		t.Fatalf("Handle v%d: %v", version, err)
	}
	r := codec.NewReader(out)
	if _, err = r.ReadInt32(); err != nil { // throttle
		t.Fatal(err)
	}
	if _, err = r.ReadInt16(); err != nil { // errCode — checked separately
		t.Fatal(err)
	}
	pid, err := r.ReadInt64()
	if err != nil {
		t.Fatal(err)
	}
	return pid
}
