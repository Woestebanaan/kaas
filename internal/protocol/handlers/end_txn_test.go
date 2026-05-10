package handlers

import (
	"testing"

	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

// fakeTxnEndStore captures every call and lets tests force the error.
type fakeTxnEndStore struct {
	err   error
	calls []txnEndCall
}

type txnEndCall struct {
	txnID  string
	pid    int64
	epoch  int16
	commit bool
}

func (f *fakeTxnEndStore) EndTxn(txnID string, pid int64, epoch int16, commit bool) error {
	f.calls = append(f.calls, txnEndCall{txnID, pid, epoch, commit})
	return f.err
}

func encodeEndTxnRequest(t *testing.T, txnID string, pid int64, epoch int16, committed bool) []byte {
	t.Helper()
	w := codec.NewWriter()
	w.WriteCompactString(txnID)
	w.WriteInt64(pid)
	w.WriteInt16(epoch)
	if committed {
		w.WriteInt8(1)
	} else {
		w.WriteInt8(0)
	}
	w.WriteEmptyTaggedFields()
	return w.Bytes()
}

func decodeEndTxnResponse(t *testing.T, body []byte) *api.EndTxnResponse {
	t.Helper()
	r := codec.NewReader(body)
	resp := &api.EndTxnResponse{}
	var err error
	resp.ThrottleTimeMs, err = r.ReadInt32()
	if err != nil {
		t.Fatalf("throttle: %v", err)
	}
	resp.ErrorCode, err = r.ReadInt16()
	if err != nil {
		t.Fatalf("errcode: %v", err)
	}
	if err := r.ReadTaggedFields(); err != nil {
		t.Fatalf("trailer: %v", err)
	}
	return resp
}

func TestEndTxnHappyCommit(t *testing.T) {
	store := &fakeTxnEndStore{}
	h := NewEndTxnHandler(store).WithTxnOwnership(fakeOwnsAll{})
	body := encodeEndTxnRequest(t, "tx-A", 100, 5, true)
	out, err := h.Handle(nil, 3, body)
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	resp := decodeEndTxnResponse(t, out)
	if resp.ErrorCode != 0 {
		t.Fatalf("errCode=%d, want 0 (NONE)", resp.ErrorCode)
	}
	if len(store.calls) != 1 {
		t.Fatalf("expected 1 store call, got %d", len(store.calls))
	}
	c := store.calls[0]
	if c.txnID != "tx-A" || c.pid != 100 || c.epoch != 5 || !c.commit {
		t.Errorf("store call mismatch: %+v", c)
	}
}

func TestEndTxnHappyAbort(t *testing.T) {
	store := &fakeTxnEndStore{}
	h := NewEndTxnHandler(store).WithTxnOwnership(fakeOwnsAll{})
	out, err := h.Handle(nil, 3, encodeEndTxnRequest(t, "tx-A", 1, 0, false))
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	if got := decodeEndTxnResponse(t, out).ErrorCode; got != 0 {
		t.Fatalf("errCode=%d, want NONE", got)
	}
	if !store.calls[0].commit == false {
		t.Errorf("commit flag drift: %+v", store.calls)
	}
	if store.calls[0].commit {
		t.Errorf("commit=true, want false (abort)")
	}
}

func TestEndTxnEmptyIDReturnsInvalidRequest(t *testing.T) {
	h := NewEndTxnHandler(&fakeTxnEndStore{})
	out, _ := h.Handle(nil, 3, encodeEndTxnRequest(t, "", 1, 0, true))
	if got := decodeEndTxnResponse(t, out).ErrorCode; got != int16(codec.ErrInvalidRequest) {
		t.Fatalf("got %d, want INVALID_REQUEST", got)
	}
}

func TestEndTxnNotCoordinatorWhenGated(t *testing.T) {
	store := &fakeTxnEndStore{}
	h := NewEndTxnHandler(store).WithTxnOwnership(fakeOwnsNone{})
	out, _ := h.Handle(nil, 3, encodeEndTxnRequest(t, "tx-A", 1, 0, true))
	if got := decodeEndTxnResponse(t, out).ErrorCode; got != int16(codec.ErrNotCoordinator) {
		t.Fatalf("got %d, want NOT_COORDINATOR", got)
	}
	if len(store.calls) != 0 {
		t.Errorf("gate should NOT have called store: %+v", store.calls)
	}
}

func TestEndTxnNoStoreReturnsRetriable(t *testing.T) {
	h := NewEndTxnHandler(nil)
	out, _ := h.Handle(nil, 3, encodeEndTxnRequest(t, "tx-A", 1, 0, true))
	if got := decodeEndTxnResponse(t, out).ErrorCode; got != int16(codec.ErrCoordinatorNotAvailable) {
		t.Fatalf("got %d, want COORDINATOR_NOT_AVAILABLE", got)
	}
}

func TestEndTxnUnknownProducerMaps(t *testing.T) {
	store := &fakeTxnEndStore{err: ErrTxnEndUnknownProducer}
	h := NewEndTxnHandler(store).WithTxnOwnership(fakeOwnsAll{})
	out, _ := h.Handle(nil, 3, encodeEndTxnRequest(t, "tx-A", 1, 0, true))
	if got := decodeEndTxnResponse(t, out).ErrorCode; got != int16(codec.ErrInvalidProducerIDMapping) {
		t.Fatalf("got %d, want INVALID_PRODUCER_ID_MAPPING", got)
	}
}

func TestEndTxnEpochFencedMaps(t *testing.T) {
	store := &fakeTxnEndStore{err: ErrTxnEndEpochFenced}
	h := NewEndTxnHandler(store).WithTxnOwnership(fakeOwnsAll{})
	out, _ := h.Handle(nil, 3, encodeEndTxnRequest(t, "tx-A", 1, 0, true))
	if got := decodeEndTxnResponse(t, out).ErrorCode; got != int16(codec.ErrProducerFenced) {
		t.Fatalf("got %d, want PRODUCER_FENCED", got)
	}
}

func TestEndTxnConcurrentMaps(t *testing.T) {
	store := &fakeTxnEndStore{err: ErrTxnEndConcurrent}
	h := NewEndTxnHandler(store).WithTxnOwnership(fakeOwnsAll{})
	out, _ := h.Handle(nil, 3, encodeEndTxnRequest(t, "tx-A", 1, 0, true))
	if got := decodeEndTxnResponse(t, out).ErrorCode; got != int16(codec.ErrConcurrentTransactions) {
		t.Fatalf("got %d, want CONCURRENT_TRANSACTIONS", got)
	}
}

func TestEndTxnInvalidStateMaps(t *testing.T) {
	store := &fakeTxnEndStore{err: ErrTxnEndInvalidState}
	h := NewEndTxnHandler(store).WithTxnOwnership(fakeOwnsAll{})
	out, _ := h.Handle(nil, 3, encodeEndTxnRequest(t, "tx-A", 1, 0, true))
	if got := decodeEndTxnResponse(t, out).ErrorCode; got != int16(codec.ErrInvalidTxnState) {
		t.Fatalf("got %d, want INVALID_TXN_STATE", got)
	}
}
