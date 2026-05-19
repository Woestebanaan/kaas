package handlers

import (
	"errors"
	"testing"

	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
)

// fakeTxnGroupStore is a recording stub for the AddOffsetsToTxn
// store dependency. Tests prog the err field to exercise each
// error-code branch in the handler.
type fakeTxnGroupStore struct {
	called  bool
	lastTxn string
	lastPID int64
	lastEp  int16
	lastGrp string
	err     error
}

func (f *fakeTxnGroupStore) AddOffsetsToTxn(txnID string, pid int64, epoch int16, groupID string) error {
	f.called = true
	f.lastTxn = txnID
	f.lastPID = pid
	f.lastEp = epoch
	f.lastGrp = groupID
	return f.err
}

// txnOwnershipStub is a simpler OwnsTxn stub for these tests — the
// init_producer_id_test.go file already owns fakeTxnOwnership with a
// richer call-log shape.
type txnOwnershipStub struct{ owns bool }

func (f txnOwnershipStub) OwnsTxn(_ string) bool { return f.owns }

// encodeAddOffsetsRequestV3 hand-encodes a v3 (flexible) request body
// matching what AdminClient sends.
func encodeAddOffsetsRequestV3(t *testing.T, txn string, pid int64, epoch int16, group string) []byte {
	t.Helper()
	w := codec.NewWriter()
	w.WriteCompactString(txn)
	w.WriteInt64(pid)
	w.WriteInt16(epoch)
	w.WriteCompactString(group)
	w.WriteEmptyTaggedFields()
	return w.Bytes()
}

// decodeAddOffsetsErrorCode reads the ErrorCode int16 from the start
// of a v3 (flexible) AddOffsetsToTxnResponse body. Layout:
//
//	throttle_time_ms (int32) | error_code (int16) | tagged_fields (1 byte)
func decodeAddOffsetsErrorCode(t *testing.T, body []byte) int16 {
	t.Helper()
	r := codec.NewReader(body)
	_, _ = r.ReadInt32() // throttle
	code, err := r.ReadInt16()
	if err != nil {
		t.Fatalf("read error code: %v", err)
	}
	return code
}

// TestAddOffsetsToTxn_HappyPath pins gh #24: a valid request with a
// non-empty txnID + groupID, store accepts → ErrorCode=0 and the
// store was called with the request fields verbatim.
func TestAddOffsetsToTxn_HappyPath(t *testing.T) {
	store := &fakeTxnGroupStore{}
	h := NewAddOffsetsToTxnHandler(store)
	body := encodeAddOffsetsRequestV3(t, "txn-1", 100, 5, "group-A")
	out, err := h.Handle(&connstate.ConnState{}, 3, body)
	if err != nil {
		t.Fatal(err)
	}
	if code := decodeAddOffsetsErrorCode(t, out); code != 0 {
		t.Errorf("ErrorCode=%d, want 0", code)
	}
	if !store.called {
		t.Error("store.AddOffsetsToTxn was not called")
	}
	if store.lastTxn != "txn-1" || store.lastPID != 100 || store.lastEp != 5 || store.lastGrp != "group-A" {
		t.Errorf("store args = (%q, %d, %d, %q); want (txn-1, 100, 5, group-A)",
			store.lastTxn, store.lastPID, store.lastEp, store.lastGrp)
	}
}

// TestAddOffsetsToTxn_EmptyTxnIDRejected pins INVALID_REQUEST (42)
// for an empty TransactionalID. Apache surfaces this same code.
func TestAddOffsetsToTxn_EmptyTxnIDRejected(t *testing.T) {
	store := &fakeTxnGroupStore{}
	h := NewAddOffsetsToTxnHandler(store)
	body := encodeAddOffsetsRequestV3(t, "", 100, 5, "group-A")
	out, _ := h.Handle(&connstate.ConnState{}, 3, body)
	if code := decodeAddOffsetsErrorCode(t, out); code != int16(codec.ErrInvalidRequest) {
		t.Errorf("ErrorCode=%d, want %d (ErrInvalidRequest)", code, codec.ErrInvalidRequest)
	}
	if store.called {
		t.Error("store.AddOffsetsToTxn was called for an empty TxnID — should short-circuit")
	}
}

// TestAddOffsetsToTxn_EmptyGroupIDRejected pins
// INVALID_GROUP_ID (24) per the same fast-path validation.
func TestAddOffsetsToTxn_EmptyGroupIDRejected(t *testing.T) {
	store := &fakeTxnGroupStore{}
	h := NewAddOffsetsToTxnHandler(store)
	body := encodeAddOffsetsRequestV3(t, "txn-1", 100, 5, "")
	out, _ := h.Handle(&connstate.ConnState{}, 3, body)
	if code := decodeAddOffsetsErrorCode(t, out); code != int16(codec.ErrInvalidGroupID) {
		t.Errorf("ErrorCode=%d, want %d (ErrInvalidGroupID)", code, codec.ErrInvalidGroupID)
	}
}

// TestAddOffsetsToTxn_NotCoordinatorRouting pins the gh #91-style
// txn-coordinator routing gate: when ownership reports !OwnsTxn, the
// handler returns NOT_COORDINATOR (16) so the client retries
// FindCoordinator.
func TestAddOffsetsToTxn_NotCoordinatorRouting(t *testing.T) {
	store := &fakeTxnGroupStore{}
	h := NewAddOffsetsToTxnHandler(store).WithTxnOwnership(txnOwnershipStub{owns: false})
	body := encodeAddOffsetsRequestV3(t, "txn-1", 100, 5, "group-A")
	out, _ := h.Handle(&connstate.ConnState{}, 3, body)
	if code := decodeAddOffsetsErrorCode(t, out); code != int16(codec.ErrNotCoordinator) {
		t.Errorf("ErrorCode=%d, want %d (ErrNotCoordinator)", code, codec.ErrNotCoordinator)
	}
	if store.called {
		t.Error("store was consulted despite !OwnsTxn — routing gate broken")
	}
}

// TestAddOffsetsToTxn_StoreErrors pins the sentinel → wire-code map.
// Each store-side error must land on the right Apache error code so
// the producer's exception-handling logic fires correctly.
func TestAddOffsetsToTxn_StoreErrors(t *testing.T) {
	cases := []struct {
		err  error
		code int16
		name string
	}{
		{ErrTxnGroupUnknownProducer, int16(codec.ErrInvalidProducerIDMapping), "unknown producer"},
		{ErrTxnGroupEpochFenced, int16(codec.ErrProducerFenced), "epoch fenced"},
		{ErrTxnGroupConcurrent, int16(codec.ErrConcurrentTransactions), "concurrent"},
		{ErrTxnGroupInvalidState, int16(codec.ErrInvalidTxnState), "invalid state"},
		{errors.New("io: short read"), int16(codec.ErrUnknownServerError), "generic falls through"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeTxnGroupStore{err: tc.err}
			h := NewAddOffsetsToTxnHandler(store)
			out, _ := h.Handle(&connstate.ConnState{}, 3, encodeAddOffsetsRequestV3(t, "txn-1", 100, 5, "group-A"))
			if code := decodeAddOffsetsErrorCode(t, out); code != tc.code {
				t.Errorf("err=%v: ErrorCode=%d, want %d", tc.err, code, tc.code)
			}
		})
	}
}

// TestAddOffsetsToTxn_NilStoreFailsCleanly pins the dev-mode path:
// when no store is wired, the handler reports
// COORDINATOR_NOT_AVAILABLE (15) — retry-friendly so the client
// doesn't crash.
func TestAddOffsetsToTxn_NilStoreFailsCleanly(t *testing.T) {
	h := NewAddOffsetsToTxnHandler(nil)
	out, _ := h.Handle(&connstate.ConnState{}, 3, encodeAddOffsetsRequestV3(t, "txn-1", 100, 5, "group-A"))
	if code := decodeAddOffsetsErrorCode(t, out); code != int16(codec.ErrCoordinatorNotAvailable) {
		t.Errorf("nil-store ErrorCode=%d, want %d", code, codec.ErrCoordinatorNotAvailable)
	}
}
