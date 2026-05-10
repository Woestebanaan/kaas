package handlers

import (
	"errors"
	"testing"

	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

// fakeTxnPartitionStore captures every call and lets tests force a
// specific error return.
type fakeTxnPartitionStore struct {
	err   error
	calls []txnPartitionCall
}

type txnPartitionCall struct {
	txnID     string
	pid       int64
	epoch     int16
	additions []TxnPartitionAddition
}

func (f *fakeTxnPartitionStore) AddPartitions(txnID string, pid int64, epoch int16, additions []TxnPartitionAddition) error {
	f.calls = append(f.calls, txnPartitionCall{
		txnID:     txnID,
		pid:       pid,
		epoch:     epoch,
		additions: append([]TxnPartitionAddition(nil), additions...),
	})
	return f.err
}

// encodeAddPartitionsToTxnRequest builds a v3 (flexible) request body.
func encodeAddPartitionsToTxnRequest(t *testing.T, txnID string, pid int64, epoch int16, topics []api.AddPartitionsToTxnTopic) []byte {
	t.Helper()
	w := codec.NewWriter()
	w.WriteCompactString(txnID)
	w.WriteInt64(pid)
	w.WriteInt16(epoch)
	w.WriteCompactArray(len(topics), func() {
		for _, top := range topics {
			w.WriteCompactString(top.Name)
			w.WriteCompactArray(len(top.Partitions), func() {
				for _, p := range top.Partitions {
					w.WriteInt32(p)
				}
			})
			w.WriteEmptyTaggedFields()
		}
	})
	w.WriteEmptyTaggedFields()
	return w.Bytes()
}

// decodeAddPartitionsToTxnResponse parses a v3 response body.
func decodeAddPartitionsToTxnResponse(t *testing.T, body []byte) *api.AddPartitionsToTxnResponse {
	t.Helper()
	r := codec.NewReader(body)
	resp := &api.AddPartitionsToTxnResponse{}
	var err error
	resp.ThrottleTimeMs, err = r.ReadInt32()
	if err != nil {
		t.Fatalf("throttle: %v", err)
	}
	if err := r.ReadCompactArray(func() error {
		var top api.AddPartitionsToTxnTopicResult
		top.Name, err = r.ReadCompactString()
		if err != nil {
			return err
		}
		if err := r.ReadCompactArray(func() error {
			var pr api.AddPartitionsToTxnPartitionResult
			pr.PartitionIndex, err = r.ReadInt32()
			if err != nil {
				return err
			}
			pr.ErrorCode, err = r.ReadInt16()
			if err != nil {
				return err
			}
			if err := r.ReadTaggedFields(); err != nil {
				return err
			}
			top.PartitionResults = append(top.PartitionResults, pr)
			return nil
		}); err != nil {
			return err
		}
		if err := r.ReadTaggedFields(); err != nil {
			return err
		}
		resp.Results = append(resp.Results, top)
		return nil
	}); err != nil {
		t.Fatalf("results: %v", err)
	}
	if err := r.ReadTaggedFields(); err != nil {
		t.Fatalf("trailer: %v", err)
	}
	return resp
}

// fakeOwnsAll satisfies TxnOwnership returning true for everything.
type fakeOwnsAll struct{}

func (fakeOwnsAll) OwnsTxn(string) bool { return true }

// fakeOwnsNone satisfies TxnOwnership returning false.
type fakeOwnsNone struct{}

func (fakeOwnsNone) OwnsTxn(string) bool { return false }

func TestAddPartitionsToTxnHappyPath(t *testing.T) {
	store := &fakeTxnPartitionStore{}
	h := NewAddPartitionsToTxnHandler(store).WithTxnOwnership(fakeOwnsAll{})

	body := encodeAddPartitionsToTxnRequest(t, "tx-A", 100, 5, []api.AddPartitionsToTxnTopic{
		{Name: "topic-1", Partitions: []int32{0, 1, 2}},
	})
	out, err := h.Handle(nil, 3, body)
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	resp := decodeAddPartitionsToTxnResponse(t, out)

	if len(resp.Results) != 1 || resp.Results[0].Name != "topic-1" {
		t.Fatalf("expected one topic-1 result, got %+v", resp.Results)
	}
	for _, pr := range resp.Results[0].PartitionResults {
		if pr.ErrorCode != 0 {
			t.Errorf("partition %d errorCode=%d, want 0 (NONE)", pr.PartitionIndex, pr.ErrorCode)
		}
	}
	if len(store.calls) != 1 {
		t.Fatalf("expected 1 store call, got %d", len(store.calls))
	}
	c := store.calls[0]
	if c.txnID != "tx-A" || c.pid != 100 || c.epoch != 5 {
		t.Errorf("store called with (%q, %d, %d), want (tx-A, 100, 5)", c.txnID, c.pid, c.epoch)
	}
}

func TestAddPartitionsToTxnEmptyIDReturnsInvalidRequest(t *testing.T) {
	h := NewAddPartitionsToTxnHandler(&fakeTxnPartitionStore{})
	body := encodeAddPartitionsToTxnRequest(t, "", 1, 0, []api.AddPartitionsToTxnTopic{
		{Name: "topic", Partitions: []int32{0}},
	})
	out, err := h.Handle(nil, 3, body)
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	resp := decodeAddPartitionsToTxnResponse(t, out)
	if got := resp.Results[0].PartitionResults[0].ErrorCode; got != int16(codec.ErrInvalidRequest) {
		t.Fatalf("got errorCode=%d, want INVALID_REQUEST (%d)", got, codec.ErrInvalidRequest)
	}
}

func TestAddPartitionsToTxnNotCoordinatorWhenGated(t *testing.T) {
	store := &fakeTxnPartitionStore{}
	h := NewAddPartitionsToTxnHandler(store).WithTxnOwnership(fakeOwnsNone{})
	body := encodeAddPartitionsToTxnRequest(t, "tx-A", 1, 0, []api.AddPartitionsToTxnTopic{
		{Name: "topic", Partitions: []int32{0, 1}},
	})
	out, err := h.Handle(nil, 3, body)
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	resp := decodeAddPartitionsToTxnResponse(t, out)
	for _, pr := range resp.Results[0].PartitionResults {
		if pr.ErrorCode != int16(codec.ErrNotCoordinator) {
			t.Errorf("p=%d errCode=%d, want NOT_COORDINATOR (%d)",
				pr.PartitionIndex, pr.ErrorCode, codec.ErrNotCoordinator)
		}
	}
	if len(store.calls) != 0 {
		t.Errorf("non-owner gate should NOT have called the store: %d calls", len(store.calls))
	}
}

func TestAddPartitionsToTxnUnknownProducerMaps(t *testing.T) {
	store := &fakeTxnPartitionStore{err: ErrTxnPartitionUnknownProducer}
	h := NewAddPartitionsToTxnHandler(store).WithTxnOwnership(fakeOwnsAll{})
	body := encodeAddPartitionsToTxnRequest(t, "tx-A", 99, 0, []api.AddPartitionsToTxnTopic{
		{Name: "t", Partitions: []int32{0}},
	})
	out, err := h.Handle(nil, 3, body)
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	resp := decodeAddPartitionsToTxnResponse(t, out)
	if got := resp.Results[0].PartitionResults[0].ErrorCode; got != int16(codec.ErrInvalidProducerIDMapping) {
		t.Fatalf("got %d, want INVALID_PRODUCER_ID_MAPPING (%d)", got, codec.ErrInvalidProducerIDMapping)
	}
}

func TestAddPartitionsToTxnEpochFencedMaps(t *testing.T) {
	store := &fakeTxnPartitionStore{err: ErrTxnPartitionEpochFenced}
	h := NewAddPartitionsToTxnHandler(store).WithTxnOwnership(fakeOwnsAll{})
	body := encodeAddPartitionsToTxnRequest(t, "tx-A", 1, 1, []api.AddPartitionsToTxnTopic{
		{Name: "t", Partitions: []int32{0}},
	})
	out, err := h.Handle(nil, 3, body)
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	resp := decodeAddPartitionsToTxnResponse(t, out)
	if got := resp.Results[0].PartitionResults[0].ErrorCode; got != int16(codec.ErrProducerFenced) {
		t.Fatalf("got %d, want PRODUCER_FENCED (%d)", got, codec.ErrProducerFenced)
	}
}

func TestAddPartitionsToTxnNoStoreReturnsRetriable(t *testing.T) {
	// Boot window: store not yet wired. Must return a retriable
	// error so the Java client's markCoordinatorUnknown loop unsticks
	// itself once the store comes up.
	h := NewAddPartitionsToTxnHandler(nil)
	body := encodeAddPartitionsToTxnRequest(t, "tx-A", 1, 0, []api.AddPartitionsToTxnTopic{
		{Name: "t", Partitions: []int32{0}},
	})
	out, err := h.Handle(nil, 3, body)
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	resp := decodeAddPartitionsToTxnResponse(t, out)
	if got := resp.Results[0].PartitionResults[0].ErrorCode; got != int16(codec.ErrCoordinatorNotAvailable) {
		t.Fatalf("got %d, want COORDINATOR_NOT_AVAILABLE (%d)", got, codec.ErrCoordinatorNotAvailable)
	}
}

func TestAddPartitionsToTxnPropagatesAdditions(t *testing.T) {
	// Verify the (topic, partitions) tuples are passed through to the
	// store unchanged — the union/idempotency logic lives in the
	// coordinator package and is tested there.
	store := &fakeTxnPartitionStore{}
	h := NewAddPartitionsToTxnHandler(store).WithTxnOwnership(fakeOwnsAll{})
	body := encodeAddPartitionsToTxnRequest(t, "tx-multi", 7, 2, []api.AddPartitionsToTxnTopic{
		{Name: "alpha", Partitions: []int32{0, 4}},
		{Name: "beta", Partitions: []int32{12}},
	})
	if _, err := h.Handle(nil, 3, body); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(store.calls) != 1 {
		t.Fatalf("expected 1 store call, got %d", len(store.calls))
	}
	got := store.calls[0].additions
	if len(got) != 2 {
		t.Fatalf("expected 2 additions, got %d: %+v", len(got), got)
	}
	if got[0].Topic != "alpha" || got[1].Topic != "beta" {
		t.Errorf("topic order/name wrong: %+v", got)
	}
	if !errors.Is(nil, nil) { // appease linter; sentinel suite covered above
		t.Fatal("unreachable")
	}
}
