package handlers

import (
	"errors"
	"fmt"

	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

// TxnGroupStore is the slim interface AddOffsetsToTxnHandler needs.
// Mirrors TxnPartitionStore from gh #23 — keeps coordinator → handler
// flow translatable without import cycle. Production wires
// coordinator.TxnStateStore via an adapter in broker.go.
type TxnGroupStore interface {
	AddOffsetsToTxn(txnID string, pid int64, epoch int16, groupID string) error
}

// Sentinels — same shape as the gh #23/#25 handler sentinels.
var (
	ErrTxnGroupEmptyID         = errors.New("addoffsets: empty transactional id")
	ErrTxnGroupUnknownProducer = errors.New("addoffsets: unknown txnID or pid mismatch")
	ErrTxnGroupEpochFenced     = errors.New("addoffsets: producer epoch fenced")
	ErrTxnGroupConcurrent      = errors.New("addoffsets: concurrent transition in progress")
	ErrTxnGroupInvalidState    = errors.New("addoffsets: invalid state transition")
)

// AddOffsetsToTxnHandler answers API key 25 v0–v3. gh #24 — sibling
// of AddPartitionsToTxn for the offset path. A transactional
// producer calls this before TxnOffsetCommit to tell the txn
// coordinator "I'm committing offsets for group G as part of this
// txn", so EndTxn can drive each group's offset commit/abort.
type AddOffsetsToTxnHandler struct {
	store     TxnGroupStore
	ownership TxnOwnership
}

func NewAddOffsetsToTxnHandler(store TxnGroupStore) *AddOffsetsToTxnHandler {
	return &AddOffsetsToTxnHandler{store: store}
}

func (h *AddOffsetsToTxnHandler) WithTxnOwnership(o TxnOwnership) *AddOffsetsToTxnHandler {
	h.ownership = o
	return h
}

func (h *AddOffsetsToTxnHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeAddOffsetsToTxnRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("add-offsets-to-txn decode: %w", err)
	}
	resp := &api.AddOffsetsToTxnResponse{ErrorCode: h.process(req)}
	w := codec.NewWriter()
	api.EncodeAddOffsetsToTxnResponse(w, resp, version)
	return w.Bytes(), nil
}

func (h *AddOffsetsToTxnHandler) process(req *api.AddOffsetsToTxnRequest) int16 {
	if req.TransactionalID == "" {
		return int16(codec.ErrInvalidRequest)
	}
	if req.GroupID == "" {
		return int16(codec.ErrInvalidGroupID)
	}
	if h.ownership != nil && !h.ownership.OwnsTxn(req.TransactionalID) {
		return int16(codec.ErrNotCoordinator)
	}
	if h.store == nil {
		return int16(codec.ErrCoordinatorNotAvailable)
	}
	err := h.store.AddOffsetsToTxn(req.TransactionalID, req.ProducerID, req.ProducerEpoch, req.GroupID)
	switch {
	case err == nil:
		return 0
	case errors.Is(err, ErrTxnGroupEmptyID):
		return int16(codec.ErrInvalidRequest)
	case errors.Is(err, ErrTxnGroupUnknownProducer):
		return int16(codec.ErrInvalidProducerIDMapping)
	case errors.Is(err, ErrTxnGroupEpochFenced):
		return int16(codec.ErrProducerFenced)
	case errors.Is(err, ErrTxnGroupConcurrent):
		return int16(codec.ErrConcurrentTransactions)
	case errors.Is(err, ErrTxnGroupInvalidState):
		return int16(codec.ErrInvalidTxnState)
	default:
		return int16(codec.ErrUnknownServerError)
	}
}
