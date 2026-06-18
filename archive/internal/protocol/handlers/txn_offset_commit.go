package handlers

import (
	"fmt"

	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

// TxnOffsetCommitter is the slim interface TxnOffsetCommitHandler
// needs from coordinator.Manager. Production wires
// (*coordinator.Manager).TxnOffsetCommit; the abstraction keeps the
// handlers package free of a Manager import-path constraint.
type TxnOffsetCommitter interface {
	TxnOffsetCommit(req *api.TxnOffsetCommitRequest) *api.TxnOffsetCommitResponse
}

// TxnOffsetCommitHandler answers API key 28 v0–v3. gh #27.
//
// Routes through the GROUP coordinator (Manager.TxnOffsetCommit
// gates on OwnsGroup). The handler is a thin shell over the
// Manager method — same pattern as OffsetCommit.
type TxnOffsetCommitHandler struct {
	mgr TxnOffsetCommitter
}

func NewTxnOffsetCommitHandler(mgr TxnOffsetCommitter) *TxnOffsetCommitHandler {
	return &TxnOffsetCommitHandler{mgr: mgr}
}

func (h *TxnOffsetCommitHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeTxnOffsetCommitRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("txn-offset-commit decode: %w", err)
	}
	resp := h.mgr.TxnOffsetCommit(req)
	w := codec.NewWriter()
	api.EncodeTxnOffsetCommitResponse(w, resp, version)
	return w.Bytes(), nil
}
