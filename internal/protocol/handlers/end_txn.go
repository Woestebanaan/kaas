package handlers

import (
	"errors"
	"fmt"

	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

// TxnEndStore is the slim interface EndTxnHandler needs from
// coordinator.TxnStateStore. Defined here to avoid the
// handlers→coordinator import cycle (same pattern as
// TxnPartitionStore for gh #23).
type TxnEndStore interface {
	EndTxn(txnID string, pid int64, epoch int16, commit bool) error
}

// Sentinel errors EndTxnHandler maps to Kafka wire codes. The store
// implementation returns these via exact-match sentinel.
var (
	ErrTxnEndEmptyID         = errors.New("endtxn: empty transactional id")
	ErrTxnEndUnknownProducer = errors.New("endtxn: unknown txnID or pid mismatch")
	ErrTxnEndEpochFenced     = errors.New("endtxn: producer epoch fenced")
	ErrTxnEndConcurrent      = errors.New("endtxn: concurrent transition in progress")
	ErrTxnEndInvalidState    = errors.New("endtxn: invalid state transition")
)

// EndTxnHandler answers API key 26 v0–v3. gh #25 (commit) + gh #26
// (abort). State-machine + persistence only — actual transactional
// marker batch writes to each (topic, partition) in the txn's
// partition list are deferred to WriteTxnMarkers (API key 27) and
// read-committed isolation (#31). Without those follow-ups EndTxn
// is a no-op from a Fetch-side consumer's perspective, but the
// state-machine path is needed for the Java producer to consider
// commitTransaction() / abortTransaction() complete.
type EndTxnHandler struct {
	store     TxnEndStore
	ownership TxnOwnership // nil ⇒ gh #91 gate disabled
}

func NewEndTxnHandler(store TxnEndStore) *EndTxnHandler {
	return &EndTxnHandler{store: store}
}

// WithTxnOwnership wires the gh #91 routing gate. Non-owners
// surface NOT_COORDINATOR.
func (h *EndTxnHandler) WithTxnOwnership(o TxnOwnership) *EndTxnHandler {
	h.ownership = o
	return h
}

func (h *EndTxnHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeEndTxnRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("end-txn decode: %w", err)
	}

	resp := &api.EndTxnResponse{ErrorCode: h.process(req)}
	w := codec.NewWriter()
	api.EncodeEndTxnResponse(w, resp, version)
	return w.Bytes(), nil
}

func (h *EndTxnHandler) process(req *api.EndTxnRequest) int16 {
	if req.TransactionalID == "" {
		return int16(codec.ErrInvalidRequest)
	}
	if h.ownership != nil && !h.ownership.OwnsTxn(req.TransactionalID) {
		return int16(codec.ErrNotCoordinator)
	}
	if h.store == nil {
		// Boot window — retryable.
		return int16(codec.ErrCoordinatorNotAvailable)
	}
	err := h.store.EndTxn(req.TransactionalID, req.ProducerID, req.ProducerEpoch, req.Committed)
	switch {
	case err == nil:
		return 0
	case errors.Is(err, ErrTxnEndEmptyID):
		return int16(codec.ErrInvalidRequest)
	case errors.Is(err, ErrTxnEndUnknownProducer):
		return int16(codec.ErrInvalidProducerIDMapping)
	case errors.Is(err, ErrTxnEndEpochFenced):
		return int16(codec.ErrProducerFenced)
	case errors.Is(err, ErrTxnEndConcurrent):
		return int16(codec.ErrConcurrentTransactions)
	case errors.Is(err, ErrTxnEndInvalidState):
		return int16(codec.ErrInvalidTxnState)
	default:
		return int16(codec.ErrUnknownServerError)
	}
}
