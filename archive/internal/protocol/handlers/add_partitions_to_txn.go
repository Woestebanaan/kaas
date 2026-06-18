package handlers

import (
	"errors"
	"fmt"

	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

// TxnPartitionStore is the slim interface AddPartitionsToTxnHandler
// needs from coordinator.TxnStateStore. Defined here to avoid a
// handlers→coordinator import cycle. Production wires the concrete
// coordinator.TxnStateStore (its AddPartitions method validates +
// persists); tests can substitute a fake.
//
// Sentinel errors mirror coordinator.Err{EmptyTxnID,
// TxnUnknownProducer, TxnEpochFenced} — declared here as variables
// so this handler can reference them without importing the
// coordinator package. The cluster runtime wires the production
// store via a thin adapter (see broker.go) that translates
// coordinator errors to these sentinels.
type TxnPartitionStore interface {
	AddPartitions(txnID string, pid int64, epoch int16, additions []TxnPartitionAddition) error
}

// TxnPartitionAddition is the (topic, partitions) tuple the handler
// passes to the store. Mirrors api.AddPartitionsToTxnTopic but
// lives in the handler package so the store doesn't have to import
// codec types.
type TxnPartitionAddition struct {
	Topic      string
	Partitions []int32
}

// Sentinel errors the AddPartitionsToTxnHandler maps to Kafka error
// codes. The store implementation returns these by exact-match
// pointer so the handler can switch on them.
var (
	ErrTxnPartitionEmptyID         = errors.New("addpartitions: empty transactional id")
	ErrTxnPartitionUnknownProducer = errors.New("addpartitions: unknown txnID or pid mismatch")
	ErrTxnPartitionEpochFenced     = errors.New("addpartitions: producer epoch fenced")
)

// AddPartitionsToTxnHandler answers API key 24 v0–v3.
//
// gh #23 — implements per-txn partition tracking. The handler is
// thin: gate on OwnsTxn (gh #91 routing), validate (PID, epoch)
// against the persisted txn state, then delegate the partition
// union to the store. v4 multi-batch is a separate KIP and not
// supported here; clients negotiating v3 max are unaffected.
type AddPartitionsToTxnHandler struct {
	store     TxnPartitionStore
	ownership TxnOwnership // nil ⇒ gh #91 gate disabled (dev mode)
}

func NewAddPartitionsToTxnHandler(store TxnPartitionStore) *AddPartitionsToTxnHandler {
	return &AddPartitionsToTxnHandler{store: store}
}

// WithTxnOwnership wires the gh #91 routing gate. When set, the
// handler returns NOT_COORDINATOR (per-partition error code) for
// any txnID this broker doesn't own — the Java client's
// markCoordinatorUnknown loop retries FindCoordinator(KeyType=1)
// and lands on the routed broker.
func (h *AddPartitionsToTxnHandler) WithTxnOwnership(o TxnOwnership) *AddPartitionsToTxnHandler {
	h.ownership = o
	return h
}

func (h *AddPartitionsToTxnHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeAddPartitionsToTxnRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("add-partitions-to-txn decode: %w", err)
	}

	resp := &api.AddPartitionsToTxnResponse{}

	// Determine the per-partition error code that applies to every
	// (topic, partition) in the request. v0-3 has no top-level
	// ErrorCode, so a top-level rejection is repeated across every
	// partition. The Java client picks any one — they're all the
	// same code anyway.
	errCode := h.classify(req)

	if errCode == 0 && h.store != nil {
		additions := make([]TxnPartitionAddition, 0, len(req.Topics))
		for _, t := range req.Topics {
			additions = append(additions, TxnPartitionAddition{
				Topic:      t.Name,
				Partitions: append([]int32(nil), t.Partitions...),
			})
		}
		if err := h.store.AddPartitions(req.TransactionalID, req.ProducerID, req.ProducerEpoch, additions); err != nil {
			errCode = mapStoreError(err)
		}
	}

	for _, t := range req.Topics {
		topicResult := api.AddPartitionsToTxnTopicResult{Name: t.Name}
		for _, p := range t.Partitions {
			topicResult.PartitionResults = append(topicResult.PartitionResults,
				api.AddPartitionsToTxnPartitionResult{
					PartitionIndex: p,
					ErrorCode:      errCode,
				})
		}
		resp.Results = append(resp.Results, topicResult)
	}

	w := codec.NewWriter()
	api.EncodeAddPartitionsToTxnResponse(w, resp, version)
	return w.Bytes(), nil
}

// classify runs the input + routing checks that don't require store
// access. Returns 0 (NONE) when the request passes; the caller then
// delegates to the store and translates store errors via
// mapStoreError. Order mirrors Apache's
// `handleAddPartitionsToTransaction`:
//  1. INVALID_REQUEST for empty transactionalId
//  2. NOT_COORDINATOR if this broker isn't the routed coordinator
func (h *AddPartitionsToTxnHandler) classify(req *api.AddPartitionsToTxnRequest) int16 {
	if req.TransactionalID == "" {
		return int16(codec.ErrInvalidRequest)
	}
	if h.ownership != nil && !h.ownership.OwnsTxn(req.TransactionalID) {
		return int16(codec.ErrNotCoordinator)
	}
	if h.store == nil {
		// Boot window: store not yet wired. COORDINATOR_NOT_AVAILABLE
		// is retryable on the Java client (markCoordinatorUnknown +
		// re-FindCoordinator); INVALID_PRODUCER_ID_MAPPING would be
		// terminal.
		return int16(codec.ErrCoordinatorNotAvailable)
	}
	return 0
}

// mapStoreError translates the coordinator-package sentinel errors
// to Kafka error codes. Apache's mapping:
//
//	ErrTxnPartitionEmptyID         → INVALID_REQUEST       (42)
//	ErrTxnPartitionUnknownProducer → INVALID_PRODUCER_ID_MAPPING (49)
//	ErrTxnPartitionEpochFenced     → PRODUCER_FENCED        (90)
func mapStoreError(err error) int16 {
	switch {
	case errors.Is(err, ErrTxnPartitionEmptyID):
		return int16(codec.ErrInvalidRequest)
	case errors.Is(err, ErrTxnPartitionUnknownProducer):
		return int16(codec.ErrInvalidProducerIDMapping)
	case errors.Is(err, ErrTxnPartitionEpochFenced):
		return int16(codec.ErrProducerFenced)
	default:
		return int16(codec.ErrUnknownServerError)
	}
}
