package handlers

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

// WriteTxnMarkersStore is the slim storage interface this handler
// needs. Mirrors handlers.ProduceStore — we use the same Append
// path, since a control batch is just a record batch with the
// isControl bit set.
type WriteTxnMarkersStore interface {
	Append(ctx context.Context, topic string, partition int32, epoch uint32, batchBytes []byte) (int64, error)
}

// WriteTxnMarkersOwnership reports whether this broker leads
// (topic, partition). Reuse the existing BrokerCoordinator.Owns
// interface — partition ownership for control-batch writes is
// exactly the same as for Produce.
type WriteTxnMarkersOwnership interface {
	Owns(topic string, partition int32) bool
}

// WriteTxnMarkersHandler answers API key 27 v0–v1. gh #114.
//
// The txn coordinator sends WriteTxnMarkersRequest to each
// partition's leader broker after EndTxn. The receiver:
//  1. Validates it leads each requested partition (else
//     NOT_LEADER_OR_FOLLOWER).
//  2. Builds a control batch for the COMMIT or ABORT marker.
//  3. Appends the batch via the storage engine; the storage layer
//     treats it like any other batch (the isControl bit makes #31
//     read-committed Fetch filter it).
//
// Cross-broker dispatch from the txn coord is gh #114's other half
// (see the EndTxn integration in cluster_runtime/broker glue).
type WriteTxnMarkersHandler struct {
	store WriteTxnMarkersStore
	owns  WriteTxnMarkersOwnership
}

func NewWriteTxnMarkersHandler(store WriteTxnMarkersStore) *WriteTxnMarkersHandler {
	return &WriteTxnMarkersHandler{store: store}
}

// WithOwnership wires the partition-leader gate. Without it, the
// handler writes every requested partition's marker regardless of
// leadership (dev mode where every broker is leader-of-all).
func (h *WriteTxnMarkersHandler) WithOwnership(o WriteTxnMarkersOwnership) *WriteTxnMarkersHandler {
	h.owns = o
	return h
}

func (h *WriteTxnMarkersHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeWriteTxnMarkersRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("write-txn-markers decode: %w", err)
	}

	resp := &api.WriteTxnMarkersResponse{}
	for _, m := range req.Markers {
		mr := api.WritableTxnMarkerResult{ProducerID: m.ProducerID}
		batch := encodeControlBatch(m.ProducerID, m.ProducerEpoch, m.TransactionResult, m.CoordinatorEpoch)

		for _, t := range m.Topics {
			tr := api.WritableTxnMarkerTopicResult{Name: t.Name}
			for _, p := range t.PartitionIndexes {
				errCode := h.writeMarker(t.Name, p, batch)
				tr.Partitions = append(tr.Partitions, api.WritableTxnMarkerPartitionResult{
					PartitionIndex: p,
					ErrorCode:      errCode,
				})
			}
			mr.Topics = append(mr.Topics, tr)
		}
		resp.Markers = append(resp.Markers, mr)
	}

	w := codec.NewWriter()
	api.EncodeWriteTxnMarkersResponse(w, resp, version)
	return w.Bytes(), nil
}

func (h *WriteTxnMarkersHandler) writeMarker(topic string, partition int32, batch []byte) int16 {
	if h.owns != nil && !h.owns.Owns(topic, partition) {
		return int16(codec.ErrNotLeaderOrFollower)
	}
	if h.store == nil {
		return int16(codec.ErrCoordinatorNotAvailable)
	}
	// epoch=0 for control batches — they're idempotence-exempt
	// (baseSequence=-1 in the encoded batch).
	if _, err := h.store.Append(context.Background(), topic, partition, 0, batch); err != nil {
		slog.Warn("write-txn-markers: append failed",
			"topic", topic, "partition", partition, "err", err)
		return int16(codec.ErrUnknownServerError)
	}
	return 0
}
