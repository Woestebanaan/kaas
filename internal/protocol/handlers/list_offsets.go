package handlers

import (
	"fmt"

	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/lease"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
	"github.com/woestebanaan/skafka/internal/storage"
)

type ListOffsetsHandler struct {
	store  storage.StorageEngine
	leases lease.LeaseManager
}

func NewListOffsetsHandler(store storage.StorageEngine, leases lease.LeaseManager) *ListOffsetsHandler {
	return &ListOffsetsHandler{store: store, leases: leases}
}

func (h *ListOffsetsHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeListOffsetsRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("list_offsets decode: %w", err)
	}

	resp := &api.ListOffsetsResponse{}
	for _, topic := range req.Topics {
		topicResp := api.ListOffsetsTopicResponse{Name: topic.Name}
		for _, p := range topic.Partitions {
			pr := api.ListOffsetsPartitionResponse{PartitionIndex: p.PartitionIndex}

			// Engine returns ErrUnknownTopicOrPartition (or similar) when
			// we don't have the partition open — i.e., TakeOver hasn't
			// run, i.e., we're not the leader per assignment.json. That
			// replaces the legacy lease.IsLeader gate (gh #75).
			var offset int64
			matchTs := int64(-1)
			switch {
			case p.Timestamp == -2: // earliest
				offset, err = h.store.LogStartOffset(topic.Name, p.PartitionIndex)
			case p.Timestamp == -1: // latest
				offset, err = h.store.HighWatermark(topic.Name, p.PartitionIndex)
			default:
				// Real-timestamp lookup (gh #5). Segment-granularity:
				// the first segment whose maxTimestamp >= request
				// returns its baseOffset. (-1, -1) signals
				// "no matching record" per Apache's wire contract.
				offset, matchTs, err = h.store.OffsetForTimestamp(topic.Name, p.PartitionIndex, p.Timestamp)
			}

			if err != nil {
				pr.ErrorCode = int16(codec.ErrUnknownTopicOrPartition)
				pr.Offset = -1
				pr.Timestamp = -1
			} else {
				pr.Offset = offset
				pr.Timestamp = matchTs
			}
			topicResp.Partitions = append(topicResp.Partitions, pr)
		}
		resp.Topics = append(resp.Topics, topicResp)
	}

	w := codec.NewWriter()
	api.EncodeListOffsetsResponse(w, resp, version)
	return w.Bytes(), nil
}
