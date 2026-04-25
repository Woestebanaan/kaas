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

			if !h.leases.IsLeader(topic.Name, p.PartitionIndex) {
				pr.ErrorCode = int16(codec.ErrNotLeaderOrFollower)
				pr.Offset = -1
				pr.Timestamp = -1
				topicResp.Partitions = append(topicResp.Partitions, pr)
				continue
			}

			var offset int64
			switch {
			case p.Timestamp == -2: // earliest
				offset, err = h.store.LogStartOffset(topic.Name, p.PartitionIndex)
			case p.Timestamp == -1: // latest
				offset, err = h.store.HighWatermark(topic.Name, p.PartitionIndex)
			default:
				// Real-timestamp lookup (search for first record with ts >= p.Timestamp).
				// Skafka does not index records by timestamp yet, so we cannot honour
				// this query. Per the Kafka spec, "no matching record" is signalled by
				// returning (offset=-1, timestamp=-1); returning a real offset with
				// timestamp=-1 makes the Java client throw IllegalArgumentException in
				// OffsetAndTimestamp's ctor (caught e.g. by kafbat's Messages page).
				offset = -1
			}

			if err != nil {
				pr.ErrorCode = int16(codec.ErrUnknownTopicOrPartition)
				pr.Offset = -1
				pr.Timestamp = -1
			} else {
				pr.Offset = offset
				pr.Timestamp = -1
			}
			topicResp.Partitions = append(topicResp.Partitions, pr)
		}
		resp.Topics = append(resp.Topics, topicResp)
	}

	w := codec.NewWriter()
	api.EncodeListOffsetsResponse(w, resp, version)
	return w.Bytes(), nil
}
