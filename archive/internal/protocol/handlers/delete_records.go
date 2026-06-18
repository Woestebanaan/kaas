package handlers

import (
	"errors"
	"fmt"

	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
	"github.com/woestebanaan/skafka/internal/storage"
	"github.com/woestebanaan/skafka/pkg/kafkaapi"
)

// DeleteRecordsHandler implements the DeleteRecords API (key 21). It's
// the broker side of "kafka-delete-records.sh" / Kafbat's "Purge
// messages" UI: per-partition, advance the log start offset to a
// caller-supplied target so earlier records become invisible to Fetch
// and eligible for retention cleanup.
type DeleteRecordsHandler struct {
	store storage.StorageEngine
	coord kafkaapi.BrokerCoordinator
}

func NewDeleteRecordsHandler(store storage.StorageEngine) *DeleteRecordsHandler {
	return &DeleteRecordsHandler{store: store}
}

// WithCoordinator wires the v3 BrokerCoordinator. When set, the
// handler refuses requests for partitions this broker doesn't own
// (matches Produce's gating).
func (h *DeleteRecordsHandler) WithCoordinator(coord kafkaapi.BrokerCoordinator) *DeleteRecordsHandler {
	h.coord = coord
	return h
}

func (h *DeleteRecordsHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeDeleteRecordsRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("delete_records decode: %w", err)
	}

	resp := &api.DeleteRecordsResponse{}
	for _, topic := range req.Topics {
		topicResp := api.DeleteRecordsTopicResult{Name: topic.Name}
		for _, p := range topic.Partitions {
			pr := api.DeleteRecordsPartitionResult{
				PartitionIndex: p.PartitionIndex,
				LowWatermark:   -1,
			}

			// Leadership gate (v3 path). Without coord, fall back to
			// "trust the storage engine" — if the partition isn't
			// open here, DeleteRecords returns
			// ErrUnknownTopicOrPartition naturally.
			if h.coord != nil && !h.coord.Owns(topic.Name, p.PartitionIndex) {
				pr.ErrorCode = int16(codec.ErrNotLeaderOrFollower)
				topicResp.Partitions = append(topicResp.Partitions, pr)
				continue
			}

			lowWatermark, err := h.store.DeleteRecords(topic.Name, p.PartitionIndex, p.Offset)
			switch {
			case err == nil:
				pr.LowWatermark = lowWatermark
			case errors.Is(err, storage.ErrOffsetOutOfRange):
				pr.LowWatermark = lowWatermark
				pr.ErrorCode = int16(codec.ErrOffsetOutOfRange)
			default:
				// Engine reports "unknown partition" via wrapped fmt.Errorf;
				// normalise to ErrUnknownTopicOrPartition for clients.
				pr.ErrorCode = int16(codec.ErrUnknownTopicOrPartition)
			}
			topicResp.Partitions = append(topicResp.Partitions, pr)
		}
		resp.Topics = append(resp.Topics, topicResp)
	}

	w := codec.NewWriter()
	api.EncodeDeleteRecordsResponse(w, resp, version)
	return w.Bytes(), nil
}
