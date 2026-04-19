package handlers

import (
	"context"
	"fmt"

	"github.com/woestebanaan/skafka/internal/auth"
	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/lease"
	"github.com/woestebanaan/skafka/internal/lock"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
	"github.com/woestebanaan/skafka/internal/storage"
)

type ProduceHandler struct {
	store  storage.StorageEngine
	leases lease.LeaseManager
	locks  lock.PartitionLock
	auth   auth.AuthEngine
}

func NewProduceHandler(
	store storage.StorageEngine,
	leases lease.LeaseManager,
	locks lock.PartitionLock,
	authEng auth.AuthEngine,
) *ProduceHandler {
	return &ProduceHandler{store: store, leases: leases, locks: locks, auth: authEng}
}

func (h *ProduceHandler) Handle(conn *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeProduceRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("produce decode: %w", err)
	}

	principal := principalFrom(conn)
	resp := &api.ProduceResponse{}

	for _, td := range req.TopicData {
		topicResp := api.ProduceTopicResponse{Name: td.Name}

		if !h.auth.Authorize(principal, auth.Resource{Type: "topic", Name: td.Name, PatternType: "literal"}, auth.OpWrite) {
			for _, pd := range td.PartitionData {
				topicResp.PartitionResponses = append(topicResp.PartitionResponses, api.ProducePartitionResponse{
					Index: pd.Index, ErrorCode: int16(codec.ErrTopicAuthorizationFailed),
					BaseOffset: -1, LogAppendTime: -1, LogStartOffset: -1,
				})
			}
			resp.Responses = append(resp.Responses, topicResp)
			continue
		}

		for _, pd := range td.PartitionData {
			pr := api.ProducePartitionResponse{
				Index: pd.Index, LogAppendTime: -1, LogStartOffset: 0,
			}

			if !h.leases.IsLeader(td.Name, pd.Index) {
				pr.ErrorCode = int16(codec.ErrNotLeaderOrFollower)
				pr.BaseOffset = -1
				topicResp.PartitionResponses = append(topicResp.PartitionResponses, pr)
				continue
			}
			if !h.locks.IsLocked(td.Name, pd.Index) {
				pr.ErrorCode = int16(codec.ErrNotLeaderOrFollower)
				pr.BaseOffset = -1
				topicResp.PartitionResponses = append(topicResp.PartitionResponses, pr)
				continue
			}

			// Decode the raw RecordBatch bytes into storage.Records.
			records, err := decodeRecords(pd.Records)
			if err != nil {
				pr.ErrorCode = int16(codec.ErrCorruptMessage)
				pr.BaseOffset = -1
				topicResp.PartitionResponses = append(topicResp.PartitionResponses, pr)
				continue
			}

			baseOffset, err := h.store.Append(context.Background(), td.Name, pd.Index, records)
			if err != nil {
				pr.ErrorCode = int16(codec.ErrUnknownServerError)
				pr.BaseOffset = -1
			} else {
				pr.BaseOffset = baseOffset
			}
			topicResp.PartitionResponses = append(topicResp.PartitionResponses, pr)
		}
		resp.Responses = append(resp.Responses, topicResp)
	}

	w := codec.NewWriter()
	api.EncodeProduceResponse(w, resp, version)
	return w.Bytes(), nil
}

// decodeRecords turns raw RecordBatch bytes into []storage.Record.
// Returns an empty slice (not an error) if the input is nil/empty — acks=0 producers
// may send empty batches.
func decodeRecords(raw []byte) ([]storage.Record, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	r := codec.NewReader(raw)
	batch, err := codec.DecodeRecordBatch(r)
	if err != nil {
		return nil, err
	}
	out := make([]storage.Record, 0, len(batch.Records))
	for _, rec := range batch.Records {
		out = append(out, storage.Record{
			Offset:    batch.BaseOffset + int64(rec.OffsetDelta),
			Timestamp: batch.BaseTimestamp + rec.TimestampDelta,
			Key:       rec.Key,
			Value:     rec.Value,
		})
	}
	return out, nil
}
