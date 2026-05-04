package handlers

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/woestebanaan/skafka/internal/auth"
	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/lease"
	"github.com/woestebanaan/skafka/internal/observability"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
	"github.com/woestebanaan/skafka/internal/storage"
)

type FetchHandler struct {
	store  storage.StorageEngine
	leases lease.LeaseManager
	auth   auth.AuthEngine
}

func NewFetchHandler(store storage.StorageEngine, leases lease.LeaseManager, authEng auth.AuthEngine) *FetchHandler {
	return &FetchHandler{store: store, leases: leases, auth: authEng}
}

func (h *FetchHandler) Handle(conn *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	start := time.Now()
	mx := observability.Global()
	defer func() {
		mx.RequestLatency.Record(context.Background(), time.Since(start).Seconds(),
			metric.WithAttributes(
				attribute.Int("api_key", 1),
				attribute.Int("version", int(version)),
			))
	}()

	r := codec.NewReader(body)
	req, err := api.DecodeFetchRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("fetch decode: %w", err)
	}

	principal := principalFrom(conn)
	resp := &api.FetchResponse{ErrorCode: 0, SessionID: req.SessionID}

	for _, topic := range req.Topics {
		topicResp := api.FetchTopicResponse{Name: topic.Name}

		if !h.auth.Authorize(principal, auth.Resource{Type: "topic", Name: topic.Name, PatternType: "literal"}, auth.OpRead) {
			for _, p := range topic.Partitions {
				topicResp.Partitions = append(topicResp.Partitions, api.FetchPartitionResponse{
					PartitionIndex:      p.PartitionIndex,
					ErrorCode:           int16(codec.ErrTopicAuthorizationFailed),
					HighWatermark:       -1,
					LastStableOffset:    -1,
					LogStartOffset:      -1,
					PreferredReadReplica: -1,
				})
			}
			resp.Responses = append(resp.Responses, topicResp)
			continue
		}

		for _, p := range topic.Partitions {
			pr := api.FetchPartitionResponse{
				PartitionIndex:      p.PartitionIndex,
				LastStableOffset:    -1,
				PreferredReadReplica: -1,
			}

			// Leadership truth is "has the engine opened this partition
			// (i.e., has TakeOver run for it)?" — surfaced via the
			// HighWatermark call below returning ErrUnknownTopicOrPartition
			// when we're not the leader. Pre-gh #75 we double-checked
			// via lease.IsLeader; that check now always returns false
			// because the per-partition Lease isn't acquired anymore.
			hwm, err := h.store.HighWatermark(topic.Name, p.PartitionIndex)
			if err != nil {
				pr.ErrorCode = int16(codec.ErrNotLeaderOrFollower)
				pr.HighWatermark = -1
				topicResp.Partitions = append(topicResp.Partitions, pr)
				continue
			}
			pr.HighWatermark = hwm
			pr.LastStableOffset = hwm
			pr.LogStartOffset, _ = h.store.LogStartOffset(topic.Name, p.PartitionIndex)

			readStart := time.Now()
			raw, err := h.store.Read(context.Background(), topic.Name, p.PartitionIndex,
				p.FetchOffset, int(p.PartitionMaxBytes))
			mx.ReadLatency.Record(context.Background(), time.Since(readStart).Seconds(),
				metric.WithAttributes(attribute.String("topic", topic.Name)))
			if err == nil {
				pr.Records = raw
				topicAttr := metric.WithAttributes(attribute.String("topic", topic.Name))
				mx.FetchBytes.Add(context.Background(), int64(len(raw)), topicAttr)
				if cnt := recordCountFromBatch(raw); cnt > 0 {
					mx.FetchRecords.Add(context.Background(), int64(cnt), topicAttr)
				}
			}
			topicResp.Partitions = append(topicResp.Partitions, pr)
		}
		resp.Responses = append(resp.Responses, topicResp)
	}

	// Quota enforcement: tally bytes returned across all partitions.
	totalBytes := 0
	for _, t := range resp.Responses {
		for _, p := range t.Partitions {
			totalBytes += len(p.Records)
		}
	}
	if throttleMs := h.auth.CheckFetchQuota(principal, totalBytes); throttleMs > 0 {
		resp.ThrottleTimeMs = throttleMs
	}

	w := codec.NewWriter()
	api.EncodeFetchResponse(w, resp, version)
	return w.Bytes(), nil
}
