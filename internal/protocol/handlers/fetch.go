package handlers

import (
	"context"
	"errors"
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
	store      storage.StorageEngine
	leases     lease.LeaseManager
	authorizer auth.Authorizer   // gh #126: cluster-wide
	quotas     auth.QuotaChecker // gh #126: cluster-wide
}

func NewFetchHandler(
	store storage.StorageEngine,
	leases lease.LeaseManager,
	authorizer auth.Authorizer,
	quotas auth.QuotaChecker,
) *FetchHandler {
	return &FetchHandler{store: store, leases: leases, authorizer: authorizer, quotas: quotas}
}

func (h *FetchHandler) Handle(conn *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	// Request latency now lives in the protocol.RequestObservability
	// middleware (gh #121 PR2.5). Per-call ReadLatency below is still
	// inline because it carries the topic label which the request-level
	// middleware deliberately doesn't.
	mx := observability.Global()

	r := codec.NewReader(body)
	req, err := api.DecodeFetchRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("fetch decode: %w", err)
	}

	principal := principalFrom(conn)
	resp := &api.FetchResponse{ErrorCode: 0, SessionID: req.SessionID}

	for _, topic := range req.Topics {
		topicResp := api.FetchTopicResponse{Name: topic.Name}

		// gh #126: cluster-wide authorizer (with superUser early-allow).
		if !h.authorizer.Authorize(principal, auth.Resource{Type: "topic", Name: topic.Name, PatternType: "literal"}, auth.OpRead) {
			for _, p := range topic.Partitions {
				topicResp.Partitions = append(topicResp.Partitions, api.FetchPartitionResponse{
					PartitionIndex:      p.PartitionIndex,
					ErrorCode:           int16(codec.ErrTopicAuthorizationFailed),
					HighWatermark:       -1,
					LastStableOffset:    -1,
					LogStartOffset:      -1,
					PreferredReadReplica: -1,
				})
				recordFetchError(mx, topic.Name, "topic_auth_failed")
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
				recordFetchError(mx, topic.Name, "not_leader")
				topicResp.Partitions = append(topicResp.Partitions, pr)
				continue
			}
			pr.HighWatermark = hwm
			// gh #31 (KIP-98 read-committed): LastStableOffset = the
			// highest offset at which every transaction is committed
			// or aborted. Until the transaction coordinator state
			// machine (gh #28) and the __transaction_state topic
			// (gh #29) land, skafka has no in-flight txn state, so
			// every record at offset < HWM is automatically committed
			// — LSO == HWM and the AbortedTransactions list is empty.
			// This is the correct read-committed answer for the
			// "no-txn-in-flight" steady state: clients with
			// isolation.level=read_committed see the same offset
			// frontier as read_uncommitted, and there are no aborted
			// records to filter. Once gh #28 lands, recompute LSO
			// from TxnStateStore's earliest open transaction.
			pr.LastStableOffset = hwm
			_ = req.IsolationLevel // explicit acknowledgement; field decoded but no branch needed yet
			pr.LogStartOffset, _ = h.store.LogStartOffset(topic.Name, p.PartitionIndex)

			readStart := time.Now()
			raw, err := h.store.Read(context.Background(), topic.Name, p.PartitionIndex,
				p.FetchOffset, int(p.PartitionMaxBytes))
			mx.ReadLatency.Record(context.Background(), time.Since(readStart).Seconds(),
				metric.WithAttributes(attribute.String("topic", topic.Name)))
			if err == nil {
				pr.Records = raw
				// gh #115 / gh #121 PR1: bump per-topic atomic
				// accumulators. ObservableCounter callback emits
				// the cumulative at every scrape — idle topics
				// (and empty Fetch responses) still show up.
				// Walks every batch in the response — recordCountFromBatch only
				// read the first batch's header, so multi-batch Fetch responses
				// undercounted (most consumers receive multi-batch responses
				// once steady-state catches up).
				mx.TopicTraffic.RecordFetch(topic.Name, codec.CountRecordsInBatches(raw), int64(len(raw)))
			} else {
				// gh #132: failures stay visible. The success counter
				// going flat on its own looked indistinguishable from
				// "no traffic"; this counter rises so the dashboard
				// shows "broker is asked but failing" during NAS
				// stalls or partition-leader race windows.
				recordFetchError(mx, topic.Name, errorClassForReadError(err))
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
	if throttleMs := h.quotas.CheckFetchQuota(principal, totalBytes); throttleMs > 0 {
		resp.ThrottleTimeMs = throttleMs
	}

	// Pre-size the encoder buffer. The CPU profile shows ~40% of broker
	// CPU in runtime.memmove during Fetch, driven by growslice doubling
	// a buf=nil writer through ~24 reallocations on a 50 MB response.
	// Estimate: records bytes (already summed for quota) + per-partition
	// fixed overhead (errorCode, HWM, LSO, LogStart, preferredReplica,
	// aborted-txn array length, tagged fields, records-length prefix) +
	// per-topic overhead (compact name + array length + tags) +
	// top-level header. Conservative — overshooting wastes a few KB
	// once; undershooting triggers exactly the growslice we're trying
	// to kill.
	estimate := totalBytes
	for _, t := range resp.Responses {
		estimate += 32 + len(t.Name) // per-topic
		estimate += 64 * len(t.Partitions)
	}
	estimate += 64 // top-level header
	w := codec.NewWriterWithCap(estimate)
	api.EncodeFetchResponse(w, resp, version)
	return w.Bytes(), nil
}

// recordFetchError is the fetch-side sibling of recordProduceError
// (see produce.go). gh #132 — bumped on every Fetch error path so the
// fetch error rate stays visible when the success counter goes flat.
func recordFetchError(mx *observability.Metrics, topic, errorCode string) {
	mx.FetchErrors.Add(context.Background(), 1,
		metric.WithAttributes(
			attribute.String("topic", topic),
			attribute.String("error_code", errorCode),
		))
}

// errorClassForReadError maps storage.Read() error sentinels onto
// bounded metric-label strings. Mirrors errorClassForAppendError on
// the produce side. Every Read() error path should have a case here;
// error_code=unknown rising on the dashboard means a new error type
// in the storage engine needs classification.
func errorClassForReadError(err error) string {
	switch {
	case errors.Is(err, storage.ErrStorageStalled):
		return "storage_stalled"
	case errors.Is(err, storage.ErrUnknownPartition):
		return "unknown_partition"
	case errors.Is(err, storage.ErrOffsetOutOfRange):
		return "offset_out_of_range"
	default:
		return "unknown"
	}
}
