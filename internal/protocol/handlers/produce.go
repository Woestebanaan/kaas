package handlers

import (
	"context"
	"encoding/binary"
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
	"github.com/woestebanaan/skafka/pkg/kafkaapi"
)

type ProduceHandler struct {
	store      storage.StorageEngine
	leases     lease.LeaseManager
	authorizer auth.Authorizer    // gh #126: cluster-wide
	quotas     auth.QuotaChecker  // gh #126: cluster-wide

	// coord is the v3 BrokerCoordinator. When set, it replaces the
	// per-partition Lease ownership check on the produce hot path: Owns +
	// IsHeartbeatFresh + CurrentEpoch are the single source of truth for
	// "may this broker append to this partition?". When nil, the handler
	// falls back to the v2.6 lease.IsLeader check. flock is gone — single-
	// writer enforcement is now epoch-prefixed segment filenames + the
	// coordinator-owned ownership decision.
	coord kafkaapi.BrokerCoordinator

	// maxMessageBytes caps the byte size of one record batch (gh #14).
	// Apache's default is `message.max.bytes=1048588` (1MB + 12 bytes
	// of batch overhead). Set via WithMaxMessageBytes; zero or negative
	// disables the cap entirely (useful in tests where the producer
	// builds purposefully-large batches that aren't the subject of the
	// test). On the hot path we compare against len(pd.Records) — the
	// full batch wire size, which matches Apache's "message.max.bytes
	// applies to the full RecordBatch" semantics.
	maxMessageBytes int32
}

// DefaultMaxMessageBytes mirrors Apache Kafka's `message.max.bytes`
// default (1048588 bytes = 1 MiB + 12 bytes for batch overhead). Used
// when nothing else sets maxMessageBytes explicitly.
const DefaultMaxMessageBytes int32 = 1048588

func NewProduceHandler(
	store storage.StorageEngine,
	leases lease.LeaseManager,
	authorizer auth.Authorizer,
	quotas auth.QuotaChecker,
) *ProduceHandler {
	return &ProduceHandler{
		store:           store,
		leases:          leases,
		authorizer:      authorizer,
		quotas:          quotas,
		maxMessageBytes: DefaultMaxMessageBytes,
	}
}

// WithMaxMessageBytes overrides the per-batch byte cap (gh #14).
// Pass 0 (or negative) to disable the cap. Operators set this via
// the chart's broker.maxMessageBytes value / SKAFKA_MAX_MESSAGE_BYTES
// env var when their workload genuinely needs the Apache > default.
func (h *ProduceHandler) WithMaxMessageBytes(n int32) *ProduceHandler {
	h.maxMessageBytes = n
	return h
}

// WithCoordinator switches the handler over to the v3 BrokerCoordinator path.
// Returning the receiver lets callers chain: NewProduceHandler(...).WithCoordinator(c).
// The legacy leases/locks fields stay populated so a nil coord still works.
func (h *ProduceHandler) WithCoordinator(coord kafkaapi.BrokerCoordinator) *ProduceHandler {
	h.coord = coord
	return h
}

func (h *ProduceHandler) Handle(conn *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	// Request latency is recorded uniformly by the
	// protocol.RequestObservability middleware (gh #121 PR2.5) — this
	// handler used to carry the histogram defer block inline, but the
	// other ~28 handlers didn't, so latency was only visible for
	// Produce/Fetch. Middleware covers all of them.
	mx := observability.Global()

	r := codec.NewReader(body)
	req, err := api.DecodeProduceRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("produce decode: %w", err)
	}

	principal := principalFrom(conn)
	resp := &api.ProduceResponse{}

	// gh #126: cluster-wide quota enforcement.
	// Total bytes across all partitions/topics in this request.
	totalBytes := 0
	for _, td := range req.TopicData {
		for _, pd := range td.PartitionData {
			totalBytes += len(pd.Records)
		}
	}
	if throttleMs := h.quotas.CheckProduceQuota(principal, totalBytes); throttleMs > 0 {
		resp.ThrottleTime = throttleMs
	}

	for _, td := range req.TopicData {
		topicResp := api.ProduceTopicResponse{Name: td.Name}

		// gh #126: cluster-wide ACL evaluation (with superUser
		// early-allow when configured). Runs on every listener now —
		// anonymous listeners no longer bypass authz.
		if !h.authorizer.Authorize(principal, auth.Resource{Type: "topic", Name: td.Name, PatternType: "literal"}, auth.OpWrite) {
			for _, pd := range td.PartitionData {
				topicResp.PartitionResponses = append(topicResp.PartitionResponses, api.ProducePartitionResponse{
					Index: pd.Index, ErrorCode: int16(codec.ErrTopicAuthorizationFailed),
					BaseOffset: -1, LogAppendTime: -1, LogStartOffset: -1,
				})
				recordProduceError(mx, td.Name, "topic_auth_failed")
			}
			resp.Responses = append(resp.Responses, topicResp)
			continue
		}

		for _, pd := range td.PartitionData {
			pr := api.ProducePartitionResponse{
				Index: pd.Index, LogAppendTime: -1, LogStartOffset: 0,
			}

			ok, epoch := h.checkOwnership(td.Name, pd.Index)
			if !ok {
				pr.ErrorCode = int16(codec.ErrNotLeaderOrFollower)
				pr.BaseOffset = -1
				recordProduceError(mx, td.Name, "not_leader")
				topicResp.PartitionResponses = append(topicResp.PartitionResponses, pr)
				continue
			}

			if !validateProduceBatches(pd.Records) {
				pr.ErrorCode = int16(codec.ErrCorruptMessage)
				pr.BaseOffset = -1
				recordProduceError(mx, td.Name, "corrupt_message")
				topicResp.PartitionResponses = append(topicResp.PartitionResponses, pr)
				continue
			}

			// gh #14: enforce broker-side max.message.bytes. Java
			// producers cap their own batches at max.request.size
			// (1MB by default) but malicious or misconfigured clients
			// can still send bigger; without this check the storage
			// engine accepts arbitrarily large batches and trips
			// downstream MaxFetchBytes loops at consume time.
			if h.maxMessageBytes > 0 && int32(len(pd.Records)) > h.maxMessageBytes {
				pr.ErrorCode = int16(codec.ErrMessageTooLarge)
				pr.BaseOffset = -1
				recordProduceError(mx, td.Name, "message_too_large")
				topicResp.PartitionResponses = append(topicResp.PartitionResponses, pr)
				continue
			}

			appendStart := time.Now()
			baseOffset, err := h.store.Append(context.Background(), td.Name, pd.Index, epoch, req.Acks, pd.Records)
			mx.WriteLatency.Record(context.Background(), time.Since(appendStart).Seconds(),
				metric.WithAttributes(attribute.String("topic", td.Name)))
			if err != nil {
				pr.ErrorCode = errCodeForAppendError(err)
				pr.BaseOffset = -1
				// gh #132: bump the per-topic produce-errors counter so
				// the dashboard sees the failure rate rise even when the
				// success counter has gone flat (NAS stalled, leader
				// fenced, etc.). errorClassForAppendError maps the
				// storage sentinel onto a short label for cardinality.
				recordProduceError(mx, td.Name, errorClassForAppendError(err))
			} else {
				// gh #115 / gh #121 PR1: bump per-topic atomic
				// accumulators. ObservableCounter callback emits
				// the cumulative at every scrape interval.
				// Multi-batch walker — Java producers usually send one batch per
				// partition per Produce request, but the idempotent path can
				// pack multiple in flight, so always walk the full payload.
				mx.TopicTraffic.RecordProduce(td.Name, codec.CountRecordsInBatches(pd.Records), int64(len(pd.Records)))
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

// checkOwnership decides whether this broker may serve a Produce for the
// given partition, and returns the epoch to pass to storage.Append. When
// the v3 BrokerCoordinator is wired (h.coord != nil) it is the only source
// of truth: Owns + IsHeartbeatFresh together replace the v2.6 lease + flock
// pair, and CurrentEpoch supplies the leader epoch for the per-batch fence.
// When coord is nil, fall back to the v2.6 lease + lock check and pass
// epoch=0 (storage skips the fence when caller's epoch is 0).
func (h *ProduceHandler) checkOwnership(topic string, partition int32) (bool, uint32) {
	if h.coord != nil {
		if !h.coord.Owns(topic, partition) {
			return false, 0
		}
		// Self-fence: a broker that has lost connectivity to the controller
		// stops acking writes within heartbeatTimeout, regardless of what
		// the (possibly stale) assignment file says it owns.
		if !heartbeatFresh(h.coord) {
			mx := observability.Global()
			mx.HeartbeatMisses.Add(context.Background(), 1)
			mx.SelfFenceEvents.Add(context.Background(), 1)
			return false, 0
		}
		epoch, _ := h.coord.CurrentEpoch(topic, partition)
		return true, epoch
	}
	if !h.leases.IsLeader(topic, partition) {
		return false, 0
	}
	return true, 0
}

// heartbeatFresh extracts the broker.Coordinator's IsHeartbeatFresh check
// without making the produce handler depend on the internal/broker package
// directly (which would create an import cycle). The kafkaapi contract
// promises LastHeartbeat; we apply the freshness window here.
func heartbeatFresh(c kafkaapi.BrokerCoordinator) bool {
	last := c.LastHeartbeat()
	if last.IsZero() {
		return false
	}
	return time.Since(last) <= produceHeartbeatTimeout
}

// errCodeForAppendError maps the storage-layer error sentinel a
// failed Append returns onto the Kafka wire error code the producer
// expects in the response. The idempotent-producer sentinels
// (ErrOutOfOrderSequence, ErrInvalidProducerEpoch) get explicit
// codes so the Java client can react correctly: 45 raises a
// fatal OutOfOrderSequenceException; 47 fences the producer
// (it stops sending and surfaces InvalidProducerEpochException).
// ErrStorageStalled (gh #95) maps to REQUEST_TIMED_OUT (7) — the
// Java client treats it as a retriable timeout, which is exactly
// what the operator wants while waiting for NFS / the storage
// backend to recover. Anything else collapses to
// UNKNOWN_SERVER_ERROR (-1) — the producer's blanket "broker
// failure" path.
func errCodeForAppendError(err error) int16 {
	switch {
	case errors.Is(err, storage.ErrOutOfOrderSequence):
		return int16(codec.ErrOutOfOrderSequenceNumber)
	case errors.Is(err, storage.ErrInvalidProducerEpoch):
		return int16(codec.ErrInvalidProducerEpoch)
	case errors.Is(err, storage.ErrStorageStalled):
		return int16(codec.ErrRequestTimedOut)
	default:
		return int16(codec.ErrUnknownServerError)
	}
}

// errorClassForAppendError is the metric-label sibling of
// errCodeForAppendError. Different output type because metric labels
// are strings, not wire ints. The label set is deliberately bounded
// — no raw error.Error() strings, which would explode cardinality.
// gh #132.
//
// Every Append() error path SHOULD map onto one of these classes; if
// the metric shows error_code=unknown rising, it means the storage
// engine grew a new error path that isn't classified here. Add a case
// rather than letting it collapse into the catch-all.
func errorClassForAppendError(err error) string {
	switch {
	case errors.Is(err, storage.ErrOutOfOrderSequence):
		return "out_of_order_sequence"
	case errors.Is(err, storage.ErrInvalidProducerEpoch):
		return "invalid_producer_epoch"
	case errors.Is(err, storage.ErrStorageStalled):
		return "storage_stalled"
	case errors.Is(err, storage.ErrEpochMismatch):
		return "epoch_mismatch"
	case errors.Is(err, storage.ErrNotLeader):
		return "not_leader"
	case errors.Is(err, storage.ErrUnknownPartition):
		return "unknown_partition"
	case errors.Is(err, storage.ErrPartitionClosing):
		return "partition_closing"
	default:
		return "unknown"
	}
}

// recordProduceError is the per-error-site helper that keeps the
// ProduceErrors counter call DRY. Pulled into a helper because the
// produce path has four distinct error exits (auth, not-leader,
// corrupt-message, append-error) and inlining the
// observability.Global().ProduceErrors.Add(...) boilerplate at each
// would obscure the actual logic.
func recordProduceError(mx *observability.Metrics, topic, errorCode string) {
	mx.ProduceErrors.Add(context.Background(), 1,
		metric.WithAttributes(
			attribute.String("topic", topic),
			attribute.String("error_code", errorCode),
		))
}

// produceHeartbeatTimeout mirrors broker.DefaultHeartbeatTimeout (3s).
// Duplicated here because importing internal/broker from
// internal/protocol/handlers would form a cycle (broker → coordinator →
// handlers via the Coordinator's own dependencies). Both constants must
// stay in sync; if they drift, fix the broker side and update this file.
const produceHeartbeatTimeout = 3 * time.Second

// validateProduceBatches walks every RecordBatch concatenated in a Produce
// request's RecordSet and validates each one's CRC32C and length-bound. Empty
// input is treated as a valid no-op (clients sometimes send empty produce
// requests as a keepalive).
//
// Wire layout per batch:
//
//	[0:8]   baseOffset
//	[8:12]  batchLength    (covers everything from byte 12 onward)
//	[12:16] partitionLeaderEpoch
//	[16]    magic          (must be 2)
//	[17:21] crc            (Castagnoli, covers bytes [21 : 12+batchLength])
//	[21:..] crcPayload     (attrs..numRecords..opaque records)
//
// The function only inspects the 21-byte header per batch — it never iterates
// individual records, preserving the bytes-are-opaque invariant.
func validateProduceBatches(records []byte) bool {
	if len(records) == 0 {
		return true
	}
	pos := 0
	for pos < len(records) {
		if len(records)-pos < 12 {
			return false
		}
		batchLength := int(int32(binary.BigEndian.Uint32(records[pos+8 : pos+12])))
		// Minimum batch body is 49 bytes:
		//   ple(4) + magic(1) + crc(4) + attrs(2) + lastOffsetDelta(4) +
		//   baseTimestamp(8) + maxTimestamp(8) + producerID(8) +
		//   producerEpoch(2) + baseSequence(4) + recordCount(4).
		if batchLength < 49 {
			return false
		}
		end := pos + 12 + batchLength
		if end > len(records) {
			return false
		}
		if records[pos+16] != 2 {
			return false // magic must be 2 for v3+ batches; v1/v0 not supported
		}
		storedCRC := binary.BigEndian.Uint32(records[pos+17 : pos+21])
		if codec.ValidateCRC(records[pos+21:end], storedCRC) != nil {
			return false
		}
		pos = end
	}
	return true
}

