package handlers

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/woestebanaan/skafka/internal/auth"
	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/lease"
	"github.com/woestebanaan/skafka/internal/lock"
	"github.com/woestebanaan/skafka/internal/observability"
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
	start := time.Now()
	mx := observability.Global()
	defer func() {
		mx.RequestLatency.Record(context.Background(), time.Since(start).Seconds(),
			metric.WithAttributes(
				attribute.Int("api_key", 0),
				attribute.Int("version", int(version)),
			))
	}()

	r := codec.NewReader(body)
	req, err := api.DecodeProduceRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("produce decode: %w", err)
	}

	principal := principalFrom(conn)
	resp := &api.ProduceResponse{}

	// Quota enforcement: total bytes across all partitions/topics in this request.
	totalBytes := 0
	for _, td := range req.TopicData {
		for _, pd := range td.PartitionData {
			totalBytes += len(pd.Records)
		}
	}
	if throttleMs := h.auth.CheckProduceQuota(principal, totalBytes); throttleMs > 0 {
		resp.ThrottleTime = throttleMs
	}

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

			if !validateProduceBatches(pd.Records) {
				pr.ErrorCode = int16(codec.ErrCorruptMessage)
				pr.BaseOffset = -1
				topicResp.PartitionResponses = append(topicResp.PartitionResponses, pr)
				continue
			}

			appendStart := time.Now()
			// Phase 1: epoch is 0 (no fence). Phase 4 reads it from BrokerCoordinator.CurrentEpoch.
			baseOffset, err := h.store.Append(context.Background(), td.Name, pd.Index, 0, pd.Records)
			mx.WriteLatency.Record(context.Background(), time.Since(appendStart).Seconds(),
				metric.WithAttributes(attribute.String("topic", td.Name)))
			if err != nil {
				pr.ErrorCode = int16(codec.ErrUnknownServerError)
				pr.BaseOffset = -1
			} else {
				topicAttr := metric.WithAttributes(attribute.String("topic", td.Name))
				mx.ProduceBytes.Add(context.Background(), int64(len(pd.Records)), topicAttr)
				// Best-effort record count from batch header.
				if cnt := recordCountFromBatch(pd.Records); cnt > 0 {
					mx.ProduceRecords.Add(context.Background(), int64(cnt), topicAttr)
				}
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

// recordCountFromBatch extracts numRecords from the RecordBatch header bytes.
// Layout (from codec/types.go): [baseOffset:8][batchLength:4][ple:4][magic:1]
// [crc:4][attrs:2][lastOffsetDelta:4][baseTimestamp:8][maxTimestamp:8]
// [producerId:8][producerEpoch:2][baseSequence:4][numRecords:4].
// numRecords starts at byte 57 (big-endian int32). Returns 0 when raw is shorter.
func recordCountFromBatch(raw []byte) int {
	const numRecordsOffset = 57
	if len(raw) < numRecordsOffset+4 {
		return 0
	}
	return int(int32(raw[numRecordsOffset])<<24 |
		int32(raw[numRecordsOffset+1])<<16 |
		int32(raw[numRecordsOffset+2])<<8 |
		int32(raw[numRecordsOffset+3]))
}
