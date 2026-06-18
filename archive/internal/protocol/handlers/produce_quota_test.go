package handlers

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/woestebanaan/skafka/internal/auth"
	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/storage"
)

// throttlingQuotaChecker is a QuotaChecker stub that returns a fixed
// throttle_time_ms regardless of the actual bytes/principal — used
// by the gh #45 wire test to confirm the handler propagates the
// value onto the ProduceResponse without exercising the full
// token-bucket math (which is covered by internal/auth/quota_test.go).
type throttlingQuotaChecker struct {
	throttleMs int32
}

func (q *throttlingQuotaChecker) CheckProduceQuota(_ auth.Principal, _ int) int32 { return q.throttleMs }
func (q *throttlingQuotaChecker) CheckFetchQuota(_ auth.Principal, _ int) int32   { return q.throttleMs }

// quotaStubStore is a minimal storage.StorageEngine for the produce
// path. Append always succeeds; Read isn't called.
type quotaStubStore struct{}

func (quotaStubStore) Append(_ context.Context, _ string, _ int32, _ uint32, _ int16, _ []byte) (int64, error) {
	return 0, nil
}
func (quotaStubStore) Read(_ context.Context, _ string, _ int32, _ int64, _ int) ([]byte, error) {
	return nil, nil
}
func (quotaStubStore) HighWatermark(_ string, _ int32) (int64, error)              { return 0, nil }
func (quotaStubStore) LogStartOffset(_ string, _ int32) (int64, error)             { return 0, nil }
func (quotaStubStore) CreatePartition(_ string, _ int32) error                     { return nil }
func (quotaStubStore) DeletePartition(_ string, _ int32) error                     { return nil }
func (quotaStubStore) PartitionSize(_ string, _ int32) int64                       { return 0 }
func (quotaStubStore) DataDir() string                                             { return "/tmp/stub" }
func (quotaStubStore) TakeOver(_ context.Context, _ string, _ int32, _ uint32) (int64, error) {
	return 0, nil
}
func (quotaStubStore) Relinquish(_ string, _ int32) error                         { return nil }
func (quotaStubStore) DeleteRecords(_ string, _ int32, _ int64) (int64, error)    { return 0, nil }
func (quotaStubStore) OffsetForLeaderEpoch(_ string, _ int32, _ int32) (int32, int64, error) {
	return -1, -1, nil
}
func (quotaStubStore) OffsetForTimestamp(_ string, _ int32, _ int64) (int64, int64, error) {
	return -1, -1, nil
}

var _ storage.StorageEngine = quotaStubStore{}

// TestProduceHandler_QuotaThrottleOnResponse pins gh #45's wire
// contract: when the QuotaChecker reports a positive throttle_ms,
// the ProduceResponse MUST carry that value in its top-level
// ThrottleTime field. Apache's KIP-219 ordering: throttle is set
// BEFORE muting the connection, so the client sees the value and
// can pre-emptively back off.
func TestProduceHandler_QuotaThrottleOnResponse(t *testing.T) {
	h := &ProduceHandler{
		store:      quotaStubStore{},
		authorizer: auth.NewAllowAllAuthorizer(),
		quotas:     &throttlingQuotaChecker{throttleMs: 500},
		coord: &stubCoord{
			owns:          map[string]uint32{"t/0": 1},
			lastHeartbeat: time.Now(),
		},
		maxMessageBytes: DefaultMaxMessageBytes,
	}
	// Hand-encode a v9 Produce request with one batch — gh #14's
	// helper buildProduceRequestV9 lives in this package's test
	// neighbourhood but isn't exported; re-build inline so this
	// test stays self-contained.
	batch := validBatch(t, 0, 1)
	w := codec.NewWriter()
	w.WriteCompactNullableString("", true) // transactional id = null
	w.WriteInt16(-1)                       // acks=all
	w.WriteInt32(5000)                     // timeout
	w.WriteCompactArray(1, func() {
		w.WriteCompactString("t")
		w.WriteCompactArray(1, func() {
			w.WriteInt32(0)
			w.WriteUvarint(uint64(len(batch) + 1))
			w.WriteBytes(batch)
			w.WriteEmptyTaggedFields()
		})
		w.WriteEmptyTaggedFields()
	})
	w.WriteEmptyTaggedFields()

	respBytes, err := h.Handle(&connstate.ConnState{}, 9, w.Bytes())
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	// v9 (flexible) ProduceResponse layout ends with:
	//   ... ThrottleTime int32 | tagged_fields byte
	// Extract the throttle from those last 5 bytes.
	if got := readThrottleAtTail(respBytes); got != 500 {
		t.Errorf("ThrottleTime=%d, want 500 (handler must propagate QuotaChecker output)", got)
	}
}

// readThrottleAtTail extracts the ThrottleTime int32 from the end
// of a v9-shaped ProduceResponse without depending on the codec
// package's missing DecodeProduceResponse.
func readThrottleAtTail(b []byte) int32 {
	if len(b) < 5 {
		return -1
	}
	return int32(binary.BigEndian.Uint32(b[len(b)-5 : len(b)-1]))
}

// TestProduceHandler_QuotaZeroNoThrottle pins the under-limit path:
// when the QuotaChecker reports 0, the response's ThrottleTime
// must also be 0 (don't synthesise a phantom throttle).
func TestProduceHandler_QuotaZeroNoThrottle(t *testing.T) {
	h := &ProduceHandler{
		store:      quotaStubStore{},
		authorizer: auth.NewAllowAllAuthorizer(),
		quotas:     &throttlingQuotaChecker{throttleMs: 0},
		coord: &stubCoord{
			owns:          map[string]uint32{"t/0": 1},
			lastHeartbeat: time.Now(),
		},
		maxMessageBytes: DefaultMaxMessageBytes,
	}
	batch := validBatch(t, 0, 1)
	w := codec.NewWriter()
	w.WriteCompactNullableString("", true)
	w.WriteInt16(-1)
	w.WriteInt32(5000)
	w.WriteCompactArray(1, func() {
		w.WriteCompactString("t")
		w.WriteCompactArray(1, func() {
			w.WriteInt32(0)
			w.WriteUvarint(uint64(len(batch) + 1))
			w.WriteBytes(batch)
			w.WriteEmptyTaggedFields()
		})
		w.WriteEmptyTaggedFields()
	})
	w.WriteEmptyTaggedFields()

	respBytes, err := h.Handle(&connstate.ConnState{}, 9, w.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if got := readThrottleAtTail(respBytes); got != 0 {
		t.Errorf("ThrottleTime=%d, want 0", got)
	}
}
