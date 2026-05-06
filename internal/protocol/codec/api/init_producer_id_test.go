package api

import (
	"testing"

	"github.com/woestebanaan/skafka/internal/protocol/codec"
)

// TestInitProducerIdRequestDecodeV0 covers the legacy non-flexible
// header path used by older clients (and franz-go when negotiating
// down). Body shape: [TransactionalID: nullable string][Timeout: int32].
func TestInitProducerIdRequestDecodeV0(t *testing.T) {
	w := codec.NewWriter()
	w.WriteNullableString("", true) // null transactional id (idempotent producer)
	w.WriteInt32(60_000)            // 60s timeout (default)

	req, err := DecodeInitProducerIdRequest(codec.NewReader(w.Bytes()), 0)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if req.TransactionalID != "" {
		t.Errorf("TransactionalID=%q, want empty", req.TransactionalID)
	}
	if req.TransactionTimeoutMs != 60_000 {
		t.Errorf("Timeout=%d, want 60000", req.TransactionTimeoutMs)
	}
	// v0 has no PID/epoch fields — defaults must remain -1 (sentinel
	// for "client did not pre-allocate").
	if req.ProducerID != -1 || req.ProducerEpoch != -1 {
		t.Errorf("v0 PID/epoch defaults: got (%d, %d), want (-1, -1)",
			req.ProducerID, req.ProducerEpoch)
	}
}

// TestInitProducerIdRequestDecodeV4 covers the modern flexible path
// (v4 = the version Java clients on Kafka 3.7 send by default).
// Body: [TransactionalID: compact nullable string][Timeout: int32]
//       [ProducerID: int64][ProducerEpoch: int16][tagged fields].
func TestInitProducerIdRequestDecodeV4(t *testing.T) {
	w := codec.NewWriter()
	w.WriteCompactNullableString("", true) // null TransactionalID
	w.WriteInt32(120_000)
	w.WriteInt64(0xdeadbeef)
	w.WriteInt16(7)
	w.WriteEmptyTaggedFields()

	req, err := DecodeInitProducerIdRequest(codec.NewReader(w.Bytes()), 4)
	if err != nil {
		t.Fatalf("decode v4: %v", err)
	}
	if req.TransactionTimeoutMs != 120_000 {
		t.Errorf("Timeout=%d, want 120000", req.TransactionTimeoutMs)
	}
	// PID/epoch must be threaded through (KIP-360 PID renewal). Stage A
	// of #12 ignores them, but the codec must not silently drop them.
	if req.ProducerID != 0xdeadbeef || req.ProducerEpoch != 7 {
		t.Errorf("PID/epoch round-trip: got (%d, %d), want (%d, 7)",
			req.ProducerID, req.ProducerEpoch, 0xdeadbeef)
	}
}

// TestInitProducerIdResponseRoundTripV0 confirms wire shape parity.
// A franz-go client decoding our v0 response expects exactly this
// byte sequence:
//
//	throttle(int32) | errCode(int16) | pid(int64) | epoch(int16)
func TestInitProducerIdResponseRoundTripV0(t *testing.T) {
	resp := &InitProducerIdResponse{
		ThrottleTimeMs: 0,
		ErrorCode:      0,
		ProducerID:     1234,
		ProducerEpoch:  3,
	}
	w := codec.NewWriter()
	EncodeInitProducerIdResponse(w, resp, 0)
	got := w.Bytes()
	if len(got) != 4+2+8+2 {
		t.Errorf("v0 length=%d, want %d", len(got), 4+2+8+2)
	}
	r := codec.NewReader(got)
	throttle, _ := r.ReadInt32()
	errCode, _ := r.ReadInt16()
	pid, _ := r.ReadInt64()
	epoch, _ := r.ReadInt16()
	if throttle != 0 || errCode != 0 || pid != 1234 || epoch != 3 {
		t.Errorf("decoded=(%d,%d,%d,%d), want (0,0,1234,3)", throttle, errCode, pid, epoch)
	}
}

// TestInitProducerIdResponseRoundTripV4 confirms the flexible-wire
// format includes the trailing empty tagged fields byte.
func TestInitProducerIdResponseRoundTripV4(t *testing.T) {
	resp := &InitProducerIdResponse{ProducerID: 7777, ProducerEpoch: 0}
	w := codec.NewWriter()
	EncodeInitProducerIdResponse(w, resp, 4)
	got := w.Bytes()
	// v4 = v0 shape + 1 byte tagged-fields sentinel (varint 0).
	if len(got) != 4+2+8+2+1 {
		t.Errorf("v4 length=%d, want %d", len(got), 4+2+8+2+1)
	}
	if got[len(got)-1] != 0x00 {
		t.Errorf("v4 last byte = %#x, want 0x00 (empty tagged fields)", got[len(got)-1])
	}
}
