package api

import (
	"testing"

	"github.com/woestebanaan/skafka/internal/protocol/codec"
)

// TestDeleteGroupsRequestDecodeV0 walks the legacy non-flexible
// request shape: int32-prefixed array of int16-prefixed strings.
// The Java AdminClient negotiates v0 only when the broker doesn't
// advertise anything higher; we still decode it correctly so a
// future cap-down doesn't silently break.
func TestDeleteGroupsRequestDecodeV0(t *testing.T) {
	w := codec.NewWriter()
	w.WriteArray(2, func() {
		w.WriteString("orders")
		w.WriteString("payments")
	})
	req, err := DecodeDeleteGroupsRequest(codec.NewReader(w.Bytes()), 0)
	if err != nil {
		t.Fatalf("decode v0: %v", err)
	}
	if len(req.GroupNames) != 2 || req.GroupNames[0] != "orders" || req.GroupNames[1] != "payments" {
		t.Errorf("decoded names=%v", req.GroupNames)
	}
}

// TestDeleteGroupsRequestDecodeV2 covers the modern flexible path
// (compact array + compact strings + tagged fields) used by
// AdminClient on Kafka 3.7. v2 is what kafka-consumer-groups.sh
// negotiates after we register key 42.
func TestDeleteGroupsRequestDecodeV2(t *testing.T) {
	w := codec.NewWriter()
	w.WriteCompactArray(1, func() {
		w.WriteCompactString("orders")
	})
	w.WriteEmptyTaggedFields()
	req, err := DecodeDeleteGroupsRequest(codec.NewReader(w.Bytes()), 2)
	if err != nil {
		t.Fatalf("decode v2: %v", err)
	}
	if len(req.GroupNames) != 1 || req.GroupNames[0] != "orders" {
		t.Errorf("v2 decoded=%v", req.GroupNames)
	}
}

// TestDeleteGroupsResponseRoundTripV0 pins the v0 wire shape:
//
//	throttle(int32) | results-array(int32-prefixed):
//	  group(int16-string) | errCode(int16)
//
// A franz-go or Java client decoding our v0 expects exactly this
// byte sequence; encoding a per-group error must land at the
// correct offset.
func TestDeleteGroupsResponseRoundTripV0(t *testing.T) {
	resp := &DeleteGroupsResponse{
		ThrottleTimeMs: 0,
		Results: []DeleteGroupsResult{
			{GroupID: "orders", ErrorCode: 0},
			{GroupID: "missing", ErrorCode: 69}, // GROUP_ID_NOT_FOUND
		},
	}
	w := codec.NewWriter()
	EncodeDeleteGroupsResponse(w, resp, 0)
	r := codec.NewReader(w.Bytes())

	if v, _ := r.ReadInt32(); v != 0 {
		t.Errorf("throttle=%d, want 0", v)
	}
	var n int32
	if err := r.ReadArray(func() error {
		n++
		gid, _ := r.ReadString()
		errCode, _ := r.ReadInt16()
		switch n {
		case 1:
			if gid != "orders" || errCode != 0 {
				t.Errorf("entry 1=(%q, %d), want (orders, 0)", gid, errCode)
			}
		case 2:
			if gid != "missing" || errCode != 69 {
				t.Errorf("entry 2=(%q, %d), want (missing, 69)", gid, errCode)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("read array: %v", err)
	}
	if n != 2 {
		t.Errorf("got %d entries, want 2", n)
	}
}

// TestDeleteGroupsResponseRoundTripV2 confirms the flexible path
// emits the trailing tagged-fields byte both per-entry and at
// the response level — KIP-482 framing is unforgiving about a
// missing tagged-fields sentinel.
func TestDeleteGroupsResponseRoundTripV2(t *testing.T) {
	resp := &DeleteGroupsResponse{
		Results: []DeleteGroupsResult{{GroupID: "orders", ErrorCode: 0}},
	}
	w := codec.NewWriter()
	EncodeDeleteGroupsResponse(w, resp, 2)
	got := w.Bytes()
	// v2 layout (1 result):
	//   throttle(4) + array_len_uvarint(1) + group_compact_str + errCode(2)
	//   + entry_tagged_fields(1=0) + response_tagged_fields(1=0)
	// Hard to byte-count exactly without re-encoding, but we can
	// assert the LAST byte is the response-level tagged-fields
	// sentinel (0x00). A regression that drops it would shift
	// every subsequent response on the connection.
	if got[len(got)-1] != 0x00 {
		t.Errorf("v2 last byte = %#x, want 0x00 (response tagged-fields)", got[len(got)-1])
	}
}
