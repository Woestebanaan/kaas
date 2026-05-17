package api

import (
	"testing"

	"github.com/woestebanaan/skafka/internal/protocol/codec"
)

// TestOffsetDeleteRequestDecodeV0 covers the wire shape Apache 3.7
// negotiates. Non-flexible: int32-prefixed arrays, int16-prefixed
// strings, no tagged fields.
//
//	group(int16-string) | topics(int32-array):
//	  topic(int16-string) | partitions(int32-array):
//	    partition_index(int32)
func TestOffsetDeleteRequestDecodeV0(t *testing.T) {
	w := codec.NewWriter()
	w.WriteString("audit")
	w.WriteArray(2, func() {
		// topic "orders" with partitions [0, 1, 2]
		w.WriteString("orders")
		w.WriteArray(3, func() {
			w.WriteInt32(0)
			w.WriteInt32(1)
			w.WriteInt32(2)
		})
		// topic "payments" with partition [4]
		w.WriteString("payments")
		w.WriteArray(1, func() {
			w.WriteInt32(4)
		})
	})

	req, err := DecodeOffsetDeleteRequest(codec.NewReader(w.Bytes()), 0)
	if err != nil {
		t.Fatalf("decode v0: %v", err)
	}
	if req.GroupID != "audit" {
		t.Errorf("group=%q, want audit", req.GroupID)
	}
	if len(req.Topics) != 2 {
		t.Fatalf("got %d topics, want 2", len(req.Topics))
	}
	if req.Topics[0].Name != "orders" || len(req.Topics[0].Partitions) != 3 ||
		req.Topics[0].Partitions[0] != 0 || req.Topics[0].Partitions[2] != 2 {
		t.Errorf("topic 0 wrong: %+v", req.Topics[0])
	}
	if req.Topics[1].Name != "payments" || len(req.Topics[1].Partitions) != 1 ||
		req.Topics[1].Partitions[0] != 4 {
		t.Errorf("topic 1 wrong: %+v", req.Topics[1])
	}
}

// TestOffsetDeleteResponseRoundTripV0 pins the v0 response shape so
// the AdminClient parser doesn't fall off mid-record. Field order is
// (error_code, throttle, topics) — note this differs from the more
// common (throttle, error_code, …) layout other admin APIs use.
//
// Per the OffsetDelete contract: a non-zero group-level error MUST
// be paired with an empty topics array. The test exercises the
// success shape with a mix of 0 / UNKNOWN_TOPIC_OR_PARTITION codes
// to cover the per-partition error path.
func TestOffsetDeleteResponseRoundTripV0(t *testing.T) {
	resp := &OffsetDeleteResponse{
		ErrorCode:      0,
		ThrottleTimeMs: 0,
		Topics: []OffsetDeleteTopicResponse{
			{
				Name: "orders",
				Partitions: []OffsetDeletePartitionResponse{
					{PartitionIndex: 0, ErrorCode: 0},
					{PartitionIndex: 1, ErrorCode: 3}, // UNKNOWN_TOPIC_OR_PARTITION
				},
			},
		},
	}
	w := codec.NewWriter()
	EncodeOffsetDeleteResponse(w, resp, 0)
	r := codec.NewReader(w.Bytes())

	if v, _ := r.ReadInt16(); v != 0 {
		t.Errorf("group error_code=%d, want 0", v)
	}
	if v, _ := r.ReadInt32(); v != 0 {
		t.Errorf("throttle=%d, want 0", v)
	}
	var topics int
	if err := r.ReadArray(func() error {
		topics++
		name, _ := r.ReadString()
		if name != "orders" {
			t.Errorf("topic name=%q, want orders", name)
		}
		var parts int
		return r.ReadArray(func() error {
			parts++
			pi, _ := r.ReadInt32()
			ec, _ := r.ReadInt16()
			switch parts {
			case 1:
				if pi != 0 || ec != 0 {
					t.Errorf("part 1=(%d,%d), want (0,0)", pi, ec)
				}
			case 2:
				if pi != 1 || ec != 3 {
					t.Errorf("part 2=(%d,%d), want (1,3)", pi, ec)
				}
			}
			return nil
		})
	}); err != nil {
		t.Fatalf("read topics: %v", err)
	}
	if topics != 1 {
		t.Errorf("got %d topics, want 1", topics)
	}
}

// TestOffsetDeleteResponseGroupLevelError covers the abort shape:
// non-zero group-level error code AND empty topics array. A buggy
// encoder that emitted a phantom (-1) topics-array length would
// crash the AdminClient parser; this pins the contract.
func TestOffsetDeleteResponseGroupLevelError(t *testing.T) {
	resp := &OffsetDeleteResponse{
		ErrorCode: 67, // NON_EMPTY_GROUP
	}
	w := codec.NewWriter()
	EncodeOffsetDeleteResponse(w, resp, 0)
	r := codec.NewReader(w.Bytes())
	if v, _ := r.ReadInt16(); v != 67 {
		t.Errorf("group error=%d, want 67", v)
	}
	if v, _ := r.ReadInt32(); v != 0 {
		t.Errorf("throttle=%d, want 0", v)
	}
	var n int
	if err := r.ReadArray(func() error {
		n++
		return nil
	}); err != nil {
		t.Fatalf("topics array: %v", err)
	}
	if n != 0 {
		t.Errorf("got %d topics on group-level error, want 0", n)
	}
}
