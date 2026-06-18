package api

import (
	"testing"

	"github.com/woestebanaan/skafka/internal/protocol/codec"
)

// TestOffsetForLeaderEpochRequestDecodeV0 covers the pre-KIP-320 path
// where there's no ReplicaID and no CurrentLeaderEpoch. v0 still
// shows up in some older client mixes.
func TestOffsetForLeaderEpochRequestDecodeV0(t *testing.T) {
	w := codec.NewWriter()
	w.WriteArray(1, func() {
		w.WriteString("orders")
		w.WriteArray(1, func() {
			w.WriteInt32(7)  // PartitionIndex
			w.WriteInt32(42) // LeaderEpoch
		})
	})

	req, err := DecodeOffsetForLeaderEpochRequest(codec.NewReader(w.Bytes()), 0)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if req.ReplicaID != -1 {
		t.Errorf("ReplicaID=%d, want -1 (default at v0)", req.ReplicaID)
	}
	if len(req.Topics) != 1 || req.Topics[0].Name != "orders" {
		t.Fatalf("topics=%+v", req.Topics)
	}
	p := req.Topics[0].Partitions[0]
	if p.PartitionIndex != 7 || p.LeaderEpoch != 42 || p.CurrentLeaderEpoch != -1 {
		t.Errorf("partition=%+v", p)
	}
}

// TestOffsetForLeaderEpochRequestDecodeV2 covers the KIP-320 path where
// CurrentLeaderEpoch fences a stale client. The handler returns
// FENCED_LEADER_EPOCH when the client thinks the current epoch is
// HIGHER than what the broker knows.
func TestOffsetForLeaderEpochRequestDecodeV2(t *testing.T) {
	w := codec.NewWriter()
	w.WriteInt32(-1) // ReplicaID = consumer
	w.WriteArray(1, func() {
		w.WriteString("orders")
		w.WriteArray(1, func() {
			w.WriteInt32(5)  // CurrentLeaderEpoch
			w.WriteInt32(0)  // PartitionIndex
			w.WriteInt32(3)  // LeaderEpoch
		})
	})

	req, err := DecodeOffsetForLeaderEpochRequest(codec.NewReader(w.Bytes()), 2)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if req.ReplicaID != -1 {
		t.Errorf("ReplicaID=%d, want -1", req.ReplicaID)
	}
	p := req.Topics[0].Partitions[0]
	if p.CurrentLeaderEpoch != 5 || p.PartitionIndex != 0 || p.LeaderEpoch != 3 {
		t.Errorf("v2 partition=%+v, want CLE=5 PI=0 LE=3", p)
	}
}

// TestOffsetForLeaderEpochRequestDecodeV3Flexible covers the KIP-482
// flexible path: compact arrays + tagged-fields at every level.
func TestOffsetForLeaderEpochRequestDecodeV3Flexible(t *testing.T) {
	w := codec.NewWriter()
	w.WriteInt32(-1)
	w.WriteCompactArray(1, func() {
		w.WriteCompactString("orders")
		w.WriteCompactArray(1, func() {
			w.WriteInt32(5)
			w.WriteInt32(0)
			w.WriteInt32(3)
			w.WriteEmptyTaggedFields()
		})
		w.WriteEmptyTaggedFields()
	})
	w.WriteEmptyTaggedFields()

	req, err := DecodeOffsetForLeaderEpochRequest(codec.NewReader(w.Bytes()), 3)
	if err != nil {
		t.Fatalf("decode v3: %v", err)
	}
	if len(req.Topics) != 1 || req.Topics[0].Name != "orders" {
		t.Errorf("topics=%+v", req.Topics)
	}
}

// TestOffsetForLeaderEpochResponseRoundTripV2 pins the v2 layout the
// Java client parses: throttle (int32), then topics, then per-partition
// (error, partition, leader_epoch, end_offset).
func TestOffsetForLeaderEpochResponseRoundTripV2(t *testing.T) {
	resp := &OffsetForLeaderEpochResponse{
		Topics: []OffsetForLeaderEpochTopicResponse{
			{
				Name: "orders",
				Partitions: []OffsetForLeaderEpochPartitionResponse{
					{PartitionIndex: 0, LeaderEpoch: 4, EndOffset: 12345},
				},
			},
		},
	}
	w := codec.NewWriter()
	EncodeOffsetForLeaderEpochResponse(w, resp, 2)
	r := codec.NewReader(w.Bytes())

	if v, _ := r.ReadInt32(); v != 0 {
		t.Errorf("throttle=%d, want 0", v)
	}
	r.ReadArray(func() error {
		name, _ := r.ReadString()
		if name != "orders" {
			t.Errorf("topic=%q", name)
		}
		return r.ReadArray(func() error {
			ec, _ := r.ReadInt16()
			pi, _ := r.ReadInt32()
			le, _ := r.ReadInt32()
			eo, _ := r.ReadInt64()
			if ec != 0 || pi != 0 || le != 4 || eo != 12345 {
				t.Errorf("partition=(err=%d, pi=%d, le=%d, eo=%d)", ec, pi, le, eo)
			}
			return nil
		})
	})
}
