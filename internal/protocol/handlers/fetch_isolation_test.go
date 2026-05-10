package handlers

import (
	"testing"

	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

// TestFetchRequestV4DecodesIsolationLevel pins the gh #31 wire
// surface: FetchRequest v4+ carries IsolationLevel (int8); v0–v3
// does not. The handler reads it via api.DecodeFetchRequest;
// this test guards the codec's round-trip from the wire bytes.
func TestFetchRequestV4DecodesIsolationLevel(t *testing.T) {
	// Build a v4 request with isolation_level = 1 (read_committed).
	w := codec.NewWriter()
	w.WriteInt32(-1)         // ReplicaID
	w.WriteInt32(100)        // MaxWaitMs
	w.WriteInt32(1)          // MinBytes
	w.WriteInt32(10_000_000) // MaxBytes
	w.WriteInt8(1)           // IsolationLevel = read_committed
	w.WriteArray(0, func() {})

	r := codec.NewReader(w.Bytes())
	req, err := api.DecodeFetchRequest(r, 4)
	if err != nil {
		t.Fatalf("decode v4: %v", err)
	}
	if req.IsolationLevel != 1 {
		t.Errorf("IsolationLevel=%d, want 1 (read_committed)", req.IsolationLevel)
	}
}

// TestFetchResponseV4EncodesEmptyAbortedTransactions pins the
// inverse — v4+ responses include the AbortedTransactions array
// (encoded as length=0 when no aborts are tracked, which is
// skafka's current "no in-flight txn state" state). Pre-fix a
// missing field would have surfaced as a Java consumer
// IllegalStateException on the v4 path; the codec was already in
// place but this test guards against future regressions.
func TestFetchResponseV4EncodesEmptyAbortedTransactions(t *testing.T) {
	resp := &api.FetchResponse{
		ThrottleTimeMs: 0,
		Responses: []api.FetchTopicResponse{
			{
				Name: "t",
				Partitions: []api.FetchPartitionResponse{
					{
						PartitionIndex:       0,
						ErrorCode:            0,
						HighWatermark:        100,
						LastStableOffset:     100,
						LogStartOffset:       0,
						AbortedTransactions:  nil, // empty
						PreferredReadReplica: -1,
						Records:              []byte{},
					},
				},
			},
		},
	}
	w := codec.NewWriter()
	api.EncodeFetchResponse(w, resp, 4)
	body := w.Bytes()

	// Re-decode just enough to confirm the array length byte is
	// present and = 0 (or = -1, the legacy null encoding).
	r := codec.NewReader(body)
	_, _ = r.ReadInt32() // ThrottleTimeMs
	// Responses array (regular, not compact in v4).
	var sawArray bool
	err := r.ReadArray(func() error {
		_, _ = r.ReadString() // topic name
		return r.ReadArray(func() error {
			_, _ = r.ReadInt32() // partitionIndex
			_, _ = r.ReadInt16() // errorCode
			_, _ = r.ReadInt64() // HW
			_, _ = r.ReadInt64() // LSO (v4+)
			// LogStartOffset is v5+; at v4 it's NOT on the wire.
			// AbortedTransactions: regular nullable array v4-11.
			n, err := r.ReadInt32()
			if err != nil {
				return err
			}
			sawArray = true
			if n > 0 {
				t.Errorf("AbortedTransactions length=%d, want 0 or -1 (no in-flight txns)", n)
			}
			// Skip the rest of the partition record for this test.
			_, _ = r.ReadBytes() // records (nullable bytes)
			return nil
		})
	})
	if err != nil {
		t.Fatalf("response re-decode: %v", err)
	}
	if !sawArray {
		t.Fatal("AbortedTransactions array slot not written into v4 response — wire surface broken")
	}
}

// TestFetchRequestV3IgnoresIsolationLevel: v0–v3 has no
// isolation_level field; the codec must not consume bytes for it.
func TestFetchRequestV3IgnoresIsolationLevel(t *testing.T) {
	w := codec.NewWriter()
	w.WriteInt32(-1)         // ReplicaID
	w.WriteInt32(100)        // MaxWaitMs
	w.WriteInt32(1)          // MinBytes
	w.WriteInt32(10_000_000) // MaxBytes (v3+)
	w.WriteArray(0, func() {})

	r := codec.NewReader(w.Bytes())
	req, err := api.DecodeFetchRequest(r, 3)
	if err != nil {
		t.Fatalf("decode v3: %v", err)
	}
	if req.IsolationLevel != 0 {
		t.Errorf("IsolationLevel=%d, want 0 (default for v3 — field not on the wire)",
			req.IsolationLevel)
	}
}
