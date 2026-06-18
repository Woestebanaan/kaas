package handlers

import (
	"testing"

	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

// fakeTxnOffsetCommitter records the request the handler hands to
// the coordinator, and returns a programmable response so we can
// verify the handler's encode path.
type fakeTxnOffsetCommitter struct {
	called    bool
	gotReq    *api.TxnOffsetCommitRequest
	respondAs *api.TxnOffsetCommitResponse
}

func (f *fakeTxnOffsetCommitter) TxnOffsetCommit(req *api.TxnOffsetCommitRequest) *api.TxnOffsetCommitResponse {
	f.called = true
	f.gotReq = req
	if f.respondAs != nil {
		return f.respondAs
	}
	// Default: empty success response.
	return &api.TxnOffsetCommitResponse{}
}

// encodeTxnOffsetCommitV3 hand-encodes a minimal v3 (flexible)
// request: one topic, one partition, no metadata.
func encodeTxnOffsetCommitV3(t *testing.T, txnID, groupID string, gen int32, memberID string, pid int64, epoch int16, topic string, partition int32, offset int64) []byte {
	t.Helper()
	w := codec.NewWriter()
	w.WriteCompactString(txnID)
	w.WriteCompactString(groupID)
	w.WriteInt64(pid)
	w.WriteInt16(epoch)
	w.WriteInt32(gen)
	w.WriteCompactString(memberID)
	w.WriteCompactNullableString("", true) // group instance id = null
	w.WriteCompactArray(1, func() {
		w.WriteCompactString(topic)
		w.WriteCompactArray(1, func() {
			w.WriteInt32(partition)
			w.WriteInt64(offset)
			w.WriteInt32(-1)                       // leader epoch
			w.WriteCompactNullableString("", true) // metadata = null
			w.WriteEmptyTaggedFields()
		})
		w.WriteEmptyTaggedFields()
	})
	w.WriteEmptyTaggedFields()
	return w.Bytes()
}

// TestTxnOffsetCommit_DelegatesToCoordinator pins gh #27: the
// handler is a thin shell — decode the request, hand it to
// Manager.TxnOffsetCommit, encode the response. The test confirms
// the request decoded fields land on the coordinator call verbatim.
func TestTxnOffsetCommit_DelegatesToCoordinator(t *testing.T) {
	stub := &fakeTxnOffsetCommitter{}
	h := NewTxnOffsetCommitHandler(stub)
	body := encodeTxnOffsetCommitV3(t, "my-txn", "my-group", -1, "", 100, 5, "events", 3, 42)

	_, err := h.Handle(&connstate.ConnState{}, 3, body)
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if !stub.called {
		t.Fatal("Manager.TxnOffsetCommit was not invoked")
	}
	got := stub.gotReq
	if got.TransactionalID != "my-txn" {
		t.Errorf("TransactionalID=%q, want my-txn", got.TransactionalID)
	}
	if got.GroupID != "my-group" {
		t.Errorf("GroupID=%q, want my-group", got.GroupID)
	}
	if got.ProducerID != 100 || got.ProducerEpoch != 5 {
		t.Errorf("PID/epoch=(%d, %d), want (100, 5)", got.ProducerID, got.ProducerEpoch)
	}
	if len(got.Topics) != 1 || got.Topics[0].Name != "events" {
		t.Fatalf("topics=%+v, want one events topic", got.Topics)
	}
	if len(got.Topics[0].Partitions) != 1 || got.Topics[0].Partitions[0].PartitionIndex != 3 || got.Topics[0].Partitions[0].CommittedOffset != 42 {
		t.Errorf("partition row=%+v, want (idx=3, offset=42)", got.Topics[0].Partitions[0])
	}
}

// TestTxnOffsetCommit_NotCoordinatorPropagates pins the wire
// contract: when the coordinator reports NOT_COORDINATOR per
// partition (group hash mismatch / handoff in progress), the
// handler encodes that error code back to the client so the
// producer can re-resolve and retry.
func TestTxnOffsetCommit_NotCoordinatorPropagates(t *testing.T) {
	stub := &fakeTxnOffsetCommitter{
		respondAs: &api.TxnOffsetCommitResponse{
			Topics: []api.TxnOffsetCommitResponseTopic{
				{
					Name: "events",
					Partitions: []api.TxnOffsetCommitResponsePartition{
						{PartitionIndex: 3, ErrorCode: int16(codec.ErrNotCoordinator)},
					},
				},
			},
		},
	}
	h := NewTxnOffsetCommitHandler(stub)
	body := encodeTxnOffsetCommitV3(t, "my-txn", "my-group", -1, "", 100, 5, "events", 3, 42)

	out, err := h.Handle(&connstate.ConnState{}, 3, body)
	if err != nil {
		t.Fatal(err)
	}

	// v3 response layout: throttle int32, topics compact array
	// (compact-string name + partition compact array { idx int32,
	// errorCode int16, tagged_fields }, tagged_fields), tagged_fields.
	// Read the first partition's error code by walking past throttle +
	// array header + topic name + partition count.
	r := codec.NewReader(out)
	if _, err := r.ReadInt32(); err != nil {
		t.Fatal(err)
	} // throttle
	if _, err := r.ReadUvarint(); err != nil { // compact array len
		t.Fatal(err)
	}
	if _, err := r.ReadCompactString(); err != nil { // topic name
		t.Fatal(err)
	}
	if _, err := r.ReadUvarint(); err != nil { // partitions array len
		t.Fatal(err)
	}
	if _, err := r.ReadInt32(); err != nil { // partition index
		t.Fatal(err)
	}
	gotCode, err := r.ReadInt16()
	if err != nil {
		t.Fatal(err)
	}
	if gotCode != int16(codec.ErrNotCoordinator) {
		t.Errorf("partition ErrorCode=%d, want %d (ErrNotCoordinator)", gotCode, codec.ErrNotCoordinator)
	}
}
