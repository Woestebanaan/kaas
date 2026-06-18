package handlers

import (
	"context"
	"errors"
	"testing"

	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

// fakeMarkerStore records every Append call.
type fakeMarkerStore struct {
	appends []markerAppend
	err     error
}

type markerAppend struct {
	topic     string
	partition int32
	batchLen  int
}

func (s *fakeMarkerStore) Append(_ context.Context, topic string, partition int32, _ uint32, _ int16, batch []byte) (int64, error) {
	s.appends = append(s.appends, markerAppend{topic, partition, len(batch)})
	if s.err != nil {
		return 0, s.err
	}
	return 42, nil
}

// fakeOwnership: nil → owns all; map → selective.
type fakeOwnership struct {
	ownsByKey map[string]bool
}

func (f fakeOwnership) Owns(topic string, partition int32) bool {
	if f.ownsByKey == nil {
		return true
	}
	return f.ownsByKey[topic+":"+itoa(partition)]
}

func itoa(p int32) string {
	if p == 0 {
		return "0"
	}
	return "p"
}

func encodeWriteTxnMarkersRequest(t *testing.T, markers []api.WritableTxnMarker, version int16) []byte {
	t.Helper()
	flexible := version >= 1
	w := codec.NewWriter()

	writeArr := func(n int, fn func()) {
		if flexible {
			w.WriteCompactArray(n, fn)
		} else {
			w.WriteArray(n, fn)
		}
	}
	writeStr := func(s string) {
		if flexible {
			w.WriteCompactString(s)
		} else {
			w.WriteString(s)
		}
	}

	writeArr(len(markers), func() {
		for _, m := range markers {
			w.WriteInt64(m.ProducerID)
			w.WriteInt16(m.ProducerEpoch)
			if m.TransactionResult {
				w.WriteInt8(1)
			} else {
				w.WriteInt8(0)
			}
			writeArr(len(m.Topics), func() {
				for _, top := range m.Topics {
					writeStr(top.Name)
					writeArr(len(top.PartitionIndexes), func() {
						for _, p := range top.PartitionIndexes {
							w.WriteInt32(p)
						}
					})
					if flexible {
						w.WriteEmptyTaggedFields()
					}
				}
			})
			w.WriteInt32(m.CoordinatorEpoch)
			if flexible {
				w.WriteEmptyTaggedFields()
			}
		}
	})
	if flexible {
		w.WriteEmptyTaggedFields()
	}
	return w.Bytes()
}

func TestWriteTxnMarkersHappyCommit(t *testing.T) {
	store := &fakeMarkerStore{}
	h := NewWriteTxnMarkersHandler(store)
	body := encodeWriteTxnMarkersRequest(t, []api.WritableTxnMarker{{
		ProducerID:        100,
		ProducerEpoch:     5,
		TransactionResult: true,
		Topics: []api.WritableTxnMarkerTopic{
			{Name: "t1", PartitionIndexes: []int32{0, 1}},
		},
		CoordinatorEpoch: 7,
	}}, 1)
	out, err := h.Handle(nil, 1, body)
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	_ = out

	if len(store.appends) != 2 {
		t.Fatalf("expected 2 marker appends (2 partitions), got %d", len(store.appends))
	}
	for _, a := range store.appends {
		if a.topic != "t1" {
			t.Errorf("topic=%q, want t1", a.topic)
		}
		// Control batch is ~70 bytes; sanity-bound it.
		if a.batchLen < 60 || a.batchLen > 200 {
			t.Errorf("batch length %d outside expected range [60, 200]", a.batchLen)
		}
	}
}

func TestWriteTxnMarkersNotLeaderForNonOwnedPartition(t *testing.T) {
	store := &fakeMarkerStore{}
	// Mark partition 0 as owned, partition 1 as NOT owned.
	h := NewWriteTxnMarkersHandler(store).WithOwnership(fakeOwnership{
		ownsByKey: map[string]bool{
			"t1:0": true,
		},
	})
	body := encodeWriteTxnMarkersRequest(t, []api.WritableTxnMarker{{
		ProducerID:        100,
		ProducerEpoch:     5,
		TransactionResult: true,
		Topics: []api.WritableTxnMarkerTopic{
			{Name: "t1", PartitionIndexes: []int32{0, 1}},
		},
		CoordinatorEpoch: 7,
	}}, 1)
	out, err := h.Handle(nil, 1, body)
	if err != nil {
		t.Fatalf("handle: %v", err)
	}

	// Decode response — only partition 0 should be PASS; partition 1
	// should be NOT_LEADER_OR_FOLLOWER.
	resp := decodeWTMResponse(t, out, 1)
	if len(resp.Markers) != 1 {
		t.Fatalf("expected 1 marker result, got %d", len(resp.Markers))
	}
	parts := resp.Markers[0].Topics[0].Partitions
	if len(parts) != 2 {
		t.Fatalf("expected 2 partition results, got %d", len(parts))
	}
	if parts[0].ErrorCode != 0 {
		t.Errorf("partition 0 errCode=%d, want 0 (owned, accepted)", parts[0].ErrorCode)
	}
	if parts[1].ErrorCode != int16(codec.ErrNotLeaderOrFollower) {
		t.Errorf("partition 1 errCode=%d, want NOT_LEADER_OR_FOLLOWER (%d)",
			parts[1].ErrorCode, codec.ErrNotLeaderOrFollower)
	}
	// Only the owned partition got Append'd.
	if len(store.appends) != 1 {
		t.Errorf("expected 1 Append (owned partition only), got %d", len(store.appends))
	}
}

func TestWriteTxnMarkersStoreErrorMapped(t *testing.T) {
	store := &fakeMarkerStore{err: errors.New("disk full")}
	h := NewWriteTxnMarkersHandler(store)
	body := encodeWriteTxnMarkersRequest(t, []api.WritableTxnMarker{{
		ProducerID:        1,
		ProducerEpoch:     0,
		TransactionResult: false, // ABORT
		Topics: []api.WritableTxnMarkerTopic{
			{Name: "t", PartitionIndexes: []int32{0}},
		},
		CoordinatorEpoch: 0,
	}}, 1)
	out, _ := h.Handle(nil, 1, body)
	resp := decodeWTMResponse(t, out, 1)
	if resp.Markers[0].Topics[0].Partitions[0].ErrorCode != int16(codec.ErrUnknownServerError) {
		t.Fatalf("got errCode=%d, want UNKNOWN_SERVER_ERROR", resp.Markers[0].Topics[0].Partitions[0].ErrorCode)
	}
}

func decodeWTMResponse(t *testing.T, body []byte, version int16) *api.WriteTxnMarkersResponse {
	t.Helper()
	flexible := version >= 1
	r := codec.NewReader(body)
	resp := &api.WriteTxnMarkersResponse{}

	readStr := func() (string, error) {
		if flexible {
			return r.ReadCompactString()
		}
		return r.ReadString()
	}
	readArr := func(fn func() error) error {
		if flexible {
			return r.ReadCompactArray(fn)
		}
		return r.ReadArray(fn)
	}

	readMarker := func() error {
		var m api.WritableTxnMarkerResult
		var err error
		m.ProducerID, err = r.ReadInt64()
		if err != nil {
			return err
		}
		readTopic := func() error {
			var top api.WritableTxnMarkerTopicResult
			name, err := readStr()
			if err != nil {
				return err
			}
			top.Name = name
			readPart := func() error {
				var p api.WritableTxnMarkerPartitionResult
				p.PartitionIndex, err = r.ReadInt32()
				if err != nil {
					return err
				}
				p.ErrorCode, err = r.ReadInt16()
				if err != nil {
					return err
				}
				if flexible {
					if err := r.ReadTaggedFields(); err != nil {
						return err
					}
				}
				top.Partitions = append(top.Partitions, p)
				return nil
			}
			if err := readArr(readPart); err != nil {
				return err
			}
			if flexible {
				if err := r.ReadTaggedFields(); err != nil {
					return err
				}
			}
			m.Topics = append(m.Topics, top)
			return nil
		}
		if err := readArr(readTopic); err != nil {
			return err
		}
		if flexible {
			if err := r.ReadTaggedFields(); err != nil {
				return err
			}
		}
		resp.Markers = append(resp.Markers, m)
		return nil
	}
	if err := readArr(readMarker); err != nil {
		t.Fatalf("decode markers: %v", err)
	}
	if flexible {
		_ = r.ReadTaggedFields()
	}
	return resp
}
