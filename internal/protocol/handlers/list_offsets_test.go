package handlers

import (
	"context"
	"testing"

	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
	"github.com/woestebanaan/skafka/internal/storage"
)

// stubStorage is the minimum StorageEngine surface ListOffsetsHandler needs.
type stubStorage struct {
	hwm    int64
	logSt  int64
	noPart bool
}

func (s stubStorage) HighWatermark(_ string, _ int32) (int64, error) {
	if s.noPart {
		return 0, errStubMissing
	}
	return s.hwm, nil
}
func (s stubStorage) LogStartOffset(_ string, _ int32) (int64, error) {
	if s.noPart {
		return 0, errStubMissing
	}
	return s.logSt, nil
}
func (stubStorage) Append(_ context.Context, _ string, _ int32, _ uint32, _ []byte) (int64, error) {
	return 0, nil
}
func (stubStorage) Read(_ context.Context, _ string, _ int32, _ int64, _ int) ([]byte, error) {
	return nil, nil
}
func (stubStorage) CreatePartition(_ string, _ int32) error { return nil }
func (stubStorage) DeletePartition(_ string, _ int32) error { return nil }
func (stubStorage) PartitionSize(_ string, _ int32) int64   { return 0 }
func (stubStorage) DataDir() string                         { return "/tmp/stub" }
func (stubStorage) TakeOver(_ context.Context, _ string, _ int32, _ uint32) (int64, error) {
	return 0, nil
}
func (stubStorage) Relinquish(_ string, _ int32) error { return nil }
func (stubStorage) DeleteRecords(_ string, _ int32, _ int64) (int64, error) {
	return 0, nil
}

var _ storage.StorageEngine = stubStorage{}

type stubError string

func (e stubError) Error() string { return string(e) }

const errStubMissing = stubError("stub: no partition")

// buildListOffsetsBody builds a v1 ListOffsets request body for one (topic, partition, ts).
func buildListOffsetsBody(t *testing.T, topic string, partition int32, ts int64) []byte {
	t.Helper()
	w := codec.NewWriter()
	w.WriteInt32(-1) // ReplicaID = -1 (consumer)
	w.WriteArray(1, func() {
		w.WriteString(topic)
		w.WriteArray(1, func() {
			w.WriteInt32(partition)
			w.WriteInt64(ts)
		})
	})
	return w.Bytes()
}

// decodeListOffsetsV1 hand-rolls a minimal v1 response decoder (the codec/api
// package only ships an encoder for responses).
func decodeListOffsetsV1(t *testing.T, body []byte) *api.ListOffsetsResponse {
	t.Helper()
	r := codec.NewReader(body)
	resp := &api.ListOffsetsResponse{}
	if err := r.ReadArray(func() error {
		var topic api.ListOffsetsTopicResponse
		name, err := r.ReadString()
		if err != nil {
			return err
		}
		topic.Name = name
		if err := r.ReadArray(func() error {
			var p api.ListOffsetsPartitionResponse
			if p.PartitionIndex, err = r.ReadInt32(); err != nil {
				return err
			}
			if p.ErrorCode, err = r.ReadInt16(); err != nil {
				return err
			}
			if p.Timestamp, err = r.ReadInt64(); err != nil {
				return err
			}
			if p.Offset, err = r.ReadInt64(); err != nil {
				return err
			}
			topic.Partitions = append(topic.Partitions, p)
			return nil
		}); err != nil {
			return err
		}
		resp.Topics = append(resp.Topics, topic)
		return nil
	}); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return resp
}

// Real-timestamp lookups must NOT return a positive offset paired with timestamp=-1
// — the Java client's OffsetAndTimestamp ctor rejects negative timestamps when offset
// is valid, which crashes kafbat's "Newest" message view (empirically observed via
// IllegalArgumentException: Invalid negative timestamp).
func TestListOffsetsRealTimestampReturnsNoMatch(t *testing.T) {
	h := NewListOffsetsHandler(stubStorage{hwm: 5, logSt: 0}, stubLeaseManager{})

	// Some real timestamp (here: now-ish, but any positive value is the same path).
	body := buildListOffsetsBody(t, "t", 0, 1700000000000)
	out, err := h.Handle(&connstate.ConnState{}, 1, body)
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	resp := decodeListOffsetsV1(t, out)
	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("unexpected response shape: %+v", resp)
	}
	p := resp.Topics[0].Partitions[0]
	if p.Offset != -1 {
		t.Errorf("real-timestamp lookup returned Offset=%d, want -1 (no-match sentinel)", p.Offset)
	}
	if p.Timestamp != -1 {
		t.Errorf("real-timestamp lookup returned Timestamp=%d, want -1", p.Timestamp)
	}
}

// EARLIEST (-2) and LATEST (-1) must keep working: offset is the real boundary,
// timestamp=-1 is conventional. The Java client uses these via beginningOffsets /
// endOffsets, which do not construct OffsetAndTimestamp, so timestamp=-1 is safe.
func TestListOffsetsLatestEarliest(t *testing.T) {
	h := NewListOffsetsHandler(stubStorage{hwm: 7, logSt: 2}, stubLeaseManager{})

	for _, tc := range []struct {
		name   string
		ts     int64
		wantOf int64
	}{
		{"latest", -1, 7},
		{"earliest", -2, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := buildListOffsetsBody(t, "t", 0, tc.ts)
			out, err := h.Handle(&connstate.ConnState{}, 1, body)
			if err != nil {
				t.Fatalf("Handle: %v", err)
			}
			resp := decodeListOffsetsV1(t, out)
			p := resp.Topics[0].Partitions[0]
			if p.Offset != tc.wantOf {
				t.Errorf("Offset=%d, want %d", p.Offset, tc.wantOf)
			}
			if p.Timestamp != -1 {
				t.Errorf("Timestamp=%d, want -1", p.Timestamp)
			}
		})
	}
}
