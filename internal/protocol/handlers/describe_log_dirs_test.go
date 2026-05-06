package handlers

import (
	"testing"

	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

// sizedStubStorage is stubStorage with a fixed PartitionSize.
type sizedStubStorage struct {
	stubStorage
	size int64
}

func (s sizedStubStorage) PartitionSize(_ string, _ int32) int64 { return s.size }
func (s sizedStubStorage) DataDir() string                       { return "/data" }

// twoTopicsSource enumerates two topics so we can prove the null-filter path
// fans out across all known topics.
type twoTopicsSource struct{}

func (twoTopicsSource) Get(name string) (int32, bool) {
	switch name {
	case "alpha":
		return 2, true
	case "beta":
		return 1, true
	}
	return 0, false
}
func (twoTopicsSource) All() []TopicEntry {
	return []TopicEntry{{Name: "alpha", Partitions: 2}, {Name: "beta", Partitions: 1}}
}

func decodeLogDirsV1(t *testing.T, body []byte) *api.DescribeLogDirsResponse {
	t.Helper()
	r := codec.NewReader(body)
	resp := &api.DescribeLogDirsResponse{}
	if _, err := r.ReadInt32(); err != nil { // ThrottleTime
		t.Fatal(err)
	}
	if err := r.ReadArray(func() error {
		var res api.DescribeLogDirsResult
		var err error
		if res.ErrorCode, err = r.ReadInt16(); err != nil {
			return err
		}
		if res.LogDir, err = r.ReadString(); err != nil {
			return err
		}
		if err := r.ReadArray(func() error {
			var top api.DescribeLogDirsResponseTopic
			if top.Name, err = r.ReadString(); err != nil {
				return err
			}
			if err := r.ReadArray(func() error {
				var p api.DescribeLogDirsResponsePartition
				if p.PartitionIndex, err = r.ReadInt32(); err != nil {
					return err
				}
				if p.PartitionSize, err = r.ReadInt64(); err != nil {
					return err
				}
				if p.OffsetLag, err = r.ReadInt64(); err != nil {
					return err
				}
				b, err := r.ReadInt8()
				if err != nil {
					return err
				}
				p.IsFutureKey = b != 0
				top.Partitions = append(top.Partitions, p)
				return nil
			}); err != nil {
				return err
			}
			res.Topics = append(res.Topics, top)
			return nil
		}); err != nil {
			return err
		}
		resp.Results = append(resp.Results, res)
		return nil
	}); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return resp
}

// Null Topics request must fan out across every known topic and partition.
func TestDescribeLogDirsNullExpandsAll(t *testing.T) {
	store := sizedStubStorage{size: 4096}
	h := NewDescribeLogDirsHandler(store, twoTopicsSource{})

	w := codec.NewWriter()
	w.WriteInt32(-1) // null Topics array
	out, err := h.Handle(&connstate.ConnState{}, 1, w.Bytes())
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	resp := decodeLogDirsV1(t, out)
	if len(resp.Results) != 1 {
		t.Fatalf("results=%d, want 1", len(resp.Results))
	}
	if resp.Results[0].LogDir != "/data" {
		t.Errorf("LogDir=%q", resp.Results[0].LogDir)
	}
	got := map[string]int{}
	for _, top := range resp.Results[0].Topics {
		got[top.Name] = len(top.Partitions)
	}
	if got["alpha"] != 2 || got["beta"] != 1 {
		t.Errorf("expanded topics: %+v, want alpha:2 beta:1", got)
	}
}

// An explicit (topic, [partitions]) request must report only the listed
// partitions, with PartitionSize coming from the storage stub.
func TestDescribeLogDirsExplicitSubsetReportsSize(t *testing.T) {
	store := sizedStubStorage{size: 256}
	h := NewDescribeLogDirsHandler(store, twoTopicsSource{})

	w := codec.NewWriter()
	w.WriteArray(1, func() {
		w.WriteString("alpha")
		w.WriteArray(1, func() { w.WriteInt32(1) })
	})
	out, err := h.Handle(&connstate.ConnState{}, 1, w.Bytes())
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	resp := decodeLogDirsV1(t, out)
	if len(resp.Results[0].Topics) != 1 || resp.Results[0].Topics[0].Name != "alpha" {
		t.Fatalf("topics=%+v", resp.Results[0].Topics)
	}
	parts := resp.Results[0].Topics[0].Partitions
	if len(parts) != 1 || parts[0].PartitionIndex != 1 || parts[0].PartitionSize != 256 {
		t.Errorf("partitions=%+v", parts)
	}
}

// TestDescribeLogDirsFilterIsolation guards gh #87: the Java
// kafka-log-dirs --topic-list flag asks the broker to return only
// the named topic. A regression where the filter is ignored would
// leak unrelated partitions into the response and the script-side
// "filter sanity" check (scripts/kafka-log-dirs.sh) would only
// catch it post-hoc, after a noisy CI run.
func TestDescribeLogDirsFilterIsolation(t *testing.T) {
	h := NewDescribeLogDirsHandler(sizedStubStorage{size: 16}, twoTopicsSource{})

	// Ask for "alpha" with all-partitions sentinel (empty inner array).
	w := codec.NewWriter()
	w.WriteArray(1, func() {
		w.WriteString("alpha")
		w.WriteArray(0, func() {}) // empty = all partitions of "alpha"
	})
	out, err := h.Handle(&connstate.ConnState{}, 1, w.Bytes())
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	resp := decodeLogDirsV1(t, out)

	// "beta" must NOT appear in the response, even though it's a
	// known topic — the explicit filter should be respected.
	for _, top := range resp.Results[0].Topics {
		if top.Name == "beta" {
			t.Errorf("filter leaked unrelated topic %q (full topics=%+v)", top.Name, resp.Results[0].Topics)
		}
	}
	// And alpha must come back with both its partitions.
	var alpha *api.DescribeLogDirsResponseTopic
	for i, top := range resp.Results[0].Topics {
		if top.Name == "alpha" {
			alpha = &resp.Results[0].Topics[i]
		}
	}
	if alpha == nil || len(alpha.Partitions) != 2 {
		t.Errorf("alpha missing or wrong partition count: %+v", alpha)
	}
}

// Unknown topic in the request is silently dropped (Kafka behaviour: clients
// see the topic absent, not an error code per topic).
func TestDescribeLogDirsUnknownTopicDropped(t *testing.T) {
	h := NewDescribeLogDirsHandler(sizedStubStorage{}, twoTopicsSource{})

	w := codec.NewWriter()
	w.WriteArray(1, func() {
		w.WriteString("does-not-exist")
		w.WriteArray(0, func() {})
	})
	out, err := h.Handle(&connstate.ConnState{}, 1, w.Bytes())
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	resp := decodeLogDirsV1(t, out)
	if len(resp.Results[0].Topics) != 0 {
		t.Errorf("expected no topics, got %+v", resp.Results[0].Topics)
	}
}
