package api

import "github.com/woestebanaan/skafka/internal/protocol/codec"

// DescribeLogDirsRequest (key 35).
//
// Versions implemented: v0–v1 (non-flexible). v2+ uses flexible encoding and
// adds TotalBytes/UsableBytes fields; clients negotiate down based on the
// max we advertise in ApiVersions.
//
// A null Topics array means "describe every log dir, every topic".
type DescribeLogDirsRequest struct {
	Topics    []DescribeLogDirsRequestTopic
	TopicNull bool // true if the request's Topics array was null
}

type DescribeLogDirsRequestTopic struct {
	Name       string
	Partitions []int32
}

// DescribeLogDirsResponse (key 35, v0–v1).
type DescribeLogDirsResponse struct {
	ThrottleTimeMs int32
	Results        []DescribeLogDirsResult
}

type DescribeLogDirsResult struct {
	ErrorCode int16
	LogDir    string
	Topics    []DescribeLogDirsResponseTopic
}

type DescribeLogDirsResponseTopic struct {
	Name       string
	Partitions []DescribeLogDirsResponsePartition
}

type DescribeLogDirsResponsePartition struct {
	PartitionIndex int32
	PartitionSize  int64
	OffsetLag      int64
	IsFutureKey    bool
}

func DecodeDescribeLogDirsRequest(r *codec.Reader, _ int16) (*DescribeLogDirsRequest, error) {
	req := &DescribeLogDirsRequest{}

	count, err := r.ReadInt32()
	if err != nil {
		return nil, err
	}
	if count < 0 {
		req.TopicNull = true
		return req, nil
	}
	for i := int32(0); i < count; i++ {
		var t DescribeLogDirsRequestTopic
		t.Name, err = r.ReadString()
		if err != nil {
			return nil, err
		}
		pCount, err := r.ReadInt32()
		if err != nil {
			return nil, err
		}
		for j := int32(0); j < pCount; j++ {
			p, err := r.ReadInt32()
			if err != nil {
				return nil, err
			}
			t.Partitions = append(t.Partitions, p)
		}
		req.Topics = append(req.Topics, t)
	}
	return req, nil
}

func EncodeDescribeLogDirsResponse(w *codec.Writer, resp *DescribeLogDirsResponse, _ int16) {
	w.WriteInt32(resp.ThrottleTimeMs)
	w.WriteArray(len(resp.Results), func() {
		for _, r := range resp.Results {
			w.WriteInt16(r.ErrorCode)
			w.WriteString(r.LogDir)
			w.WriteArray(len(r.Topics), func() {
				for _, t := range r.Topics {
					w.WriteString(t.Name)
					w.WriteArray(len(t.Partitions), func() {
						for _, p := range t.Partitions {
							w.WriteInt32(p.PartitionIndex)
							w.WriteInt64(p.PartitionSize)
							w.WriteInt64(p.OffsetLag)
							writeBool(w, p.IsFutureKey)
						}
					})
				}
			})
		}
	})
}
