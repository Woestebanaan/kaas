package api

import "github.com/woestebanaan/skafka/internal/protocol/codec"

// ListOffsetsRequest (key 2, v1–v7).
type ListOffsetsRequest struct {
	ReplicaID      int32  // always -1 for clients
	IsolationLevel int8   // v2+
	Topics         []ListOffsetsTopic
}

type ListOffsetsTopic struct {
	Name       string
	Partitions []ListOffsetsPartition
}

type ListOffsetsPartition struct {
	PartitionIndex     int32
	CurrentLeaderEpoch int32 // v4+
	Timestamp          int64 // -1=latest, -2=earliest
}

// ListOffsetsResponse (key 2, v1–v7).
type ListOffsetsResponse struct {
	ThrottleTimeMs int32 // v2+
	Topics         []ListOffsetsTopicResponse
}

type ListOffsetsTopicResponse struct {
	Name       string
	Partitions []ListOffsetsPartitionResponse
}

type ListOffsetsPartitionResponse struct {
	PartitionIndex int32
	ErrorCode      int16
	Timestamp      int64 // v1+
	Offset         int64 // v1+
	LeaderEpoch    int32 // v4+
}

func DecodeListOffsetsRequest(r *codec.Reader, version int16) (*ListOffsetsRequest, error) {
	req := &ListOffsetsRequest{}
	flexible := version >= 6

	var err error
	if req.ReplicaID, err = r.ReadInt32(); err != nil {
		return nil, err
	}
	if version >= 2 {
		v, err := r.ReadInt8()
		if err != nil {
			return nil, err
		}
		req.IsolationLevel = v
	}

	readTopics := func() error {
		var t ListOffsetsTopic
		var err error
		if t.Name, err = readString(r, flexible); err != nil {
			return err
		}
		readPartitions := func() error {
			var p ListOffsetsPartition
			var err error
			if p.PartitionIndex, err = r.ReadInt32(); err != nil {
				return err
			}
			if version >= 4 {
				if p.CurrentLeaderEpoch, err = r.ReadInt32(); err != nil {
					return err
				}
			}
			if p.Timestamp, err = r.ReadInt64(); err != nil {
				return err
			}
			if flexible {
				if err := r.ReadTaggedFields(); err != nil {
					return err
				}
			}
			t.Partitions = append(t.Partitions, p)
			return nil
		}
		if flexible {
			err = r.ReadCompactArray(readPartitions)
		} else {
			err = r.ReadArray(readPartitions)
		}
		if err != nil {
			return err
		}
		if flexible {
			if err := r.ReadTaggedFields(); err != nil {
				return err
			}
		}
		req.Topics = append(req.Topics, t)
		return nil
	}

	if flexible {
		if err := r.ReadCompactArray(readTopics); err != nil {
			return nil, err
		}
		return req, r.ReadTaggedFields()
	}
	return req, r.ReadArray(readTopics)
}

func EncodeListOffsetsResponse(w *codec.Writer, resp *ListOffsetsResponse, version int16) {
	flexible := version >= 6

	if version >= 2 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}

	writeTopics := func() {
		for _, t := range resp.Topics {
			writeString(w, t.Name, flexible)
			writePartitions := func() {
				for _, p := range t.Partitions {
					w.WriteInt32(p.PartitionIndex)
					w.WriteInt16(p.ErrorCode)
					if version >= 1 {
						w.WriteInt64(p.Timestamp)
						w.WriteInt64(p.Offset)
					}
					if version >= 4 {
						w.WriteInt32(p.LeaderEpoch)
					}
					if flexible {
						w.WriteEmptyTaggedFields()
					}
				}
			}
			if flexible {
				w.WriteCompactArray(len(t.Partitions), writePartitions)
			} else {
				w.WriteArray(len(t.Partitions), writePartitions)
			}
			if flexible {
				w.WriteEmptyTaggedFields()
			}
		}
	}
	if flexible {
		w.WriteCompactArray(len(resp.Topics), writeTopics)
	} else {
		w.WriteArray(len(resp.Topics), writeTopics)
	}

	if flexible {
		w.WriteEmptyTaggedFields()
	}
}
