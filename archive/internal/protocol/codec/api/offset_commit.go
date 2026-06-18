package api

import "github.com/woestebanaan/skafka/internal/protocol/codec"

// OffsetCommitRequest (key 8, v2–v8).
type OffsetCommitRequest struct {
	GroupID         string
	GenerationID    int32  // v1+
	MemberID        string // v1+
	GroupInstanceID string // v7+, nullable
	Topics          []OffsetCommitTopic
}

type OffsetCommitTopic struct {
	Name       string
	Partitions []OffsetCommitPartition
}

type OffsetCommitPartition struct {
	PartitionIndex      int32
	CommittedOffset     int64
	CommittedLeaderEpoch int32  // v6+
	CommittedMetadata   string // nullable
}

// OffsetCommitResponse (key 8, v2–v8).
type OffsetCommitResponse struct {
	ThrottleTimeMs int32 // v3+
	Topics         []OffsetCommitTopicResponse
}

type OffsetCommitTopicResponse struct {
	Name       string
	Partitions []OffsetCommitPartitionResponse
}

type OffsetCommitPartitionResponse struct {
	PartitionIndex int32
	ErrorCode      int16
}

func DecodeOffsetCommitRequest(r *codec.Reader, version int16) (*OffsetCommitRequest, error) {
	req := &OffsetCommitRequest{}
	flexible := version >= 8
	var err error

	if req.GroupID, err = readString(r, flexible); err != nil {
		return nil, err
	}
	if version >= 1 {
		if req.GenerationID, err = r.ReadInt32(); err != nil {
			return nil, err
		}
		if req.MemberID, err = readString(r, flexible); err != nil {
			return nil, err
		}
	}
	if version >= 7 {
		s, _, err := nullableString(r, flexible)
		if err != nil {
			return nil, err
		}
		req.GroupInstanceID = s
	}

	readTopics := func() error {
		var t OffsetCommitTopic
		var err error
		if t.Name, err = readString(r, flexible); err != nil {
			return err
		}
		readPartitions := func() error {
			var p OffsetCommitPartition
			var err error
			if p.PartitionIndex, err = r.ReadInt32(); err != nil {
				return err
			}
			if p.CommittedOffset, err = r.ReadInt64(); err != nil {
				return err
			}
			if version >= 6 {
				if p.CommittedLeaderEpoch, err = r.ReadInt32(); err != nil {
					return err
				}
			}
			meta, _, err := nullableString(r, flexible)
			if err != nil {
				return err
			}
			p.CommittedMetadata = meta
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

func EncodeOffsetCommitResponse(w *codec.Writer, resp *OffsetCommitResponse, version int16) {
	flexible := version >= 8
	if version >= 3 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}
	writeTopics := func() {
		for _, t := range resp.Topics {
			writeString(w, t.Name, flexible)
			writePartitions := func() {
				for _, p := range t.Partitions {
					w.WriteInt32(p.PartitionIndex)
					w.WriteInt16(p.ErrorCode)
					if flexible {
						w.WriteEmptyTaggedFields()
					}
				}
			}
			if flexible {
				w.WriteCompactArray(len(t.Partitions), writePartitions)
				w.WriteEmptyTaggedFields()
			} else {
				w.WriteArray(len(t.Partitions), writePartitions)
			}
		}
	}
	if flexible {
		w.WriteCompactArray(len(resp.Topics), writeTopics)
		w.WriteEmptyTaggedFields()
	} else {
		w.WriteArray(len(resp.Topics), writeTopics)
	}
}
