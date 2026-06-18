package api

import "github.com/woestebanaan/skafka/internal/protocol/codec"

// OffsetFetchRequest (key 9, v1–v8).
type OffsetFetchRequest struct {
	// v1–v7: single group
	GroupID string
	Topics  []OffsetFetchTopic // nil = fetch all topics
	// v8+: multiple groups
	Groups            []OffsetFetchGroup
	RequireStable     bool // v7+
}

type OffsetFetchTopic struct {
	Name             string
	PartitionIndexes []int32
}

type OffsetFetchGroup struct {
	GroupID string
	Topics  []OffsetFetchTopic
}

// OffsetFetchResponse (key 9, v1–v8).
type OffsetFetchResponse struct {
	ThrottleTimeMs int32  // v3+
	// v1–v7: flat topic list
	Topics    []OffsetFetchTopicResponse
	ErrorCode int16 // v2+
	// v8+: per-group responses
	Groups []OffsetFetchGroupResponse
}

type OffsetFetchTopicResponse struct {
	Name       string
	Partitions []OffsetFetchPartitionResponse
}

type OffsetFetchPartitionResponse struct {
	PartitionIndex      int32
	CommittedOffset     int64
	CommittedLeaderEpoch int32  // v5+
	Metadata            string // nullable
	ErrorCode           int16
}

type OffsetFetchGroupResponse struct {
	GroupID   string
	Topics    []OffsetFetchTopicResponse
	ErrorCode int16
}

func DecodeOffsetFetchRequest(r *codec.Reader, version int16) (*OffsetFetchRequest, error) {
	req := &OffsetFetchRequest{}
	flexible := version >= 6

	if version >= 8 {
		readGroup := func() error {
			var g OffsetFetchGroup
			var err error
			if g.GroupID, err = r.ReadCompactString(); err != nil {
				return err
			}
			if err := r.ReadCompactArray(func() error {
				var t OffsetFetchTopic
				var err error
				if t.Name, err = r.ReadCompactString(); err != nil {
					return err
				}
				if err := r.ReadCompactArray(func() error {
					idx, err := r.ReadInt32()
					t.PartitionIndexes = append(t.PartitionIndexes, idx)
					return err
				}); err != nil {
					return err
				}
				if err := r.ReadTaggedFields(); err != nil {
					return err
				}
				g.Topics = append(g.Topics, t)
				return nil
			}); err != nil {
				return err
			}
			if err := r.ReadTaggedFields(); err != nil {
				return err
			}
			req.Groups = append(req.Groups, g)
			return nil
		}
		if err := r.ReadCompactArray(readGroup); err != nil {
			return nil, err
		}
		v, err := r.ReadInt8()
		if err != nil {
			return nil, err
		}
		req.RequireStable = v != 0
		return req, r.ReadTaggedFields()
	}

	var err error
	if req.GroupID, err = readString(r, flexible); err != nil {
		return nil, err
	}

	readTopics := func() error {
		var t OffsetFetchTopic
		var err error
		if t.Name, err = readString(r, flexible); err != nil {
			return err
		}
		readIdx := func() error {
			idx, err := r.ReadInt32()
			t.PartitionIndexes = append(t.PartitionIndexes, idx)
			return err
		}
		if flexible {
			err = r.ReadCompactArray(readIdx)
		} else {
			err = r.ReadArray(readIdx)
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
	} else {
		if err := r.ReadArray(readTopics); err != nil {
			return nil, err
		}
	}

	if version >= 7 {
		v, err := r.ReadInt8()
		if err != nil {
			return nil, err
		}
		req.RequireStable = v != 0
	}
	if flexible {
		return req, r.ReadTaggedFields()
	}
	return req, nil
}

func EncodeOffsetFetchResponse(w *codec.Writer, resp *OffsetFetchResponse, version int16) {
	flexible := version >= 6

	if version >= 3 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}

	if version >= 8 {
		w.WriteCompactArray(len(resp.Groups), func() {
			for _, g := range resp.Groups {
				w.WriteCompactString(g.GroupID)
				w.WriteCompactArray(len(g.Topics), func() {
					for _, t := range g.Topics {
						w.WriteCompactString(t.Name)
						w.WriteCompactArray(len(t.Partitions), func() {
							for _, p := range t.Partitions {
								encodeOffsetFetchPartition(w, p, version, true)
							}
						})
						w.WriteEmptyTaggedFields()
					}
				})
				w.WriteInt16(g.ErrorCode)
				w.WriteEmptyTaggedFields()
			}
		})
		w.WriteEmptyTaggedFields()
		return
	}

	writeTopics := func() {
		for _, t := range resp.Topics {
			writeString(w, t.Name, flexible)
			writePartitions := func() {
				for _, p := range t.Partitions {
					encodeOffsetFetchPartition(w, p, version, flexible)
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
	} else {
		w.WriteArray(len(resp.Topics), writeTopics)
	}

	if version >= 2 {
		w.WriteInt16(resp.ErrorCode)
	}
	if flexible {
		w.WriteEmptyTaggedFields()
	}
}

func encodeOffsetFetchPartition(w *codec.Writer, p OffsetFetchPartitionResponse, version int16, flexible bool) {
	w.WriteInt32(p.PartitionIndex)
	w.WriteInt64(p.CommittedOffset)
	if version >= 5 {
		w.WriteInt32(p.CommittedLeaderEpoch)
	}
	if flexible {
		w.WriteCompactNullableString(p.Metadata, p.Metadata == "")
	} else {
		w.WriteNullableString(p.Metadata, p.Metadata == "")
	}
	w.WriteInt16(p.ErrorCode)
	if flexible {
		w.WriteEmptyTaggedFields()
	}
}
