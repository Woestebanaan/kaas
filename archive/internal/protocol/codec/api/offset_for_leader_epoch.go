package api

import "github.com/woestebanaan/skafka/internal/protocol/codec"

// OffsetForLeaderEpochRequest (key 23, v0–v4). KIP-101: Java consumers
// call this after Fetch responses carrying a leader_epoch field, on
// assign(), and during seekToTimestamp to detect "this offset was
// written by a stale leader and never committed" and snap to a safe
// truncation boundary.
//
// Wire-format versions:
//   - v0–v1: non-flexible. v0 has no ReplicaID; v1 adds the field at
//     the front. (Skafka's parity target is v0+, treating v0 as a
//     consumer-only path with ReplicaID=-1.)
//   - v2: adds CurrentLeaderEpoch at the per-partition level so the
//     server can detect a fenced client and return FENCED_LEADER_EPOCH.
//   - v3: flexible at v3+ (KIP-482 tagged-fields).
//   - v4: identical schema, but at v4 the broker MUST honor the
//     CurrentLeaderEpoch check (pre-v4 it was optional).
type OffsetForLeaderEpochRequest struct {
	ReplicaID int32 // v1+; -1 for consumer
	Topics    []OffsetForLeaderEpochTopic
}

type OffsetForLeaderEpochTopic struct {
	Name       string
	Partitions []OffsetForLeaderEpochPartition
}

type OffsetForLeaderEpochPartition struct {
	PartitionIndex      int32
	CurrentLeaderEpoch  int32 // v2+; -1 when unknown
	LeaderEpoch         int32
}

type OffsetForLeaderEpochResponse struct {
	ThrottleTimeMs int32 // v2+
	Topics         []OffsetForLeaderEpochTopicResponse
}

type OffsetForLeaderEpochTopicResponse struct {
	Name       string
	Partitions []OffsetForLeaderEpochPartitionResponse
}

type OffsetForLeaderEpochPartitionResponse struct {
	ErrorCode      int16
	PartitionIndex int32
	LeaderEpoch    int32 // v1+
	EndOffset      int64
}

func DecodeOffsetForLeaderEpochRequest(r *codec.Reader, version int16) (*OffsetForLeaderEpochRequest, error) {
	req := &OffsetForLeaderEpochRequest{ReplicaID: -1}
	flexible := version >= 3

	if version >= 1 {
		rid, err := r.ReadInt32()
		if err != nil {
			return nil, err
		}
		req.ReplicaID = rid
	}

	readTopic := func() error {
		var t OffsetForLeaderEpochTopic
		name, err := readString(r, flexible)
		if err != nil {
			return err
		}
		t.Name = name
		readPart := func() error {
			var p OffsetForLeaderEpochPartition
			if version >= 2 {
				cle, perr := r.ReadInt32()
				if perr != nil {
					return perr
				}
				p.CurrentLeaderEpoch = cle
			} else {
				p.CurrentLeaderEpoch = -1
			}
			pi, perr := r.ReadInt32()
			if perr != nil {
				return perr
			}
			p.PartitionIndex = pi
			le, perr := r.ReadInt32()
			if perr != nil {
				return perr
			}
			p.LeaderEpoch = le
			if flexible {
				if perr := r.ReadTaggedFields(); perr != nil {
					return perr
				}
			}
			t.Partitions = append(t.Partitions, p)
			return nil
		}
		if flexible {
			err = r.ReadCompactArray(readPart)
		} else {
			err = r.ReadArray(readPart)
		}
		if err != nil {
			return err
		}
		if flexible {
			if terr := r.ReadTaggedFields(); terr != nil {
				return terr
			}
		}
		req.Topics = append(req.Topics, t)
		return nil
	}
	var topErr error
	if flexible {
		topErr = r.ReadCompactArray(readTopic)
	} else {
		topErr = r.ReadArray(readTopic)
	}
	if topErr != nil {
		return nil, topErr
	}
	if flexible {
		if terr := r.ReadTaggedFields(); terr != nil {
			return nil, terr
		}
	}
	return req, nil
}

func EncodeOffsetForLeaderEpochResponse(w *codec.Writer, resp *OffsetForLeaderEpochResponse, version int16) {
	flexible := version >= 3
	if version >= 2 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}
	writeTopic := func() {
		for _, t := range resp.Topics {
			writeString(w, t.Name, flexible)
			writePart := func() {
				for _, p := range t.Partitions {
					w.WriteInt16(p.ErrorCode)
					w.WriteInt32(p.PartitionIndex)
					if version >= 1 {
						w.WriteInt32(p.LeaderEpoch)
					}
					w.WriteInt64(p.EndOffset)
					if flexible {
						w.WriteEmptyTaggedFields()
					}
				}
			}
			if flexible {
				w.WriteCompactArray(len(t.Partitions), writePart)
				w.WriteEmptyTaggedFields()
			} else {
				w.WriteArray(len(t.Partitions), writePart)
			}
		}
	}
	if flexible {
		w.WriteCompactArray(len(resp.Topics), writeTopic)
		w.WriteEmptyTaggedFields()
	} else {
		w.WriteArray(len(resp.Topics), writeTopic)
	}
}
