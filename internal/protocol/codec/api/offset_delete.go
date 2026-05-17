package api

import "github.com/woestebanaan/skafka/internal/protocol/codec"

// OffsetDeleteRequest (key 47, v0). Drives AdminClient
// .deleteConsumerGroupOffsets() and kafka-consumer-groups.sh
// --delete-offsets — drops specific (topic, partition) committed
// offsets without dropping the whole group (DeleteGroups, key 42,
// handles the whole-group case).
//
// Apache Kafka 3.7 ships only v0 (non-flexible). The decoder is
// structured so a later flexible v1 can be added with one extra
// branch.
type OffsetDeleteRequest struct {
	GroupID string
	Topics  []OffsetDeleteTopic
}

type OffsetDeleteTopic struct {
	Name       string
	Partitions []int32
}

// OffsetDeleteResponse (key 47, v0). ErrorCode is the group-level
// error: NOT_COORDINATOR (16), GROUP_ID_NOT_FOUND (69), NON_EMPTY_GROUP
// (67), GROUP_AUTHORIZATION_FAILED (30). Per-partition errors —
// UNKNOWN_TOPIC_OR_PARTITION (3), TOPIC_AUTHORIZATION_FAILED (29) —
// live on each PartitionResponse and are emitted only when the
// group-level ErrorCode is 0.
type OffsetDeleteResponse struct {
	ErrorCode      int16
	ThrottleTimeMs int32
	Topics         []OffsetDeleteTopicResponse
}

type OffsetDeleteTopicResponse struct {
	Name       string
	Partitions []OffsetDeletePartitionResponse
}

type OffsetDeletePartitionResponse struct {
	PartitionIndex int32
	ErrorCode      int16
}

func DecodeOffsetDeleteRequest(r *codec.Reader, version int16) (*OffsetDeleteRequest, error) {
	req := &OffsetDeleteRequest{}
	var err error

	if req.GroupID, err = readString(r, false); err != nil {
		return nil, err
	}
	readTopic := func() error {
		var t OffsetDeleteTopic
		if t.Name, err = readString(r, false); err != nil {
			return err
		}
		readPart := func() error {
			p, err := r.ReadInt32()
			if err != nil {
				return err
			}
			t.Partitions = append(t.Partitions, p)
			return nil
		}
		if err := r.ReadArray(readPart); err != nil {
			return err
		}
		req.Topics = append(req.Topics, t)
		return nil
	}
	return req, r.ReadArray(readTopic)
}

func EncodeOffsetDeleteResponse(w *codec.Writer, resp *OffsetDeleteResponse, version int16) {
	// Group-level ErrorCode precedes ThrottleTime in OffsetDelete v0 —
	// note this differs from the more common (throttle, error_code,
	// topics) order used by e.g. DeleteGroups.
	w.WriteInt16(resp.ErrorCode)
	w.WriteInt32(resp.ThrottleTimeMs)
	writeTopic := func() {
		for _, t := range resp.Topics {
			writeString(w, t.Name, false)
			writePart := func() {
				for _, p := range t.Partitions {
					w.WriteInt32(p.PartitionIndex)
					w.WriteInt16(p.ErrorCode)
				}
			}
			w.WriteArray(len(t.Partitions), writePart)
		}
	}
	w.WriteArray(len(resp.Topics), writeTopic)
}
