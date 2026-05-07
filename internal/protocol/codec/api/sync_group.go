package api

import "github.com/woestebanaan/skafka/internal/protocol/codec"

// SyncGroupRequest (key 14, v0–v5).
type SyncGroupRequest struct {
	GroupID         string
	GenerationID    int32
	MemberID        string
	GroupInstanceID string           // v3+, nullable
	ProtocolType    string           // v5+, nullable
	ProtocolName    string           // v5+, nullable
	Assignments     []SyncAssignment
}

type SyncAssignment struct {
	MemberID   string
	Assignment []byte
}

// SyncGroupResponse (key 14, v0–v5).
type SyncGroupResponse struct {
	ThrottleTimeMs int32  // v1+
	ErrorCode      int16
	ProtocolType   string // v5+, nullable
	ProtocolName   string // v5+, nullable
	Assignment     []byte
}

func DecodeSyncGroupRequest(r *codec.Reader, version int16) (*SyncGroupRequest, error) {
	req := &SyncGroupRequest{}
	flexible := version >= 4
	var err error

	if req.GroupID, err = readString(r, flexible); err != nil {
		return nil, err
	}
	if req.GenerationID, err = r.ReadInt32(); err != nil {
		return nil, err
	}
	if req.MemberID, err = readString(r, flexible); err != nil {
		return nil, err
	}
	if version >= 3 {
		s, _, err := nullableString(r, flexible)
		if err != nil {
			return nil, err
		}
		req.GroupInstanceID = s
	}
	if version >= 5 {
		s, _, err := nullableString(r, flexible)
		if err != nil {
			return nil, err
		}
		req.ProtocolType = s
		s, _, err = nullableString(r, flexible)
		if err != nil {
			return nil, err
		}
		req.ProtocolName = s
	}

	readAssignment := func() error {
		var a SyncAssignment
		var err error
		if a.MemberID, err = readString(r, flexible); err != nil {
			return err
		}
		if flexible {
			a.Assignment, err = r.ReadCompactNullableBytes()
		} else {
			a.Assignment, err = r.ReadNullableBytes()
		}
		if err != nil {
			return err
		}
		if flexible {
			if err := r.ReadTaggedFields(); err != nil {
				return err
			}
		}
		req.Assignments = append(req.Assignments, a)
		return nil
	}
	if flexible {
		if err := r.ReadCompactArray(readAssignment); err != nil {
			return nil, err
		}
		return req, r.ReadTaggedFields()
	}
	return req, r.ReadArray(readAssignment)
}

func EncodeSyncGroupResponse(w *codec.Writer, resp *SyncGroupResponse, version int16) {
	flexible := version >= 4
	if version >= 1 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}
	w.WriteInt16(resp.ErrorCode)
	if version >= 5 {
		if flexible {
			w.WriteCompactNullableString(resp.ProtocolType, resp.ProtocolType == "")
			w.WriteCompactNullableString(resp.ProtocolName, resp.ProtocolName == "")
		} else {
			w.WriteNullableString(resp.ProtocolType, resp.ProtocolType == "")
			w.WriteNullableString(resp.ProtocolName, resp.ProtocolName == "")
		}
	}
	// Assignment is NON-nullable in Apache Kafka's schema for every
	// version of SyncGroupResponse — error responses ship an empty
	// ByteBuffer, not null. Skafka used to emit it via the nullable
	// writers, which writes int32(-1) / varint(0) when Assignment ==
	// nil; the Java client's generated decoder throws
	// `RuntimeException: non-nullable field assignment was serialized
	// as null` during the next rebalance, killing the consumer.
	// Mirror Apache by always writing length-prefixed empty bytes.
	if flexible {
		w.WriteCompactBytes(resp.Assignment)
		w.WriteEmptyTaggedFields()
	} else {
		w.WriteBytes(resp.Assignment)
	}
}
