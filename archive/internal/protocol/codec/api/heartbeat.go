package api

import "github.com/woestebanaan/skafka/internal/protocol/codec"

// HeartbeatRequest (key 12, v0–v4).
type HeartbeatRequest struct {
	GroupID         string
	GenerationID    int32
	MemberID        string
	GroupInstanceID string // v3+, nullable
}

// HeartbeatResponse (key 12, v0–v4).
type HeartbeatResponse struct {
	ThrottleTimeMs int32 // v1+
	ErrorCode      int16
}

func DecodeHeartbeatRequest(r *codec.Reader, version int16) (*HeartbeatRequest, error) {
	req := &HeartbeatRequest{}
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
	if flexible {
		return req, r.ReadTaggedFields()
	}
	return req, nil
}

func EncodeHeartbeatResponse(w *codec.Writer, resp *HeartbeatResponse, version int16) {
	flexible := version >= 4
	if version >= 1 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}
	w.WriteInt16(resp.ErrorCode)
	if flexible {
		w.WriteEmptyTaggedFields()
	}
}
