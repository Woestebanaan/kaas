package api

import "github.com/woestebanaan/skafka/internal/protocol/codec"

// LeaveGroupRequest (key 13, v0–v4).
type LeaveGroupRequest struct {
	GroupID  string
	MemberID string          // v0–v2
	Members  []LeaveMember   // v3+
}

type LeaveMember struct {
	MemberID        string
	GroupInstanceID string // nullable
}

// LeaveGroupResponse (key 13, v0–v4).
type LeaveGroupResponse struct {
	ThrottleTimeMs int32        // v1+
	ErrorCode      int16
	Members        []LeaveMemberResponse // v3+
}

type LeaveMemberResponse struct {
	MemberID        string
	GroupInstanceID string // nullable
	ErrorCode       int16
}

func DecodeLeaveGroupRequest(r *codec.Reader, version int16) (*LeaveGroupRequest, error) {
	req := &LeaveGroupRequest{}
	flexible := version >= 4
	var err error

	if req.GroupID, err = readString(r, flexible); err != nil {
		return nil, err
	}
	if version <= 2 {
		if req.MemberID, err = readString(r, flexible); err != nil {
			return nil, err
		}
		return req, nil
	}

	readMember := func() error {
		var m LeaveMember
		var err error
		if m.MemberID, err = readString(r, flexible); err != nil {
			return err
		}
		s, _, err := nullableString(r, flexible)
		if err != nil {
			return err
		}
		m.GroupInstanceID = s
		if flexible {
			if err := r.ReadTaggedFields(); err != nil {
				return err
			}
		}
		req.Members = append(req.Members, m)
		return nil
	}
	if flexible {
		if err := r.ReadCompactArray(readMember); err != nil {
			return nil, err
		}
		return req, r.ReadTaggedFields()
	}
	return req, r.ReadArray(readMember)
}

func EncodeLeaveGroupResponse(w *codec.Writer, resp *LeaveGroupResponse, version int16) {
	flexible := version >= 4
	if version >= 1 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}
	w.WriteInt16(resp.ErrorCode)
	if version >= 3 {
		writeMembers := func() {
			for _, m := range resp.Members {
				writeString(w, m.MemberID, flexible)
				if flexible {
					w.WriteCompactNullableString(m.GroupInstanceID, m.GroupInstanceID == "")
				} else {
					w.WriteNullableString(m.GroupInstanceID, m.GroupInstanceID == "")
				}
				w.WriteInt16(m.ErrorCode)
				if flexible {
					w.WriteEmptyTaggedFields()
				}
			}
		}
		if flexible {
			w.WriteCompactArray(len(resp.Members), writeMembers)
		} else {
			w.WriteArray(len(resp.Members), writeMembers)
		}
	}
	if flexible {
		w.WriteEmptyTaggedFields()
	}
}
