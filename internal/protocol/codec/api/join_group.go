package api

import "github.com/woestebanaan/skafka/internal/protocol/codec"

// JoinGroupRequest (key 11, v2–v9).
type JoinGroupRequest struct {
	GroupID            string
	SessionTimeoutMs   int32
	RebalanceTimeoutMs int32
	MemberID           string
	GroupInstanceID    string // v5+, nullable
	ProtocolType       string
	Protocols          []JoinGroupProtocol
	Reason             string // v8+, nullable
}

type JoinGroupProtocol struct {
	Name     string
	Metadata []byte
}

// JoinGroupResponse (key 11, v2–v9).
type JoinGroupResponse struct {
	ThrottleTimeMs int32  // v2+
	ErrorCode      int16
	GenerationID   int32
	ProtocolType   string // v7+, nullable
	ProtocolName   string
	Leader         string
	SkipAssignment bool   // v9+
	MemberID       string
	Members        []JoinGroupMember
}

type JoinGroupMember struct {
	MemberID        string
	GroupInstanceID string // v5+, nullable
	Metadata        []byte
}

func DecodeJoinGroupRequest(r *codec.Reader, version int16) (*JoinGroupRequest, error) {
	req := &JoinGroupRequest{}
	flexible := version >= 6
	var err error

	if req.GroupID, err = readString(r, flexible); err != nil {
		return nil, err
	}
	if req.SessionTimeoutMs, err = r.ReadInt32(); err != nil {
		return nil, err
	}
	if req.RebalanceTimeoutMs, err = r.ReadInt32(); err != nil {
		return nil, err
	}
	if req.MemberID, err = readString(r, flexible); err != nil {
		return nil, err
	}
	if version >= 5 {
		s, _, err := nullableString(r, flexible)
		if err != nil {
			return nil, err
		}
		req.GroupInstanceID = s
	}
	if req.ProtocolType, err = readString(r, flexible); err != nil {
		return nil, err
	}

	readProtocol := func() error {
		var p JoinGroupProtocol
		var err error
		if p.Name, err = readString(r, flexible); err != nil {
			return err
		}
		if flexible {
			p.Metadata, err = r.ReadCompactNullableBytes()
		} else {
			p.Metadata, err = r.ReadNullableBytes()
		}
		if err != nil {
			return err
		}
		if flexible {
			if err := r.ReadTaggedFields(); err != nil {
				return err
			}
		}
		req.Protocols = append(req.Protocols, p)
		return nil
	}
	if flexible {
		if err := r.ReadCompactArray(readProtocol); err != nil {
			return nil, err
		}
	} else {
		if err := r.ReadArray(readProtocol); err != nil {
			return nil, err
		}
	}

	if version >= 8 {
		s, _, err := nullableString(r, flexible)
		if err != nil {
			return nil, err
		}
		req.Reason = s
	}
	if flexible {
		return req, r.ReadTaggedFields()
	}
	return req, nil
}

func EncodeJoinGroupResponse(w *codec.Writer, resp *JoinGroupResponse, version int16) {
	flexible := version >= 6

	if version >= 2 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}
	w.WriteInt16(resp.ErrorCode)
	w.WriteInt32(resp.GenerationID)
	if version >= 7 {
		writeString(w, resp.ProtocolType, flexible)
	}
	writeString(w, resp.ProtocolName, flexible)
	writeString(w, resp.Leader, flexible)
	if version >= 9 {
		if resp.SkipAssignment {
			w.WriteInt8(1)
		} else {
			w.WriteInt8(0)
		}
	}
	writeString(w, resp.MemberID, flexible)

	writeMembers := func() {
		for _, m := range resp.Members {
			writeString(w, m.MemberID, flexible)
			if version >= 5 {
				w.WriteCompactNullableString(m.GroupInstanceID, m.GroupInstanceID == "")
			}
			// Members[].Metadata is non-nullable in Apache Kafka's
			// schema (gh #96 fix mirror) — same wire-encoding bug
			// class as SyncGroupResponse.Assignment. Non-leader
			// members get an empty-but-non-null bytes payload; the
			// leader gets each member's protocol metadata so it
			// can run assign(). Writing a null marker would crash
			// the Java consumer's generated decoder during the
			// rebalance.
			if flexible {
				w.WriteCompactBytes(m.Metadata)
			} else {
				w.WriteBytes(m.Metadata)
			}
			if flexible {
				w.WriteEmptyTaggedFields()
			}
		}
	}
	if flexible {
		w.WriteCompactArray(len(resp.Members), writeMembers)
		w.WriteEmptyTaggedFields()
	} else {
		w.WriteArray(len(resp.Members), writeMembers)
	}
}
