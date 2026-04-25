package api

import "github.com/woestebanaan/skafka/internal/protocol/codec"

// DescribeGroupsRequest (key 15, v0–v5).
type DescribeGroupsRequest struct {
	Groups                      []string
	IncludeAuthorizedOperations bool // v3+
}

type DescribedGroupMember struct {
	MemberID        string
	GroupInstanceID string // v4+, nullable
	ClientID        string
	ClientHost      string
	MemberMetadata  []byte
	MemberAssignment []byte
}

type DescribedGroup struct {
	ErrorCode              int16
	GroupID                string
	GroupState             string
	ProtocolType           string
	ProtocolData           string
	Members                []DescribedGroupMember
	AuthorizedOperations   int32 // v3+
}

// DescribeGroupsResponse (key 15, v0–v5).
type DescribeGroupsResponse struct {
	ThrottleTimeMs int32 // v1+
	Groups         []DescribedGroup
}

func DecodeDescribeGroupsRequest(r *codec.Reader, version int16) (*DescribeGroupsRequest, error) {
	req := &DescribeGroupsRequest{}
	flexible := version >= 5

	readGroup := func() error {
		s, err := readString(r, flexible)
		if err != nil {
			return err
		}
		req.Groups = append(req.Groups, s)
		return nil
	}
	if flexible {
		if err := r.ReadCompactArray(readGroup); err != nil {
			return nil, err
		}
	} else {
		if err := r.ReadArray(readGroup); err != nil {
			return nil, err
		}
	}
	if version >= 3 {
		v, err := r.ReadInt8()
		if err != nil {
			return nil, err
		}
		req.IncludeAuthorizedOperations = v != 0
	}
	if flexible {
		return req, r.ReadTaggedFields()
	}
	return req, nil
}

func EncodeDescribeGroupsResponse(w *codec.Writer, resp *DescribeGroupsResponse, version int16) {
	flexible := version >= 5
	if version >= 1 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}
	writeGroups := func() {
		for _, g := range resp.Groups {
			w.WriteInt16(g.ErrorCode)
			writeString(w, g.GroupID, flexible)
			writeString(w, g.GroupState, flexible)
			writeString(w, g.ProtocolType, flexible)
			writeString(w, g.ProtocolData, flexible)
			writeMembers := func() {
				for _, m := range g.Members {
					writeString(w, m.MemberID, flexible)
					if version >= 4 {
						if flexible {
							w.WriteCompactNullableString(m.GroupInstanceID, m.GroupInstanceID == "")
						} else {
							w.WriteNullableString(m.GroupInstanceID, m.GroupInstanceID == "")
						}
					}
					writeString(w, m.ClientID, flexible)
					writeString(w, m.ClientHost, flexible)
					// MemberMetadata and MemberAssignment are non-nullable BYTES per
					// the spec; the Java client throws "non-nullable field foo was
					// serialized as null" on -1/0 length sentinels and dies the
					// AdminClient thread (cascades into "Connection could not be
					// established" loops). Use the non-nullable writers — they encode
					// nil as zero-length bytes, which is the correct shape.
					if flexible {
						w.WriteCompactBytes(m.MemberMetadata)
						w.WriteCompactBytes(m.MemberAssignment)
						w.WriteEmptyTaggedFields()
					} else {
						w.WriteBytes(m.MemberMetadata)
						w.WriteBytes(m.MemberAssignment)
					}
				}
			}
			if flexible {
				w.WriteCompactArray(len(g.Members), writeMembers)
			} else {
				w.WriteArray(len(g.Members), writeMembers)
			}
			if version >= 3 {
				w.WriteInt32(g.AuthorizedOperations)
			}
			if flexible {
				w.WriteEmptyTaggedFields()
			}
		}
	}
	if flexible {
		w.WriteCompactArray(len(resp.Groups), writeGroups)
		w.WriteEmptyTaggedFields()
	} else {
		w.WriteArray(len(resp.Groups), writeGroups)
	}
}
