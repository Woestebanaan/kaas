package api

import "github.com/woestebanaan/skafka/internal/protocol/codec"

// ListGroupsRequest (key 16, v0–v4).
type ListGroupsRequest struct {
	StatesFilter []string // v4+
}

type ListedGroup struct {
	GroupID      string
	ProtocolType string
	GroupState   string // v4+
}

// ListGroupsResponse (key 16, v0–v4).
type ListGroupsResponse struct {
	ThrottleTimeMs int32 // v1+
	ErrorCode      int16
	Groups         []ListedGroup
}

func DecodeListGroupsRequest(r *codec.Reader, version int16) (*ListGroupsRequest, error) {
	req := &ListGroupsRequest{}
	flexible := version >= 3

	if version >= 4 {
		if err := r.ReadCompactArray(func() error {
			s, err := r.ReadCompactString()
			if err != nil {
				return err
			}
			req.StatesFilter = append(req.StatesFilter, s)
			return nil
		}); err != nil {
			return nil, err
		}
	}
	if flexible {
		return req, r.ReadTaggedFields()
	}
	return req, nil
}

func EncodeListGroupsResponse(w *codec.Writer, resp *ListGroupsResponse, version int16) {
	flexible := version >= 3
	if version >= 1 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}
	w.WriteInt16(resp.ErrorCode)
	writeGroups := func() {
		for _, g := range resp.Groups {
			writeString(w, g.GroupID, flexible)
			writeString(w, g.ProtocolType, flexible)
			if version >= 4 {
				writeString(w, g.GroupState, flexible)
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
