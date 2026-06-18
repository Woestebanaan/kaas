package api

import "github.com/woestebanaan/skafka/internal/protocol/codec"

// DeleteGroupsRequest (key 42, v0–v2). Used by AdminClient
// .deleteConsumerGroups() and kafka-consumer-groups.sh --delete to
// drop a consumer group's coordinator-side state plus its committed
// offsets. v2 introduces flexible (KIP-482 tagged-fields) framing.
type DeleteGroupsRequest struct {
	GroupNames []string
}

// DeleteGroupsResponse (key 42, v0–v2).
type DeleteGroupsResponse struct {
	ThrottleTimeMs int32
	Results        []DeleteGroupsResult
}

type DeleteGroupsResult struct {
	GroupID   string
	ErrorCode int16
}

func DecodeDeleteGroupsRequest(r *codec.Reader, version int16) (*DeleteGroupsRequest, error) {
	req := &DeleteGroupsRequest{}
	flexible := version >= 2

	readGroup := func() error {
		name, err := readString(r, flexible)
		if err != nil {
			return err
		}
		req.GroupNames = append(req.GroupNames, name)
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
	if flexible {
		if err := r.ReadTaggedFields(); err != nil {
			return nil, err
		}
	}
	return req, nil
}

func EncodeDeleteGroupsResponse(w *codec.Writer, resp *DeleteGroupsResponse, version int16) {
	flexible := version >= 2
	w.WriteInt32(resp.ThrottleTimeMs)

	writeResult := func() {
		for _, r := range resp.Results {
			writeString(w, r.GroupID, flexible)
			w.WriteInt16(r.ErrorCode)
			if flexible {
				w.WriteEmptyTaggedFields()
			}
		}
	}
	if flexible {
		w.WriteCompactArray(len(resp.Results), writeResult)
	} else {
		w.WriteArray(len(resp.Results), writeResult)
	}
	if flexible {
		w.WriteEmptyTaggedFields()
	}
}
