package api

import "github.com/woestebanaan/skafka/internal/protocol/codec"

// DeleteTopicsRequest (key 20, v0–v6).
type DeleteTopicsRequest struct {
	TopicNames []string // v0–v5
	TimeoutMs  int32
}

// DeleteTopicsResponse (key 20, v0–v5). v6 added topic_id and made
// name nullable; not yet supported here, so the broker registers
// version range 0–5.
type DeleteTopicsResponse struct {
	ThrottleTimeMs int32 // v1+
	Responses      []DeletableTopicResult
}

type DeletableTopicResult struct {
	Name         string
	ErrorCode    int16
	ErrorMessage string // v5+ (nullable; empty == null on the wire)
}

func DecodeDeleteTopicsRequest(r *codec.Reader, version int16) (*DeleteTopicsRequest, error) {
	req := &DeleteTopicsRequest{}
	flexible := version >= 4

	readName := func() error {
		name, err := readString(r, flexible)
		if err != nil {
			return err
		}
		req.TopicNames = append(req.TopicNames, name)
		return nil
	}
	if flexible {
		if err := r.ReadCompactArray(readName); err != nil {
			return nil, err
		}
	} else {
		if err := r.ReadArray(readName); err != nil {
			return nil, err
		}
	}
	var err error
	if req.TimeoutMs, err = r.ReadInt32(); err != nil {
		return nil, err
	}
	if flexible {
		return req, r.ReadTaggedFields()
	}
	return req, nil
}

func EncodeDeleteTopicsResponse(w *codec.Writer, resp *DeleteTopicsResponse, version int16) {
	flexible := version >= 4
	if version >= 1 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}
	writeResponses := func() {
		for _, r := range resp.Responses {
			writeString(w, r.Name, flexible)
			w.WriteInt16(r.ErrorCode)
			// v5+ adds ErrorMessage as COMPACT_NULLABLE_STRING. Empty
			// string on our side serialises as null, which is what
			// the official spec says the absence of an error means.
			if version >= 5 {
				writeNullableString(w, r.ErrorMessage, flexible)
			}
			if flexible {
				w.WriteEmptyTaggedFields()
			}
		}
	}
	if flexible {
		w.WriteCompactArray(len(resp.Responses), writeResponses)
		w.WriteEmptyTaggedFields()
	} else {
		w.WriteArray(len(resp.Responses), writeResponses)
	}
}
