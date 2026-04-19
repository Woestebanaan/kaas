package api

import "github.com/woestebanaan/skafka/internal/protocol/codec"

// FindCoordinatorRequest (key 10, v0–v4).
type FindCoordinatorRequest struct {
	Key             string   // v0–v2 only
	KeyType         int8     // v1+: 0=group, 1=transaction
	CoordinatorKeys []string // v3+
}

// CoordinatorResult is a single coordinator lookup result (v3+).
type CoordinatorResult struct {
	Key          string
	NodeID       int32
	Host         string
	Port         int32
	ErrorCode    int16
	ErrorMessage string
}

// FindCoordinatorResponse (key 10, v0–v4).
type FindCoordinatorResponse struct {
	ThrottleTimeMs int32  // v1+
	ErrorCode      int16  // v0–v2
	ErrorMessage   string // v1–v2, nullable
	NodeID         int32  // v0–v2
	Host           string // v0–v2
	Port           int32  // v0–v2
	Coordinators   []CoordinatorResult // v3+
}

func DecodeFindCoordinatorRequest(r *codec.Reader, version int16) (*FindCoordinatorRequest, error) {
	req := &FindCoordinatorRequest{}
	flexible := version >= 3

	if version <= 2 {
		key, err := readString(r, flexible)
		if err != nil {
			return nil, err
		}
		req.Key = key
	}
	if version >= 1 {
		kt, err := r.ReadInt8()
		if err != nil {
			return nil, err
		}
		req.KeyType = kt
	}
	if version >= 3 {
		if err := r.ReadCompactArray(func() error {
			k, err := r.ReadCompactString()
			if err != nil {
				return err
			}
			req.CoordinatorKeys = append(req.CoordinatorKeys, k)
			return nil
		}); err != nil {
			return nil, err
		}
		return req, r.ReadTaggedFields()
	}
	return req, nil
}

func EncodeFindCoordinatorResponse(w *codec.Writer, resp *FindCoordinatorResponse, version int16) {
	flexible := version >= 3

	if version >= 1 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}

	if version >= 3 {
		w.WriteCompactArray(len(resp.Coordinators), func() {
			for _, c := range resp.Coordinators {
				w.WriteCompactString(c.Key)
				w.WriteInt32(c.NodeID)
				w.WriteCompactString(c.Host)
				w.WriteInt32(c.Port)
				w.WriteInt16(c.ErrorCode)
				w.WriteCompactNullableString(c.ErrorMessage, c.ErrorMessage == "")
				w.WriteEmptyTaggedFields()
			}
		})
		w.WriteEmptyTaggedFields()
		return
	}

	w.WriteInt16(resp.ErrorCode)
	if version >= 1 {
		w.WriteNullableString(resp.ErrorMessage, resp.ErrorMessage == "")
	}
	w.WriteInt32(resp.NodeID)
	if flexible {
		w.WriteCompactString(resp.Host)
	} else {
		w.WriteString(resp.Host)
	}
	w.WriteInt32(resp.Port)
	if flexible {
		w.WriteEmptyTaggedFields()
	}
}
