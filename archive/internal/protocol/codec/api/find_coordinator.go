package api

import "github.com/woestebanaan/skafka/internal/protocol/codec"

// FindCoordinatorRequest (key 10, v0–v4). Apache schema field
// version ranges (clients/src/main/resources/common/message/
// FindCoordinatorRequest.json):
//
//	Key:             "0-3"   (single coordinator key)
//	KeyType:         "1+"    (0=group, 1=transaction)
//	CoordinatorKeys: "4+"    (array — supersedes Key at v4)
//	flexibleVersions:"3+"
//
// gh #91 PR 3 fixed the previous "v3 uses array" bug — clients
// negotiating v3 send a single Key + flexible header, NOT an array.
// The single-key branch is what franz-go and Java AdminClient
// actually use when the broker advertises v3 max; v4 array is what
// they switch to once we advertise v4.
type FindCoordinatorRequest struct {
	Key             string   // v0–v3
	KeyType         int8     // v1+: 0=group, 1=transaction
	CoordinatorKeys []string // v4+
}

// CoordinatorResult is a single coordinator lookup result (v4+).
type CoordinatorResult struct {
	Key          string
	NodeID       int32
	Host         string
	Port         int32
	ErrorCode    int16
	ErrorMessage string
}

// FindCoordinatorResponse (key 10, v0–v4). Field version ranges
// (FindCoordinatorResponse.json):
//
//	ErrorCode/ErrorMessage/NodeID/Host/Port: "0-3"
//	Coordinators (array):                    "4+"
//	flexibleVersions:                        "3+"
//
// At v3 the wire shape is the legacy single-coordinator form
// wrapped in flexible tagged fields — NOT the Coordinators array.
type FindCoordinatorResponse struct {
	ThrottleTimeMs int32               // v1+
	ErrorCode      int16               // v0–v3
	ErrorMessage   string              // v1–v3, nullable
	NodeID         int32               // v0–v3
	Host           string               // v0–v3
	Port           int32               // v0–v3
	Coordinators   []CoordinatorResult // v4+
}

func DecodeFindCoordinatorRequest(r *codec.Reader, version int16) (*FindCoordinatorRequest, error) {
	req := &FindCoordinatorRequest{}
	flexible := version >= 3

	if version <= 3 {
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
	if version >= 4 {
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
	}
	if flexible {
		if err := r.ReadTaggedFields(); err != nil {
			return nil, err
		}
	}
	return req, nil
}

func EncodeFindCoordinatorResponse(w *codec.Writer, resp *FindCoordinatorResponse, version int16) {
	flexible := version >= 3

	if version >= 1 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}

	if version >= 4 {
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

	// v0–v3: single-coordinator form. v3 wraps it in flexible
	// tagged fields; v0–v2 stay legacy.
	w.WriteInt16(resp.ErrorCode)
	if version >= 1 {
		if flexible {
			w.WriteCompactNullableString(resp.ErrorMessage, resp.ErrorMessage == "")
		} else {
			w.WriteNullableString(resp.ErrorMessage, resp.ErrorMessage == "")
		}
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
