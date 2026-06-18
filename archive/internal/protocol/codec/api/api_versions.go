package api

import (
	"github.com/woestebanaan/skafka/internal/protocol/codec"
)

// APIVersionsRequest (key 18). v0-v2 have no fields; v3+ add client software info.
type APIVersionsRequest struct {
	ClientSoftwareName    string // v3+
	ClientSoftwareVersion string // v3+
}

// APIVersion describes one supported API key and its version range.
type APIVersion struct {
	APIKey     int16
	MinVersion int16
	MaxVersion int16
}

// APIVersionsResponse (key 18).
type APIVersionsResponse struct {
	ErrorCode    int16
	APIVersions  []APIVersion
	ThrottleTime int32 // v1+
}

func DecodeAPIVersionsRequest(r *codec.Reader, version int16) (*APIVersionsRequest, error) {
	req := &APIVersionsRequest{}
	if version >= 3 { // v4 uses the same format as v3
		var err error
		if req.ClientSoftwareName, err = r.ReadCompactString(); err != nil {
			return nil, err
		}
		if req.ClientSoftwareVersion, err = r.ReadCompactString(); err != nil {
			return nil, err
		}
		if err := r.ReadTaggedFields(); err != nil {
			return nil, err
		}
	}
	return req, nil
}

func EncodeAPIVersionsResponse(w *codec.Writer, resp *APIVersionsResponse, version int16) {
	w.WriteInt16(resp.ErrorCode)

	if version >= 3 { // v4 uses the same format as v3
		// Flexible: compact array, each entry has trailing tagged fields.
		w.WriteCompactArray(len(resp.APIVersions), func() {
			for _, v := range resp.APIVersions {
				w.WriteInt16(v.APIKey)
				w.WriteInt16(v.MinVersion)
				w.WriteInt16(v.MaxVersion)
				w.WriteEmptyTaggedFields()
			}
		})
	} else {
		w.WriteArray(len(resp.APIVersions), func() {
			for _, v := range resp.APIVersions {
				w.WriteInt16(v.APIKey)
				w.WriteInt16(v.MinVersion)
				w.WriteInt16(v.MaxVersion)
			}
		})
	}

	if version >= 1 {
		w.WriteInt32(resp.ThrottleTime)
	}

	if version >= 3 {
		w.WriteEmptyTaggedFields()
	}
}
