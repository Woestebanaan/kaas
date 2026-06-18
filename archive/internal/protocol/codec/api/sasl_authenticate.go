package api

import "github.com/woestebanaan/skafka/internal/protocol/codec"

// SaslAuthenticateRequest (key 36, v0–v2).
type SaslAuthenticateRequest struct {
	AuthBytes []byte
}

// SaslAuthenticateResponse (key 36, v0–v2).
type SaslAuthenticateResponse struct {
	ErrorCode    int16
	ErrorMessage string // nullable
	AuthBytes    []byte
	SessionTTLMs int64 // v1+
}

func DecodeSaslAuthenticateRequest(r *codec.Reader, version int16) (*SaslAuthenticateRequest, error) {
	flexible := version >= 2
	var b []byte
	var err error
	if flexible {
		b, err = r.ReadCompactNullableBytes()
	} else {
		b, err = r.ReadNullableBytes()
	}
	if err != nil {
		return nil, err
	}
	if flexible {
		if err := r.ReadTaggedFields(); err != nil {
			return nil, err
		}
	}
	return &SaslAuthenticateRequest{AuthBytes: b}, nil
}

func EncodeSaslAuthenticateResponse(w *codec.Writer, resp *SaslAuthenticateResponse, version int16) {
	flexible := version >= 2
	w.WriteInt16(resp.ErrorCode)
	if flexible {
		w.WriteCompactNullableString(resp.ErrorMessage, resp.ErrorMessage == "")
		// AuthBytes is non-nullable in Apache Kafka's schema
		// (gh #96 fix mirror). Even on auth completion when the
		// server has no further challenge to issue, the wire
		// payload is empty bytes, not null.
		w.WriteCompactBytes(resp.AuthBytes)
	} else {
		w.WriteNullableString(resp.ErrorMessage, resp.ErrorMessage == "")
		w.WriteBytes(resp.AuthBytes)
	}
	if version >= 1 {
		w.WriteInt64(resp.SessionTTLMs)
	}
	if flexible {
		w.WriteEmptyTaggedFields()
	}
}
