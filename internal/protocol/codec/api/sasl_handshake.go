package api

import "github.com/woestebanaan/skafka/internal/protocol/codec"

// SaslHandshakeRequest (key 17, v0–v1). Never flexible (max version is 1).
type SaslHandshakeRequest struct {
	Mechanism string
}

// SaslHandshakeResponse (key 17, v0–v1).
type SaslHandshakeResponse struct {
	ErrorCode  int16
	Mechanisms []string // enabled mechanisms
}

func DecodeSaslHandshakeRequest(r *codec.Reader, version int16) (*SaslHandshakeRequest, error) {
	name, err := r.ReadString()
	if err != nil {
		return nil, err
	}
	return &SaslHandshakeRequest{Mechanism: name}, nil
}

func EncodeSaslHandshakeResponse(w *codec.Writer, resp *SaslHandshakeResponse, version int16) {
	w.WriteInt16(resp.ErrorCode)
	w.WriteArray(len(resp.Mechanisms), func() {
		for _, m := range resp.Mechanisms {
			w.WriteString(m)
		}
	})
}
