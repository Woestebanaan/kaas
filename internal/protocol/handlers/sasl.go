package handlers

import (
	"context"
	"fmt"

	"github.com/woestebanaan/skafka/internal/auth"
	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

// SaslHandshakeHandler advertises supported SASL mechanisms.
type SaslHandshakeHandler struct {
	mechanisms []string
}

func NewSaslHandshakeHandler() *SaslHandshakeHandler {
	return &SaslHandshakeHandler{mechanisms: []string{"SCRAM-SHA-512", "PLAIN"}}
}

func (h *SaslHandshakeHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeSaslHandshakeRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("sasl_handshake decode: %w", err)
	}

	errCode := int16(codec.ErrUnsupportedSaslMechanism)
	for _, m := range h.mechanisms {
		if m == req.Mechanism {
			errCode = 0
			break
		}
	}

	resp := &api.SaslHandshakeResponse{ErrorCode: errCode, Mechanisms: h.mechanisms}
	w := codec.NewWriter()
	api.EncodeSaslHandshakeResponse(w, resp, version)
	return w.Bytes(), nil
}

// SaslAuthenticateHandler delegates SASL exchange to the AuthEngine.
type SaslAuthenticateHandler struct {
	auth auth.AuthEngine
}

func NewSaslAuthenticateHandler(authEng auth.AuthEngine) *SaslAuthenticateHandler {
	return &SaslAuthenticateHandler{auth: authEng}
}

func (h *SaslAuthenticateHandler) Handle(conn *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeSaslAuthenticateRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("sasl_authenticate decode: %w", err)
	}

	principal, err := h.auth.Authenticate(context.Background(), auth.Credentials{Payload: req.AuthBytes})
	if err != nil {
		resp := &api.SaslAuthenticateResponse{ErrorCode: int16(codec.ErrNetworkException), ErrorMessage: err.Error()}
		w := codec.NewWriter()
		api.EncodeSaslAuthenticateResponse(w, resp, version)
		return w.Bytes(), nil
	}

	conn.Principal = &principal
	conn.SASLDone = true

	resp := &api.SaslAuthenticateResponse{ErrorCode: 0}
	w := codec.NewWriter()
	api.EncodeSaslAuthenticateResponse(w, resp, version)
	return w.Bytes(), nil
}
