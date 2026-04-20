package handlers

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/woestebanaan/skafka/internal/auth"
	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/observability"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

// SaslHandshakeHandler advertises supported SASL mechanisms and records the
// chosen mechanism on the connection state.
type SaslHandshakeHandler struct {
	mechanisms []string
}

func NewSaslHandshakeHandler() *SaslHandshakeHandler {
	return &SaslHandshakeHandler{mechanisms: []string{"SCRAM-SHA-512", "PLAIN"}}
}

func (h *SaslHandshakeHandler) Handle(conn *connstate.ConnState, version int16, body []byte) ([]byte, error) {
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
	if errCode == 0 && conn != nil {
		conn.SASLMechanism = req.Mechanism
	}

	resp := &api.SaslHandshakeResponse{ErrorCode: errCode, Mechanisms: h.mechanisms}
	w := codec.NewWriter()
	api.EncodeSaslHandshakeResponse(w, resp, version)
	return w.Bytes(), nil
}

// SaslAuthenticateHandler drives a multi-step SASL exchange.
// On the first call it creates the exchange; subsequent calls continue it until done.
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

	// Reject PLAIN over non-TLS connections.
	if conn != nil && conn.SASLMechanism == "PLAIN" && !conn.IsTLS {
		resp := &api.SaslAuthenticateResponse{
			ErrorCode:    int16(codec.ErrNetworkException),
			ErrorMessage: "PLAIN mechanism requires TLS",
		}
		w := codec.NewWriter()
		api.EncodeSaslAuthenticateResponse(w, resp, version)
		return w.Bytes(), nil
	}

	// Create the exchange on the first call.
	if conn != nil && conn.SASLState == nil {
		mechanism := "SCRAM-SHA-512" // default if no handshake was done
		if conn.SASLMechanism != "" {
			mechanism = conn.SASLMechanism
		}
		exch, err := h.auth.NewSASLExchange(mechanism)
		if err != nil {
			resp := &api.SaslAuthenticateResponse{
				ErrorCode:    int16(codec.ErrUnsupportedSaslMechanism),
				ErrorMessage: err.Error(),
			}
			w := codec.NewWriter()
			api.EncodeSaslAuthenticateResponse(w, resp, version)
			return w.Bytes(), nil
		}
		conn.SASLState = exch
	}

	var serverMsg []byte
	var done bool

	if conn != nil && conn.SASLState != nil {
		serverMsg, done, err = conn.SASLState.Step(req.AuthBytes)
		if err != nil {
			observability.Global().AuthFailure.Add(context.Background(), 1,
				metric.WithAttributes(
					attribute.String("mechanism", conn.SASLMechanism),
					attribute.String("reason", "step_error"),
				))
			resp := &api.SaslAuthenticateResponse{
				ErrorCode:    int16(codec.ErrNetworkException),
				ErrorMessage: err.Error(),
			}
			w := codec.NewWriter()
			api.EncodeSaslAuthenticateResponse(w, resp, version)
			return w.Bytes(), nil
		}
		if done {
			p := conn.SASLState.Principal()
			conn.Principal = &p
			conn.SASLDone = true
			observability.Global().AuthSuccess.Add(context.Background(), 1,
				metric.WithAttributes(attribute.String("mechanism", conn.SASLMechanism)))
		}
	}

	resp := &api.SaslAuthenticateResponse{AuthBytes: serverMsg}
	w := codec.NewWriter()
	api.EncodeSaslAuthenticateResponse(w, resp, version)
	return w.Bytes(), nil
}
