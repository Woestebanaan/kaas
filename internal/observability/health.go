package observability

import (
	"encoding/json"
	"net/http"
	"os"
	"sync/atomic"
)

// HealthState is a snapshot returned by /healthz. The handler tallies the
// current state each request — there is no caching and no ticking goroutine.
type HealthState struct {
	Status    string   `json:"status"`
	BrokerID  int32    `json:"broker_id,omitempty"`
	Listeners []string `json:"listeners"`
	TLS       *TLSInfo `json:"tls,omitempty"`
}

// TLSInfo reports TLS listener readiness. ExternalHost is the advertised external
// hostname; empty until the operator injects it for the external listener.
type TLSInfo struct {
	Enabled        bool   `json:"enabled"`
	ExternalHost   string `json:"external_host,omitempty"`
}

// readySnapshot is the state for the /readyz handler: true iff the broker is
// fully in-service (listener bound AND, in k8s mode, external host injected).
var readySnapshot atomic.Bool

// SetReady flips the /readyz state. Called by the broker main once all listeners
// are up and any required env vars are present.
func SetReady(v bool) { readySnapshot.Store(v) }

// Ready reports the current /readyz state.
func Ready() bool { return readySnapshot.Load() }

// HealthHandler returns a JSON /healthz handler. brokerID may be 0 in local-dev.
// listeners lists active listener names ("internal", "external").
func HealthHandler(brokerID int32, listeners []string, tls *TLSInfo) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		state := HealthState{
			Status:    "ok",
			BrokerID:  brokerID,
			Listeners: listeners,
			TLS:       tls,
		}
		w.Header().Set("content-type", "application/json")
		_ = json.NewEncoder(w).Encode(state)
	}
}

// ReadyHandler returns a JSON /readyz handler that respects the atomic readiness
// flag and also requires EXTERNAL_ADVERTISED_HOST to be set when the external
// listener is active.
func ReadyHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		ready := readySnapshot.Load()
		// If the external listener is meant to be live, refuse readiness until
		// the advertised host is injected — prevents incorrect Metadata responses
		// from reaching external clients during LB-IP discovery.
		if os.Getenv("SKAFKA_TLS_CERT_FILE") != "" && os.Getenv("EXTERNAL_HOSTNAME_PATTERN") == "" {
			ready = false
		}
		w.Header().Set("content-type", "application/json")
		if !ready {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]any{"ready": false})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ready": true})
	}
}
