package observability

import (
	"encoding/json"
	"net/http"
	"os"
	"sync/atomic"
)

// RuntimeState is the v3 broker runtime view that /healthz reports.
// Implementations MUST be safe to call from any goroutine.
//
// A nil RuntimeState is acceptable — the handler returns zero values
// for the runtime fields, which is the right answer in local-dev mode
// where no controller / coordinator / heartbeat client is running.
//
// Methods that have no measurement yet should return -1 (for the
// *Ms / latency fields) so the handler can render `null` in JSON
// instead of a misleading zero. Counters return 0.
type RuntimeState interface {
	IsController() bool
	ControllerID() string
	ControllerEpoch() int64
	HeartbeatRTTMs() int64
	HeartbeatAgeMs() int64
	AssignmentVersion() uint64
	AssignmentAgeMs() int64
	PartitionsLed() int
	PartitionsAssigned() int
	PartitionsRecovering() int
	// StorageStalled reports whether at least one partition's most
	// recent committer fsync timed out per Config.FsyncMaxLatency
	// (gh #95). Lets healthz surface a "storage backend wedged" signal
	// before the broker accumulates enough queued appenders to look
	// outwardly idle. Implementations that don't track storage health
	// should return false.
	StorageStalled() bool
}

// HealthState matches the schema in skafka-plan-v3.md (Phase 10). The
// json tags use the plan's field names verbatim so dashboards and
// scripts written against the plan work unchanged.
type HealthState struct {
	Status    string   `json:"status"`
	BrokerID  string   `json:"broker_id,omitempty"`
	Listeners []string `json:"listeners"`
	TLS       *TLSInfo `json:"tls,omitempty"`

	// v3 runtime fields. Zero-valued (or null via *int64 pointers) when
	// no RuntimeState source is wired (local-dev) or when no measurement
	// has happened yet.
	IsController         bool   `json:"is_controller"`
	ControllerID         string `json:"controller_id,omitempty"`
	ControllerEpoch      int64  `json:"controller_epoch,omitempty"`
	HeartbeatRTTMs       *int64 `json:"heartbeat_rtt_ms,omitempty"`
	HeartbeatAgeMs       *int64 `json:"heartbeat_age_ms,omitempty"`
	AssignmentVersion    uint64 `json:"assignment_version,omitempty"`
	AssignmentAgeMs      *int64 `json:"assignment_age_ms,omitempty"`
	PartitionsLed        int    `json:"partitions_led"`
	PartitionsAssigned   int    `json:"partitions_assigned"`
	PartitionsRecovering int    `json:"partitions_recovering"`
	StorageStalled       bool   `json:"storage_stalled,omitempty"`
}

// TLSInfo reports TLS listener readiness.
type TLSInfo struct {
	Enabled      bool   `json:"enabled"`
	ExternalHost string `json:"external_host,omitempty"`
}

var readySnapshot atomic.Bool

// SetReady flips the /readyz state. Called by the broker main once all
// listeners are up and any required env vars are present.
func SetReady(v bool) { readySnapshot.Store(v) }

// Ready reports the current /readyz state.
func Ready() bool { return readySnapshot.Load() }

// HealthHandler returns a JSON /healthz handler. brokerID is the v3
// identifier ("skafka-0"); pass empty in local-dev. listeners enumerates
// active listener names. source is the v3 runtime view; nil is fine.
func HealthHandler(brokerID string, listeners []string, tls *TLSInfo, source RuntimeState) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		s := HealthState{
			Status:    "ok",
			BrokerID:  brokerID,
			Listeners: listeners,
			TLS:       tls,
		}
		if source != nil {
			s.IsController = source.IsController()
			s.ControllerID = source.ControllerID()
			s.ControllerEpoch = source.ControllerEpoch()
			if v := source.HeartbeatRTTMs(); v >= 0 {
				s.HeartbeatRTTMs = &v
			}
			if v := source.HeartbeatAgeMs(); v >= 0 {
				s.HeartbeatAgeMs = &v
			}
			s.AssignmentVersion = source.AssignmentVersion()
			if v := source.AssignmentAgeMs(); v >= 0 {
				s.AssignmentAgeMs = &v
			}
			s.PartitionsLed = source.PartitionsLed()
			s.PartitionsAssigned = source.PartitionsAssigned()
			s.PartitionsRecovering = source.PartitionsRecovering()
			s.StorageStalled = source.StorageStalled()
		}
		w.Header().Set("content-type", "application/json")
		_ = json.NewEncoder(w).Encode(s)
	}
}

// ReadyHandler returns a JSON /readyz handler that respects the atomic
// readiness flag and also requires EXTERNAL_HOSTNAME_PATTERN to be set
// when the external listener is active — prevents incorrect Metadata
// responses from reaching external clients during LB-IP discovery.
//
// gh #131: keys off SKAFKA_TLS_PORT (which the chart only emits for the
// external listener) rather than SKAFKA_TLS_CERT_FILE — internal-only
// TLS sets the cert but never an external hostname, and would otherwise
// be stuck reporting unready forever.
func ReadyHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		ready := readySnapshot.Load()
		if os.Getenv("SKAFKA_TLS_PORT") != "" && os.Getenv("EXTERNAL_HOSTNAME_PATTERN") == "" {
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
