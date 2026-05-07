package observability

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
)

// stubRuntime is a fixed RuntimeState for the handler test.
type stubRuntime struct {
	isController         bool
	controllerID         string
	controllerEpoch      int64
	heartbeatRTTMs       int64
	heartbeatAgeMs       int64
	assignmentVersion    uint64
	assignmentAgeMs      int64
	partitionsLed        int
	partitionsAssigned   int
	partitionsRecovering int
	storageStalled       bool
}

func (s *stubRuntime) IsController() bool         { return s.isController }
func (s *stubRuntime) ControllerID() string       { return s.controllerID }
func (s *stubRuntime) ControllerEpoch() int64     { return s.controllerEpoch }
func (s *stubRuntime) HeartbeatRTTMs() int64      { return s.heartbeatRTTMs }
func (s *stubRuntime) HeartbeatAgeMs() int64      { return s.heartbeatAgeMs }
func (s *stubRuntime) AssignmentVersion() uint64  { return s.assignmentVersion }
func (s *stubRuntime) AssignmentAgeMs() int64     { return s.assignmentAgeMs }
func (s *stubRuntime) PartitionsLed() int         { return s.partitionsLed }
func (s *stubRuntime) PartitionsAssigned() int    { return s.partitionsAssigned }
func (s *stubRuntime) PartitionsRecovering() int  { return s.partitionsRecovering }
func (s *stubRuntime) StorageStalled() bool       { return s.storageStalled }

// hitHandler runs the handler once and returns the decoded body.
func hitHandler(t *testing.T, brokerID string, listeners []string, tls *TLSInfo, src RuntimeState) map[string]any {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/healthz", nil)
	HealthHandler(brokerID, listeners, tls, src)(rec, req)
	if got := rec.Header().Get("content-type"); got != "application/json" {
		t.Errorf("content-type=%q, want application/json", got)
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode body %q: %v", rec.Body.String(), err)
	}
	return got
}

func TestHealthHandlerNoRuntime(t *testing.T) {
	got := hitHandler(t, "skafka-0", []string{"internal"}, nil, nil)

	if got["status"] != "ok" {
		t.Errorf("status=%v, want ok", got["status"])
	}
	if got["broker_id"] != "skafka-0" {
		t.Errorf("broker_id=%v, want skafka-0", got["broker_id"])
	}
	// Without a RuntimeState source the runtime fields render as their
	// JSON zero/null defaults — operators get a 4-field response that
	// matches the v2.6 path.
	if v, ok := got["heartbeat_rtt_ms"]; ok {
		t.Errorf("heartbeat_rtt_ms should be omitted when nil, got %v", v)
	}
	if got["is_controller"] != false {
		t.Errorf("is_controller=%v, want false", got["is_controller"])
	}
	if got["partitions_led"] != float64(0) {
		t.Errorf("partitions_led=%v, want 0", got["partitions_led"])
	}
}

func TestHealthHandlerWithRuntime(t *testing.T) {
	src := &stubRuntime{
		isController:         false,
		controllerID:         "skafka-1",
		controllerEpoch:      43,
		heartbeatRTTMs:       12,
		heartbeatAgeMs:       230,
		assignmentVersion:    12847,
		assignmentAgeMs:      450,
		partitionsLed:        4,
		partitionsAssigned:   4,
		partitionsRecovering: 0,
	}
	got := hitHandler(t, "skafka-0", []string{"internal", "external"}, &TLSInfo{Enabled: true}, src)

	// Schema match against the plan's example /healthz response.
	want := map[string]any{
		"status":                "ok",
		"broker_id":             "skafka-0",
		"is_controller":         false,
		"controller_id":         "skafka-1",
		"controller_epoch":      float64(43),
		"heartbeat_rtt_ms":      float64(12),
		"heartbeat_age_ms":      float64(230),
		"assignment_version":    float64(12847),
		"assignment_age_ms":     float64(450),
		"partitions_led":        float64(4),
		"partitions_assigned":   float64(4),
		"partitions_recovering": float64(0),
	}
	for k, w := range want {
		if got[k] != w {
			t.Errorf("%s=%v, want %v", k, got[k], w)
		}
	}
	// listeners is a slice; spot-check it separately.
	listeners, ok := got["listeners"].([]any)
	if !ok || len(listeners) != 2 || listeners[0] != "internal" || listeners[1] != "external" {
		t.Errorf("listeners=%v, want [internal external]", got["listeners"])
	}
	tls, ok := got["tls"].(map[string]any)
	if !ok || tls["enabled"] != true {
		t.Errorf("tls=%v, want enabled=true", got["tls"])
	}
}

// "no measurement yet" — RTT and AgeMs return -1 from the source, the
// handler must omit them rather than render misleading -1 values.
func TestHealthHandlerOmitsNegativeMeasurements(t *testing.T) {
	src := &stubRuntime{
		controllerID:    "skafka-1",
		heartbeatRTTMs:  -1,
		heartbeatAgeMs:  -1,
		assignmentAgeMs: -1,
	}
	got := hitHandler(t, "skafka-0", nil, nil, src)
	for _, key := range []string{"heartbeat_rtt_ms", "heartbeat_age_ms", "assignment_age_ms"} {
		if v, ok := got[key]; ok {
			t.Errorf("%s should be omitted on -1, got %v", key, v)
		}
	}
	// controller_id must still render — it's a present-but-empty case.
	if got["controller_id"] != "skafka-1" {
		t.Errorf("controller_id=%v, want skafka-1", got["controller_id"])
	}
}
