package protocol

import (
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/woestebanaan/skafka/internal/connstate"
)

// TestRequestTracingEmitsSpan pins gh #121 PR4's central contract:
// every Dispatch call produces exactly one span on the tracer
// provider, named "kafka.api_key=<n>" and attributed with api_key,
// version, client_id, request_bytes, response_bytes.
func TestRequestTracingEmitsSpan(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(rec),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(prev)

	d := NewDispatcher()
	d.Use(RequestTracing())
	// Register the handler over the version range we plan to dispatch at —
	// (min=0, max=0) would fail the version check inside Dispatch before
	// the handler runs, masking the test (silently wraps but never fires).
	d.Register(3, 0, 12, HandlerFunc(func(c *connstate.ConnState, v int16, b []byte) ([]byte, error) {
		return []byte{0, 0, 0, 0}, nil
	}))

	state := &connstate.ConnState{ClientID: "test-client"}
	if _, err := d.Dispatch(RequestHeader{APIKey: 3, APIVersion: 5, CorrelationID: 1}, []byte{1, 2, 3}, state); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	spans := rec.Ended()
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}
	s := spans[0]
	if s.Name() != "kafka.api_key=3" {
		t.Errorf("name=%q, want kafka.api_key=3", s.Name())
	}

	attrs := map[string]any{}
	for _, kv := range s.Attributes() {
		attrs[string(kv.Key)] = kv.Value.AsInterface()
	}
	if v, ok := attrs["kafka.api_key"].(int64); !ok || v != 3 {
		t.Errorf("kafka.api_key=%v, want 3", attrs["kafka.api_key"])
	}
	if v, ok := attrs["kafka.version"].(int64); !ok || v != 5 {
		t.Errorf("kafka.version=%v, want 5", attrs["kafka.version"])
	}
	if v, ok := attrs["kafka.client_id"].(string); !ok || v != "test-client" {
		t.Errorf("kafka.client_id=%v, want test-client", attrs["kafka.client_id"])
	}
	if v, ok := attrs["kafka.request_bytes"].(int64); !ok || v != 3 {
		t.Errorf("kafka.request_bytes=%v, want 3", attrs["kafka.request_bytes"])
	}
	if v, ok := attrs["kafka.response_bytes"].(int64); !ok || v != 4 {
		t.Errorf("kafka.response_bytes=%v, want 4", attrs["kafka.response_bytes"])
	}
}
