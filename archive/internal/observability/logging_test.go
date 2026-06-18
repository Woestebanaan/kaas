package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/otel/sdk/trace"
	"k8s.io/klog/v2"
)

func TestCorrelationHandlerWithActiveSpan(t *testing.T) {
	var buf bytes.Buffer
	inner := slog.NewJSONHandler(&buf, nil)
	h := &CorrelationHandler{inner: inner}
	log := slog.New(h)

	tp := trace.NewTracerProvider()
	tracer := tp.Tracer("test")
	ctx, span := tracer.Start(context.Background(), "op")
	log.InfoContext(ctx, "hello")
	span.End()

	var entry map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &entry); err != nil {
		t.Fatal(err)
	}
	if _, ok := entry["trace_id"]; !ok {
		t.Error("trace_id missing from log entry")
	}
	if _, ok := entry["span_id"]; !ok {
		t.Error("span_id missing from log entry")
	}
}

func TestCorrelationHandlerWithoutSpan(t *testing.T) {
	var buf bytes.Buffer
	inner := slog.NewJSONHandler(&buf, nil)
	h := &CorrelationHandler{inner: inner}
	log := slog.New(h)

	log.InfoContext(context.Background(), "no-span")

	if strings.Contains(buf.String(), "trace_id") {
		t.Errorf("trace_id should be absent when no span: %s", buf.String())
	}
}

// TestKlogBridgeEmitsJSON pins the gh #95 follow-up: client-go's
// klog calls (leaderelection / reflectors / informers) must funnel
// through the same slog handler skafka uses, so kubectl logs sees
// one log shape instead of two. Without the bridge, klog would
// write its native format ("I0507 21:20:19 1 file.go:42] ...")
// straight to stderr and break JSON parsing.
func TestKlogBridgeEmitsJSON(t *testing.T) {
	var buf bytes.Buffer
	inner := slog.NewJSONHandler(&buf, nil)
	h := &CorrelationHandler{inner: inner}
	klog.SetLogger(logr.FromSlogHandler(slog.New(h).Handler()))
	defer klog.ClearLogger()

	klog.InfoS("leaderelection-style message", "lock", "skafka/skafka-controller")

	out := strings.TrimSpace(buf.String())
	if out == "" {
		t.Fatalf("klog did not produce any output through the bridge")
	}
	var entry map[string]any
	if err := json.Unmarshal([]byte(out), &entry); err != nil {
		t.Fatalf("klog output is not JSON: %v\nraw=%s", err, out)
	}
	if entry["msg"] != "leaderelection-style message" {
		t.Errorf("msg=%v, want \"leaderelection-style message\"", entry["msg"])
	}
	if entry["lock"] != "skafka/skafka-controller" {
		t.Errorf("lock=%v, want \"skafka/skafka-controller\"", entry["lock"])
	}
}

func TestParseLevel(t *testing.T) {
	cases := []struct {
		in   string
		want slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"info", slog.LevelInfo},
		{"warn", slog.LevelWarn},
		{"warning", slog.LevelWarn},
		{"error", slog.LevelError},
		{"", slog.LevelInfo},
		{"whatever", slog.LevelInfo},
	}
	for _, tc := range cases {
		if got := parseLevel(tc.in); got != tc.want {
			t.Errorf("parseLevel(%q)=%v, want %v", tc.in, got, tc.want)
		}
	}
}
