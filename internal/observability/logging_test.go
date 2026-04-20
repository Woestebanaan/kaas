package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/sdk/trace"
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
