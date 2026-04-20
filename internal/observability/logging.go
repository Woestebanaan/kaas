package observability

import (
	"context"
	"log/slog"
	"os"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

// CorrelationHandler wraps a slog.Handler and adds trace_id / span_id attributes
// to every record whose context carries an active span.
type CorrelationHandler struct {
	inner slog.Handler
}

func (h *CorrelationHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *CorrelationHandler) Handle(ctx context.Context, r slog.Record) error {
	if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
		sc := span.SpanContext()
		r.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.inner.Handle(ctx, r)
}

func (h *CorrelationHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &CorrelationHandler{inner: h.inner.WithAttrs(attrs)}
}

func (h *CorrelationHandler) WithGroup(name string) slog.Handler {
	return &CorrelationHandler{inner: h.inner.WithGroup(name)}
}

// InstallLogger replaces the default slog handler with one that adds OTel trace
// correlation. Honors SKAFKA_LOG_LEVEL (debug|info|warn|error) and
// SKAFKA_LOG_FORMAT (json|text). Call once at startup.
func InstallLogger() {
	level := parseLevel(os.Getenv("SKAFKA_LOG_LEVEL"))
	opts := &slog.HandlerOptions{Level: level}

	var inner slog.Handler
	if strings.EqualFold(os.Getenv("SKAFKA_LOG_FORMAT"), "text") {
		inner = slog.NewTextHandler(os.Stderr, opts)
	} else {
		inner = slog.NewJSONHandler(os.Stderr, opts)
	}
	slog.SetDefault(slog.New(&CorrelationHandler{inner: inner}))
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
