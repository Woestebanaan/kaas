package observability

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// Tracer returns the package tracer. Uses the global tracer provider set up
// by Bootstrap; falls back to a no-op tracer when bootstrap hasn't run (tests).
func Tracer() trace.Tracer {
	return otel.Tracer("github.com/woestebanaan/skafka")
}

// StartSpan is a thin wrapper that starts a span on the package tracer.
// The returned context carries the active span so downstream code sees it.
func StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return Tracer().Start(ctx, name, opts...)
}
