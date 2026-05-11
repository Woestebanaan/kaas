package protocol

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/observability"
)

// RequestTracing is the second built-in middleware (gh #121 PR4).
// Starts an OTel span around every handler invocation, named
// "kafka.<apiKey>" with attributes for api_key, version, client_id,
// and response_bytes. The span's lifetime is exactly the
// handler.Handle call.
//
// PR2.5 (handler middleware) is the load-bearing pre-req: without it
// this would need a defer Span.End() block in every handler. With
// middleware it's one chain entry covering all of them.
//
// Tracing-sampling decisions are made by the global tracer provider
// (configured in Bootstrap via OTEL_TRACES_SAMPLER_ARG). At the
// default 0.1 ratio only ~10% of requests get a span, so the
// allocation cost is bounded.
//
// The span is created on context.Background(), not on a request-scoped
// context, because the existing Handler signature doesn't carry a
// context.Context (yet). Plumbing one through is a follow-up — for
// now spans are detached. Downstream code that wants the span can
// reach for trace.SpanFromContext(ctx) only when given a derived
// context (e.g. storage operations call observability.StartSpan
// internally).
func RequestTracing() Middleware {
	return func(apiKey int16, next Handler) Handler {
		spanName := fmt.Sprintf("kafka.api_key=%d", apiKey)
		keyAttr := attribute.Int("kafka.api_key", int(apiKey))
		return HandlerFunc(func(conn *connstate.ConnState, version int16, body []byte) ([]byte, error) {
			_, span := observability.Tracer().Start(
				context.Background(),
				spanName,
				trace.WithAttributes(
					keyAttr,
					attribute.Int("kafka.version", int(version)),
					attribute.String("kafka.client_id", conn.ClientID),
					attribute.Int("kafka.request_bytes", len(body)),
				),
			)
			resp, err := next.Handle(conn, version, body)
			span.SetAttributes(attribute.Int("kafka.response_bytes", len(resp)))
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			}
			span.End()
			return resp, err
		})
	}
}
