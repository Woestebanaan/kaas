package protocol

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/observability"
)

// RequestObservability is the built-in middleware that records request
// latency uniformly across every registered API handler. Before this
// existed, only the Produce and Fetch handlers carried a
// `defer mx.RequestLatency.Record(...)` block — the other ~28
// handlers silently missed the timeseries entirely, so the
// `skafka_request_latency_seconds` histogram was useless for any API
// other than 0/1.
//
// Wired once on the Dispatcher via d.Use(RequestObservability()), this
// middleware covers Metadata, JoinGroup, OffsetCommit, InitProducerId,
// FindCoordinator, every admin API, etc. Labels: api_key (set at
// Register time so it's allocation-free in the hot path) and version
// (negotiated per-request).
//
// The middleware records latency post-call regardless of whether the
// handler returned an error — a handler that fails fast at decode
// time still contributes to p50/p95. This is intentional: a sudden
// surge in fast-erroring requests is a useful signal that the
// percentile dashboards should pick up.
//
// The function is a constructor returning a Middleware so that future
// callers can pass an explicit *observability.Metrics for testing.
// At the broker bootstrap we pull from observability.Global() — the
// same singleton the handlers already use — so we don't need to
// thread a Metrics pointer through main.go.
func RequestObservability() Middleware {
	return func(apiKey int16, next Handler) Handler {
		keyAttr := attribute.Int("api_key", int(apiKey))
		return HandlerFunc(func(conn *connstate.ConnState, version int16, body []byte) ([]byte, error) {
			start := time.Now()
			resp, err := next.Handle(conn, version, body)
			observability.Global().RequestLatency.Record(
				context.Background(),
				time.Since(start).Seconds(),
				metric.WithAttributes(
					keyAttr,
					attribute.Int("version", int(version)),
				),
			)
			return resp, err
		})
	}
}
