package observability

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// BumpCodecRecordDecode is the canonical entry point for the
// byte-opacity tripwire. Every code path that decodes an individual
// record (rather than treating the RecordBatch as opaque bytes) MUST
// call this — and that should be never. site is a short string that
// names the offender so the alert payload is actionable.
//
// As of v1, no skafka code path calls this. If you find yourself
// adding the first call, stop: the v3.3 plan is explicit that the
// broker is a byte mover, not a byte interpreter. A use case that
// genuinely requires record-level inspection (e.g. a future ksqlDB
// integration) needs a separate design discussion before it lands.
func BumpCodecRecordDecode(ctx context.Context, site string) {
	Global().CodecRecordDecode.Add(ctx, 1,
		metric.WithAttributes(attribute.String("site", site)))
	slog.Warn("byte-opacity tripwire fired (record decode)", "site", site)
}

// BumpCodecBatchReencode is the sibling tripwire for re-encoding a
// RecordBatch. Same contract as BumpCodecRecordDecode: every increment
// is a bug, and the slog.Warn line is meant to surface in production
// logs so an investigation starts before alerts even fire.
func BumpCodecBatchReencode(ctx context.Context, site string) {
	Global().CodecBatchReencode.Add(ctx, 1,
		metric.WithAttributes(attribute.String("site", site)))
	slog.Warn("byte-opacity tripwire fired (batch reencode)", "site", site)
}
