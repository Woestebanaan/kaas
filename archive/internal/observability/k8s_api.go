package observability

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// RecordK8sCall wraps a single apiserver call so the duration +
// result lands on K8sAPILatency / K8sAPICalls. Call as:
//
//	err := observability.RecordK8sCall(ctx, "List", "KafkaTopic", func() error {
//		return w.client.List(ctx, &list)
//	})
//
// Operation labels: "Get", "List", "Watch", "Patch", "Update", "Create",
// "Delete". Resource labels: the Kind for K8s native objects
// ("Lease", "Pod", "EndpointSlice", "KafkaTopic", ...).
//
// fn's return value is wired straight through; the caller's existing
// error handling is unchanged. Cardinality stays small because the
// operation/resource space is bounded by the broker's actual calls
// (under a dozen pairs in practice).
func RecordK8sCall(ctx context.Context, operation, resource string, fn func() error) error {
	mx := Global()
	start := time.Now()
	err := fn()

	attrs := metric.WithAttributes(
		attribute.String("operation", operation),
		attribute.String("resource", resource),
	)
	mx.K8sAPILatency.Record(ctx, time.Since(start).Seconds(), attrs)

	result := "ok"
	if err != nil {
		result = "error"
	}
	mx.K8sAPICalls.Add(ctx, 1, metric.WithAttributes(
		attribute.String("operation", operation),
		attribute.String("resource", resource),
		attribute.String("result", result),
	))
	return err
}
