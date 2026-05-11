package controllers

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/woestebanaan/skafka/internal/observability"
)

// Observed wraps any reconcile.Reconciler so every Reconcile call
// records to observability.Global() — OperatorReconciles counter
// (labelled by kind + result) and OperatorReconcileDuration histogram.
//
// gh #121 PR5: pre-PR5 the operator inherited controller-runtime's
// built-in Prometheus client_golang metrics, which never flow through
// OTLP and therefore never reach Grafana. Wrapping each reconciler
// here gives skafka-namespaced OTel metrics that go through the same
// pipeline as the broker's instruments.
//
// Result classification:
//   - "error"   — Reconcile returned a non-nil error (will requeue with backoff)
//   - "requeue" — returned Result.Requeue==true or RequeueAfter>0
//   - "ok"      — empty Result, nil error
//
// Wire by replacing `.Complete(r)` with `.Complete(Observed("KafkaTopic", r))`
// in each controller's SetupWithManager.
func Observed(kind string, inner reconcile.Reconciler) reconcile.Reconciler {
	return &observedReconciler{kind: kind, inner: inner}
}

type observedReconciler struct {
	kind  string
	inner reconcile.Reconciler
}

func (r *observedReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	mx := observability.Global()
	start := time.Now()
	res, err := r.inner.Reconcile(ctx, req)

	kindAttr := attribute.String("kind", r.kind)
	mx.OperatorReconcileDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(kindAttr))

	result := "ok"
	switch {
	case err != nil:
		result = "error"
	case res.Requeue || res.RequeueAfter > 0:
		result = "requeue"
	}
	mx.OperatorReconciles.Add(ctx, 1,
		metric.WithAttributes(kindAttr, attribute.String("result", result)))

	return res, err
}
