package controllers

import (
	"context"
	"errors"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/woestebanaan/skafka/internal/observability"
)

// stubReconciler returns canned results for testing the observer.
type stubReconciler struct {
	res ctrl.Result
	err error
}

func (s *stubReconciler) Reconcile(context.Context, ctrl.Request) (ctrl.Result, error) {
	return s.res, s.err
}

// TestObservedReconcilerClassifies pins gh #121 PR5's result mapping:
// (nil, nil) => ok; non-nil err => error; Requeue/RequeueAfter => requeue.
func TestObservedReconcilerClassifies(t *testing.T) {
	cases := []struct {
		name       string
		inner      reconcile.Reconciler
		wantResult string
	}{
		{
			name:       "ok",
			inner:      &stubReconciler{res: ctrl.Result{}, err: nil},
			wantResult: "ok",
		},
		{
			name:       "requeue via Requeue flag",
			inner:      &stubReconciler{res: ctrl.Result{Requeue: true}, err: nil},
			wantResult: "requeue",
		},
		{
			name:       "requeue via RequeueAfter",
			inner:      &stubReconciler{res: ctrl.Result{RequeueAfter: 5 * time.Second}, err: nil},
			wantResult: "requeue",
		},
		{
			name:       "error",
			inner:      &stubReconciler{res: ctrl.Result{}, err: errors.New("boom")},
			wantResult: "error",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			m, err := observability.NewMetrics(mp.Meter("skafka-test"))
			if err != nil {
				t.Fatal(err)
			}
			observability.SetGlobal(m)
			defer observability.SetGlobal(nil)

			r := Observed("KafkaTopic", tc.inner)
			_, _ = r.Reconcile(context.Background(), ctrl.Request{})

			var rm metricdata.ResourceMetrics
			if err := reader.Collect(context.Background(), &rm); err != nil {
				t.Fatal(err)
			}

			if got := resultFor(t, rm, "skafka.operator.reconciles", "KafkaTopic"); got != tc.wantResult {
				t.Errorf("result=%q, want %q", got, tc.wantResult)
			}
			if !hasDurationPoint(t, rm, "skafka.operator.reconcile.duration", "KafkaTopic") {
				t.Error("reconcile.duration: no data point for KafkaTopic")
			}
		})
	}
}

// resultFor returns the result label of the first data point with value>=1 for the
// given kind, or "" if absent.
func resultFor(t *testing.T, rm metricdata.ResourceMetrics, name, kind string) string {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, inst := range sm.Metrics {
			if inst.Name != name {
				continue
			}
			s, ok := inst.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, dp := range s.DataPoints {
				gotKind, _ := dp.Attributes.Value("kind")
				if gotKind.AsString() != kind {
					continue
				}
				gotResult, _ := dp.Attributes.Value("result")
				if dp.Value >= 1 {
					return gotResult.AsString()
				}
			}
		}
	}
	return ""
}

func hasDurationPoint(t *testing.T, rm metricdata.ResourceMetrics, name, kind string) bool {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, inst := range sm.Metrics {
			if inst.Name != name {
				continue
			}
			h, ok := inst.Data.(metricdata.Histogram[float64])
			if !ok {
				continue
			}
			for _, dp := range h.DataPoints {
				gotKind, _ := dp.Attributes.Value("kind")
				if gotKind.AsString() == kind && dp.Count > 0 {
					return true
				}
			}
		}
	}
	return false
}
