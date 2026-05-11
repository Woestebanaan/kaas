package protocol

import (
	"context"
	"sync/atomic"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/observability"
)

// TestMiddlewareWrapsHandler verifies that Use+Register composes a
// middleware around the base Handler so a single Dispatch call passes
// through both — gh #121 PR2.5 core invariant.
func TestMiddlewareWrapsHandler(t *testing.T) {
	d := NewDispatcher()

	var wrapped atomic.Int32
	d.Use(func(apiKey int16, next Handler) Handler {
		return HandlerFunc(func(c *connstate.ConnState, v int16, b []byte) ([]byte, error) {
			wrapped.Add(1)
			return next.Handle(c, v, b)
		})
	})

	var base atomic.Int32
	d.Register(99, 0, 0, HandlerFunc(func(c *connstate.ConnState, v int16, b []byte) ([]byte, error) {
		base.Add(1)
		return []byte{0, 0}, nil
	}))

	resp, err := d.Dispatch(RequestHeader{APIKey: 99, APIVersion: 0, CorrelationID: 1}, nil, &connstate.ConnState{})
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if len(resp) == 0 {
		t.Fatal("empty response")
	}
	if base.Load() != 1 || wrapped.Load() != 1 {
		t.Errorf("expected both base=1 and wrapped=1, got base=%d wrapped=%d", base.Load(), wrapped.Load())
	}
}

// TestMiddlewareOrderingIsOuterToInner pins onion-order semantics:
// the first Use() call is the OUTERMOST layer (runs first on the way
// in, last on the way out).
func TestMiddlewareOrderingIsOuterToInner(t *testing.T) {
	d := NewDispatcher()
	var order []string

	d.Use(func(apiKey int16, next Handler) Handler {
		return HandlerFunc(func(c *connstate.ConnState, v int16, b []byte) ([]byte, error) {
			order = append(order, "outer-pre")
			resp, err := next.Handle(c, v, b)
			order = append(order, "outer-post")
			return resp, err
		})
	})
	d.Use(func(apiKey int16, next Handler) Handler {
		return HandlerFunc(func(c *connstate.ConnState, v int16, b []byte) ([]byte, error) {
			order = append(order, "inner-pre")
			resp, err := next.Handle(c, v, b)
			order = append(order, "inner-post")
			return resp, err
		})
	})
	d.Register(99, 0, 0, HandlerFunc(func(c *connstate.ConnState, v int16, b []byte) ([]byte, error) {
		order = append(order, "handler")
		return []byte{0, 0}, nil
	}))

	if _, err := d.Dispatch(RequestHeader{APIKey: 99, APIVersion: 0}, nil, &connstate.ConnState{}); err != nil {
		t.Fatal(err)
	}
	want := []string{"outer-pre", "inner-pre", "handler", "inner-post", "outer-post"}
	if len(order) != len(want) {
		t.Fatalf("order length=%d want=%d (%v)", len(order), len(want), order)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Errorf("order[%d]=%q want %q (full: %v)", i, order[i], want[i], order)
		}
	}
}

// TestRequestObservabilityUniformCoverage is the regression guard for
// gh #121 PR2.5's central claim: every registered API key produces a
// latency observation, not just Produce/Fetch. Before the middleware,
// 28 of ~30 handlers silently skipped the histogram. We register
// three dummy handlers across different api_keys, fire one Dispatch
// at each, and confirm three distinct (api_key, version) data points
// land on the histogram.
func TestRequestObservabilityUniformCoverage(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m, err := observability.NewMetrics(mp.Meter("skafka-test"))
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}
	observability.SetGlobal(m)
	defer observability.SetGlobal(nil)

	d := NewDispatcher()
	d.Use(RequestObservability())

	dummy := HandlerFunc(func(c *connstate.ConnState, v int16, b []byte) ([]byte, error) {
		return []byte{0, 0}, nil
	})
	for _, k := range []int16{3, 11, 25} { // Metadata, JoinGroup, AddOffsetsToTxn
		d.Register(k, 0, 0, dummy)
	}

	for _, k := range []int16{3, 11, 25} {
		if _, err := d.Dispatch(RequestHeader{APIKey: k, APIVersion: 0}, nil, &connstate.ConnState{}); err != nil {
			t.Fatalf("dispatch k=%d: %v", k, err)
		}
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}

	seen := map[int64]bool{}
	// gh #128 follow-up: every datapoint also carries the api_name
	// string attribute. wantNames maps the int key to the expected
	// human-readable name produced by APIName().
	wantNames := map[int64]string{3: "Metadata", 11: "JoinGroup", 25: "AddOffsetsToTxn"}
	for _, sm := range rm.ScopeMetrics {
		for _, inst := range sm.Metrics {
			if inst.Name != "skafka.request.latency" {
				continue
			}
			h, ok := inst.Data.(metricdata.Histogram[float64])
			if !ok {
				continue
			}
			for _, dp := range h.DataPoints {
				keyV, hasKey := dp.Attributes.Value("api_key")
				nameV, hasName := dp.Attributes.Value("api_name")
				if !hasKey {
					continue
				}
				seen[keyV.AsInt64()] = true
				if want, ok := wantNames[keyV.AsInt64()]; ok {
					if !hasName || nameV.AsString() != want {
						t.Errorf("api_key=%d carries api_name=%q, want %q", keyV.AsInt64(), nameV.AsString(), want)
					}
				}
			}
		}
	}
	for _, want := range []int64{3, 11, 25} {
		if !seen[want] {
			t.Errorf("api_key=%d missing from latency histogram — middleware did not fire for that key", want)
		}
	}
}
