package observability

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestRecordK8sCallSuccess(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m, err := NewMetrics(mp.Meter("skafka-test"))
	if err != nil {
		t.Fatal(err)
	}
	SetGlobal(m)
	defer SetGlobal(nil)

	if err := RecordK8sCall(context.Background(), "List", "KafkaTopic", func() error {
		return nil
	}); err != nil {
		t.Fatalf("RecordK8sCall: %v", err)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}

	gotResult, gotCount := k8sCallSeen(t, rm, "List", "KafkaTopic")
	if gotResult != "ok" || gotCount < 1 {
		t.Errorf("after success: result=%q count=%d, want result=ok count>=1", gotResult, gotCount)
	}
}

func TestRecordK8sCallFailure(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m, err := NewMetrics(mp.Meter("skafka-test"))
	if err != nil {
		t.Fatal(err)
	}
	SetGlobal(m)
	defer SetGlobal(nil)

	want := errors.New("apiserver down")
	if err := RecordK8sCall(context.Background(), "Patch", "Pod", func() error { return want }); !errors.Is(err, want) {
		t.Errorf("err pass-through broken: %v", err)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}

	gotResult, gotCount := k8sCallSeen(t, rm, "Patch", "Pod")
	if gotResult != "error" || gotCount < 1 {
		t.Errorf("after failure: result=%q count=%d, want result=error count>=1", gotResult, gotCount)
	}
}

// k8sCallSeen returns (result_label, count) for the first datapoint
// matching (operation, resource) on skafka.k8s.api.calls.
func k8sCallSeen(t *testing.T, rm metricdata.ResourceMetrics, op, res string) (string, int64) {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, inst := range sm.Metrics {
			if inst.Name != "skafka.k8s.api.calls" {
				continue
			}
			s, ok := inst.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, dp := range s.DataPoints {
				gotOp, _ := dp.Attributes.Value(attribute.Key("operation"))
				gotRes, _ := dp.Attributes.Value(attribute.Key("resource"))
				if gotOp.AsString() != op || gotRes.AsString() != res {
					continue
				}
				gotResult, _ := dp.Attributes.Value(attribute.Key("result"))
				return gotResult.AsString(), dp.Value
			}
		}
	}
	return "", 0
}
