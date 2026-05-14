package storage

import (
	"context"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/woestebanaan/skafka/internal/observability"
	"github.com/woestebanaan/skafka/tests/testutil/recordbatch"
)

// TestCleanerEmitsRunMetric pins gh #121 PR3's contract: every
// retention cleaner pass increments CleanerRuns and CleanerDuration,
// so dashboards can show "is the cleaner running, how often, how long
// does each pass take" instead of grepping slog.
func TestCleanerEmitsRunMetric(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m, err := observability.NewMetrics(mp.Meter("skafka-test"))
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}
	observability.SetGlobal(m)
	defer observability.SetGlobal(nil)

	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.FlushIntervalMessages = 0
	e, err := NewDiskStorageEngine(dir, &neverLeaderLeases{}, cfg)
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	if err := e.CreatePartition("t", 0); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := e.TakeOver(context.Background(), "t", 0, 1); err != nil {
		t.Fatalf("takeover: %v", err)
	}

	ps, ok := e.getPartition("t", 0)
	if !ok {
		t.Fatal("partition state missing")
	}
	c := &RetentionCleaner{engine: e}
	c.cleanPartition(PartitionID{Topic: "t", Partition: 0})
	_ = ps

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}

	if sum := sumIntCounter(t, rm, "skafka.cleaner.runs"); sum < 1 {
		t.Errorf("skafka.cleaner.runs=%d, want >=1", sum)
	}
	if !histogramHasDataPoints(t, rm, "skafka.cleaner.duration") {
		t.Error("skafka.cleaner.duration: no data points; cleaner duration histogram never recorded")
	}
}

// TestCompactionEmitsRunMetric pins the compactor-side equivalent of
// TestCleanerEmitsRunMetric. Even an empty-partition no-op compaction
// (no closed segments yet) should emit a runs counter, because we
// still want to count attempted runs for the "is the compactor
// healthy" dashboard panel.
func TestCompactionEmitsRunMetric(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m, err := observability.NewMetrics(mp.Meter("skafka-test"))
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}
	observability.SetGlobal(m)
	defer observability.SetGlobal(nil)

	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.SegmentBytes = 4 * 1024
	cfg.FlushIntervalMessages = 0

	e, err := NewDiskStorageEngine(dir, &neverLeaderLeases{}, cfg)
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	if err := e.CreatePartition("kt", 0); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := e.TakeOver(context.Background(), "kt", 0, 1); err != nil {
		t.Fatalf("takeover: %v", err)
	}

	// Drive enough appends to roll at least one closed segment so the
	// compactor has something to chew on.
	for i := 0; i < 20; i++ {
		key := []byte{byte('a' + i%3)}
		value := make([]byte, 500)
		value[0] = byte(i)
		batch := recordbatch.Encode(nil, &recordbatch.RecordBatch{
			BaseOffset: 0, LastOffsetDelta: 0,
			BaseTimestamp: 1700000000000 + int64(i),
			MaxTimestamp:  1700000000000 + int64(i),
			ProducerID:    -1, ProducerEpoch: -1, BaseSequence: -1,
			Records: []recordbatch.Record{
				{OffsetDelta: 0, Key: key, Value: value},
			},
		})
		if _, err := e.Append(context.Background(), "kt", 0, 1, -1, batch); err != nil {
			t.Fatalf("append i=%d: %v", i, err)
		}
	}

	ps, ok := e.getPartition("kt", 0)
	if !ok {
		t.Fatal("partition state missing")
	}
	if _, _, err := e.compactPartition(ps); err != nil {
		t.Fatalf("compactPartition: %v", err)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}

	if sum := sumIntCounter(t, rm, "skafka.compaction.runs"); sum < 1 {
		t.Errorf("skafka.compaction.runs=%d, want >=1", sum)
	}
	if !histogramHasDataPoints(t, rm, "skafka.compaction.duration") {
		t.Error("skafka.compaction.duration: no data points")
	}
	if sumIntCounter(t, rm, "skafka.compaction.records.kept")+sumIntCounter(t, rm, "skafka.compaction.records.dropped") == 0 {
		t.Error("compaction kept+dropped both zero — instruments did not fire")
	}
	if sumIntCounter(t, rm, "skafka.compaction.bytes.in") == 0 {
		t.Error("skafka.compaction.bytes.in=0 — should have scanned at least one closed segment")
	}
}

func sumIntCounter(t *testing.T, rm metricdata.ResourceMetrics, name string) int64 {
	t.Helper()
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, inst := range sm.Metrics {
			if inst.Name != name {
				continue
			}
			if s, ok := inst.Data.(metricdata.Sum[int64]); ok {
				for _, dp := range s.DataPoints {
					total += dp.Value
				}
			}
		}
	}
	return total
}

func histogramHasDataPoints(t *testing.T, rm metricdata.ResourceMetrics, name string) bool {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, inst := range sm.Metrics {
			if inst.Name != name {
				continue
			}
			if h, ok := inst.Data.(metricdata.Histogram[float64]); ok && len(h.DataPoints) > 0 {
				return true
			}
		}
	}
	return false
}
