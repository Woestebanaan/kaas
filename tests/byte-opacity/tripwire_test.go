// Package byteopacity holds the byte-opacity round-trip tests required
// by v3.3 plan constraint #22 (the broker is a byte mover, not a byte
// interpreter). The codec must not decode RecordBatch payloads; the
// storage engine must not re-encode them.
//
// This file enforces that with two layers of protection:
//
//   - A real produce path round-trip (broker.MemoryStorage Append → Read)
//     asserts request bytes equal response bytes byte-for-byte across
//     compression codecs the broker should never need to interpret.
//   - The byte-opacity tripwire counters
//     (skafka_codec_record_decode_total, skafka_codec_batch_reencode_total)
//     are checked at zero at the end of every round-trip. If a future
//     refactor decodes a record or re-encodes a batch, the counter will
//     increment via observability.BumpCodecRecordDecode /
//     BumpCodecBatchReencode and the test will fail loudly.
package byteopacity

import (
	"bytes"
	"context"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/woestebanaan/skafka/internal/broker"
	"github.com/woestebanaan/skafka/internal/observability"
	"github.com/woestebanaan/skafka/tests/testutil/recordbatch"
)

// installTestMetrics swaps the global registry for one backed by a
// ManualReader so the test can collect counter values synchronously.
// The returned restore func re-installs the previous registry.
func installTestMetrics(t *testing.T) (*sdkmetric.ManualReader, func()) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m, err := observability.NewMetrics(mp.Meter("byteopacity-test"))
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}
	prev := observability.Global()
	observability.SetGlobal(m)
	return reader, func() { observability.SetGlobal(prev) }
}

// TestStorageRoundTripIsByteIdentical: bytes given to MemoryStorage.Append
// come back from .Read unchanged across multiple batches with different
// compression codes (snappy, gzip, none, lz4). If the storage path ever
// decodes or re-encodes, this fails. The compression bits are
// declarative — no actual compression happens — but the broker is
// supposed to treat them as opaque, so the lie is the point.
func TestStorageRoundTripIsByteIdentical(t *testing.T) {
	reader, restore := installTestMetrics(t)
	defer restore()

	store := broker.NewMemoryStorage()
	ctx := context.Background()

	type batchCase struct {
		name        string
		numRecords  int
		compression int16 // 0=none 1=gzip 2=snappy 3=lz4 4=zstd
	}
	cases := []batchCase{
		{"snappy", 5, 2},
		{"none", 3, 0},
		{"gzip", 4, 1},
		{"lz4", 7, 3},
		{"zstd", 2, 4},
	}

	var baseOffset int64
	var combined []byte
	for _, c := range cases {
		batch := buildBatch(t, baseOffset, c.numRecords, c.compression)
		baseOffset += int64(c.numRecords)

		if _, err := store.Append(ctx, "byteopacity-topic", 0, 0, batch); err != nil {
			t.Fatalf("[%s] Append: %v", c.name, err)
		}
		combined = append(combined, batch...)
	}

	got, err := store.Read(ctx, "byteopacity-topic", 0, 0, len(combined)+1024)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	if !bytes.Equal(got, combined) {
		t.Errorf("byte-identical round-trip FAIL: got %d bytes, want %d bytes",
			len(got), len(combined))
		// Hint where they diverged for the easy case.
		mismatchIdx := -1
		for i := 0; i < len(got) && i < len(combined); i++ {
			if got[i] != combined[i] {
				mismatchIdx = i
				break
			}
		}
		if mismatchIdx >= 0 {
			t.Errorf("first mismatch at byte %d: got 0x%02x, want 0x%02x",
				mismatchIdx, got[mismatchIdx], combined[mismatchIdx])
		}
	}

	assertTripwiresZero(t, ctx, reader)
}

// TestBumpCodecRecordDecodeIncrements is the meta-test for the tripwire
// helpers — proves that IF a future violator calls
// observability.BumpCodecRecordDecode, the counter does increment, so
// alerts wired against skafka_codec_record_decode_total will fire.
//
// This is the only test that calls Bump* by design; production code
// must never call it.
func TestBumpCodecRecordDecodeIncrements(t *testing.T) {
	reader, restore := installTestMetrics(t)
	defer restore()
	ctx := context.Background()

	observability.BumpCodecRecordDecode(ctx, "byteopacity-test-meta")
	observability.BumpCodecBatchReencode(ctx, "byteopacity-test-meta")

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	got := readTripwireCounts(rm)
	if got["skafka.codec.record.decode"] != 1 {
		t.Errorf("CodecRecordDecode = %d, want 1", got["skafka.codec.record.decode"])
	}
	if got["skafka.codec.batch.reencode"] != 1 {
		t.Errorf("CodecBatchReencode = %d, want 1", got["skafka.codec.batch.reencode"])
	}
}

// assertTripwiresZero collects metrics and asserts the byte-opacity
// counters are at zero. Failing this assertion means some code path
// in the round-trip violated the byte-mover invariant.
func assertTripwiresZero(t *testing.T, ctx context.Context, reader *sdkmetric.ManualReader) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	got := readTripwireCounts(rm)
	if v := got["skafka.codec.record.decode"]; v > 0 {
		t.Errorf("byte-opacity violated: skafka.codec.record.decode = %d (must be 0). "+
			"Some code path called observability.BumpCodecRecordDecode — see the slog warning for the site.", v)
	}
	if v := got["skafka.codec.batch.reencode"]; v > 0 {
		t.Errorf("byte-opacity violated: skafka.codec.batch.reencode = %d (must be 0). "+
			"Some code path called observability.BumpCodecBatchReencode.", v)
	}
}

// readTripwireCounts pulls both tripwire counters into a single map.
// Returns 0 for missing instruments (no emit yet → ManualReader sees
// nothing).
func readTripwireCounts(rm metricdata.ResourceMetrics) map[string]int64 {
	out := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, inst := range sm.Metrics {
			if inst.Name != "skafka.codec.record.decode" && inst.Name != "skafka.codec.batch.reencode" {
				continue
			}
			sum, ok := inst.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			var total int64
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
			out[inst.Name] = total
		}
	}
	return out
}

// buildBatch constructs a real Kafka v2 RecordBatch with the given
// compression bits in attributes. The records' Value bytes are just
// counter values — what matters for the test is that the encode/decode
// happens entirely within the test (recordbatch.Encode is in
// tests/testutil), not on the broker side.
func buildBatch(t *testing.T, baseOffset int64, numRecords int, compressionBits int16) []byte {
	t.Helper()
	batch := &recordbatch.RecordBatch{
		BaseOffset:      baseOffset,
		Attributes:      compressionBits,
		LastOffsetDelta: int32(numRecords - 1),
		ProducerID:      -1,
		ProducerEpoch:   -1,
		BaseSequence:    -1,
	}
	for i := 0; i < numRecords; i++ {
		batch.Records = append(batch.Records, recordbatch.Record{
			OffsetDelta: int32(i),
			Value:       []byte{byte(i)},
		})
	}
	return recordbatch.Encode(nil, batch)
}
