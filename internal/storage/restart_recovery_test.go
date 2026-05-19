package storage

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/woestebanaan/skafka/tests/testutil/recordbatch"
)

// TestRestartRecovery_ReplaysManifestAndValidatesSegments pins gh #50:
// after a clean engine close, a fresh NewDiskStorageEngine pointed at
// the same data dir must replay the manifest, find the existing
// segments, and report the same HighWatermark as before.
//
// This is the load-bearing contract for graceful broker restarts.
// A regression here would mean every restart loses committed records
// — the symptom that motivated gh #139.
func TestRestartRecovery_ReplaysManifestAndValidatesSegments(t *testing.T) {
	dir := t.TempDir()
	leases := &neverLeaderLeases{}
	cfg := DefaultConfig()

	// Phase 1: write some records, take a fresh engine through a
	// graceful close.
	e1, err := NewDiskStorageEngine(dir, leases, cfg)
	if err != nil {
		t.Fatalf("engine1: %v", err)
	}
	if err := e1.CreatePartition("t", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := e1.TakeOver(context.Background(), "t", 0, 1); err != nil {
		t.Fatal(err)
	}

	const N = 20
	ctx := context.Background()
	for i := 0; i < N; i++ {
		batch := recordbatch.Encode(nil, &recordbatch.RecordBatch{
			BaseOffset:      int64(i),
			LastOffsetDelta: 0,
			ProducerID:      -1,
			ProducerEpoch:   -1,
			BaseSequence:    -1,
			Records: []recordbatch.Record{
				{OffsetDelta: 0, Value: []byte(strings.Repeat("v", 16))},
			},
		})
		if _, err := e1.Append(ctx, "t", 0, 0, -1, batch); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	hwm1, err := e1.HighWatermark("t", 0)
	if err != nil {
		t.Fatalf("HighWatermark: %v", err)
	}
	if hwm1 != int64(N) {
		t.Fatalf("phase1: HWM=%d, want %d", hwm1, N)
	}

	// Persist manifest by relinquishing — the production engine's
	// shutdown path calls Relinquish on every owned partition.
	if err := e1.Relinquish("t", 0); err != nil {
		t.Fatalf("relinquish: %v", err)
	}

	// Phase 2: open a brand-new engine on the same dir. It must
	// observe the same HWM after takeover.
	e2, err := NewDiskStorageEngine(dir, leases, cfg)
	if err != nil {
		t.Fatalf("engine2: %v", err)
	}
	if err := e2.CreatePartition("t", 0); err != nil {
		t.Fatal(err)
	}
	hwmAfter, err := e2.TakeOver(context.Background(), "t", 0, 2)
	if err != nil {
		t.Fatalf("takeover2: %v", err)
	}
	if hwmAfter != int64(N) {
		t.Errorf("phase2 takeover HWM=%d, want %d (restart recovery dropped records)", hwmAfter, N)
	}
	got, _ := e2.HighWatermark("t", 0)
	if got != int64(N) {
		t.Errorf("phase2 HWM via HighWatermark()=%d, want %d", got, N)
	}
}

// TestRestartRecovery_HandlesMissingManifest pins the cold path:
// when manifest.json doesn't exist (first boot, or corrupted),
// openPartition + takeover must reconstruct HWM by scanning the log
// directory. Without this, a startup-sweep that loses the manifest
// (e.g. tmp-rename interrupted) would advertise HWM=0 and lose
// records — exactly the gh #139 symptom.
func TestRestartRecovery_HandlesMissingManifest(t *testing.T) {
	dir := t.TempDir()
	leases := &neverLeaderLeases{}
	cfg := DefaultConfig()

	e1, err := NewDiskStorageEngine(dir, leases, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := e1.CreatePartition("t", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := e1.TakeOver(context.Background(), "t", 0, 1); err != nil {
		t.Fatal(err)
	}
	const N = 7
	ctx := context.Background()
	for i := 0; i < N; i++ {
		batch := recordbatch.Encode(nil, &recordbatch.RecordBatch{
			BaseOffset:      int64(i),
			LastOffsetDelta: 0,
			ProducerID:      -1,
			ProducerEpoch:   -1,
			BaseSequence:    -1,
			Records: []recordbatch.Record{
				{OffsetDelta: 0, Value: []byte("v")},
			},
		})
		if _, err := e1.Append(ctx, "t", 0, 0, -1, batch); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	_ = e1.Relinquish("t", 0)

	// Simulate a manifest loss between shutdown and next start.
	manifestPath := manifestFile(dir, "t", 0)
	if err := removeIfExists(t, manifestPath); err != nil {
		t.Fatalf("remove manifest: %v", err)
	}

	e2, err := NewDiskStorageEngine(dir, leases, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := e2.CreatePartition("t", 0); err != nil {
		t.Fatal(err)
	}
	hwm, err := e2.TakeOver(context.Background(), "t", 0, 2)
	if err != nil {
		t.Fatalf("takeover after manifest loss: %v", err)
	}
	if hwm != int64(N) {
		t.Errorf("HWM after manifest loss=%d, want %d (recoverSegment scan should have rebuilt it)", hwm, N)
	}
}

func manifestFile(dir, topic string, partition int32) string {
	return dir + "/" + topic + "/" + intToStr(int(partition)) + "/manifest.json"
}

func intToStr(n int) string {
	if n == 0 {
		return "0"
	}
	const digits = "0123456789"
	var buf [8]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = digits[n%10]
		n /= 10
	}
	return string(buf[i:])
}

func removeIfExists(t *testing.T, path string) error {
	t.Helper()
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil
	}
	return os.Remove(path)
}
