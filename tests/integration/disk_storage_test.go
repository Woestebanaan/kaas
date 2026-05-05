package integration

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/woestebanaan/skafka/internal/lease"
	"github.com/woestebanaan/skafka/internal/storage"
	"github.com/woestebanaan/skafka/tests/testutil/recordbatch"
)

// --- stub implementations ---

type stubLeaseManager struct{ leader bool }

func (s *stubLeaseManager) Acquire(_ context.Context, _ string, _ int32) error { return nil }
func (s *stubLeaseManager) Release(_ string, _ int32) error                    { return nil }
func (s *stubLeaseManager) IsLeader(_ string, _ int32) bool                   { return s.leader }
func (s *stubLeaseManager) LeaderFor(_ string, _ int32) int32                 { return 0 }
func (s *stubLeaseManager) WatchLeaders(_ context.Context) (<-chan lease.LeaderChange, error) {
	return make(chan lease.LeaderChange), nil
}

// compile-time interface check
var _ lease.LeaseManager = (*stubLeaseManager)(nil)

// newEngine creates a DiskStorageEngine that always considers itself leader.
// Phase 4 dropped the flock parameter — single-writer enforcement is now
// BrokerCoordinator.Owns + epoch-prefixed segment filenames.
func newEngine(t *testing.T, dir string) *storage.DiskStorageEngine {
	t.Helper()
	e, err := storage.NewDiskStorageEngine(
		dir,
		&stubLeaseManager{leader: true},
		storage.DefaultConfig(),
	)
	if err != nil {
		t.Fatalf("NewDiskStorageEngine: %v", err)
	}
	return e
}

// makeBatch encodes a RecordBatch with numRecords single-byte records starting at baseOffset.
func makeBatch(baseOffset int64, numRecords int) []byte {
	batch := &recordbatch.RecordBatch{
		BaseOffset:      baseOffset,
		BaseTimestamp:   time.Now().UnixMilli(),
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

// --- tests ---

func TestProduceConsumeRoundTrip(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	engine := newEngine(t, dir)

	if err := engine.CreatePartition("test-topic", 0); err != nil {
		t.Fatalf("CreatePartition: %v", err)
	}
	if _, err := engine.TakeOver(ctx, "test-topic", 0, 1); err != nil {
		t.Fatalf("TakeOver: %v", err)
	}

	var allBatches [][]byte
	for i := 0; i < 100; i++ {
		batch := makeBatch(int64(i*10), 10)
		allBatches = append(allBatches, batch)
		base, err := engine.Append(ctx, "test-topic", 0, 0, batch)
		if err != nil {
			t.Fatalf("Append batch %d: %v", i, err)
		}
		if base != int64(i*10) {
			t.Errorf("batch %d: baseOffset=%d, want %d", i, base, i*10)
		}
	}

	hwm, err := engine.HighWatermark("test-topic", 0)
	if err != nil {
		t.Fatalf("HighWatermark: %v", err)
	}
	if hwm != 1000 {
		t.Errorf("HighWatermark=%d, want 1000", hwm)
	}

	raw, err := engine.Read(ctx, "test-topic", 0, 0, 64*1024*1024)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	var want []byte
	for _, b := range allBatches {
		want = append(want, b...)
	}
	if !bytes.Equal(raw, want) {
		t.Errorf("Read: got %d bytes, want %d bytes", len(raw), len(want))
	}

	// Reopen to verify persistence. Manifest's HWM at last takeover
	// is 0 (no event between TakeOver and Appends since #80 dropped
	// per-Produce manifest writes); a TakeOver after reopen runs
	// recoverSegment which scans the durable log and lifts HWM.
	engine2 := newEngine(t, dir)
	if err := engine2.CreatePartition("test-topic", 0); err != nil {
		t.Fatalf("reopen CreatePartition: %v", err)
	}
	if _, err := engine2.TakeOver(ctx, "test-topic", 0, 2); err != nil {
		t.Fatalf("reopen TakeOver: %v", err)
	}

	hwm2, err := engine2.HighWatermark("test-topic", 0)
	if err != nil {
		t.Fatalf("reopen HighWatermark: %v", err)
	}
	if hwm2 != 1000 {
		t.Errorf("reopen HighWatermark=%d, want 1000", hwm2)
	}

	raw2, err := engine2.Read(ctx, "test-topic", 0, 0, 64*1024*1024)
	if err != nil {
		t.Fatalf("reopen Read: %v", err)
	}
	if !bytes.Equal(raw2, want) {
		t.Errorf("reopen Read: bytes mismatch after restart")
	}
}

func TestReadFromOffset(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	engine := newEngine(t, dir)

	if err := engine.CreatePartition("topic", 0); err != nil {
		t.Fatal(err)
	}

	b0 := makeBatch(0, 10)
	b1 := makeBatch(10, 10)
	b2 := makeBatch(20, 10)
	for _, b := range [][]byte{b0, b1, b2} {
		if _, err := engine.Append(ctx, "topic", 0, 0, b); err != nil {
			t.Fatal(err)
		}
	}

	// Read starting at offset 10 — must not include b0.
	raw, err := engine.Read(ctx, "topic", 0, 10, 64*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	var want []byte
	want = append(want, b1...)
	want = append(want, b2...)
	if !bytes.Equal(raw, want) {
		t.Errorf("ReadFromOffset: got %d bytes, want %d bytes", len(raw), len(want))
	}
}

func TestRecoveryAfterPartialWrite(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	engine := newEngine(t, dir)

	if err := engine.CreatePartition("topic", 0); err != nil {
		t.Fatal(err)
	}

	b0 := makeBatch(0, 5)
	b1 := makeBatch(5, 5)
	if _, err := engine.Append(ctx, "topic", 0, 0, b0); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Append(ctx, "topic", 0, 0, b1); err != nil {
		t.Fatal(err)
	}

	// Corrupt the log by truncating it mid-way through b1. The file name
	// includes a v3.3 epoch prefix (`{epoch:08x}-{base_offset:020d}.log`)
	// so glob for it rather than hardcoding the legacy name.
	matches, err := filepath.Glob(dir + "/topic/0/*.log")
	if err != nil || len(matches) == 0 {
		t.Fatalf("locate log file: %v matches=%v", err, matches)
	}
	logPath := matches[0]
	f, err := os.OpenFile(logPath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("open log for truncation: %v", err)
	}
	// Truncate at len(b0) + 5 bytes into b1 (partial batch).
	if err := f.Truncate(int64(len(b0)) + 5); err != nil {
		_ = f.Close()
		t.Fatalf("truncate: %v", err)
	}
	_ = f.Close()

	// Reopen and recover.
	engine2 := newEngine(t, dir)
	if err := engine2.CreatePartition("topic", 0); err != nil {
		t.Fatal(err)
	}
	if err := engine2.TakeoverPartition("topic", 0, 1); err != nil {
		t.Fatalf("TakeoverPartition: %v", err)
	}

	hwm, err := engine2.HighWatermark("topic", 0)
	if err != nil {
		t.Fatal(err)
	}
	if hwm != 5 {
		t.Errorf("after recovery: hwm=%d, want 5", hwm)
	}

	// The intact batch must still be readable.
	raw, err := engine2.Read(ctx, "topic", 0, 0, 64*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, b0) {
		t.Errorf("after recovery: read bytes mismatch")
	}
}

func TestWatcherCallbackFires(t *testing.T) {
	dir := t.TempDir()
	aclsPath := dir + "/acls.json"
	credsPath := dir + "/credentials.json"

	// Create the files so fsnotify can watch them.
	for _, p := range []string{aclsPath, credsPath} {
		if err := os.WriteFile(p, []byte("{}"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	fired := make(chan string, 4)
	w := storage.NewClusterFileWatcher(aclsPath, credsPath,
		func(p string) { fired <- "acl" },
		func(p string) { fired <- "cred" },
	)

	done := make(chan struct{})
	defer close(done)
	go func() { _ = w.Run(done) }()

	time.Sleep(50 * time.Millisecond) // let watcher start

	if err := os.WriteFile(aclsPath, []byte(`{"acls":[]}`), 0644); err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-fired:
		if got != "acl" {
			t.Errorf("unexpected callback: %s", got)
		}
	case <-time.After(500 * time.Millisecond):
		t.Error("watcher callback did not fire within 500ms")
	}
}

// TestSegmentRollAndRead verifies that reads work correctly across segment boundaries.
func TestSegmentRollAndRead(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// Use a tiny segment size so we roll frequently.
	cfg := storage.DefaultConfig()
	cfg.SegmentBytes = 512

	engine, err := storage.NewDiskStorageEngine(dir, &stubLeaseManager{leader: true}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.CreatePartition("topic", 0); err != nil {
		t.Fatal(err)
	}

	var allBatches [][]byte
	for i := 0; i < 20; i++ {
		b := makeBatch(int64(i*5), 5)
		allBatches = append(allBatches, b)
		if _, err := engine.Append(ctx, "topic", 0, 0, b); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	// Read all from offset 0.
	raw, err := engine.Read(ctx, "topic", 0, 0, 64*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	var want []byte
	for _, b := range allBatches {
		want = append(want, b...)
	}
	if !bytes.Equal(raw, want) {
		t.Errorf("cross-segment read: got %d bytes, want %d bytes", len(raw), len(want))
	}
}

// TestPartitionSizeReflectsAppends verifies the engine reports increasing
// disk usage as records are appended. Backs the DescribeLogDirs handler.
func TestPartitionSizeReflectsAppends(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	engine := newEngine(t, dir)
	if err := engine.CreatePartition("topic", 0); err != nil {
		t.Fatal(err)
	}

	before := engine.PartitionSize("topic", 0)
	if _, err := engine.Append(ctx, "topic", 0, 0, makeBatch(0, 10)); err != nil {
		t.Fatal(err)
	}
	after := engine.PartitionSize("topic", 0)
	if after <= before {
		t.Errorf("size did not grow after Append: before=%d after=%d", before, after)
	}

	if got := engine.PartitionSize("missing", 0); got != 0 {
		t.Errorf("unknown partition: got %d, want 0", got)
	}

	if engine.DataDir() != dir {
		t.Errorf("DataDir=%q, want %q", engine.DataDir(), dir)
	}
}

// TestAppendAssignsOffsets verifies the engine rewrites the producer-supplied
// baseOffset (always 0 on the wire) to the partition's high watermark, so
// offsets advance monotonically across batches.
func TestAppendAssignsOffsets(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	engine := newEngine(t, dir)
	if err := engine.CreatePartition("topic", 0); err != nil {
		t.Fatal(err)
	}

	// 1 + 1 + 3 records → expected baseOffsets 0, 1, 2; final HWM 5.
	wantOffsets := []int64{0, 1, 2}
	recordCounts := []int{1, 1, 3}
	for i, n := range recordCounts {
		// Caller always sends baseOffset=0, like a real producer.
		base, err := engine.Append(ctx, "topic", 0, 0, makeBatch(0, n))
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		if base != wantOffsets[i] {
			t.Errorf("Append %d: base=%d, want %d", i, base, wantOffsets[i])
		}
	}

	hwm, err := engine.HighWatermark("topic", 0)
	if err != nil {
		t.Fatal(err)
	}
	if hwm != 5 {
		t.Errorf("HighWatermark=%d, want 5", hwm)
	}

	raw, err := engine.Read(ctx, "topic", 0, 0, 64*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	// Walk the returned batches and confirm baseOffsets are 0, 1, 4.
	var got []int64
	for pos := 0; pos < len(raw); {
		if pos+12 > len(raw) {
			t.Fatalf("truncated batch header at pos %d", pos)
		}
		base := int64(binary.BigEndian.Uint64(raw[pos : pos+8]))
		batchLen := int32(binary.BigEndian.Uint32(raw[pos+8 : pos+12]))
		got = append(got, base)
		pos += 12 + int(batchLen)
	}
	if len(got) != len(wantOffsets) {
		t.Fatalf("got %d batches, want %d", len(got), len(wantOffsets))
	}
	for i, w := range wantOffsets {
		if got[i] != w {
			t.Errorf("batch %d baseOffset on disk: got %d, want %d", i, got[i], w)
		}
	}
}

// TestAppendEpochFenceRejectsStaleCaller verifies the data-plane half of
// the v3.3 epoch fence: once TakeOver(epoch=N) has run, an Append(epoch=K)
// with K < N must return ErrEpochMismatch instead of silently accepting
// the write. Both K=0 (v2.6 path) and K==N continue to succeed — the fence
// activates only when both sides report a real epoch and they disagree.
func TestAppendEpochFenceRejectsStaleCaller(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	engine := newEngine(t, dir)
	if err := engine.CreatePartition("topic", 0); err != nil {
		t.Fatalf("CreatePartition: %v", err)
	}

	// Pre-fence: epoch=0 caller works fine (backwards compat).
	if _, err := engine.Append(ctx, "topic", 0, 0, makeBatch(0, 1)); err != nil {
		t.Fatalf("Append(epoch=0) before TakeOver: %v", err)
	}

	// Take over at epoch=5 — partition's stored epoch is now 5.
	if _, err := engine.TakeOver(ctx, "topic", 0, 5); err != nil {
		t.Fatalf("TakeOver: %v", err)
	}

	// Caller at the matching epoch succeeds.
	if _, err := engine.Append(ctx, "topic", 0, 5, makeBatch(0, 1)); err != nil {
		t.Errorf("Append(epoch=5) after TakeOver(5): unexpected err %v", err)
	}

	// Caller at a stale epoch is rejected.
	if _, err := engine.Append(ctx, "topic", 0, 4, makeBatch(0, 1)); !errors.Is(err, storage.ErrEpochMismatch) {
		t.Errorf("Append(epoch=4) after TakeOver(5): want ErrEpochMismatch, got %v", err)
	}

	// Caller running ahead is also rejected (strict equality — plan §"Append flow").
	if _, err := engine.Append(ctx, "topic", 0, 6, makeBatch(0, 1)); !errors.Is(err, storage.ErrEpochMismatch) {
		t.Errorf("Append(epoch=6) after TakeOver(5): want ErrEpochMismatch, got %v", err)
	}

	// epoch=0 still works as the v2.6 compat sentinel.
	if _, err := engine.Append(ctx, "topic", 0, 0, makeBatch(0, 1)); err != nil {
		t.Errorf("Append(epoch=0) after TakeOver: should still work for v2.6 callers; got %v", err)
	}
}

// TestFlushPolicyDataSurvivesCrash verifies the flush policy: with
// FlushIntervalMessages=1 (default), the *log* is fsynced per record so
// the data itself survives a crash. The MANIFEST, post-#80, is not
// rewritten per Produce — it's only persisted on slow-changing events
// (segment roll, takeover, cleaner advance, partition open). On
// recovery, takeoverInternal calls recoverSegment which CRC-walks the
// log and lifts the in-memory HWM back to its real value. So the test
// asserts: a fresh engine reopened against the same dir + a takeover
// arrives at HWM=5, regardless of whether the manifest happens to
// reflect that on disk.
func TestFlushPolicyDataSurvivesCrash(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	cfg := storage.DefaultConfig()
	if cfg.FlushIntervalMessages != 1 {
		t.Fatalf("DefaultConfig.FlushIntervalMessages=%d, want 1", cfg.FlushIntervalMessages)
	}
	e, err := storage.NewDiskStorageEngine(
		dir,
		&stubLeaseManager{leader: true},
		cfg,
	)
	if err != nil {
		t.Fatalf("NewDiskStorageEngine: %v", err)
	}
	if err := e.CreatePartition("topic", 0); err != nil {
		t.Fatalf("CreatePartition: %v", err)
	}

	// Single Append, then drop the engine without a clean close —
	// simulates the broker process being SIGKILL'd between batches.
	if _, err := e.Append(ctx, "topic", 0, 0, makeBatch(0, 5)); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Recovery: open a fresh engine, run takeoverInternal which
	// scans the active segment and rebuilds HWM from the durable log.
	// Bumped epoch (1) ensures recoverSegment runs.
	e2, err := storage.NewDiskStorageEngine(
		dir,
		&stubLeaseManager{leader: true},
		cfg,
	)
	if err != nil {
		t.Fatalf("reopen NewDiskStorageEngine: %v", err)
	}
	hwm, err := e2.TakeOver(ctx, "topic", 0, 1)
	if err != nil {
		t.Fatalf("TakeOver: %v", err)
	}
	if hwm != 5 {
		t.Errorf("recovered HWM=%d after Append+crash+TakeOver, want 5", hwm)
	}
}

// TestFlushPolicyDisabledDelaysSync verifies that
// FlushIntervalMessages=0 disables message-driven flushing — manifest only
// gets refreshed at segment roll. Useful guardrail so the config knob isn't
// silently ignored.
func TestFlushPolicyDisabledDelaysSync(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	cfg := storage.DefaultConfig()
	cfg.FlushIntervalMessages = 0 // disable
	e, err := storage.NewDiskStorageEngine(
		dir,
		&stubLeaseManager{leader: true},
		cfg,
	)
	if err != nil {
		t.Fatalf("NewDiskStorageEngine: %v", err)
	}
	if err := e.CreatePartition("topic", 0); err != nil {
		t.Fatalf("CreatePartition: %v", err)
	}

	// First Append creates the manifest at openPartition with HWM=0.
	// With flushing disabled, subsequent Appends do NOT update the manifest
	// until the segment rolls — so the on-disk HWM stays at 0.
	if _, err := e.Append(ctx, "topic", 0, 0, makeBatch(0, 3)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	data, err := os.ReadFile(dir + "/topic/0/manifest.json")
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if !bytes.Contains(data, []byte(`"highWatermark":0`)) {
		t.Errorf("manifest should still show HWM=0 with FlushIntervalMessages=0; got: %s", data)
	}
}

// TestManifestPersistedAcrossRestart verifies the v3.3 per-partition
// manifest is written on first open + on roll/takeover/cleaner, and is
// consulted on reopen as a fast path (no full segment scan). After #80
// the manifest no longer updates per-Produce — its HWM reflects the
// last controller event (takeover / segment roll / cleaner). On reopen
// the manifest's HWM is therefore "what the controller knew at last
// event", and it's the takeover-after-restart that lifts HWM via
// recoverSegment to whatever the durable log actually contains.
//
// This test pins both behaviours in sequence: post-restart fast path
// reports the manifest's HWM, then a TakeOver call recovers the true
// HWM via the segment scan.
func TestManifestPersistedAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	engine := newEngine(t, dir)

	if err := engine.CreatePartition("topic", 0); err != nil {
		t.Fatalf("CreatePartition: %v", err)
	}
	if _, err := engine.TakeOver(ctx, "topic", 0, 1); err != nil {
		t.Fatalf("TakeOver: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := engine.Append(ctx, "topic", 0, 1, makeBatch(int64(i*10), 10)); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	// Manifest should now exist on disk for the partition.
	manifestPath := dir + "/topic/0/manifest.json"
	if _, err := os.Stat(manifestPath); err != nil {
		t.Fatalf("manifest.json missing after appends: %v", err)
	}

	// Reopen — the manifest fast path consults manifest's HWM directly
	// (no segment scan), but that HWM only reflects state at the last
	// takeover/roll/cleaner. With no event between takeover and the
	// 5 appends, the on-disk HWM is 0.
	engine2 := newEngine(t, dir)
	if err := engine2.CreatePartition("topic", 0); err != nil {
		t.Fatalf("reopen CreatePartition: %v", err)
	}
	hwm2, err := engine2.HighWatermark("topic", 0)
	if err != nil {
		t.Fatalf("reopen HighWatermark: %v", err)
	}
	if hwm2 != 0 {
		t.Errorf("reopen HWM=%d, want 0 (manifest at last takeover; fast path doesn't scan)", hwm2)
	}

	// A TakeOver after restart triggers recoverSegment, which scans
	// the active log and lifts HWM to its real value.
	hwmAfterTakeover, err := engine2.TakeOver(ctx, "topic", 0, 2)
	if err != nil {
		t.Fatalf("TakeOver after restart: %v", err)
	}
	if hwmAfterTakeover != 50 {
		t.Errorf("HWM after takeover=%d, want 50 (recoverSegment didn't lift HWM)", hwmAfterTakeover)
	}
}

// TestTakeOverWritesManifestEpoch verifies TakeOver(epoch) persists the new
// epoch via manifest.json so a subsequent reopen sees it without scanning.
func TestTakeOverWritesManifestEpoch(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	engine := newEngine(t, dir)
	if err := engine.CreatePartition("topic", 0); err != nil {
		t.Fatalf("CreatePartition: %v", err)
	}
	if _, err := engine.Append(ctx, "topic", 0, 0, makeBatch(0, 3)); err != nil {
		t.Fatalf("Append: %v", err)
	}

	if _, err := engine.TakeOver(ctx, "topic", 0, 7); err != nil {
		t.Fatalf("TakeOver: %v", err)
	}

	// Read manifest directly off disk and confirm the epoch landed.
	data, err := os.ReadFile(dir + "/topic/0/manifest.json")
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if !bytes.Contains(data, []byte(`"epoch":7`)) {
		t.Errorf("manifest does not record epoch=7: %s", data)
	}

	// And the legacy .leader-epoch file should have been cleaned up.
	if _, err := os.Stat(dir + "/topic/0/.leader-epoch"); !os.IsNotExist(err) {
		t.Errorf("legacy .leader-epoch still present after TakeOver: err=%v", err)
	}
}

// batchTotalSizeCheck is a white-box sanity check: the wire size must match what
// EncodeRecordBatch produces.
func TestBatchHeaderParsing(t *testing.T) {
	raw := makeBatch(42, 7)
	if len(raw) < 27 {
		t.Fatalf("batch too short: %d", len(raw))
	}

	baseOffset := int64(binary.BigEndian.Uint64(raw[0:8]))
	lastOffsetDelta := int32(binary.BigEndian.Uint32(raw[23:27]))
	batchLength := int32(binary.BigEndian.Uint32(raw[8:12]))
	totalSize := 12 + int(batchLength)

	if baseOffset != 42 {
		t.Errorf("baseOffset=%d, want 42", baseOffset)
	}
	if lastOffsetDelta != 6 {
		t.Errorf("lastOffsetDelta=%d, want 6", lastOffsetDelta)
	}
	if totalSize != len(raw) {
		t.Errorf("totalSize=%d, but len(raw)=%d", totalSize, len(raw))
	}
}
