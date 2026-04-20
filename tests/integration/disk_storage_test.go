package integration

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"testing"
	"time"

	"github.com/woestebanaan/skafka/internal/lease"
	"github.com/woestebanaan/skafka/internal/lock"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/storage"
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

type stubLock struct{ locked bool }

func (s *stubLock) Lock(_ string, _ int32) error    { return nil }
func (s *stubLock) Unlock(_ string, _ int32) error  { return nil }
func (s *stubLock) IsLocked(_ string, _ int32) bool { return s.locked }

// compile-time interface checks
var _ lease.LeaseManager = (*stubLeaseManager)(nil)
var _ lock.PartitionLock = (*stubLock)(nil)

// newEngine creates a DiskStorageEngine that always considers itself leader+locked.
func newEngine(t *testing.T, dir string) *storage.DiskStorageEngine {
	t.Helper()
	e, err := storage.NewDiskStorageEngine(
		dir,
		&stubLeaseManager{leader: true},
		&stubLock{locked: true},
		storage.DefaultConfig(),
	)
	if err != nil {
		t.Fatalf("NewDiskStorageEngine: %v", err)
	}
	return e
}

// makeBatch encodes a RecordBatch with numRecords single-byte records starting at baseOffset.
func makeBatch(baseOffset int64, numRecords int) []byte {
	batch := &codec.RecordBatch{
		BaseOffset:      baseOffset,
		BaseTimestamp:   time.Now().UnixMilli(),
		LastOffsetDelta: int32(numRecords - 1),
		ProducerID:      -1,
		ProducerEpoch:   -1,
		BaseSequence:    -1,
	}
	for i := 0; i < numRecords; i++ {
		batch.Records = append(batch.Records, codec.Record{
			OffsetDelta: int32(i),
			Value:       []byte{byte(i)},
		})
	}
	return codec.EncodeRecordBatch(nil, batch)
}

// --- tests ---

func TestProduceConsumeRoundTrip(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	engine := newEngine(t, dir)

	if err := engine.CreatePartition("test-topic", 0); err != nil {
		t.Fatalf("CreatePartition: %v", err)
	}

	var allBatches [][]byte
	for i := 0; i < 100; i++ {
		batch := makeBatch(int64(i*10), 10)
		allBatches = append(allBatches, batch)
		base, err := engine.Append(ctx, "test-topic", 0, batch)
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

	// Reopen to verify persistence.
	engine2 := newEngine(t, dir)
	if err := engine2.CreatePartition("test-topic", 0); err != nil {
		t.Fatalf("reopen CreatePartition: %v", err)
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
		if _, err := engine.Append(ctx, "topic", 0, b); err != nil {
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

func TestTwoLockEnforcement(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	e, err := storage.NewDiskStorageEngine(
		dir,
		&stubLeaseManager{leader: true},
		&stubLock{locked: false}, // lock not held
		storage.DefaultConfig(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.CreatePartition("topic", 0); err != nil {
		t.Fatal(err)
	}

	_, err = e.Append(ctx, "topic", 0, makeBatch(0, 1))
	if err != storage.ErrLockNotHeld {
		t.Errorf("expected ErrLockNotHeld, got %v", err)
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
	if _, err := engine.Append(ctx, "topic", 0, b0); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Append(ctx, "topic", 0, b1); err != nil {
		t.Fatal(err)
	}

	// Corrupt the log by truncating it mid-way through b1.
	logPath := dir + "/topic/0/00000000000000000000.log"
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

	engine, err := storage.NewDiskStorageEngine(dir, &stubLeaseManager{leader: true}, &stubLock{locked: true}, cfg)
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
		if _, err := engine.Append(ctx, "topic", 0, b); err != nil {
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
