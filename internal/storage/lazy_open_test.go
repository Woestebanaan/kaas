package storage

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/woestebanaan/skafka/tests/testutil/recordbatch"
)

// TestRelinquishClosesFileHandles guards the lazy-open / NFS-silly-
// rename fix: after Relinquish, the active segment's file handles
// must be released, so a peer broker that subsequently runs
// DeleteRecords (or segment-roll-driven os.Remove) can actually free
// the bytes on NFS.
//
// We can't directly observe "fds closed" from inside the engine, but
// we can assert the *activeSegment fields are nil — the contract
// closeHandles enforces.
func TestRelinquishClosesFileHandles(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.FlushIntervalMessages = 1

	e, err := NewDiskStorageEngine(dir, &neverLeaderLeases{}, cfg)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	if err := e.CreatePartition("t", 0); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := e.TakeOver(context.Background(), "t", 0, 1); err != nil {
		t.Fatalf("takeover: %v", err)
	}

	// After TakeOver, handles are open.
	ps, _ := e.getPartition("t", 0)
	ps.mu.Lock()
	if ps.active.logFile == nil || ps.active.indexFile == nil {
		ps.mu.Unlock()
		t.Fatal("expected file handles open after TakeOver")
	}
	ps.mu.Unlock()

	// Relinquish should close them.
	if err := e.Relinquish("t", 0); err != nil {
		t.Fatalf("Relinquish: %v", err)
	}
	ps.mu.Lock()
	if ps.active.logFile != nil || ps.active.indexFile != nil {
		ps.mu.Unlock()
		t.Errorf("file handles still open after Relinquish: log=%v index=%v",
			ps.active.logFile, ps.active.indexFile)
	}
	ps.mu.Unlock()

	// Idempotent: a second Relinquish is a no-op.
	if err := e.Relinquish("t", 0); err != nil {
		t.Errorf("second Relinquish: %v", err)
	}

	// TakeOver after Relinquish must re-open handles.
	if _, err := e.TakeOver(context.Background(), "t", 0, 2); err != nil {
		t.Fatalf("re-takeover: %v", err)
	}
	ps.mu.Lock()
	if ps.active.logFile == nil || ps.active.indexFile == nil {
		ps.mu.Unlock()
		t.Errorf("file handles not re-opened after TakeOver: log=%v index=%v",
			ps.active.logFile, ps.active.indexFile)
	}
	ps.mu.Unlock()
}

// TestStartupOpensWithoutFileHandles guards the lazy-open contract:
// loadExisting / openPartition must NOT open log/index file handles.
// A new engine created against an existing data dir should leave the
// active segment with nil file handles, so non-leader brokers don't
// hold fds that block a peer's segment unlinks.
func TestStartupOpensWithoutFileHandles(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.FlushIntervalMessages = 1

	// Set up a partition with one batch and close cleanly.
	e1, err := NewDiskStorageEngine(dir, &neverLeaderLeases{}, cfg)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	if err := e1.CreatePartition("t", 0); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := e1.TakeOver(context.Background(), "t", 0, 1); err != nil {
		t.Fatalf("takeover: %v", err)
	}
	batch := &recordbatch.RecordBatch{
		BaseOffset: 0, LastOffsetDelta: 0,
		ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1,
		Records:    []recordbatch.Record{{OffsetDelta: 0, Value: []byte("x")}},
	}
	if _, err := e1.Append(context.Background(), "t", 0, 1, -1, recordbatch.Encode(nil, batch)); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := e1.ClosePartition("t", 0); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen via NewDiskStorageEngine — this is the "broker startup"
	// path. Without a TakeOver, the broker is a follower; handles
	// must stay closed.
	e2, err := NewDiskStorageEngine(dir, &neverLeaderLeases{}, cfg)
	if err != nil {
		t.Fatalf("reopen engine: %v", err)
	}
	ps, ok := e2.getPartition("t", 0)
	if !ok {
		t.Fatal("partition not loaded on reopen")
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if ps.active == nil {
		t.Fatal("active segment missing after reopen")
	}
	if ps.active.logFile != nil {
		t.Errorf("log file handle open without TakeOver — followers should not hold fds: %v", ps.active.logFile)
	}
	if ps.active.indexFile != nil {
		t.Errorf("index file handle open without TakeOver — followers should not hold fds: %v", ps.active.indexFile)
	}
	// logSize should still be populated from stat — needed for size-
	// based retention checks even on followers.
	if ps.active.logSize <= 0 {
		t.Errorf("logSize=%d, expected stat to populate it without opening fds", ps.active.logSize)
	}
}

// listSegmentsHelper is a tiny convenience used by other roll/delete
// tests in this package; copied here only because the existing helper
// name overlap with segment.go's listSegments confused me when
// drafting this file. Inline to keep the test self-contained.
func listLogFiles(t *testing.T, partitionDir string) []string {
	t.Helper()
	out := []string{}
	entries, err := filepath.Glob(filepath.Join(partitionDir, "*.log"))
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range entries {
		out = append(out, filepath.Base(p))
	}
	return out
}

// TestFollowerDoesNotBlockLeaderUnlink simulates the production
// silly-rename scenario: two engines pointing at the same data dir
// (analogous to two brokers sharing an NFS PVC). The "leader" engine
// runs DeleteRecords and unlinks the active segment; the "follower"
// engine — which has not been TakeOver'd — must not hold any fd that
// would silly-rename the file.
//
// We assert this by listing the partition directory after the unlink
// and checking that no .log file remains. With the pre-fix behaviour
// the unlink would succeed at the inode level but silly-rename would
// leave a .nfsXXXX file in place; on a local filesystem (this test's
// environment) silly-rename doesn't apply, so the test instead
// asserts that the follower's file handles never opened — which is
// the upstream cause of silly-rename on NFS.
func TestFollowerDoesNotBlockLeaderUnlink(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.FlushIntervalMessages = 1

	// "Leader" — TakeOvers, appends, then DeleteRecords.
	leader, err := NewDiskStorageEngine(dir, &neverLeaderLeases{}, cfg)
	if err != nil {
		t.Fatalf("leader engine: %v", err)
	}
	if err := leader.CreatePartition("t", 0); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := leader.TakeOver(context.Background(), "t", 0, 1); err != nil {
		t.Fatalf("leader takeover: %v", err)
	}
	for i := 0; i < 5; i++ {
		batch := &recordbatch.RecordBatch{
			BaseOffset: 0, LastOffsetDelta: 0,
			ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1,
			Records: []recordbatch.Record{{
				OffsetDelta: 0, Value: make([]byte, 1024),
			}},
		}
		if _, err := leader.Append(context.Background(), "t", 0, 1, -1, recordbatch.Encode(nil, batch)); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	// "Follower" — opens the same dir, discovers the partition via
	// loadExisting, but never takes ownership. It should NOT have
	// file handles open.
	follower, err := NewDiskStorageEngine(dir, &neverLeaderLeases{}, cfg)
	if err != nil {
		t.Fatalf("follower engine: %v", err)
	}
	fps, ok := follower.getPartition("t", 0)
	if !ok {
		t.Fatal("follower didn't discover partition")
	}
	fps.mu.Lock()
	hadFds := fps.active == nil || fps.active.logFile != nil || fps.active.indexFile != nil
	logFile := fps.active.logFile
	idxFile := fps.active.indexFile
	fps.mu.Unlock()
	if hadFds {
		t.Errorf("follower has fds open — would block leader's unlink on NFS: log=%v index=%v",
			logFile, idxFile)
	}

	// Leader purges + reclaims active. The unlink targets the file
	// the follower can see on disk; with no follower fds, NFS
	// (production) doesn't silly-rename and the bytes are freed.
	if _, err := leader.DeleteRecords("t", 0, -1); err != nil {
		t.Fatalf("DeleteRecords: %v", err)
	}

	// On local fs we just assert the file is unlinked. (On NFS this
	// only holds if the follower has no handles — which we asserted
	// above.)
	pdir := filepath.Join(dir, "t", "0")
	files := listLogFiles(t, pdir)
	if len(files) > 1 {
		t.Errorf("expected ≤1 .log file post-purge (the new empty active), got %d: %v",
			len(files), files)
	}
	for _, f := range files {
		// New active segment starts at the post-purge HWM; old segment
		// (baseOffset 0) must be gone.
		if strings.HasPrefix(f, "00000001-00000000000000000000.log") {
			t.Errorf("old active segment file %q still present after purge", f)
		}
	}
}
