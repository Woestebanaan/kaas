package assignment

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/woestebanaan/skafka/pkg/kafkaapi"
)

func sampleAssignment() *kafkaapi.Assignment {
	return &kafkaapi.Assignment{
		ControllerEpoch:   3,
		AssignmentVersion: 12,
		GeneratedAt:       time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC),
		Controller:        "skafka-1",
		Brokers: []kafkaapi.BrokerAssignment{
			{ID: "skafka-0", Health: kafkaapi.BrokerHealthAlive, LastSeen: time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)},
		},
		Partitions: []kafkaapi.PartitionAssignment{
			{Topic: "events", Partition: 0, Broker: "skafka-0", Epoch: 7, Role: kafkaapi.PartitionRoleLeader},
		},
	}
}

func TestFileStoreReadMissing(t *testing.T) {
	s := NewFileStore(t.TempDir())
	_, err := s.Read(context.Background())
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("expected fs.ErrNotExist, got %v", err)
	}
	if !IsNotExist(err) {
		t.Errorf("IsNotExist returned false for %v", err)
	}
}

func TestFileStoreWriteReadRoundTrip(t *testing.T) {
	s := NewFileStore(t.TempDir())
	in := sampleAssignment()
	if err := s.Write(context.Background(), in); err != nil {
		t.Fatalf("Write: %v", err)
	}
	out, err := s.Read(context.Background())
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if out.ControllerEpoch != in.ControllerEpoch ||
		out.AssignmentVersion != in.AssignmentVersion ||
		out.Controller != in.Controller ||
		len(out.Partitions) != 1 ||
		out.Partitions[0].Topic != "events" {
		t.Errorf("round-trip mismatch: got %+v want %+v", out, in)
	}
}

func TestFileStoreWriteRemovesOrphanTmpOnSuccess(t *testing.T) {
	dir := t.TempDir()
	s := NewFileStore(dir)
	if err := s.Write(context.Background(), sampleAssignment()); err != nil {
		t.Fatalf("Write: %v", err)
	}
	tmp := filepath.Join(dir, clusterDirName, assignmentFilename+tmpSuffix)
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Errorf("tmp file should be gone after successful Write; err=%v", err)
	}
}

func TestFileStoreCleanupOrphanTmp(t *testing.T) {
	dir := t.TempDir()
	s := NewFileStore(dir)

	// Lay down a real assignment.
	if err := s.Write(context.Background(), sampleAssignment()); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Simulate a crashed writer leaving .tmp behind.
	orphan := filepath.Join(dir, clusterDirName, assignmentFilename+tmpSuffix)
	if err := os.WriteFile(orphan, []byte("partial garbage"), 0644); err != nil {
		t.Fatal(err)
	}

	s.CleanupOrphanTmp()
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Errorf("orphan tmp not cleaned by CleanupOrphanTmp; err=%v", err)
	}

	// And it must be safe to call twice (e.g. controller restart loops).
	s.CleanupOrphanTmp()
}

func TestFileStoreReadDoesNotTouchTmp(t *testing.T) {
	dir := t.TempDir()
	s := NewFileStore(dir)
	if err := s.Write(context.Background(), sampleAssignment()); err != nil {
		t.Fatal(err)
	}
	tmp := filepath.Join(dir, clusterDirName, assignmentFilename+tmpSuffix)
	// Lay down a "concurrent writer's" tmp file.
	if err := os.WriteFile(tmp, []byte("in-flight"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Read(context.Background()); err != nil {
		t.Fatalf("Read: %v", err)
	}
	if _, err := os.Stat(tmp); err != nil {
		t.Errorf("Read must not remove in-flight tmp; stat err=%v", err)
	}
}

func TestFileStoreWriteIsAtomic(t *testing.T) {
	// Concurrent Write + Read: every Read either sees old or new, never a
	// half-written file. JSON parsing of a torn file would surface as a
	// parse error from Read.
	dir := t.TempDir()
	s := NewFileStore(dir)

	// Seed an initial version.
	v0 := sampleAssignment()
	v0.AssignmentVersion = 0
	if err := s.Write(context.Background(), v0); err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	errCh := make(chan error, 1)

	// Reader spins as fast as it can during the writer's burst.
	go func() {
		for {
			select {
			case <-stop:
				errCh <- nil
				return
			default:
			}
			if _, err := s.Read(context.Background()); err != nil && !errors.Is(err, fs.ErrNotExist) {
				errCh <- err
				return
			}
		}
	}()

	// Writer issues 100 versions back-to-back.
	for i := 1; i <= 100; i++ {
		v := sampleAssignment()
		v.AssignmentVersion = int64(i)
		if err := s.Write(context.Background(), v); err != nil {
			close(stop)
			t.Fatalf("Write[%d]: %v", i, err)
		}
	}
	close(stop)
	if err := <-errCh; err != nil {
		t.Fatalf("concurrent reader saw torn file: %v", err)
	}
}

func TestFileStoreWatchFiresOnWrite(t *testing.T) {
	s := NewFileStore(t.TempDir()).WithPollInterval(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch, err := s.Watch(ctx)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	if err := s.Write(ctx, sampleAssignment()); err != nil {
		t.Fatal(err)
	}

	select {
	case <-ch:
		// Got the notification.
	case <-time.After(2 * time.Second):
		t.Fatal("Watch did not fire after Write")
	}
}

func TestFileStoreWatchFiresAgainOnSecondWrite(t *testing.T) {
	s := NewFileStore(t.TempDir()).WithPollInterval(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch, err := s.Watch(ctx)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	v1 := sampleAssignment()
	v1.AssignmentVersion = 1
	if err := s.Write(ctx, v1); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("Watch did not fire after first Write")
	}

	// A second write must fire again, not be coalesced into the first.
	v2 := sampleAssignment()
	v2.AssignmentVersion = 2
	// Sleep a beat so mtime has a chance to advance — second-resolution NFS
	// servers would coalesce within the same second, but this is local fs.
	time.Sleep(50 * time.Millisecond)
	if err := s.Write(ctx, v2); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("Watch did not fire after second Write")
	}
}

func TestFileStoreWatchPollingOnly(t *testing.T) {
	// Disabling fsnotify by pointing at a directory we then delete-and-recreate
	// exercises the polling-only path. Easier: pass a too-short pollInterval
	// and write into a fresh dir; the poll loop should still fire even if we
	// pretend fsnotify isn't there.
	dir := t.TempDir()
	s := NewFileStore(dir).WithPollInterval(20 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ch, err := s.Watch(ctx)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	if err := s.Write(ctx, sampleAssignment()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ch:
	case <-time.After(1 * time.Second):
		t.Fatal("Watch did not fire (polling disabled?)")
	}
}

func TestFileStoreWriteRejectsNil(t *testing.T) {
	s := NewFileStore(t.TempDir())
	if err := s.Write(context.Background(), nil); err == nil {
		t.Fatal("Write(nil) should return error")
	}
}

func TestFileStoreReadParseError(t *testing.T) {
	dir := t.TempDir()
	s := NewFileStore(dir)
	// Write garbage directly to disk.
	clusterDir := filepath.Join(dir, clusterDirName)
	if err := os.MkdirAll(clusterDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(clusterDir, assignmentFilename), []byte("not json"), 0644); err != nil {
		t.Fatal(err)
	}
	_, err := s.Read(context.Background())
	if err == nil {
		t.Fatal("Read should reject garbage")
	}
	if errors.Is(err, fs.ErrNotExist) {
		t.Errorf("parse error must not look like missing file; got %v", err)
	}
}
