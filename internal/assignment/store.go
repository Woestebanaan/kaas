// Package assignment is the file-backed implementation of
// kafkaapi.AssignmentStore — the persistence layer for the cluster's
// authoritative partition assignment under v3.3.
//
// Layout on the shared PVC:
//
//	/data/__cluster/
//	    assignment.json          ← authoritative cluster state
//	    assignment.json.tmp      ← staging file during writes (never read)
//
// Single writer (the elected controller). Many readers (every broker).
// All access goes through FileStore so the orphan-.tmp cleanup, atomic
// rename, and fsnotify+polling watch are written once.
package assignment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/woestebanaan/skafka/internal/fsutil"
	"github.com/woestebanaan/skafka/pkg/kafkaapi"
)

const (
	clusterDirName     = "__cluster"
	assignmentFilename = "assignment.json"
	tmpSuffix          = ".tmp"
)

// FileStore implements kafkaapi.AssignmentStore over a shared filesystem.
// dataDir is the broker's /data root; the actual file lives under
// {dataDir}/__cluster/assignment.json.
type FileStore struct {
	dataDir string

	// pollInterval drives the mtime-polling safety net (default 1s). 0 disables
	// polling — fsnotify becomes the only signal, useful for fast-path tests.
	pollInterval time.Duration
	// fullReadInterval forces a re-read regardless of mtime, defending against
	// NFS attribute caching and second-resolution mtime (default 30s).
	fullReadInterval time.Duration
}

// NewFileStore returns a FileStore rooted at dataDir. The __cluster directory
// is created on demand by the first Write.
func NewFileStore(dataDir string) *FileStore {
	return &FileStore{
		dataDir:          dataDir,
		pollInterval:     1 * time.Second,
		fullReadInterval: 30 * time.Second,
	}
}

// WithPollInterval overrides the mtime poll interval. Pass 0 to disable
// polling and rely on fsnotify alone (used in tests).
func (s *FileStore) WithPollInterval(d time.Duration) *FileStore {
	s.pollInterval = d
	return s
}

// path returns the full filesystem path for the assignment file.
func (s *FileStore) path() string {
	return filepath.Join(s.dataDir, clusterDirName, assignmentFilename)
}

// tmpPath returns the path of the staging file used for atomic writes.
func (s *FileStore) tmpPath() string {
	return s.path() + tmpSuffix
}

// dir returns the cluster-state directory, creating it on demand. Callers
// that only intend to read should not call dir() (it would mask a
// missing-data-dir misconfiguration as a successful zero state).
func (s *FileStore) dir() string {
	return filepath.Join(s.dataDir, clusterDirName)
}

// Read loads the current assignment from disk. Returns fs.ErrNotExist
// (wrapped) when the file is missing — controller bootstrap and brokers
// joining a fresh cluster both have to handle that as "no assignment yet".
//
// Read does NOT touch the .tmp staging file: a concurrent Write may be in
// the middle of using it, and removing a live tmp would torpedo the rename.
// Orphan-tmp cleanup is a controller-bootstrap concern; see CleanupOrphanTmp.
func (s *FileStore) Read(_ context.Context) (*kafkaapi.Assignment, error) {
	data, err := os.ReadFile(s.path())
	if err != nil {
		return nil, err
	}
	var a kafkaapi.Assignment
	if err := json.Unmarshal(data, &a); err != nil {
		return nil, fmt.Errorf("assignment: parse %s: %w", s.path(), err)
	}
	return &a, nil
}

// CleanupOrphanTmp removes a stale assignment.json.tmp left by a crashed
// writer. Safe to call only when no writer is active — i.e., on controller
// startup before the controller begins issuing Write calls. Best-effort:
// missing file or removal failure is not an error worth surfacing because
// the next successful Write will clobber whatever is there.
func (s *FileStore) CleanupOrphanTmp() {
	if _, err := os.Stat(s.tmpPath()); err == nil {
		_ = os.Remove(s.tmpPath())
	}
}

// Write replaces the current assignment atomically: write to tmp, fsync,
// rename. NFSv4 guarantees same-directory rename atomicity, so a reader
// either sees the previous version or the new version — never a torn file.
//
// Caller is responsible for setting Assignment.ControllerEpoch (the writer's
// leaseTransitions value) and AssignmentVersion (controller-local monotonic).
// Callers also typically set GeneratedAt and Controller.
func (s *FileStore) Write(_ context.Context, a *kafkaapi.Assignment) error {
	if a == nil {
		return errors.New("assignment: nil Assignment")
	}
	if err := os.MkdirAll(s.dir(), 0755); err != nil {
		return err
	}

	data, err := json.Marshal(a)
	if err != nil {
		return err
	}

	tmp := s.tmpPath()
	final := s.path()

	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

// Watch returns a channel that fires whenever the assignment file changes.
// The merged fsnotify + 1s mtime poll + 30s full-fire loop lives in
// internal/fsutil; this method just adapts the per-file callback into a
// non-blocking channel send so rapid bursts of changes deliver a single
// tick (consumers re-read via Read()).
//
// The goroutine exits when ctx is cancelled.
func (s *FileStore) Watch(ctx context.Context) (<-chan struct{}, error) {
	out := make(chan struct{}, 1)
	notify := func() {
		select {
		case out <- struct{}{}:
		default:
		}
	}

	w := fsutil.New([]fsutil.FileSpec{
		{Path: s.path(), OnChange: notify},
	}).
		WithPollInterval(s.pollInterval).
		WithFullReadInterval(s.fullReadInterval)

	go func() { _ = w.Run(ctx) }()
	return out, nil
}

// IsNotExist is a small convenience for callers that need to distinguish
// "no assignment yet" from a real I/O error after Read.
func IsNotExist(err error) bool {
	return errors.Is(err, fs.ErrNotExist)
}

// Compile-time assertion: FileStore satisfies the kafkaapi.AssignmentStore
// contract that Phase 1 defined.
var _ kafkaapi.AssignmentStore = (*FileStore)(nil)

