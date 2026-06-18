package storage

import (
	"context"
	"log/slog"
	"time"

	"github.com/woestebanaan/skafka/internal/fsutil"
)

// ClusterFileWatcher watches __cluster/acls.json and __cluster/credentials.json
// and fires debounced callbacks when either file changes.
//
// As of the Phase 3 fsutil follow-up, the watcher is a thin shim over
// internal/fsutil.FileWatcher: that's where the merged fsnotify + 1s mtime
// poll + 30s full-fire fallback lives. The shim adds a 100ms debounce so a
// burst of writes (kubectl apply -f writes the secret in pieces, the
// fsnotify watcher fires multiple times within 100ms) collapses into a
// single reload.
type ClusterFileWatcher struct {
	aclsPath        string
	credentialsPath string
	onACLReload     func(path string)
	onCredReload    func(path string)
	debounce        time.Duration
}

// NewClusterFileWatcher creates a watcher for the two cluster config files.
// onACLReload and onCredReload are called (in a goroutine) after a 100ms debounce.
func NewClusterFileWatcher(aclsPath, credentialsPath string, onACLReload, onCredReload func(path string)) *ClusterFileWatcher {
	return &ClusterFileWatcher{
		aclsPath:        aclsPath,
		credentialsPath: credentialsPath,
		onACLReload:     onACLReload,
		onCredReload:    onCredReload,
		debounce:        100 * time.Millisecond,
	}
}

// Run starts the watcher and blocks until the done channel is closed.
//
// The Run signature takes <-chan struct{} for backwards compatibility with
// existing callers in cmd/skafka and the tests; internally it adapts to a
// context.Context for fsutil.FileWatcher.
func (w *ClusterFileWatcher) Run(done <-chan struct{}) error {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-done
		cancel()
	}()

	var (
		aclTimer  *time.Timer
		credTimer *time.Timer
	)

	debounceACL := func() {
		if aclTimer != nil {
			aclTimer.Stop()
		}
		path := w.aclsPath
		aclTimer = time.AfterFunc(w.debounce, func() {
			slog.Info("cluster file watcher: reloading ACLs", "path", path)
			if w.onACLReload != nil {
				w.onACLReload(path)
			}
		})
	}

	debounceCreds := func() {
		if credTimer != nil {
			credTimer.Stop()
		}
		path := w.credentialsPath
		credTimer = time.AfterFunc(w.debounce, func() {
			slog.Info("cluster file watcher: reloading credentials", "path", path)
			if w.onCredReload != nil {
				w.onCredReload(path)
			}
		})
	}

	fw := fsutil.New([]fsutil.FileSpec{
		{Path: w.aclsPath, OnChange: debounceACL},
		{Path: w.credentialsPath, OnChange: debounceCreds},
	})
	return fw.Run(ctx)
}
