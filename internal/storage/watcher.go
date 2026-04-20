package storage

import (
	"log/slog"
	"time"

	"github.com/fsnotify/fsnotify"
)

// ClusterFileWatcher watches __cluster/acls.json and __cluster/credentials.json
// and fires debounced callbacks when either file changes. The actual reload logic
// for ACLs and credentials is wired in Phase 7.
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
func (w *ClusterFileWatcher) Run(done <-chan struct{}) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer watcher.Close()

	for _, path := range []string{w.aclsPath, w.credentialsPath} {
		if err := watcher.Add(path); err != nil {
			slog.Warn("cluster file watcher: cannot watch path", "path", path, "err", err)
		}
	}

	var (
		aclTimer  *time.Timer
		credTimer *time.Timer
	)

	for {
		select {
		case <-done:
			return nil

		case event, ok := <-watcher.Events:
			if !ok {
				return nil
			}
			if event.Has(fsnotify.Write) || event.Has(fsnotify.Create) {
				switch event.Name {
				case w.aclsPath:
					if aclTimer != nil {
						aclTimer.Stop()
					}
					path := event.Name
					aclTimer = time.AfterFunc(w.debounce, func() {
						slog.Info("cluster file watcher: reloading ACLs", "path", path)
						if w.onACLReload != nil {
							w.onACLReload(path)
						}
					})

				case w.credentialsPath:
					if credTimer != nil {
						credTimer.Stop()
					}
					path := event.Name
					credTimer = time.AfterFunc(w.debounce, func() {
						slog.Info("cluster file watcher: reloading credentials", "path", path)
						if w.onCredReload != nil {
							w.onCredReload(path)
						}
					})
				}
			}

		case err, ok := <-watcher.Errors:
			if !ok {
				return nil
			}
			slog.Error("cluster file watcher: error", "err", err)
		}
	}
}
