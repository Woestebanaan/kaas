// Package fsutil holds tiny filesystem helpers shared across the broker.
//
// FileWatcher is the merged fsnotify + mtime-poll + full-fire-fallback
// loop used by every cluster-state-on-disk watcher: assignment.json
// (internal/assignment.FileStore.Watch) and the cluster auth files
// (internal/storage.ClusterFileWatcher). All three layered mechanisms
// matter:
//
//   - fsnotify on the parent directory: sub-second latency on local fs.
//     Falls over silently on NFS where inotify is not propagated across
//     the mount boundary.
//   - mtime poll (default 1s): the safety net that makes the watcher
//     work on csi-driver-nfs / EFS / Azure Files / etc. The plan §"The
//     polling safety net" calls this out as the v3.3 mount-options story.
//   - full-fire fallback (default 30s): defends against NFS attribute
//     caching and second-resolution mtime — if a file is rewritten
//     within the same wall-clock second, the mtime check misses it
//     until the next full-fire tick re-reads regardless.
//
// The helper does not debounce or coalesce; callers wrap with their own
// per-file dedup (assignment uses non-blocking channel sends; cluster
// auth uses 100ms debounce timers).
package fsutil

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
)

// FileSpec is one file the watcher should track. OnChange fires
// synchronously on the watcher goroutine — long work belongs in a
// goroutine the callback spawns, not inline.
type FileSpec struct {
	Path     string
	OnChange func()
}

// FileWatcher tracks a set of files. Run blocks until ctx is cancelled
// and dispatches OnChange when any file is observed to change.
type FileWatcher struct {
	files            []FileSpec
	pollInterval     time.Duration
	fullReadInterval time.Duration
}

// New constructs a FileWatcher for the given files. Defaults: 1s poll,
// 30s full-fire. Override via WithPollInterval / WithFullReadInterval.
func New(files []FileSpec) *FileWatcher {
	return &FileWatcher{
		files:            files,
		pollInterval:     1 * time.Second,
		fullReadInterval: 30 * time.Second,
	}
}

// WithPollInterval overrides the mtime poll cadence. 0 disables poll
// (rely on fsnotify alone — useful for tests on local fs that want
// fast, deterministic timing).
func (w *FileWatcher) WithPollInterval(d time.Duration) *FileWatcher {
	w.pollInterval = d
	return w
}

// WithFullReadInterval overrides the full-fire fallback cadence. 0
// disables it (the mtime poll alone is enough on local fs).
func (w *FileWatcher) WithFullReadInterval(d time.Duration) *FileWatcher {
	w.fullReadInterval = d
	return w
}

// fileState holds per-file mtime for dedup. Internal to the watch loop.
type fileState struct {
	spec  FileSpec
	mtime time.Time
}

// Run blocks, dispatching OnChange callbacks until ctx is cancelled.
// fsnotify init failures degrade gracefully to polling-only — the plan
// §"inotify on config files" allows that path.
func (w *FileWatcher) Run(ctx context.Context) error {
	if len(w.files) == 0 {
		<-ctx.Done()
		return nil
	}

	// Best-effort fsnotify on each unique parent directory. If init or
	// AddPath fails for any reason, we silently fall back to polling-only.
	fs := setupFsnotify(w.files)
	if fs != nil {
		defer fs.Close()
	}

	states := make(map[string]*fileState, len(w.files))
	for _, f := range w.files {
		st := &fileState{spec: f}
		// Initial mtime snapshot — avoids a spurious fire on the first
		// poll tick when nothing has actually changed since startup.
		if info, err := os.Stat(f.Path); err == nil {
			st.mtime = info.ModTime()
		}
		states[f.Path] = st
	}

	// Tickers; nil channels disable the corresponding mechanism.
	var pollC, fullC <-chan time.Time
	if w.pollInterval > 0 {
		pt := time.NewTicker(w.pollInterval)
		defer pt.Stop()
		pollC = pt.C
	}
	if w.fullReadInterval > 0 {
		ft := time.NewTicker(w.fullReadInterval)
		defer ft.Stop()
		fullC = ft.C
	}

	var fsEvents <-chan fsnotify.Event
	if fs != nil {
		fsEvents = fs.Events
	}

	for {
		select {
		case <-ctx.Done():
			return nil

		case ev, ok := <-fsEvents:
			if !ok {
				fsEvents = nil
				continue
			}
			st := lookupByEventName(states, ev.Name)
			if st == nil {
				continue
			}
			if ev.Op&(fsnotify.Create|fsnotify.Write|fsnotify.Rename) == 0 {
				continue
			}
			// Update cached mtime so the polling loop doesn't re-fire
			// on the same change.
			if info, err := os.Stat(st.spec.Path); err == nil {
				st.mtime = info.ModTime()
			}
			if st.spec.OnChange != nil {
				st.spec.OnChange()
			}

		case <-pollC:
			for _, st := range states {
				info, err := os.Stat(st.spec.Path)
				if err != nil {
					continue
				}
				if !info.ModTime().Equal(st.mtime) {
					st.mtime = info.ModTime()
					if st.spec.OnChange != nil {
						st.spec.OnChange()
					}
				}
			}

		case <-fullC:
			// Full-fire: invoke every callback regardless of mtime.
			// Defends against same-second writes on second-resolution
			// NFS mtime and against NFS attribute caching.
			for _, st := range states {
				if st.spec.OnChange != nil {
					st.spec.OnChange()
				}
				if info, err := os.Stat(st.spec.Path); err == nil {
					st.mtime = info.ModTime()
				}
			}
		}
	}
}

// setupFsnotify creates a watcher and registers each unique parent
// directory of the given files. Returns nil when fsnotify isn't
// available — Run treats that as polling-only mode.
func setupFsnotify(files []FileSpec) *fsnotify.Watcher {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil
	}
	registered := 0
	dirs := make(map[string]struct{}, len(files))
	for _, f := range files {
		dir := filepath.Dir(f.Path)
		if _, seen := dirs[dir]; seen {
			continue
		}
		// MkdirAll ensures fsnotify doesn't fail on a not-yet-created
		// staging dir (e.g. /data/__cluster on a fresh broker startup).
		if err := os.MkdirAll(dir, 0755); err != nil {
			continue
		}
		if err := w.Add(dir); err != nil {
			continue
		}
		dirs[dir] = struct{}{}
		registered++
	}
	if registered == 0 {
		_ = w.Close()
		return nil
	}
	return w
}

// lookupByEventName matches an fsnotify event's Name against the watched
// files — first by full path, then by basename. Linear scan; fine for
// the small N (1-5) any caller has.
func lookupByEventName(states map[string]*fileState, eventName string) *fileState {
	if st, ok := states[eventName]; ok {
		return st
	}
	base := filepath.Base(eventName)
	for _, st := range states {
		if filepath.Base(st.spec.Path) == base {
			return st
		}
	}
	return nil
}
