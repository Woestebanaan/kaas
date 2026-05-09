package broker

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/woestebanaan/skafka/internal/protocol/handlers"
)

// FenceWatcher polls the producer-fences directory on the shared
// RWX PVC and applies any new fence entries to the local storage
// engine via ProducerEpochFencer.FenceProducerEpoch. Closes the
// cross-broker fence gap (gh #108 phase 2): when a coordinator
// broker bumps a producer's epoch on InitProducerId, partitions
// led by *other* brokers see no signal until the new session
// writes there — an in-flight zombie from the old session can
// slip through. The watcher applies every peer's outbound fences
// so every broker's storage view of every PID's epoch monotonically
// catches up to the cluster max.
//
// Read-only: the watcher never writes. Self-loop avoidance is
// enforced by skipping the file whose name matches our own
// brokerID — fences this broker emitted are already applied
// locally by the in-process call to FenceProducerEpoch.
//
// Idempotent: per-file (PID → highest-epoch-applied) cache means
// a fence already applied isn't re-applied on the next tick.
// Idempotency at the engine layer is also guarded
// (FenceProducerEpoch is a no-op when entry.epoch >= epoch),
// but tracking here saves the engine-wide RLock loop on every
// tick when nothing changed.
type FenceWatcher struct {
	dir       string
	selfFile  string // basename of our own outbound fence file (skip)
	fencer    handlers.ProducerEpochFencer
	pollEvery time.Duration

	mu      sync.Mutex
	applied map[string]map[int64]int16 // peerFile → PID → highest epoch applied
}

// NewFenceWatcher constructs a watcher rooted at the producer-
// fences directory. selfFile is the basename ("from-<brokerID>.json")
// the watcher will skip — its content is fences this broker
// emitted, already applied in-process.
func NewFenceWatcher(dir, selfFile string, fencer handlers.ProducerEpochFencer) *FenceWatcher {
	return &FenceWatcher{
		dir:       dir,
		selfFile:  selfFile,
		fencer:    fencer,
		pollEvery: 2 * time.Second,
		applied:   make(map[string]map[int64]int16),
	}
}

// Run drives the polling loop until ctx is cancelled. Errors
// reading individual peer files are logged and the loop continues
// — a half-written file under a peer's tmp+rename is transient
// and the next tick picks it up.
func (w *FenceWatcher) Run(ctx context.Context) {
	t := time.NewTicker(w.pollEvery)
	defer t.Stop()
	// Tick once on entry so a peer's pre-existing file (e.g. after
	// our own restart) is applied without waiting a full interval.
	w.Tick()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.Tick()
		}
	}
}

// Tick is a single-pass scan of the fence directory. Exported so
// integration tests can drive the watcher synchronously instead
// of waiting for the 2s poll interval. Run() also calls this on
// entry and on every ticker fire.
func (w *FenceWatcher) Tick() {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return // dir doesn't exist yet — no fences emitted anywhere
		}
		slog.Warn("fence watcher: read dir", "dir", w.dir, "err", err)
		return
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			continue
		}
		if !strings.HasPrefix(name, "from-") || !strings.HasSuffix(name, ".json") {
			continue
		}
		if name == w.selfFile {
			continue
		}
		w.applyPeer(name)
	}
}

func (w *FenceWatcher) applyPeer(name string) {
	path := filepath.Join(w.dir, name)
	data, err := os.ReadFile(path)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			slog.Warn("fence watcher: read peer", "file", path, "err", err)
		}
		return
	}
	if len(data) == 0 {
		return
	}
	var state map[string]int16
	if err := json.Unmarshal(data, &state); err != nil {
		slog.Warn("fence watcher: decode peer", "file", path, "err", err)
		return
	}

	w.mu.Lock()
	last, ok := w.applied[name]
	if !ok {
		last = make(map[int64]int16)
		w.applied[name] = last
	}
	pending := make(map[int64]int16, len(state))
	for k, epoch := range state {
		pid, err := strconv.ParseInt(k, 10, 64)
		if err != nil {
			continue
		}
		if existing, ok := last[pid]; ok && existing >= epoch {
			continue
		}
		pending[pid] = epoch
		last[pid] = epoch
	}
	w.mu.Unlock()

	for pid, epoch := range pending {
		w.fencer.FenceProducerEpoch(pid, epoch)
	}
}
