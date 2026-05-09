package coordinator

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

// FenceLog is the outbound producer-fence broadcast file owned by
// this broker. When the broker is the routed coordinator for a
// transactional.id and InitProducerId bumps the epoch (gh #22),
// the broker calls FenceProducerEpoch locally to fence partitions
// it leads — but partitions led by *other* brokers see no signal
// until the new session's first batch lands there. A zombie batch
// from the old session can sneak in during that window.
//
// Phase 2 of gh #108 closes the gap by writing every fence event
// to this broker's outbound file; peer brokers' FenceWatcher
// (internal/broker/fence_watcher.go) polls the directory and
// applies any new (PID, highest_epoch) entries to their local
// engine. The broadcast piggy-backs on the shared RWX PVC, so no
// new gRPC surface is needed; latency is bounded by the watcher
// poll interval (2s default).
//
// On-disk shape: <dataDir>/__cluster/producer_fences/from-<brokerID>.json
// containing {"<pid>": <epoch>, ...}. Idempotent: appending an
// epoch ≤ current is a no-op. JSON keys are stringified int64
// because encoding/json doesn't support non-string map keys.
type FenceLog struct {
	path string

	mu sync.Mutex
}

// NewFenceLog opens (creates) the per-broker fence log under
// <dataDir>/__cluster/producer_fences/from-<brokerID>.json.
// Missing directory is created; missing file is treated as an
// empty map (no fences emitted yet).
func NewFenceLog(dir, brokerID string) (*FenceLog, error) {
	if brokerID == "" {
		return nil, errors.New("fence log: empty broker ID")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &FenceLog{
		path: filepath.Join(dir, fmt.Sprintf("from-%s.json", brokerID)),
	}, nil
}

// Append records (pid, epoch) into the outbound fence file. If the
// file already has a higher-or-equal epoch for this PID, no-op
// (idempotent). Atomic tmp+rename so peers reading mid-write see
// the prior consistent state, never a half-written file.
func (l *FenceLog) Append(pid int64, epoch int16) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	state, err := l.loadLocked()
	if err != nil {
		return err
	}
	key := strconv.FormatInt(pid, 10)
	if existing, ok := state[key]; ok && existing >= epoch {
		return nil
	}
	state[key] = epoch
	return l.persistLocked(state)
}

// Snapshot returns a copy of the current outbound state. Used by
// tests to inspect the on-disk shape without poking into private
// fields.
func (l *FenceLog) Snapshot() map[int64]int16 {
	l.mu.Lock()
	defer l.mu.Unlock()
	state, err := l.loadLocked()
	if err != nil {
		return nil
	}
	out := make(map[int64]int16, len(state))
	for k, v := range state {
		pid, err := strconv.ParseInt(k, 10, 64)
		if err != nil {
			continue
		}
		out[pid] = v
	}
	return out
}

func (l *FenceLog) loadLocked() (map[string]int16, error) {
	data, err := os.ReadFile(l.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return make(map[string]int16), nil
		}
		return nil, err
	}
	state := make(map[string]int16)
	if len(data) == 0 {
		return state, nil
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("fence log: decode %s: %w", l.path, err)
	}
	return state, nil
}

func (l *FenceLog) persistLocked(state map[string]int16) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	tmp := l.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, l.path)
}

// Path returns the on-disk file path. Used by tests + by the peer
// FenceWatcher to know which file to skip ("don't apply our own
// outbound entries to ourselves — they're already locally
// fenced").
func (l *FenceLog) Path() string {
	return l.path
}

// FenceLogDir is the conventional directory for fence log files.
// Both FenceLog (writer) and FenceWatcher (reader) use this.
func FenceLogDir(clusterDir string) string {
	return filepath.Join(clusterDir, "producer_fences")
}
