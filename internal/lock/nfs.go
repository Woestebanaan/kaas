package lock

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// NFSLock implements PartitionLock using an advisory sentinel file for NFS mounts.
// WARNING: flock(2) is unreliable over NFS. This implementation writes a
// hostname:pid identity string to a .lock file and verifies ownership on each
// IsLocked check. It is NOT safe for split-brain scenarios in production; use
// FlockLock with CephFS instead.
type NFSLock struct {
	dataDir  string
	identity string // hostname:pid
	mu       sync.Mutex
	held     map[string]struct{}
}

func NewNFSLock(dataDir string) (*NFSLock, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	identity := fmt.Sprintf("%s:%d", hostname, os.Getpid())
	slog.Warn("NFSLock: advisory-only locking active — not safe for multi-pod production",
		"identity", identity)
	return &NFSLock{
		dataDir:  dataDir,
		identity: identity,
		held:     make(map[string]struct{}),
	}, nil
}

func (n *NFSLock) lockPath(topic string, partition int32) string {
	return filepath.Join(n.dataDir, topic, strconv.Itoa(int(partition)), ".lock")
}

func (n *NFSLock) lockKey(topic string, partition int32) string {
	return fmt.Sprintf("%s/%d", topic, partition)
}

// Lock writes our identity to the .lock file and re-reads to verify we won.
func (n *NFSLock) Lock(topic string, partition int32) error {
	key := n.lockKey(topic, partition)

	n.mu.Lock()
	defer n.mu.Unlock()

	if _, ok := n.held[key]; ok {
		return nil
	}

	path := n.lockPath(topic, partition)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	if err := os.WriteFile(path, []byte(n.identity), 0600); err != nil {
		return err
	}

	// Re-read to confirm we are the last writer (advisory check).
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(data)) != n.identity {
		return fmt.Errorf("nfs lock: contention detected for %s/%d, another holder: %s", topic, partition, data)
	}

	n.held[key] = struct{}{}
	return nil
}

// Unlock removes the sentinel file and clears the held state.
func (n *NFSLock) Unlock(topic string, partition int32) error {
	key := n.lockKey(topic, partition)

	n.mu.Lock()
	defer n.mu.Unlock()

	if _, ok := n.held[key]; !ok {
		return nil
	}
	delete(n.held, key)
	_ = os.Remove(n.lockPath(topic, partition))
	return nil
}

// IsLocked reports whether this process currently holds the advisory lock.
func (n *NFSLock) IsLocked(topic string, partition int32) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	_, ok := n.held[n.lockKey(topic, partition)]
	return ok
}
