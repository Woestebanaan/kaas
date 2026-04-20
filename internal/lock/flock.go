package lock

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
)

// FlockLock implements PartitionLock using POSIX flock(2) — suitable for CephFS.
// flock provides kernel-enforced exclusive locking across processes on the same node;
// CephFS propagates flock state cluster-wide, making this safe for multi-pod deployments.
type FlockLock struct {
	dataDir string
	mu      sync.Mutex
	held    map[string]*os.File // partition key → open lock file fd
}

func NewFlockLock(dataDir string) *FlockLock {
	return &FlockLock{
		dataDir: dataDir,
		held:    make(map[string]*os.File),
	}
}

func (f *FlockLock) lockPath(topic string, partition int32) string {
	return filepath.Join(f.dataDir, topic, strconv.Itoa(int(partition)), ".lock")
}

func (f *FlockLock) lockKey(topic string, partition int32) string {
	return fmt.Sprintf("%s/%d", topic, partition)
}

// Lock acquires an exclusive non-blocking flock on the partition .lock file.
// Returns an error (including syscall.EWOULDBLOCK) if another process holds the lock.
func (f *FlockLock) Lock(topic string, partition int32) error {
	key := f.lockKey(topic, partition)

	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.held[key]; ok {
		return nil // already held
	}

	path := f.lockPath(topic, partition)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}

	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		return fmt.Errorf("flock: acquire lock for %s/%d: %w", topic, partition, err)
	}

	f.held[key] = file
	return nil
}

// Unlock releases the flock and closes the lock file.
func (f *FlockLock) Unlock(topic string, partition int32) error {
	key := f.lockKey(topic, partition)

	f.mu.Lock()
	defer f.mu.Unlock()

	file, ok := f.held[key]
	if !ok {
		return nil
	}
	delete(f.held, key)
	_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
	return file.Close()
}

// IsLocked reports whether this process currently holds the lock for the given partition.
func (f *FlockLock) IsLocked(topic string, partition int32) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.held[f.lockKey(topic, partition)]
	return ok
}
