package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/woestebanaan/skafka/internal/auth"
	"github.com/woestebanaan/skafka/internal/lease"
	"github.com/woestebanaan/skafka/internal/lock"
	"github.com/woestebanaan/skafka/internal/storage"
)

// ---- MemoryStorage ---- //

// MemoryStorage is an in-memory StorageEngine used for development and testing.
// It is NOT safe for production — data is lost on restart.
type MemoryStorage struct {
	mu         sync.RWMutex
	partitions map[string][]storage.Record // key: "topic/partition"
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{partitions: make(map[string][]storage.Record)}
}

func (m *MemoryStorage) key(topic string, partition int32) string {
	return fmt.Sprintf("%s/%d", topic, partition)
}

func (m *MemoryStorage) Append(_ context.Context, topic string, partition int32, records []storage.Record) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := m.key(topic, partition)
	base := int64(len(m.partitions[k]))
	for i, r := range records {
		r.Offset = base + int64(i)
		m.partitions[k] = append(m.partitions[k], r)
	}
	return base, nil
}

func (m *MemoryStorage) Read(_ context.Context, topic string, partition int32, startOffset int64, maxBytes int) ([]storage.Record, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	k := m.key(topic, partition)
	all := m.partitions[k]
	if startOffset >= int64(len(all)) {
		return nil, nil
	}
	out := all[startOffset:]
	// Rough byte cap.
	total := 0
	for i, r := range out {
		total += len(r.Key) + len(r.Value) + 64
		if total > maxBytes && i > 0 {
			out = out[:i]
			break
		}
	}
	return out, nil
}

func (m *MemoryStorage) HighWatermark(topic string, partition int32) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return int64(len(m.partitions[m.key(topic, partition)])), nil
}

func (m *MemoryStorage) LogStartOffset(topic string, partition int32) (int64, error) {
	return 0, nil
}

func (m *MemoryStorage) CreatePartition(topic string, partition int32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.partitions[m.key(topic, partition)] = nil
	return nil
}

func (m *MemoryStorage) DeletePartition(topic string, partition int32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.partitions, m.key(topic, partition))
	return nil
}

// ---- LocalLeaseManager ---- //

// LocalLeaseManager is a single-broker stub: this node is always the leader.
type LocalLeaseManager struct{}

func NewLocalLeaseManager() *LocalLeaseManager { return &LocalLeaseManager{} }

func (l *LocalLeaseManager) Acquire(_ context.Context, _ string, _ int32) error { return nil }
func (l *LocalLeaseManager) Release(_ string, _ int32) error                    { return nil }
func (l *LocalLeaseManager) IsLeader(_ string, _ int32) bool                   { return true }
func (l *LocalLeaseManager) WatchLeaders(_ context.Context) (<-chan lease.LeaderChange, error) {
	return make(chan lease.LeaderChange), nil
}

// ---- LocalPartitionLock ---- //

// LocalPartitionLock is a stub: always reports locked (single broker, no contention).
type LocalPartitionLock struct{}

func NewLocalPartitionLock() *LocalPartitionLock { return &LocalPartitionLock{} }

func (l *LocalPartitionLock) Lock(_ string, _ int32) error   { return nil }
func (l *LocalPartitionLock) Unlock(_ string, _ int32) error { return nil }
func (l *LocalPartitionLock) IsLocked(_ string, _ int32) bool { return true }

// ---- AllowAllAuthEngine ---- //

// AllowAllAuthEngine authenticates every connection as ANONYMOUS and permits all operations.
// Used in development; replaced by scram.go/tls.go in production (Phase 7).
type AllowAllAuthEngine struct{}

func NewAllowAllAuthEngine() *AllowAllAuthEngine { return &AllowAllAuthEngine{} }

func (a *AllowAllAuthEngine) Authenticate(_ context.Context, _ auth.Credentials) (auth.Principal, error) {
	return auth.Principal{Name: "ANONYMOUS", Kind: "User"}, nil
}

func (a *AllowAllAuthEngine) Authorize(_ auth.Principal, _ auth.Resource, _ auth.Operation) bool {
	return true
}

// ---- DenyAllAuthEngine ---- //

// DenyAllAuthEngine rejects all authentication — useful for testing ACL enforcement.
type DenyAllAuthEngine struct{}

func NewDenyAllAuthEngine() *DenyAllAuthEngine { return &DenyAllAuthEngine{} }

func (d *DenyAllAuthEngine) Authenticate(_ context.Context, _ auth.Credentials) (auth.Principal, error) {
	return auth.Principal{}, errors.New("authentication required")
}

func (d *DenyAllAuthEngine) Authorize(_ auth.Principal, _ auth.Resource, _ auth.Operation) bool {
	return false
}

// Verify interfaces at compile time.
var _ storage.StorageEngine = (*MemoryStorage)(nil)
var _ lease.LeaseManager = (*LocalLeaseManager)(nil)
var _ lock.PartitionLock = (*LocalPartitionLock)(nil)
var _ auth.AuthEngine = (*AllowAllAuthEngine)(nil)
var _ auth.AuthEngine = (*DenyAllAuthEngine)(nil)
