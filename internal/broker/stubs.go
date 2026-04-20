package broker

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/woestebanaan/skafka/internal/auth"
	"github.com/woestebanaan/skafka/internal/lease"
	"github.com/woestebanaan/skafka/internal/lock"
	"github.com/woestebanaan/skafka/internal/storage"
)

// ---- MemoryStorage ---- //

// MemoryStorage is an in-memory StorageEngine for development and testing.
// It stores raw RecordBatch bytes and is NOT safe for production — data is lost on restart.
type MemoryStorage struct {
	mu         sync.RWMutex
	partitions map[string]*memPartition
}

type memPartition struct {
	batches   []memBatch
	highWater int64
}

type memBatch struct {
	baseOffset      int64
	lastOffsetDelta int32
	raw             []byte
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{partitions: make(map[string]*memPartition)}
}

func (m *MemoryStorage) key(topic string, partition int32) string {
	return fmt.Sprintf("%s/%d", topic, partition)
}

func (m *MemoryStorage) getOrCreate(topic string, partition int32) *memPartition {
	k := m.key(topic, partition)
	p := m.partitions[k]
	if p == nil {
		p = &memPartition{}
		m.partitions[k] = p
	}
	return p
}

func (m *MemoryStorage) Append(_ context.Context, topic string, partition int32, rawBatch []byte) (int64, error) {
	if len(rawBatch) == 0 {
		m.mu.RLock()
		p := m.partitions[m.key(topic, partition)]
		m.mu.RUnlock()
		if p == nil {
			return 0, nil
		}
		return p.highWater, nil
	}
	if len(rawBatch) < 27 {
		return -1, fmt.Errorf("memory storage: batch too short: %d bytes", len(rawBatch))
	}

	baseOffset := int64(binary.BigEndian.Uint64(rawBatch[0:8]))
	lastOffsetDelta := int32(binary.BigEndian.Uint32(rawBatch[23:27]))

	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.getOrCreate(topic, partition)
	p.batches = append(p.batches, memBatch{
		baseOffset:      baseOffset,
		lastOffsetDelta: lastOffsetDelta,
		raw:             rawBatch,
	})
	p.highWater = baseOffset + int64(lastOffsetDelta) + 1
	return baseOffset, nil
}

func (m *MemoryStorage) Read(_ context.Context, topic string, partition int32, startOffset int64, maxBytes int) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	p := m.partitions[m.key(topic, partition)]
	if p == nil {
		return nil, nil
	}

	var out []byte
	for _, b := range p.batches {
		if b.baseOffset+int64(b.lastOffsetDelta) < startOffset {
			continue
		}
		out = append(out, b.raw...)
		if len(out) >= maxBytes {
			break
		}
	}
	return out, nil
}

func (m *MemoryStorage) HighWatermark(topic string, partition int32) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	p := m.partitions[m.key(topic, partition)]
	if p == nil {
		return 0, nil
	}
	return p.highWater, nil
}

func (m *MemoryStorage) LogStartOffset(_ string, _ int32) (int64, error) {
	return 0, nil
}

func (m *MemoryStorage) CreatePartition(topic string, partition int32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getOrCreate(topic, partition)
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
func (l *LocalLeaseManager) LeaderFor(_ string, _ int32) int32                 { return 0 }
func (l *LocalLeaseManager) WatchLeaders(_ context.Context) (<-chan lease.LeaderChange, error) {
	return make(chan lease.LeaderChange), nil
}
func (l *LocalLeaseManager) AcquireCoordinator(_ context.Context, _ string) error { return nil }
func (l *LocalLeaseManager) ReleaseCoordinator(_ string) error                    { return nil }
func (l *LocalLeaseManager) IsCoordinator(_ string) bool                          { return true }
func (l *LocalLeaseManager) CoordinatorFor(_ string) int32                        { return 0 }
func (l *LocalLeaseManager) WaitForCoordinator(_ context.Context, _ string) bool  { return true }

var _ lease.CoordinatorLeaseManager = (*LocalLeaseManager)(nil)

// ---- LocalPartitionLock ---- //

// LocalPartitionLock is a stub: always reports locked (single broker, no contention).
type LocalPartitionLock struct{}

func NewLocalPartitionLock() *LocalPartitionLock { return &LocalPartitionLock{} }

func (l *LocalPartitionLock) Lock(_ string, _ int32) error    { return nil }
func (l *LocalPartitionLock) Unlock(_ string, _ int32) error  { return nil }
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
