package broker

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/woestebanaan/skafka/internal/auth"
	"github.com/woestebanaan/skafka/internal/lease"
	"github.com/woestebanaan/skafka/internal/storage"
)

var _ = context.Background // keep context import for lease manager stubs below

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

func (m *MemoryStorage) Append(_ context.Context, topic string, partition int32, _ uint32, _ int16, rawBatch []byte) (int64, error) {
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

	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.getOrCreate(topic, partition)

	// Brokers own offsets: rewrite baseOffset to current HWM (CRC covers
	// attrs..records, not baseOffset). See DiskStorageEngine.Append.
	binary.BigEndian.PutUint64(rawBatch[0:8], uint64(p.highWater))
	baseOffset := int64(binary.BigEndian.Uint64(rawBatch[0:8]))
	lastOffsetDelta := int32(binary.BigEndian.Uint32(rawBatch[23:27]))

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

// OffsetForLeaderEpoch is a no-op in dev mode: MemoryStorage doesn't
// model epochs (single-process, no controller failover), so the
// answer is always "nothing to truncate to" — sentinel (-1, -1, nil).
// Java consumers connected to dev-mode brokers won't issue this in
// practice (it requires a non-zero leader_epoch from a Fetch / Metadata
// response, and MemoryStorage never bumps epoch above 0).
func (m *MemoryStorage) OffsetForLeaderEpoch(_ string, _ int32, _ int32) (int32, int64, error) {
	return -1, -1, nil
}

// PartitionSize is always 0 in memory storage; there are no segment files.
func (m *MemoryStorage) PartitionSize(_ string, _ int32) int64 { return 0 }

// DataDir returns a sentinel path so DescribeLogDirs can still answer.
func (m *MemoryStorage) DataDir() string { return "memory://" }

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

// TakeOver returns the in-memory high watermark; in-memory storage has no
// disk state to recover, so the only meaningful value is the current HWM.
func (m *MemoryStorage) TakeOver(_ context.Context, topic string, partition int32, _ uint32) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	p := m.partitions[m.key(topic, partition)]
	if p == nil {
		return 0, nil
	}
	return p.highWater, nil
}

func (m *MemoryStorage) Relinquish(_ string, _ int32) error { return nil }

func (m *MemoryStorage) DeleteRecords(_ string, _ int32, target int64) (int64, error) {
	if target < 0 {
		return 0, nil
	}
	return target, nil
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

// ---- LocalGroupSource ---- //

// LocalGroupSource is a stub coordinator.GroupAssignmentSource for
// single-broker dev / test setups: this broker is the assigned
// coordinator for every group it's asked about. Replaces the v2.6
// LocalLeaseManager.IsCoordinator path under the Phase 5 rewire.
type LocalGroupSource struct {
	BrokerID string
}

func NewLocalGroupSource(brokerID string) *LocalGroupSource {
	return &LocalGroupSource{BrokerID: brokerID}
}

func (l *LocalGroupSource) OwnsGroup(_ string) bool { return true }
func (l *LocalGroupSource) GroupCoordinator(_ string) (string, bool) {
	return l.BrokerID, true
}

// ---- AllowAllAuthEngine ---- //

// AllowAllAuthEngine authenticates every connection as ANONYMOUS and permits all operations.
// Used in development; replaced by RealAuthEngine in production (Phase 7).
type AllowAllAuthEngine struct{}

func NewAllowAllAuthEngine() *AllowAllAuthEngine { return &AllowAllAuthEngine{} }

func (a *AllowAllAuthEngine) NewSASLExchange(_ string) (auth.SASLExchange, error) {
	return &allowAllExchange{}, nil
}
func (a *AllowAllAuthEngine) AuthenticateTLS(_ string) (auth.Principal, error) {
	return auth.Principal{Name: "ANONYMOUS", Kind: "User"}, nil
}
func (a *AllowAllAuthEngine) Authorize(_ auth.Principal, _ auth.Resource, _ auth.Operation) bool {
	return true
}
func (a *AllowAllAuthEngine) CheckProduceQuota(_ auth.Principal, _ int) int32 { return 0 }
func (a *AllowAllAuthEngine) CheckFetchQuota(_ auth.Principal, _ int) int32   { return 0 }

// RequiresPreAuth returns false — anonymous-OK listeners must let
// non-pre-SASL APIs through without first completing SASL.
func (a *AllowAllAuthEngine) RequiresPreAuth() bool { return false }

// allowAllExchange completes immediately with an ANONYMOUS principal.
type allowAllExchange struct{}

func (e *allowAllExchange) Step(_ []byte) ([]byte, bool, error) {
	return nil, true, nil
}
func (e *allowAllExchange) Principal() auth.Principal {
	return auth.Principal{Name: "ANONYMOUS", Kind: "User"}
}

// ---- DenyAllAuthEngine ---- //

// DenyAllAuthEngine rejects all authentication — useful for testing ACL enforcement.
type DenyAllAuthEngine struct{}

func NewDenyAllAuthEngine() *DenyAllAuthEngine { return &DenyAllAuthEngine{} }

func (d *DenyAllAuthEngine) NewSASLExchange(_ string) (auth.SASLExchange, error) {
	return &denyAllExchange{}, nil
}
func (d *DenyAllAuthEngine) AuthenticateTLS(_ string) (auth.Principal, error) {
	return auth.Principal{}, errors.New("authentication required")
}
func (d *DenyAllAuthEngine) Authorize(_ auth.Principal, _ auth.Resource, _ auth.Operation) bool {
	return false
}
func (d *DenyAllAuthEngine) CheckProduceQuota(_ auth.Principal, _ int) int32 { return 0 }
func (d *DenyAllAuthEngine) CheckFetchQuota(_ auth.Principal, _ int) int32   { return 0 }

// RequiresPreAuth returns true — a deny-all engine treats every
// connection as authentication-required so non-pre-SASL APIs are
// rejected before the (already-doomed) authorization call runs.
func (d *DenyAllAuthEngine) RequiresPreAuth() bool { return true }

type denyAllExchange struct{}

func (e *denyAllExchange) Step(_ []byte) ([]byte, bool, error) {
	return nil, false, errors.New("authentication required")
}
func (e *denyAllExchange) Principal() auth.Principal { return auth.Principal{} }

// Verify interfaces at compile time.
var _ storage.StorageEngine = (*MemoryStorage)(nil)
var _ lease.LeaseManager = (*LocalLeaseManager)(nil)
var _ auth.AuthEngine = (*AllowAllAuthEngine)(nil)
var _ auth.AuthEngine = (*DenyAllAuthEngine)(nil)
