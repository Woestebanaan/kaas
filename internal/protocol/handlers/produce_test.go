package handlers

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/woestebanaan/skafka/internal/lease"
	"github.com/woestebanaan/skafka/pkg/kafkaapi"
	"github.com/woestebanaan/skafka/tests/testutil/recordbatch"
)

// stubCoord is a minimal kafkaapi.BrokerCoordinator for testing the produce
// handler's coordinator code path. We only populate the fields the handler
// actually reads; everything else returns zero values / no-op.
type stubCoord struct {
	owns          map[string]uint32 // "topic/partition" → epoch
	lastHeartbeat time.Time
}

func (s *stubCoord) Start(_ context.Context) error                        { return nil }
func (s *stubCoord) Stop() error                                          { return nil }
func (s *stubCoord) OnAssignmentChange(_ kafkaapi.AssignmentChangeHandler) {}
func (s *stubCoord) LastHeartbeat() time.Time                             { return s.lastHeartbeat }
func (s *stubCoord) LeaderFor(_ string, _ int32) int32                    { return 0 }

func (s *stubCoord) Owns(topic string, partition int32) bool {
	_, ok := s.owns[stubKey(topic, partition)]
	return ok
}
func (s *stubCoord) CurrentEpoch(topic string, partition int32) (uint32, bool) {
	e, ok := s.owns[stubKey(topic, partition)]
	return e, ok
}

func stubKey(topic string, partition int32) string {
	return fmt.Sprintf("%s/%d", topic, partition)
}

var _ kafkaapi.BrokerCoordinator = (*stubCoord)(nil)

func validBatch(t *testing.T, baseOffset int64, numRecords int) []byte {
	t.Helper()
	batch := &recordbatch.RecordBatch{
		BaseOffset:      baseOffset,
		LastOffsetDelta: int32(numRecords - 1),
		ProducerID:      -1,
		ProducerEpoch:   -1,
		BaseSequence:    -1,
	}
	for i := 0; i < numRecords; i++ {
		batch.Records = append(batch.Records, recordbatch.Record{
			OffsetDelta: int32(i),
			Value:       []byte{byte(i)},
		})
	}
	return recordbatch.Encode(nil, batch)
}

func TestValidateProduceBatches_Empty(t *testing.T) {
	if !validateProduceBatches(nil) {
		t.Fatal("nil records should validate")
	}
	if !validateProduceBatches([]byte{}) {
		t.Fatal("empty records should validate")
	}
}

func TestValidateProduceBatches_OneValidBatch(t *testing.T) {
	if !validateProduceBatches(validBatch(t, 0, 5)) {
		t.Fatal("valid batch rejected")
	}
}

func TestValidateProduceBatches_TwoValidBatchesConcatenated(t *testing.T) {
	combined := append(validBatch(t, 0, 3), validBatch(t, 3, 2)...)
	if !validateProduceBatches(combined) {
		t.Fatal("two concatenated valid batches rejected")
	}
}

func TestValidateProduceBatches_TruncatedHeader(t *testing.T) {
	if validateProduceBatches([]byte{0, 0, 0, 0}) {
		t.Fatal("truncated header should fail")
	}
}

func TestValidateProduceBatches_TruncatedBody(t *testing.T) {
	b := validBatch(t, 0, 1)
	if validateProduceBatches(b[:len(b)-5]) {
		t.Fatal("truncated body should fail")
	}
}

func TestValidateProduceBatches_BadMagic(t *testing.T) {
	b := validBatch(t, 0, 1)
	b[16] = 1 // magic byte
	if validateProduceBatches(b) {
		t.Fatal("magic=1 should fail")
	}
}

func TestValidateProduceBatches_CorruptedCRCPayload(t *testing.T) {
	b := validBatch(t, 0, 1)
	b[len(b)-1] ^= 0xFF // corrupt the records area; CRC is unchanged
	if validateProduceBatches(b) {
		t.Fatal("corrupted CRC payload should fail")
	}
}

func TestValidateProduceBatches_FlippedCRC(t *testing.T) {
	b := validBatch(t, 0, 1)
	binary.BigEndian.PutUint32(b[17:21], 0xDEADBEEF)
	if validateProduceBatches(b) {
		t.Fatal("wrong stored CRC should fail")
	}
}

func TestValidateProduceBatches_BatchLengthBelowMinimum(t *testing.T) {
	b := validBatch(t, 0, 1)
	binary.BigEndian.PutUint32(b[8:12], 10) // claim batchLength=10 (below 49 minimum)
	if validateProduceBatches(b) {
		t.Fatal("batchLength<49 should fail")
	}
}

// --- coordinator-path checkOwnership tests ---
//
// checkOwnership is the shim that decides which gating model the handler
// uses. With a coordinator wired, lease/lock are ignored entirely; without
// one, they're the source of truth (v2.6 path).

func TestCheckOwnership_CoordinatorOwnsAndFresh(t *testing.T) {
	coord := &stubCoord{
		owns:          map[string]uint32{"events/0": 7},
		lastHeartbeat: time.Now(),
	}
	h := &ProduceHandler{coord: coord}
	ok, epoch := h.checkOwnership("events", 0)
	if !ok {
		t.Fatal("checkOwnership should return true when coord owns + heartbeat fresh")
	}
	if epoch != 7 {
		t.Errorf("epoch=%d, want 7", epoch)
	}
}

func TestCheckOwnership_CoordinatorDoesNotOwn(t *testing.T) {
	coord := &stubCoord{
		owns:          map[string]uint32{"events/0": 7},
		lastHeartbeat: time.Now(),
	}
	h := &ProduceHandler{coord: coord}
	if ok, _ := h.checkOwnership("events", 1); ok {
		t.Error("checkOwnership should return false when coord does not own the partition")
	}
}

func TestCheckOwnership_StaleHeartbeat(t *testing.T) {
	coord := &stubCoord{
		owns:          map[string]uint32{"events/0": 7},
		lastHeartbeat: time.Now().Add(-10 * time.Second), // way past 3s window
	}
	h := &ProduceHandler{coord: coord}
	if ok, _ := h.checkOwnership("events", 0); ok {
		t.Error("checkOwnership should return false when heartbeat is stale")
	}
}

func TestCheckOwnership_NoHeartbeatYet(t *testing.T) {
	coord := &stubCoord{
		owns: map[string]uint32{"events/0": 7},
		// lastHeartbeat zero — no command received yet.
	}
	h := &ProduceHandler{coord: coord}
	if ok, _ := h.checkOwnership("events", 0); ok {
		t.Error("checkOwnership should return false before any heartbeat is received")
	}
}

func TestCheckOwnership_LegacyPathWhenNoCoordinator(t *testing.T) {
	// With coord==nil the handler must fall back to lease.IsLeader.
	// flock is gone in Phase 4; the only remaining v2.6-compatible gate
	// is the per-partition Kubernetes Lease.
	leases := &legacyLeases{leader: true}
	h := &ProduceHandler{leases: leases}

	ok, epoch := h.checkOwnership("events", 0)
	if !ok {
		t.Fatal("legacy path should succeed when lease holds")
	}
	if epoch != 0 {
		t.Errorf("legacy path must pass epoch=0 (no fence configured); got %d", epoch)
	}

	// Flip lease off — should fail.
	leases.leader = false
	if ok, _ := h.checkOwnership("events", 0); ok {
		t.Error("legacy path should fail when not leader")
	}
}

// legacyLeases is a minimal stub for the v2.6 fallback path.
type legacyLeases struct{ leader bool }

func (l *legacyLeases) Acquire(_ context.Context, _ string, _ int32) error { return nil }
func (l *legacyLeases) Release(_ string, _ int32) error                    { return nil }
func (l *legacyLeases) IsLeader(_ string, _ int32) bool                    { return l.leader }
func (l *legacyLeases) LeaderFor(_ string, _ int32) int32                  { return 0 }
func (l *legacyLeases) WatchLeaders(_ context.Context) (<-chan lease.LeaderChange, error) {
	return nil, nil
}

type legacyLocks struct{ locked bool }

func (l *legacyLocks) Lock(_ string, _ int32) error    { return nil }
func (l *legacyLocks) Unlock(_ string, _ int32) error  { return nil }
func (l *legacyLocks) IsLocked(_ string, _ int32) bool { return l.locked }
