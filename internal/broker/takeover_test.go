package broker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/woestebanaan/skafka/internal/storage"
	"github.com/woestebanaan/skafka/pkg/kafkaapi"
)

// recordingStorage stubs storage.StorageEngine, recording the sequence of
// TakeOver / Relinquish calls. Only the methods the takeover driver
// actually invokes are populated; the rest panic if a refactor accidentally
// calls them, surfacing the mistake immediately.
type recordingStorage struct {
	mu       sync.Mutex
	takeover []takeoverCall
	relinq   []relinqCall
}

type takeoverCall struct {
	topic     string
	partition int32
	epoch     uint32
}

type relinqCall struct {
	topic     string
	partition int32
}

func (r *recordingStorage) TakeOver(_ context.Context, topic string, partition int32, epoch uint32) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.takeover = append(r.takeover, takeoverCall{topic, partition, epoch})
	return 0, nil
}

func (r *recordingStorage) Relinquish(topic string, partition int32) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.relinq = append(r.relinq, relinqCall{topic, partition})
	return nil
}

func (r *recordingStorage) Append(_ context.Context, _ string, _ int32, _ uint32, _ int16, _ []byte) (int64, error) {
	panic("recordingStorage: Append unexpectedly called")
}
func (r *recordingStorage) Read(_ context.Context, _ string, _ int32, _ int64, _ int) ([]byte, error) {
	panic("recordingStorage: Read unexpectedly called")
}
func (r *recordingStorage) HighWatermark(_ string, _ int32) (int64, error)  { return 0, nil }
func (r *recordingStorage) LogStartOffset(_ string, _ int32) (int64, error) { return 0, nil }
func (r *recordingStorage) CreatePartition(_ string, _ int32) error         { return nil }
func (r *recordingStorage) DeletePartition(_ string, _ int32) error         { return nil }
func (r *recordingStorage) PartitionSize(_ string, _ int32) int64           { return 0 }
func (r *recordingStorage) DataDir() string                                 { return "/tmp/recording" }
func (r *recordingStorage) DeleteRecords(_ string, _ int32, _ int64) (int64, error) {
	return 0, nil
}

var _ storage.StorageEngine = (*recordingStorage)(nil)

func mkAssignment(brokerID string, parts []kafkaapi.PartitionAssignment) *kafkaapi.Assignment {
	for i := range parts {
		// Default empty broker → owned by us, so call sites can keep the
		// fixtures terse.
		if parts[i].Broker == "" {
			parts[i].Broker = brokerID
		}
	}
	return &kafkaapi.Assignment{
		ControllerEpoch:   1,
		AssignmentVersion: 1,
		Partitions:        parts,
	}
}

func TestTakeoverDriverNewPartitions(t *testing.T) {
	st := &recordingStorage{}
	d := NewTakeoverDriver(st, "broker-7")

	prev := mkAssignment("broker-7", nil)
	next := mkAssignment("broker-7", []kafkaapi.PartitionAssignment{
		{Topic: "events", Partition: 0, Epoch: 5, Role: kafkaapi.PartitionRoleLeader},
		{Topic: "events", Partition: 1, Epoch: 5, Role: kafkaapi.PartitionRoleLeader},
	})

	d.OnAssignmentChange(context.Background(), prev, next)

	if got := len(st.takeover); got != 2 {
		t.Fatalf("TakeOver call count: got %d want 2", got)
	}
	for _, c := range st.takeover {
		if c.epoch != 5 {
			t.Errorf("TakeOver(%s/%d) epoch=%d, want 5", c.topic, c.partition, c.epoch)
		}
	}
	if len(st.relinq) != 0 {
		t.Errorf("Relinquish should not be called when no partitions lost; got %d", len(st.relinq))
	}
}

func TestTakeoverDriverLostPartitions(t *testing.T) {
	st := &recordingStorage{}
	d := NewTakeoverDriver(st, "broker-7")

	prev := mkAssignment("broker-7", []kafkaapi.PartitionAssignment{
		{Topic: "events", Partition: 0, Epoch: 5},
		{Topic: "events", Partition: 1, Epoch: 5},
	})
	next := mkAssignment("broker-7", []kafkaapi.PartitionAssignment{
		// events/0 reassigned to another broker.
		{Topic: "events", Partition: 0, Broker: "other-broker", Epoch: 6},
		{Topic: "events", Partition: 1, Epoch: 5},
	})

	d.OnAssignmentChange(context.Background(), prev, next)

	if got := len(st.relinq); got != 1 || st.relinq[0].partition != 0 {
		t.Errorf("Relinquish: got %+v, want one call for partition 0", st.relinq)
	}
	// events/1 unchanged → no TakeOver.
	if got := len(st.takeover); got != 0 {
		t.Errorf("TakeOver should not fire for unchanged ownership; got %d", got)
	}
}

func TestTakeoverDriverEpochBumpReTakesOver(t *testing.T) {
	st := &recordingStorage{}
	d := NewTakeoverDriver(st, "broker-7")

	prev := mkAssignment("broker-7", []kafkaapi.PartitionAssignment{
		{Topic: "events", Partition: 0, Epoch: 5},
	})
	next := mkAssignment("broker-7", []kafkaapi.PartitionAssignment{
		{Topic: "events", Partition: 0, Epoch: 6},
	})

	d.OnAssignmentChange(context.Background(), prev, next)
	if got := len(st.takeover); got != 1 || st.takeover[0].epoch != 6 {
		t.Errorf("expected one TakeOver at epoch=6, got %+v", st.takeover)
	}
}

func TestTakeoverDriverNilPrevTreatedAsEmpty(t *testing.T) {
	st := &recordingStorage{}
	d := NewTakeoverDriver(st, "broker-7")

	// Initial assignment after broker startup — prev is nil.
	next := mkAssignment("broker-7", []kafkaapi.PartitionAssignment{
		{Topic: "events", Partition: 0, Epoch: 1},
	})

	d.OnAssignmentChange(context.Background(), nil, next)
	if len(st.takeover) != 1 {
		t.Errorf("nil prev should be treated as empty ownership; got %d takeovers", len(st.takeover))
	}
}

func TestIsHeartbeatFresh(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name string
		last time.Time
		want bool
	}{
		{"never received", time.Time{}, false},
		{"just received", now, true},
		{"received 1s ago", now.Add(-1 * time.Second), true},
		{"received 4s ago (past 3s timeout)", now.Add(-4 * time.Second), false},
	}
	for _, tc := range cases {
		if got := IsHeartbeatFresh(tc.last, DefaultHeartbeatTimeout); got != tc.want {
			t.Errorf("%s: got %v want %v", tc.name, got, tc.want)
		}
	}
}

func TestCoordinatorIsHeartbeatFreshTracksClient(t *testing.T) {
	// Build a Coordinator with no heartbeat client at all — the broker has
	// not yet established a heartbeat stream. IsHeartbeatFresh must return
	// false in this state.
	c := NewCoordinator("broker-7", nil, nil, nil)
	if c.IsHeartbeatFresh() {
		t.Error("IsHeartbeatFresh should be false when no heartbeat client is wired")
	}
}
