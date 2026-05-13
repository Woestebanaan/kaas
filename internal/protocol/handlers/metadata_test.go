package handlers

import (
	"context"
	"testing"

	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/lease"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

// ---- test helpers ----

type stubBrokerSource struct {
	self BrokerEndpoint
	all  []BrokerEndpoint
}

func (s stubBrokerSource) Self() BrokerEndpoint     { return s.self }
func (s stubBrokerSource) All() []BrokerEndpoint    { return s.all }

type stubLeaseManager struct{}

func (stubLeaseManager) Acquire(_ context.Context, _ string, _ int32) error { return nil }
func (stubLeaseManager) Release(_ string, _ int32) error                    { return nil }
func (stubLeaseManager) IsLeader(_ string, _ int32) bool                   { return true }
func (stubLeaseManager) LeaderFor(_ string, _ int32) int32                 { return 0 }
func (stubLeaseManager) WatchLeaders(_ context.Context) (<-chan lease.LeaderChange, error) {
	return make(chan lease.LeaderChange), nil
}

type stubTopics struct{}

func (stubTopics) Get(name string) (int32, bool) {
	if name == "t" {
		return 1, true
	}
	return 0, false
}
func (stubTopics) All() []TopicEntry { return []TopicEntry{{Name: "t", Partitions: 1}} }

// stubLeaderSource returns a fixed leader per partition; substitutable
// via a map. -1 means "unknown" (the gh #75 fallback path).
type stubLeaderSource struct{ leaders map[int32]int32 }

func (s stubLeaderSource) LeaderFor(_ string, partition int32) int32 {
	if v, ok := s.leaders[partition]; ok {
		return v
	}
	return -1
}

// multiPartTopics is a TopicSource for tests that want >1 partition.
type multiPartTopics struct {
	name       string
	partitions int32
}

func (m multiPartTopics) Get(name string) (int32, bool) {
	if name == m.name {
		return m.partitions, true
	}
	return 0, false
}
func (m multiPartTopics) All() []TopicEntry {
	return []TopicEntry{{Name: m.name, Partitions: m.partitions}}
}

// ---- tests ----

// Metadata response on the internal listener must advertise the internal headless DNS,
// not the external hostname.
func TestMetadataHandlerInternalListener(t *testing.T) {
	src := stubBrokerSource{
		self: BrokerEndpoint{
			NodeID:       0,
			Host:         "skafka-0.skafka-headless.kafka.svc",
			Port:         9092,
			ExternalHost: "broker-0.kafka.example.com",
			ExternalPort: 9093,
		},
		all: []BrokerEndpoint{
			{NodeID: 0, Host: "skafka-0.skafka-headless.kafka.svc", Port: 9092,
				ExternalHost: "broker-0.kafka.example.com", ExternalPort: 9093},
			{NodeID: 1, Host: "skafka-1.skafka-headless.kafka.svc", Port: 9092,
				ExternalHost: "broker-1.kafka.example.com", ExternalPort: 9093},
		},
	}
	h := NewMetadataHandlerWithSource(src, "test-cluster", stubTopics{}, stubLeaseManager{})

	resp := decodeMetadata(t, h, &connstate.ConnState{Listener: connstate.ListenerName("internal")})
	if len(resp.Brokers) != 2 {
		t.Fatalf("brokers=%d, want 2", len(resp.Brokers))
	}
	if resp.Brokers[0].Host != "skafka-0.skafka-headless.kafka.svc" {
		t.Errorf("internal listener got Host=%q, want internal DNS", resp.Brokers[0].Host)
	}
	if resp.Brokers[0].Port != 9092 {
		t.Errorf("internal listener got Port=%d, want 9092", resp.Brokers[0].Port)
	}
}

// Metadata response on the external listener must advertise per-broker external
// hostnames. This is the key behaviour that enables standard Kafka NOT_LEADER retry.
func TestMetadataHandlerExternalListener(t *testing.T) {
	src := stubBrokerSource{
		self: BrokerEndpoint{
			NodeID: 0, Host: "skafka-0.internal", Port: 9092,
			ExternalHost: "broker-0.kafka.example.com", ExternalPort: 9093,
		},
		all: []BrokerEndpoint{
			{NodeID: 0, Host: "skafka-0.internal", Port: 9092,
				ExternalHost: "broker-0.kafka.example.com", ExternalPort: 9093},
			{NodeID: 1, Host: "skafka-1.internal", Port: 9092,
				ExternalHost: "broker-1.kafka.example.com", ExternalPort: 9093},
			{NodeID: 2, Host: "skafka-2.internal", Port: 9092,
				ExternalHost: "broker-2.kafka.example.com", ExternalPort: 9093},
		},
	}
	h := NewMetadataHandlerWithSource(src, "test-cluster", stubTopics{}, stubLeaseManager{})

	resp := decodeMetadata(t, h, &connstate.ConnState{Listener: connstate.ListenerName("external"), IsTLS: true})
	if len(resp.Brokers) != 3 {
		t.Fatalf("brokers=%d, want 3", len(resp.Brokers))
	}
	for i, b := range resp.Brokers {
		wantHost := []string{
			"broker-0.kafka.example.com",
			"broker-1.kafka.example.com",
			"broker-2.kafka.example.com",
		}[i]
		if b.Host != wantHost {
			t.Errorf("broker[%d].Host=%q, want %q", i, b.Host, wantHost)
		}
		if b.Port != 9093 {
			t.Errorf("broker[%d].Port=%d, want 9093", i, b.Port)
		}
	}
}

// When ExternalHost is empty, the external listener falls back to the internal host.
// This covers the local-dev case and the backward-compatible BrokerInfo stub.
func TestMetadataHandlerExternalFallback(t *testing.T) {
	src := stubBrokerSource{
		self: BrokerEndpoint{NodeID: 0, Host: "localhost", Port: 9092},
		all:  []BrokerEndpoint{{NodeID: 0, Host: "localhost", Port: 9092}},
	}
	h := NewMetadataHandlerWithSource(src, "test", stubTopics{}, stubLeaseManager{})
	resp := decodeMetadata(t, h, &connstate.ConnState{Listener: connstate.ListenerName("external")})
	if resp.Brokers[0].Host != "localhost" {
		t.Errorf("fallback: Host=%q, want localhost", resp.Brokers[0].Host)
	}
}

// TestMetadataHandlerLeaderFromSource guards gh #75: the handler must
// route partition leadership lookups through PartitionLeaderSource and
// emit Replicas/ISR containing the leader (not the responding broker).
// Pre-fix, Replicas was always self.NodeID — that broke the Java
// AdminClient's listOffsets path with "Timed out waiting for a node
// assignment" because Leader was no longer in the Replicas list once
// Coordinator and Lease started disagreeing.
func TestMetadataHandlerLeaderFromSource(t *testing.T) {
	src := stubBrokerSource{
		self: BrokerEndpoint{NodeID: 0, Host: "skafka-0", Port: 9092},
		all: []BrokerEndpoint{
			{NodeID: 0, Host: "skafka-0", Port: 9092},
			{NodeID: 1, Host: "skafka-1", Port: 9092},
			{NodeID: 2, Host: "skafka-2", Port: 9092},
		},
	}
	topics := multiPartTopics{name: "kperf", partitions: 3}
	leaders := stubLeaderSource{leaders: map[int32]int32{0: 1, 1: 2, 2: 0}}
	h := NewMetadataHandlerWithSource(src, "test", topics, leaders)

	resp := decodeMetadata(t, h, &connstate.ConnState{Listener: connstate.ListenerName("internal")})
	if len(resp.Topics) != 1 {
		t.Fatalf("topics=%d, want 1", len(resp.Topics))
	}
	want := map[int32]int32{0: 1, 1: 2, 2: 0}
	for _, p := range resp.Topics[0].Partitions {
		expected := want[p.PartitionIndex]
		if p.LeaderID != expected {
			t.Errorf("partition %d Leader=%d, want %d (gh #75)", p.PartitionIndex, p.LeaderID, expected)
		}
		// Replicas/ISR must contain the leader so the Java AdminClient
		// can route listOffsets etc. to it.
		if len(p.ReplicaNodes) != 1 || p.ReplicaNodes[0] != expected {
			t.Errorf("partition %d Replicas=%v, want [%d]", p.PartitionIndex, p.ReplicaNodes, expected)
		}
		if len(p.IsrNodes) != 1 || p.IsrNodes[0] != expected {
			t.Errorf("partition %d Isr=%v, want [%d]", p.PartitionIndex, p.IsrNodes, expected)
		}
	}
}

// TestMetadataHandlerUnknownLeaderFallsBackToSelf covers the brief
// window between a topic's CR appearing and the controller's first
// recompute that includes it. LeaderFor returns -1; the response must
// stay well-formed so the client can retry on its next refresh.
func TestMetadataHandlerUnknownLeaderFallsBackToSelf(t *testing.T) {
	src := stubBrokerSource{
		self: BrokerEndpoint{NodeID: 7, Host: "skafka-7", Port: 9092},
		all:  []BrokerEndpoint{{NodeID: 7, Host: "skafka-7", Port: 9092}},
	}
	leaders := stubLeaderSource{leaders: map[int32]int32{}} // every lookup returns -1
	h := NewMetadataHandlerWithSource(src, "test", stubTopics{}, leaders)

	resp := decodeMetadata(t, h, &connstate.ConnState{Listener: connstate.ListenerName("internal")})
	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("unexpected topics shape: %+v", resp.Topics)
	}
	p := resp.Topics[0].Partitions[0]
	if p.LeaderID != -1 {
		t.Errorf("LeaderID=%d, want -1 when unknown", p.LeaderID)
	}
	if len(p.ReplicaNodes) != 1 || p.ReplicaNodes[0] != 7 {
		t.Errorf("Replicas=%v, want [7] (self fallback)", p.ReplicaNodes)
	}
}

// Nil ConnState defaults to the internal listener (existing server_test.go callers
// pass a zero-value ConnState — this must not break).
func TestMetadataHandlerNilConnState(t *testing.T) {
	src := stubBrokerSource{
		self: BrokerEndpoint{NodeID: 0, Host: "localhost", Port: 9092, ExternalHost: "external", ExternalPort: 9093},
		all:  []BrokerEndpoint{{NodeID: 0, Host: "localhost", Port: 9092, ExternalHost: "external", ExternalPort: 9093}},
	}
	h := NewMetadataHandlerWithSource(src, "test", stubTopics{}, stubLeaseManager{})

	resp := decodeMetadata(t, h, nil)
	if resp.Brokers[0].Host != "localhost" {
		t.Errorf("nil connstate: Host=%q, want internal localhost", resp.Brokers[0].Host)
	}
}

// TestMetadataHandlerEmitsClusterID guards gh #85: kafka-cluster.sh
// (and the AdminClient's describeCluster) reads the ClusterID off the
// Metadata v2+ response. If we ever stop threading the configured ID
// through to the response, the Java tool prints "Cluster ID: " with a
// trailing blank and existing infra (kafkactl, Kafbat) that keys
// on cluster ID will silently lose its anchor.
func TestMetadataHandlerEmitsClusterID(t *testing.T) {
	src := stubBrokerSource{
		self: BrokerEndpoint{NodeID: 0, Host: "skafka-0", Port: 9092},
		all:  []BrokerEndpoint{{NodeID: 0, Host: "skafka-0", Port: 9092}},
	}
	h := NewMetadataHandlerWithSource(src, "skafka-test-cluster", stubTopics{}, stubLeaseManager{})

	w := codec.NewWriter()
	w.WriteArray(0, func() {}) // empty Topics array (non-flexible v2)
	out, err := h.Handle(&connstate.ConnState{Listener: connstate.ListenerName("internal")}, 2, w.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	resp, err := api.DecodeMetadataResponse(codec.NewReader(out), 2)
	if err != nil {
		t.Fatal(err)
	}
	if resp.ClusterID != "skafka-test-cluster" {
		t.Errorf("ClusterID=%q, want %q", resp.ClusterID, "skafka-test-cluster")
	}
}

// TestMetadataHandlerClusterIDFlexible guards the same gh #85 behaviour
// on the modern flexible (v9+) wire format used by Java AdminClient and
// franz-go: the cluster ID is written as a compact nullable string,
// not the legacy nullable string. A regression that picks the wrong
// encoder would deserialize as null/garbage on the client side.
func TestMetadataHandlerClusterIDFlexible(t *testing.T) {
	src := stubBrokerSource{
		self: BrokerEndpoint{NodeID: 0, Host: "skafka-0", Port: 9092},
		all:  []BrokerEndpoint{{NodeID: 0, Host: "skafka-0", Port: 9092}},
	}
	h := NewMetadataHandlerWithSource(src, "skafka-flex-cluster", stubTopics{}, stubLeaseManager{})

	// v9 (flexible) request body: empty compact array of topics + empty
	// auto-create / authorized-ops flags + empty tagged fields.
	w := codec.NewWriter()
	w.WriteCompactArray(0, func() {})
	w.WriteInt8(0) // AllowAutoTopicCreation
	// v9 has IncludeClusterAuthorizedOperations + IncludeTopicAuthorizedOperations
	w.WriteInt8(0)
	w.WriteInt8(0)
	w.WriteEmptyTaggedFields()

	out, err := h.Handle(&connstate.ConnState{Listener: connstate.ListenerName("internal")}, 9, w.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	resp, err := api.DecodeMetadataResponse(codec.NewReader(out), 9)
	if err != nil {
		t.Fatalf("decode v9: %v", err)
	}
	if resp.ClusterID != "skafka-flex-cluster" {
		t.Errorf("v9 ClusterID=%q, want %q", resp.ClusterID, "skafka-flex-cluster")
	}
}

// decodeMetadata sends an empty Metadata v1 request through the handler and parses
// the response body.
func decodeMetadata(t *testing.T, h *MetadataHandler, conn *connstate.ConnState) *api.MetadataResponse {
	t.Helper()
	w := codec.NewWriter()
	w.WriteArray(0, func() {}) // empty Topics array (non-flexible v1)
	out, err := h.Handle(conn, 1, w.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	r := codec.NewReader(out)
	resp, err := api.DecodeMetadataResponse(r, 1)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}
