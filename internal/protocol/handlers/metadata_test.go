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

	resp := decodeMetadata(t, h, &connstate.ConnState{Listener: connstate.ListenerInternal})
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

	resp := decodeMetadata(t, h, &connstate.ConnState{Listener: connstate.ListenerExternal, IsTLS: true})
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
	resp := decodeMetadata(t, h, &connstate.ConnState{Listener: connstate.ListenerExternal})
	if resp.Brokers[0].Host != "localhost" {
		t.Errorf("fallback: Host=%q, want localhost", resp.Brokers[0].Host)
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
