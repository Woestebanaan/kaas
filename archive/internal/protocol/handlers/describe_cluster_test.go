package handlers

import (
	"testing"

	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

// stubControllerID is a fixed-value provider for tests.
type stubControllerID int32

func (s stubControllerID) ControllerID() int32 { return int32(s) }

// encodeDescribeClusterRequest builds a wire-format request body for
// the given version. Mirrors how a real client encodes:
// flexibleVersions=0+ so all versions carry tagged-fields trailer.
func encodeDescribeClusterRequest(t *testing.T, includeAuthOps bool, endpointType int8, version int16) []byte {
	t.Helper()
	w := codec.NewWriter()
	if includeAuthOps {
		w.WriteInt8(1)
	} else {
		w.WriteInt8(0)
	}
	if version >= 1 {
		w.WriteInt8(endpointType)
	}
	w.WriteEmptyTaggedFields()
	return w.Bytes()
}

// decodeDescribeClusterResponse parses a response body for assertions.
func decodeDescribeClusterResponse(t *testing.T, body []byte, version int16) *api.DescribeClusterResponse {
	t.Helper()
	r := codec.NewReader(body)
	resp := &api.DescribeClusterResponse{}
	var err error
	resp.ThrottleTimeMs, err = r.ReadInt32()
	if err != nil {
		t.Fatalf("throttle: %v", err)
	}
	resp.ErrorCode, err = r.ReadInt16()
	if err != nil {
		t.Fatalf("errcode: %v", err)
	}
	resp.ErrorMessage, _, err = r.ReadCompactNullableString()
	if err != nil {
		t.Fatalf("errmsg: %v", err)
	}
	if version >= 1 {
		resp.EndpointType, err = r.ReadInt8()
		if err != nil {
			t.Fatalf("endpointType: %v", err)
		}
	}
	resp.ClusterID, err = r.ReadCompactString()
	if err != nil {
		t.Fatalf("clusterid: %v", err)
	}
	resp.ControllerID, err = r.ReadInt32()
	if err != nil {
		t.Fatalf("controllerid: %v", err)
	}
	if err := r.ReadCompactArray(func() error {
		var b api.DescribeClusterBroker
		b.BrokerID, err = r.ReadInt32()
		if err != nil {
			return err
		}
		b.Host, err = r.ReadCompactString()
		if err != nil {
			return err
		}
		b.Port, err = r.ReadInt32()
		if err != nil {
			return err
		}
		b.Rack, _, err = r.ReadCompactNullableString()
		if err != nil {
			return err
		}
		if err := r.ReadTaggedFields(); err != nil {
			return err
		}
		resp.Brokers = append(resp.Brokers, b)
		return nil
	}); err != nil {
		t.Fatalf("brokers: %v", err)
	}
	resp.ClusterAuthorizedOperations, err = r.ReadInt32()
	if err != nil {
		t.Fatalf("authops: %v", err)
	}
	if err := r.ReadTaggedFields(); err != nil {
		t.Fatalf("trailer: %v", err)
	}
	return resp
}

// stubBrokers is a tiny BrokerSource for tests.
type stubBrokers struct {
	self BrokerEndpoint
	all  []BrokerEndpoint
}

func (s stubBrokers) Self() BrokerEndpoint   { return s.self }
func (s stubBrokers) All() []BrokerEndpoint  { return s.all }

func TestDescribeClusterReturnsAllBrokersAndClusterID(t *testing.T) {
	src := stubBrokers{
		self: BrokerEndpoint{NodeID: 0, Host: "skafka-0", Port: 9092},
		all: []BrokerEndpoint{
			{NodeID: 0, Host: "skafka-0", Port: 9092},
			{NodeID: 1, Host: "skafka-1", Port: 9092},
			{NodeID: 2, Host: "skafka-2", Port: 9092},
		},
	}
	h := NewDescribeClusterHandler(src, "skafka-prod")

	body := encodeDescribeClusterRequest(t, false, 1, 1)
	out, err := h.Handle(&connstate.ConnState{Listener: connstate.ListenerName("internal")}, 1, body)
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	resp := decodeDescribeClusterResponse(t, out, 1)

	if resp.ErrorCode != 0 {
		t.Fatalf("errCode=%d errMsg=%q", resp.ErrorCode, resp.ErrorMessage)
	}
	if resp.ClusterID != "skafka-prod" {
		t.Errorf("ClusterID=%q, want skafka-prod", resp.ClusterID)
	}
	if resp.EndpointType != 1 {
		t.Errorf("EndpointType=%d, want 1 (brokers)", resp.EndpointType)
	}
	if len(resp.Brokers) != 3 {
		t.Fatalf("Brokers=%d entries, want 3", len(resp.Brokers))
	}
	for i, b := range resp.Brokers {
		if b.BrokerID != int32(i) {
			t.Errorf("brokers[%d].BrokerID=%d, want %d", i, b.BrokerID, i)
		}
	}
	// Apache default: Int32.MIN_VALUE signals "not requested" / "not authorized".
	// IncludeClusterAuthorizedOperations=false in this request.
	if resp.ClusterAuthorizedOperations != -2147483648 {
		t.Errorf("ClusterAuthorizedOperations=%d, want -2147483648 (not requested)", resp.ClusterAuthorizedOperations)
	}
}

func TestDescribeClusterControllerIDFallsBackToSelf(t *testing.T) {
	// No ControllerIDProvider wired — handler should fall back to Self().NodeID.
	src := stubBrokers{
		self: BrokerEndpoint{NodeID: 7, Host: "skafka-7", Port: 9092},
		all:  []BrokerEndpoint{{NodeID: 7, Host: "skafka-7", Port: 9092}},
	}
	h := NewDescribeClusterHandler(src, "test")

	out, err := h.Handle(nil, 0, encodeDescribeClusterRequest(t, false, 1, 0))
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	resp := decodeDescribeClusterResponse(t, out, 0)
	if resp.ControllerID != 7 {
		t.Fatalf("ControllerID=%d, want 7 (Self().NodeID fallback)", resp.ControllerID)
	}
}

func TestDescribeClusterControllerIDUsesProviderWhenWired(t *testing.T) {
	src := stubBrokers{
		self: BrokerEndpoint{NodeID: 0, Host: "skafka-0", Port: 9092},
		all:  []BrokerEndpoint{{NodeID: 0, Host: "skafka-0", Port: 9092}},
	}
	h := NewDescribeClusterHandler(src, "test").
		WithController(stubControllerID(2))

	out, err := h.Handle(nil, 1, encodeDescribeClusterRequest(t, false, 1, 1))
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	resp := decodeDescribeClusterResponse(t, out, 1)
	if resp.ControllerID != 2 {
		t.Fatalf("ControllerID=%d, want 2 (from provider)", resp.ControllerID)
	}
}

func TestDescribeClusterControllerIDFallbackOnNegativeProvider(t *testing.T) {
	// Provider returns -1 (no controller known yet, boot window).
	// Handler should fall back to Self() rather than emit -1, so the
	// AdminClient gets a usable answer instead of "no controller".
	src := stubBrokers{
		self: BrokerEndpoint{NodeID: 1, Host: "skafka-1", Port: 9092},
		all:  []BrokerEndpoint{{NodeID: 1, Host: "skafka-1", Port: 9092}},
	}
	h := NewDescribeClusterHandler(src, "test").
		WithController(stubControllerID(-1))

	out, err := h.Handle(nil, 1, encodeDescribeClusterRequest(t, false, 1, 1))
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	resp := decodeDescribeClusterResponse(t, out, 1)
	if resp.ControllerID != 1 {
		t.Fatalf("ControllerID=%d, want 1 (Self fallback when provider=-1)", resp.ControllerID)
	}
}

func TestDescribeClusterRejectsKRaftControllerEndpointType(t *testing.T) {
	// EndpointType=2 is the KRaft controller-quorum endpoint — non-goal
	// for skafka. Surface UNSUPPORTED_VERSION rather than a misleading
	// empty broker list which clients would interpret as "cluster down".
	src := stubBrokers{
		self: BrokerEndpoint{NodeID: 0, Host: "skafka-0", Port: 9092},
		all:  []BrokerEndpoint{{NodeID: 0, Host: "skafka-0", Port: 9092}},
	}
	h := NewDescribeClusterHandler(src, "test")

	out, err := h.Handle(nil, 1, encodeDescribeClusterRequest(t, false, 2, 1))
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	resp := decodeDescribeClusterResponse(t, out, 1)
	if resp.ErrorCode != int16(codec.ErrUnsupportedVersion) {
		t.Fatalf("ErrorCode=%d, want %d (UNSUPPORTED_VERSION for EndpointType=2)",
			resp.ErrorCode, int16(codec.ErrUnsupportedVersion))
	}
	if len(resp.Brokers) != 0 {
		t.Fatalf("expected no brokers in error response, got %d", len(resp.Brokers))
	}
}

func TestDescribeClusterPerListenerHostExternal(t *testing.T) {
	// On the external listener, brokers should advertise their
	// per-broker external hostname/port (gh #97 pattern).
	src := stubBrokers{
		self: BrokerEndpoint{NodeID: 0, Host: "skafka-0", Port: 9092, ExternalHost: "skafka-broker-0.kafka.example.com", ExternalPort: 9094},
		all: []BrokerEndpoint{
			{NodeID: 0, Host: "skafka-0", Port: 9092, ExternalHost: "skafka-broker-0.kafka.example.com", ExternalPort: 9094},
			{NodeID: 1, Host: "skafka-1", Port: 9092, ExternalHost: "skafka-broker-1.kafka.example.com", ExternalPort: 9094},
		},
	}
	h := NewDescribeClusterHandler(src, "test")

	out, err := h.Handle(&connstate.ConnState{Listener: connstate.ListenerName("external")}, 1,
		encodeDescribeClusterRequest(t, false, 1, 1))
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	resp := decodeDescribeClusterResponse(t, out, 1)
	if len(resp.Brokers) != 2 {
		t.Fatalf("got %d brokers", len(resp.Brokers))
	}
	if resp.Brokers[0].Host != "skafka-broker-0.kafka.example.com" || resp.Brokers[0].Port != 9094 {
		t.Errorf("brokers[0] external host/port=%s:%d, want skafka-broker-0.kafka.example.com:9094",
			resp.Brokers[0].Host, resp.Brokers[0].Port)
	}
}
