package handlers

import (
	"testing"

	"github.com/woestebanaan/skafka/internal/connstate"
)

// TestBrokerEndpointAddressForUsesListenerPorts pins gh #125: when a
// BrokerEndpoint has a ListenerPorts entry for the connection's
// listener, addressFor returns (Host, that port). Without this, a
// client that bootstraps on the authed listener (:9095) gets a
// Metadata response pointing at the default port (:9092) and the
// follow-up SCRAM handshake breaks because the anon listener's
// AllowAll engine returns an empty SCRAM first-message.
func TestBrokerEndpointAddressForUsesListenerPorts(t *testing.T) {
	ep := BrokerEndpoint{
		NodeID: 0,
		Host:   "broker.example.svc",
		Port:   9092,
		ListenerPorts: map[string]int32{
			"internal": 9092,
			"authed":   9095,
		},
	}

	cases := []struct {
		listener   connstate.ListenerName
		wantHost   string
		wantPort   int32
		annotation string
	}{
		{"internal", "broker.example.svc", 9092, "matches default"},
		{"authed", "broker.example.svc", 9095, "non-default port"},
		{"unknown", "broker.example.svc", 9092, "fallback to default Port"},
		{"", "broker.example.svc", 9092, "empty fallback"},
	}
	for _, tc := range cases {
		host, port := ep.addressFor(tc.listener)
		if host != tc.wantHost || port != tc.wantPort {
			t.Errorf("addressFor(%q): got (%s, %d), want (%s, %d) — %s",
				tc.listener, host, port, tc.wantHost, tc.wantPort, tc.annotation)
		}
	}
}

// TestBrokerEndpointAddressForExternalStillUsesExternalHost: the
// external listener path (ExternalHost/ExternalPort) still wins even
// when ListenerPorts has an "external" entry — external clients route
// through Gateway/TLSRoute via the per-broker FQDN, not the headless
// Service DNS.
func TestBrokerEndpointAddressForExternalStillUsesExternalHost(t *testing.T) {
	ep := BrokerEndpoint{
		NodeID:       0,
		Host:         "internal.svc",
		Port:         9092,
		ExternalHost: "broker-0.kafka.example.com",
		ExternalPort: 9093,
		ListenerPorts: map[string]int32{
			"internal": 9092,
			"external": 9094, // intentionally wrong — ExternalHost path should win
		},
	}
	host, port := ep.addressFor("external")
	if host != "broker-0.kafka.example.com" || port != 9093 {
		t.Errorf("external listener routed through ListenerPorts; got (%s, %d), want (broker-0.kafka.example.com, 9093)",
			host, port)
	}
}
