package k8s

import (
	"strings"
	"testing"

	discoveryv1 "k8s.io/api/discovery/v1"
)

// TestBrokerRegistryAdvertisesFQDNNotIP guards gh #97: peer brokers
// must be advertised by their stable per-broker Service FQDN
// (skafka-broker-N.skafka.svc.cluster.local), NOT the raw pod IP
// from EndpointSlice.Endpoints[].Addresses[0]. Pod IPs change
// across restarts; the Service's ClusterIP doesn't, so a producer's
// cached metadata stays valid.
func TestBrokerRegistryAdvertisesFQDNNotIP(t *testing.T) {
	dns := DNSConfig{
		Namespace:            "skafka",
		BrokerServicePattern: "skafka-broker-%d",
		ClusterDomain:        "cluster.local",
	}
	self := BrokerEndpoint{
		NodeID: 0,
		Host:   dns.FQDN(0),
		Port:   9092,
		Ready:  true,
	}
	r := NewBrokerRegistry(self, dns, nil)

	es := newEndpointSlice([]endpointSpec{
		{podName: "skafka-1", podIP: "10.42.0.55", ready: true},
		{podName: "skafka-2", podIP: "10.42.0.99", ready: true},
	})
	r.applySlice(es)

	all := r.All()
	if len(all) != 3 {
		t.Fatalf("expected 3 brokers (self + 2 peers), got %d: %+v", len(all), all)
	}
	for _, b := range all {
		if strings.Contains(b.Host, "10.42.") {
			t.Errorf("broker %d advertised raw pod IP %q — gh #97 contract broken", b.NodeID, b.Host)
		}
		if !strings.HasSuffix(b.Host, ".skafka.svc.cluster.local") {
			t.Errorf("broker %d host=%q doesn't match expected FQDN suffix", b.NodeID, b.Host)
		}
		if !strings.HasPrefix(b.Host, "skafka-broker-") {
			t.Errorf("broker %d host=%q doesn't start with the per-broker Service name", b.NodeID, b.Host)
		}
	}
}

// TestBrokerRegistryFallsBackToIPWhenDNSEmpty: tests that pass a
// zero DNSConfig still get a working registry — the legacy code
// path, used by tests that don't care about FQDNs and by
// hypothetical future non-StatefulSet deployments.
func TestBrokerRegistryFallsBackToIPWhenDNSEmpty(t *testing.T) {
	self := BrokerEndpoint{NodeID: 0, Host: "self-host", Port: 9092, Ready: true}
	r := NewBrokerRegistry(self, DNSConfig{}, nil)

	es := newEndpointSlice([]endpointSpec{
		{podName: "skafka-1", podIP: "10.42.0.55", ready: true},
	})
	r.applySlice(es)

	got := r.brokers[1]
	if got.Host != "10.42.0.55" {
		t.Errorf("with zero DNSConfig, expected raw IP fallback; got Host=%q", got.Host)
	}
}

// TestBrokerRegistryClusterDomainOverride: the cluster DNS suffix
// flows through to peer hosts. Required for clusters whose CoreDNS
// uses a non-default domain (rare — tracked here to guard the
// SKAFKA_CLUSTER_DOMAIN plumbing).
func TestBrokerRegistryClusterDomainOverride(t *testing.T) {
	dns := DNSConfig{
		Namespace:            "kafka",
		BrokerServicePattern: "skafka-broker-%d",
		ClusterDomain:        "cluster.dev",
	}
	self := BrokerEndpoint{NodeID: 0, Host: dns.FQDN(0), Port: 9092, Ready: true}
	r := NewBrokerRegistry(self, dns, nil)

	es := newEndpointSlice([]endpointSpec{
		{podName: "skafka-3", podIP: "10.0.0.7", ready: true},
	})
	r.applySlice(es)

	want := "skafka-broker-3.kafka.svc.cluster.dev"
	if got := r.brokers[3].Host; got != want {
		t.Errorf("peer host=%q, want %q (custom clusterDomain didn't propagate)", got, want)
	}
}

// TestBrokerRegistrySkipsUnreadyPeer: an unready endpoint shouldn't
// land in the registry. Existing behaviour, regression-pinned in
// the gh #97 context.
func TestBrokerRegistrySkipsUnreadyPeer(t *testing.T) {
	dns := DNSConfig{Namespace: "skafka", BrokerServicePattern: "skafka-broker-%d", ClusterDomain: "cluster.local"}
	self := BrokerEndpoint{NodeID: 0, Host: dns.FQDN(0), Port: 9092, Ready: true}
	r := NewBrokerRegistry(self, dns, nil)

	es := newEndpointSlice([]endpointSpec{
		{podName: "skafka-1", podIP: "10.42.0.55", ready: false},
	})
	r.applySlice(es)

	if _, present := r.brokers[1]; present {
		t.Errorf("unready peer landed in registry — should be filtered")
	}
}

// TestBrokerRegistryCustomServicePattern: the BrokerServicePattern
// in DNSConfig flows through to peer hosts. Required for
// non-default chart fullnames (e.g. multiple skafka clusters in
// one namespace via two Helm releases).
func TestBrokerRegistryCustomServicePattern(t *testing.T) {
	dns := DNSConfig{
		Namespace:            "skafka",
		BrokerServicePattern: "skafka-stage-broker-%d",
		ClusterDomain:        "cluster.local",
	}
	self := BrokerEndpoint{NodeID: 0, Host: dns.FQDN(0), Port: 9092, Ready: true}
	r := NewBrokerRegistry(self, dns, nil)

	es := newEndpointSlice([]endpointSpec{
		{podName: "skafka-stage-2", podIP: "10.0.0.5", ready: true},
	})
	r.applySlice(es)

	want := "skafka-stage-broker-2.skafka.svc.cluster.local"
	if got := r.brokers[2].Host; got != want {
		t.Errorf("peer host=%q, want %q (custom service pattern didn't propagate)", got, want)
	}
}

// --- test helpers ---

type endpointSpec struct {
	podName string
	podIP   string
	ready   bool
}

func newEndpointSlice(specs []endpointSpec) *discoveryv1.EndpointSlice {
	portName := "kafka"
	port := int32(9092)
	es := &discoveryv1.EndpointSlice{
		Ports: []discoveryv1.EndpointPort{
			{Name: &portName, Port: &port},
		},
	}
	for _, sp := range specs {
		hostname := sp.podName
		ready := sp.ready
		es.Endpoints = append(es.Endpoints, discoveryv1.Endpoint{
			Hostname:   &hostname,
			Addresses:  []string{sp.podIP},
			Conditions: discoveryv1.EndpointConditions{Ready: &ready},
		})
	}
	return es
}
