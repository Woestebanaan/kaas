package k8s

import (
	"os"
	"testing"
)

func TestNewBrokerIdentityParsesOrdinal(t *testing.T) {
	cases := []struct {
		podName  string
		ordinal  int32
		wantErr  bool
	}{
		{"broker-0", 0, false},
		{"broker-2", 2, false},
		{"skafka-broker-12", 12, false},
		{"", 0, true},      // MY_POD_NAME not set
		{"broker", 0, true}, // no ordinal suffix
	}
	for _, tc := range cases {
		t.Run(tc.podName, func(t *testing.T) {
			os.Setenv("MY_POD_NAME", tc.podName)
			defer os.Unsetenv("MY_POD_NAME")

			id, err := NewBrokerIdentity("test-ns", "skafka-headless", 9092)
			if tc.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if id.Ordinal != tc.ordinal {
				t.Errorf("Ordinal=%d, want %d", id.Ordinal, tc.ordinal)
			}
			if id.PodName != tc.podName {
				t.Errorf("PodName=%q, want %q", id.PodName, tc.podName)
			}
			if id.Namespace != "test-ns" {
				t.Errorf("Namespace=%q, want test-ns", id.Namespace)
			}
		})
	}
}

func TestNewBrokerIdentityBuildsFQDN(t *testing.T) {
	os.Setenv("MY_POD_NAME", "skafka-broker-3")
	defer os.Unsetenv("MY_POD_NAME")

	id, err := NewBrokerIdentity("kafka", "skafka-headless", 9092)
	if err != nil {
		t.Fatal(err)
	}
	want := "skafka-broker-3.skafka-headless.kafka.svc.cluster.local"
	if id.Host != want {
		t.Errorf("Host=%q, want %q", id.Host, want)
	}
	// DNS sub-struct must be populated so BrokerRegistry can reuse
	// the same shape for peer FQDNs (gh #97).
	if id.DNS.HeadlessSvc != "skafka-headless" {
		t.Errorf("DNS.HeadlessSvc=%q, want skafka-headless", id.DNS.HeadlessSvc)
	}
	if id.DNS.Namespace != "kafka" {
		t.Errorf("DNS.Namespace=%q, want kafka", id.DNS.Namespace)
	}
	if id.DNS.ClusterDomain != "cluster.local" {
		t.Errorf("DNS.ClusterDomain=%q, want cluster.local", id.DNS.ClusterDomain)
	}
}

// TestNewBrokerIdentityHonorsClusterDomainEnv: gh #97. SKAFKA_CLUSTER_DOMAIN
// overrides the default "cluster.local" — required for clusters whose
// CoreDNS uses a non-default suffix.
func TestNewBrokerIdentityHonorsClusterDomainEnv(t *testing.T) {
	os.Setenv("MY_POD_NAME", "skafka-1")
	os.Setenv("SKAFKA_CLUSTER_DOMAIN", "cluster.dev")
	defer os.Unsetenv("MY_POD_NAME")
	defer os.Unsetenv("SKAFKA_CLUSTER_DOMAIN")

	id, err := NewBrokerIdentity("skafka", "skafka-headless", 9092)
	if err != nil {
		t.Fatal(err)
	}
	want := "skafka-1.skafka-headless.skafka.svc.cluster.dev"
	if id.Host != want {
		t.Errorf("Host=%q, want %q", id.Host, want)
	}
	if id.DNS.ClusterDomain != "cluster.dev" {
		t.Errorf("DNS.ClusterDomain=%q, want cluster.dev", id.DNS.ClusterDomain)
	}
}

// TestDNSConfigFQDN pins the FQDN shape used by both BrokerIdentity
// (self) and BrokerRegistry (peers). Drift between the two would
// surface as a Metadata response inconsistency where self and peer
// hosts disagree on the DNS suffix — clients hit DNS NXDOMAIN.
func TestDNSConfigFQDN(t *testing.T) {
	d := DNSConfig{
		Namespace:     "skafka",
		HeadlessSvc:   "skafka-headless",
		ClusterDomain: "cluster.local",
	}
	cases := []struct{ pod, want string }{
		{"skafka-0", "skafka-0.skafka-headless.skafka.svc.cluster.local"},
		{"skafka-1", "skafka-1.skafka-headless.skafka.svc.cluster.local"},
		{"skafka-broker-12", "skafka-broker-12.skafka-headless.skafka.svc.cluster.local"},
	}
	for _, tc := range cases {
		got := d.FQDN(tc.pod)
		if got != tc.want {
			t.Errorf("FQDN(%q)=%q, want %q", tc.pod, got, tc.want)
		}
	}
}

func TestParseOrdinalFromIdentity(t *testing.T) {
	cases := []struct{ in string; want int32 }{
		{"broker-0", 0},
		{"broker-7", 7},
		{"skafka-broker-42", 42},
		{"no-digits", -1},
		{"", -1},
	}
	for _, tc := range cases {
		got := ParseOrdinalFromIdentity(tc.in)
		if got != tc.want {
			t.Errorf("ParseOrdinalFromIdentity(%q)=%d, want %d", tc.in, got, tc.want)
		}
	}
}
