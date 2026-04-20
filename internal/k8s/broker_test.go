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
