package lease

import (
	"context"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
)

func newTestManager(client *fake.Clientset, podName string) *KubernetesLeaseManager {
	return NewKubernetesLeaseManager(client, "default", podName, nil, nil)
}

func TestLeaseNameSanitisation(t *testing.T) {
	cases := []struct {
		topic     string
		partition int32
		wantLen   bool // just check it's non-empty and ≤253
	}{
		{"payments.events", 0, true},
		{"PAYMENTS_EVENTS", 5, true},
		{"a/b/c", 99, true},
		{"a-very-long-topic-name-that-exceeds-normal-expectations-for-naming-conventions-in-kafka-systems", 0, true},
	}
	for _, tc := range cases {
		name := leaseName(tc.topic, tc.partition)
		if name == "" {
			t.Errorf("leaseName(%q,%d) = empty", tc.topic, tc.partition)
		}
		if len(name) > 253 {
			t.Errorf("leaseName(%q,%d) len=%d > 253", tc.topic, tc.partition, len(name))
		}
	}
}

func TestParseOrdinalFromIdentityLocal(t *testing.T) {
	cases := []struct{ in string; want int32 }{
		{"broker-0", 0},
		{"broker-3", 3},
		{"skafka-broker-12", 12},
		{"bad", -1},
	}
	for _, tc := range cases {
		got := parseOrdinalFromIdentity(tc.in)
		if got != tc.want {
			t.Errorf("parseOrdinalFromIdentity(%q)=%d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestAcquireBecomesLeader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fakeClient := fake.NewSimpleClientset()
	started := make(chan struct{})

	m := NewKubernetesLeaseManager(fakeClient, "default", "broker-0", nil, nil)
	m.SetOnStartedLeading(func(topic string, partition int32, epoch int64) {
		close(started)
	})

	if err := m.Acquire(ctx, "test-topic", 0); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("OnStartedLeading did not fire within 10s")
	}

	if !m.IsLeader("test-topic", 0) {
		t.Error("IsLeader should be true after OnStartedLeading")
	}
	if m.LeaderFor("test-topic", 0) != 0 {
		t.Errorf("LeaderFor=%d, want 0", m.LeaderFor("test-topic", 0))
	}
}

func TestReleaseStopsLeadership(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fakeClient := fake.NewSimpleClientset()
	started := make(chan struct{})
	stopped := make(chan struct{})

	m := NewKubernetesLeaseManager(fakeClient, "default", "broker-0", nil, nil)
	m.SetOnStartedLeading(func(_ string, _ int32, _ int64) { close(started) })
	m.SetOnStoppedLeading(func(_ string, _ int32) { close(stopped) })

	if err := m.Acquire(ctx, "test-topic", 0); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("OnStartedLeading did not fire")
	}

	if err := m.Release("test-topic", 0); err != nil {
		t.Fatalf("Release: %v", err)
	}

	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("OnStoppedLeading did not fire after Release")
	}

	if m.IsLeader("test-topic", 0) {
		t.Error("IsLeader should be false after Release")
	}
}

func TestIdempotentAcquire(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fakeClient := fake.NewSimpleClientset()
	m := newTestManager(fakeClient, "broker-0")

	// Calling Acquire twice should not start two goroutines.
	if err := m.Acquire(ctx, "topic", 0); err != nil {
		t.Fatalf("first Acquire: %v", err)
	}
	if err := m.Acquire(ctx, "topic", 0); err != nil {
		t.Fatalf("second Acquire: %v", err)
	}

	m.mu.Lock()
	n := len(m.cancels)
	m.mu.Unlock()
	if n != 1 {
		t.Errorf("expected 1 cancel func, got %d", n)
	}
}

func TestWatchLeadersReceivesChanges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fakeClient := fake.NewSimpleClientset()
	started := make(chan struct{})
	m := NewKubernetesLeaseManager(fakeClient, "default", "broker-0", nil, nil)
	m.SetOnStartedLeading(func(_ string, _ int32, _ int64) { close(started) })

	changes, err := m.WatchLeaders(ctx)
	if err != nil {
		t.Fatalf("WatchLeaders: %v", err)
	}

	if err := m.Acquire(ctx, "topic", 0); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("OnStartedLeading did not fire")
	}

	select {
	case lc := <-changes:
		if lc.Topic != "topic" || lc.Partition != 0 {
			t.Errorf("unexpected change: %+v", lc)
		}
	case <-time.After(5 * time.Second):
		t.Error("WatchLeaders channel did not receive a change")
	}
}
