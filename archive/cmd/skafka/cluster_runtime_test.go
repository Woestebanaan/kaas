package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/woestebanaan/skafka/pkg/kafkaapi"
)

// fakeBrokerSrc is a controller.BrokerSource we can mutate from tests.
type fakeBrokerSrc struct {
	mu      sync.Mutex
	brokers []string
}

func (f *fakeBrokerSrc) AliveBrokers() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.brokers))
	copy(out, f.brokers)
	return out
}

func (f *fakeBrokerSrc) set(brokers []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.brokers = brokers
}

// fakeSink captures UpdateAssignment calls for assertions.
type fakeSink struct{ ch chan kafkaapi.AssignmentChange }

func (f *fakeSink) UpdateAssignment(_ context.Context, change kafkaapi.AssignmentChange) error {
	f.ch <- change
	return nil
}

// TestNotifyTopicChangeForwardsToActiveLoop guards gh #74:
// when this broker holds the controller Lease (activeLoop set), a
// topic-watcher event must reach the AssignmentLoop's UpdateAssignment
// path so the controller recomputes assignment.json. Without this
// wiring, runtime KafkaTopic CRs never make it into the file and
// produces silently fail with NotLeaderOrFollowerException.
func TestNotifyTopicChangeForwardsToActiveLoop(t *testing.T) {
	f := &fakeSink{ch: make(chan kafkaapi.AssignmentChange, 1)}
	rt := &clusterRuntime{activeLoop: f}

	rt.NotifyTopicChange(context.Background(), kafkaapi.AssignmentReasonTopicCreated, "kperf")

	select {
	case got := <-f.ch:
		if got.Reason != kafkaapi.AssignmentReasonTopicCreated {
			t.Errorf("Reason = %q, want %q", got.Reason, kafkaapi.AssignmentReasonTopicCreated)
		}
		if got.Topic != "kperf" {
			t.Errorf("Topic = %q, want %q", got.Topic, "kperf")
		}
	case <-time.After(time.Second):
		t.Fatal("UpdateAssignment was not called within 1s")
	}
}

// TestNotifyTopicChangeIsNoopWhenNotController exercises the
// non-controller broker path: every broker runs the topic watcher but
// only the Lease holder should queue assignment changes. With a nil
// activeLoop the call must return cleanly without panic or side effect.
func TestNotifyTopicChangeIsNoopWhenNotController(t *testing.T) {
	rt := &clusterRuntime{} // activeLoop is nil
	rt.NotifyTopicChange(context.Background(), kafkaapi.AssignmentReasonTopicCreated, "kperf")
	// No panic, no observable effect — pass.
}

// TestNotifyTopicChangeIsNoopOnNilReceiver exercises single-broker
// dev mode where the v3 runtime never starts (rt is the zero value of
// the *clusterRuntime pointer in main.go).
func TestNotifyTopicChangeIsNoopOnNilReceiver(t *testing.T) {
	var rt *clusterRuntime
	rt.NotifyTopicChange(context.Background(), kafkaapi.AssignmentReasonTopicCreated, "kperf")
}

// TestWatchBrokerSetTriggersOnDeath guards gh #77:
// when a broker disappears from the alive set, the watcher must queue
// an UpdateAssignment with reason=BrokerDead so the controller
// recomputes the assignment. Before this wiring, killing a broker left
// its partitions stuck on the dead pod for ~30s (until the StatefulSet
// recreated the pod) — assignmentVersion never moved.
func TestWatchBrokerSetTriggersOnDeath(t *testing.T) {
	src := &fakeBrokerSrc{brokers: []string{"skafka-0", "skafka-1", "skafka-2"}}
	sink := &fakeSink{ch: make(chan kafkaapi.AssignmentChange, 4)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go watchBrokerSet(ctx, src, sink)

	// First tick captures the initial set with no diff, so we should
	// see no event yet. Wait briefly to give the goroutine a chance.
	time.Sleep(100 * time.Millisecond)

	// Drop a broker and wait for the next 2s tick to fire.
	src.set([]string{"skafka-0", "skafka-1"})

	select {
	case got := <-sink.ch:
		if got.Reason != kafkaapi.AssignmentReasonBrokerDead {
			t.Errorf("Reason = %q, want %q", got.Reason, kafkaapi.AssignmentReasonBrokerDead)
		}
		if got.BrokerID != "skafka-2" {
			t.Errorf("BrokerID = %q, want %q", got.BrokerID, "skafka-2")
		}
	case <-time.After(4 * time.Second):
		t.Fatal("watcher did not fire UpdateAssignment within 4s of broker removal")
	}
}

// TestWatchBrokerSetTriggersOnJoin confirms the mirror case — a broker
// being added (a pod coming back, a new replica being scaled in) also
// triggers a recompute so the new broker can pick up partitions.
func TestWatchBrokerSetTriggersOnJoin(t *testing.T) {
	src := &fakeBrokerSrc{brokers: []string{"skafka-0", "skafka-1"}}
	sink := &fakeSink{ch: make(chan kafkaapi.AssignmentChange, 4)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go watchBrokerSet(ctx, src, sink)
	time.Sleep(100 * time.Millisecond)

	src.set([]string{"skafka-0", "skafka-1", "skafka-2"})

	select {
	case got := <-sink.ch:
		if got.Reason != kafkaapi.AssignmentReasonBrokerJoined {
			t.Errorf("Reason = %q, want %q", got.Reason, kafkaapi.AssignmentReasonBrokerJoined)
		}
		if got.BrokerID != "skafka-2" {
			t.Errorf("BrokerID = %q, want %q", got.BrokerID, "skafka-2")
		}
	case <-time.After(4 * time.Second):
		t.Fatal("watcher did not fire UpdateAssignment within 4s of broker join")
	}
}
