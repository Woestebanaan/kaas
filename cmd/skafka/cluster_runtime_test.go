package main

import (
	"context"
	"testing"
	"time"

	"github.com/woestebanaan/skafka/pkg/kafkaapi"
)

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
