package broker

import (
	"testing"

	"github.com/woestebanaan/skafka/internal/protocol/handlers"
)

// TestCleanupPolicyClassification pins the gh #48 dispatch-key
// behavior: IsCompact / IsDelete decide which cleaner path a
// partition takes. The "compact,delete" combo runs both passes,
// matching Apache Kafka semantics and what Streams sets on
// changelog topics under EOS.
func TestCleanupPolicyClassification(t *testing.T) {
	cases := []struct {
		policy           CleanupPolicy
		wantCompact      bool
		wantDelete       bool
	}{
		{"", false, true},                      // unset = default = delete
		{CleanupPolicyDelete, false, true},
		{CleanupPolicyCompact, true, false},
		{CleanupPolicyCompactDelete, true, true},
		{"unknown-string", false, false},        // unknown fail-closed
	}
	for _, tc := range cases {
		t.Run(string(tc.policy), func(t *testing.T) {
			if got := tc.policy.IsCompact(); got != tc.wantCompact {
				t.Errorf("IsCompact()=%v, want %v", got, tc.wantCompact)
			}
			if got := tc.policy.IsDelete(); got != tc.wantDelete {
				t.Errorf("IsDelete()=%v, want %v", got, tc.wantDelete)
			}
		})
	}
}

// TestTopicRegistryAddPreservesPolicy: a CR-driven SetCleanupPolicy
// can arrive BEFORE the protocol-driven Add (ordering depends on
// which goroutine runs first). The registry must preserve the
// already-set policy when a later Add only knows the partition
// count. Without this, the cleaner could miss the compact dispatch
// for a topic whose CR was observed first.
func TestTopicRegistryAddPreservesPolicy(t *testing.T) {
	r := NewTopicRegistry()
	r.SetCleanupPolicy("foo", CleanupPolicyCompact)
	r.Add("foo", 3)

	if got := r.CleanupPolicy("foo"); got != CleanupPolicyCompact {
		t.Errorf("CleanupPolicy after Add=%q, want %q", got, CleanupPolicyCompact)
	}
	parts, ok := r.Get("foo")
	if !ok || parts != 3 {
		t.Errorf("Get partitions=%d ok=%v, want 3, true", parts, ok)
	}
}

// TestTopicRegistrySetCleanupPolicyAcceptsLateUpdate: CR mutation
// from delete to compact (operator-side or kubectl edit) must
// flip the registry so the next cleaner cycle dispatches
// correctly.
func TestTopicRegistrySetCleanupPolicyAcceptsLateUpdate(t *testing.T) {
	r := NewTopicRegistry()
	r.Add("foo", 1)

	if got := r.CleanupPolicy("foo"); got != "" {
		t.Errorf("default CleanupPolicy=%q, want empty (= treat as delete)", got)
	}
	r.SetCleanupPolicy("foo", CleanupPolicyCompact)
	if got := r.CleanupPolicy("foo"); got != CleanupPolicyCompact {
		t.Errorf("post-update CleanupPolicy=%q, want %q", got, CleanupPolicyCompact)
	}
	r.SetCleanupPolicy("foo", CleanupPolicyDelete)
	if got := r.CleanupPolicy("foo"); got != CleanupPolicyDelete {
		t.Errorf("post-revert CleanupPolicy=%q, want %q", got, CleanupPolicyDelete)
	}
}

// TestTopicRegistryCleanupPolicyForUnknownTopicSafe: querying a
// topic the registry doesn't know about returns "" (default =
// delete). Defense in depth — the cleaner should NEVER silently
// start compacting a topic the broker hasn't observed.
func TestTopicRegistryCleanupPolicyForUnknownTopicSafe(t *testing.T) {
	r := NewTopicRegistry()
	if got := r.CleanupPolicy("ghost"); got != "" {
		t.Errorf("unknown-topic CleanupPolicy=%q, want empty", got)
	}
	if !CleanupPolicy(r.CleanupPolicy("ghost")).IsDelete() {
		t.Error("unknown-topic should default to IsDelete()=true (fail-safe)")
	}
}

// TestTopicRegistrySetTopicConfigStoresAndReturns is the gh #93
// round-trip: SetTopicConfig writes the full per-topic CR config
// into the registry, and TopicConfig returns it as a value. Used
// by the DescribeConfigs handler's TopicConfigSource path.
func TestTopicRegistrySetTopicConfigStoresAndReturns(t *testing.T) {
	r := NewTopicRegistry()
	r.Add("foo", 1)
	rms := int64(86400000)
	sb := int64(524288)
	cfg := handlers.TopicConfig{
		CleanupPolicy: "compact",
		RetentionMs:   &rms,
		SegmentBytes:  &sb,
	}
	r.SetTopicConfig("foo", cfg)

	got, ok := r.TopicConfig("foo")
	if !ok {
		t.Fatalf("TopicConfig: ok=false, want true")
	}
	if got.CleanupPolicy != "compact" {
		t.Errorf("CleanupPolicy=%q, want compact", got.CleanupPolicy)
	}
	if got.RetentionMs == nil || *got.RetentionMs != 86400000 {
		t.Errorf("RetentionMs=%v, want 86400000", got.RetentionMs)
	}
	if got.SegmentBytes == nil || *got.SegmentBytes != 524288 {
		t.Errorf("SegmentBytes=%v, want 524288", got.SegmentBytes)
	}
	// And the cleaner-facing CleanupPolicy view stays consistent.
	if r.CleanupPolicy("foo") != CleanupPolicyCompact {
		t.Errorf("post-SetTopicConfig CleanupPolicy=%q, want compact (cleaner dispatch invariant)",
			r.CleanupPolicy("foo"))
	}
}

// TestTopicRegistryTopicConfigUnknownTopic: a TopicConfig for an
// unknown topic returns ok=false so the DescribeConfigs handler's
// fallthrough path activates (return broker defaults instead of a
// zero-value override pretending to be set). The handler doesn't
// rely on this — its UNKNOWN_TOPIC_OR_PARTITION error gates first
// — but the registry's contract should match what the interface
// promises.
func TestTopicRegistryTopicConfigUnknownTopic(t *testing.T) {
	r := NewTopicRegistry()
	got, ok := r.TopicConfig("ghost")
	if ok {
		t.Errorf("ok=true for unknown topic, want false")
	}
	if got != (handlers.TopicConfig{}) {
		t.Errorf("zero-value config expected, got %+v", got)
	}
}
