package broker

import "testing"

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
