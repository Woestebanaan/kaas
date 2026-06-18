package k8s

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/types"

	"github.com/woestebanaan/skafka/internal/protocol/handlers"
	v1alpha1 "github.com/woestebanaan/skafka/operator/api/v1alpha1"
)

// TestTopicCRWriter_UpdateTopicConfig_SetAndDelete pins gh #9: SET
// writes the typed field; DELETE clears it back to nil. The known
// scalar config keys (retention.ms, retention.bytes, segment.bytes,
// min.compaction.lag.ms, delete.retention.ms) all flow through.
func TestTopicCRWriter_UpdateTopicConfig_SetAndDelete(t *testing.T) {
	c := newFakeClient(t)
	w := NewTopicCRWriter(c, "skafka", ArgoCDConfig{})
	ctx := context.Background()

	if err := w.CreateTopic(ctx, "events", 1, nil); err != nil {
		t.Fatal(err)
	}

	// SET retention.ms = 60000.
	if err := w.UpdateTopicConfig(ctx, "events", []handlers.TopicConfigMutation{
		{Key: "retention.ms", Op: 0, Value: "60000"},
	}); err != nil {
		t.Fatalf("SET: %v", err)
	}
	var got v1alpha1.KafkaTopic
	_ = c.Get(ctx, types.NamespacedName{Namespace: "skafka", Name: "events"}, &got)
	if got.Spec.Config.RetentionMs == nil || *got.Spec.Config.RetentionMs != 60000 {
		t.Errorf("retention.ms=%v, want 60000", got.Spec.Config.RetentionMs)
	}

	// DELETE retention.ms.
	if err := w.UpdateTopicConfig(ctx, "events", []handlers.TopicConfigMutation{
		{Key: "retention.ms", Op: 1},
	}); err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	_ = c.Get(ctx, types.NamespacedName{Namespace: "skafka", Name: "events"}, &got)
	if got.Spec.Config.RetentionMs != nil {
		t.Errorf("retention.ms after DELETE=%v, want nil", got.Spec.Config.RetentionMs)
	}
}

// TestTopicCRWriter_UpdateTopicConfig_CleanupPolicyAppend pins
// APPEND semantics for the comma-list cleanup.policy: appending
// "compact" to "delete" produces "delete,compact"; appending
// "compact" again is a no-op.
func TestTopicCRWriter_UpdateTopicConfig_CleanupPolicyAppend(t *testing.T) {
	c := newFakeClient(t)
	w := NewTopicCRWriter(c, "skafka", ArgoCDConfig{})
	ctx := context.Background()

	if err := w.CreateTopic(ctx, "events", 1, map[string]string{"cleanup.policy": "delete"}); err != nil {
		t.Fatal(err)
	}
	if err := w.UpdateTopicConfig(ctx, "events", []handlers.TopicConfigMutation{
		{Key: "cleanup.policy", Op: 2, Value: "compact"}, // APPEND
	}); err != nil {
		t.Fatal(err)
	}
	var got v1alpha1.KafkaTopic
	_ = c.Get(ctx, types.NamespacedName{Namespace: "skafka", Name: "events"}, &got)
	if got.Spec.Config.CleanupPolicy != "delete,compact" {
		t.Errorf("after APPEND compact: got=%q, want 'delete,compact'", got.Spec.Config.CleanupPolicy)
	}

	// Idempotent: appending again is a no-op.
	if err := w.UpdateTopicConfig(ctx, "events", []handlers.TopicConfigMutation{
		{Key: "cleanup.policy", Op: 2, Value: "compact"},
	}); err != nil {
		t.Fatal(err)
	}
	_ = c.Get(ctx, types.NamespacedName{Namespace: "skafka", Name: "events"}, &got)
	if got.Spec.Config.CleanupPolicy != "delete,compact" {
		t.Errorf("after APPEND compact (second time): got=%q, want 'delete,compact'", got.Spec.Config.CleanupPolicy)
	}
}

// TestTopicCRWriter_UpdateTopicConfig_UnknownTopic pins not-found.
func TestTopicCRWriter_UpdateTopicConfig_UnknownTopic(t *testing.T) {
	c := newFakeClient(t)
	w := NewTopicCRWriter(c, "skafka", ArgoCDConfig{})

	err := w.UpdateTopicConfig(context.Background(), "ghost", []handlers.TopicConfigMutation{
		{Key: "retention.ms", Op: 0, Value: "1000"},
	})
	if err == nil {
		t.Fatal("expected error for unknown topic")
	}
}

// TestTopicCRWriter_UpdateTopicConfig_UnknownKeyIgnored pins the
// "be liberal in what you accept" semantic: unknown keys are
// silently dropped so clients sending a full config snapshot don't
// get rejected.
func TestTopicCRWriter_UpdateTopicConfig_UnknownKeyIgnored(t *testing.T) {
	c := newFakeClient(t)
	w := NewTopicCRWriter(c, "skafka", ArgoCDConfig{})
	ctx := context.Background()

	if err := w.CreateTopic(ctx, "events", 1, nil); err != nil {
		t.Fatal(err)
	}
	if err := w.UpdateTopicConfig(ctx, "events", []handlers.TopicConfigMutation{
		{Key: "unsupported.knob", Op: 0, Value: "x"},
		{Key: "retention.ms", Op: 0, Value: "5000"},
	}); err != nil {
		t.Fatalf("UpdateTopicConfig: %v", err)
	}
	var got v1alpha1.KafkaTopic
	_ = c.Get(ctx, types.NamespacedName{Namespace: "skafka", Name: "events"}, &got)
	if got.Spec.Config.RetentionMs == nil || *got.Spec.Config.RetentionMs != 5000 {
		t.Errorf("retention.ms=%v, want 5000 (known keys still applied)", got.Spec.Config.RetentionMs)
	}
}
