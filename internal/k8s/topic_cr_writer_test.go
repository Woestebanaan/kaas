package k8s

import (
	"context"
	"errors"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/woestebanaan/skafka/internal/protocol/handlers"
	v1alpha1 "github.com/woestebanaan/skafka/operator/api/v1alpha1"
)

func newFakeClient(t *testing.T) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).Build()
}

// TestNameForCR_PassthroughForValidNames pins the common case: a Kafka
// topic name that's already a valid RFC-1123 resource name lands on
// metadata.name as-is, and spec.topicName is left empty (per Strimzi's
// recommendation). This keeps the GitOps + kubectl-friendly experience
// for the 99% of topics that look like "events", "audit-log", etc.
func TestNameForCR_PassthroughForValidNames(t *testing.T) {
	for _, name := range []string{
		"events",
		"audit-log",
		"my.namespaced.topic",
		"my-topic-with-numbers-123",
		"a", // single char, valid
	} {
		t.Run(name, func(t *testing.T) {
			meta, topicName := nameForCR(name)
			if meta != name {
				t.Errorf("metadata.name=%q, want %q", meta, name)
			}
			if topicName != "" {
				t.Errorf("spec.topicName=%q, expected empty for RFC-1123-valid name", topicName)
			}
		})
	}
}

// TestNameForCR_SynthesisedForInvalidNames covers the gh #86 path:
// names that violate RFC 1123 (uppercase, underscores, leading/trailing
// dots, too long, etc.) get a deterministic synthetic metadata.name
// and the literal Kafka name lands in spec.topicName.
func TestNameForCR_SynthesisedForInvalidNames(t *testing.T) {
	cases := []string{
		"KSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition", // Streams internal
		"My_Important_Topic",            // underscores + uppercase
		"trailing.dot.",                 // ends with non-alnum
		".leading.dot",                  // starts with non-alnum
		"UPPER",                         // pure uppercase
		strings.Repeat("a", 254),        // too long for RFC 1123 subdomain (>253)
		"a/b",                           // slash not allowed
	}
	for _, name := range cases {
		t.Run(name[:min(20, len(name))], func(t *testing.T) {
			meta, topicName := nameForCR(name)
			if !strings.HasPrefix(meta, "skafka-topic-") {
				t.Errorf("metadata.name=%q, expected synthetic 'skafka-topic-' prefix", meta)
			}
			if !rfc1123.MatchString(meta) {
				t.Errorf("synthesised metadata.name %q is not RFC-1123-valid", meta)
			}
			if len(meta) > 253 {
				t.Errorf("synthesised metadata.name %q exceeds 253 chars", meta)
			}
			if topicName != name {
				t.Errorf("spec.topicName=%q, want %q (literal Kafka name)", topicName, name)
			}
		})
	}
}

// TestNameForCR_Deterministic guards the idempotency invariant:
// re-creating a topic with the same Kafka name MUST hit the same CR
// (so apierrors.IsAlreadyExists fires → handler returns
// TOPIC_ALREADY_EXISTS) rather than spawning a new CR each time. If
// the synthetic mapping ever picks up entropy (random bytes, time-
// based seed, etc.) this test fails.
func TestNameForCR_Deterministic(t *testing.T) {
	const name = "MyVery_Specific_Topic"
	a, _ := nameForCR(name)
	b, _ := nameForCR(name)
	if a != b {
		t.Errorf("nameForCR is non-deterministic: %q vs %q for %q", a, b, name)
	}
}

// TestTopicCRWriter_Roundtrip covers the full Create/Delete contract
// against a fake apiserver, exercising both the passthrough and
// synthesised-name paths.
func TestTopicCRWriter_Roundtrip(t *testing.T) {
	ctx := context.Background()
	cli := newFakeClient(t)
	w := NewTopicCRWriter(cli, "skafka")

	// Valid name — metadata.name is the Kafka name, spec.topicName empty.
	if err := w.CreateTopic(ctx, "events", 3, nil); err != nil {
		t.Fatalf("CreateTopic events: %v", err)
	}
	var got v1alpha1.KafkaTopic
	if err := cli.Get(ctx, client.ObjectKey{Namespace: "skafka", Name: "events"}, &got); err != nil {
		t.Fatalf("Get events: %v", err)
	}
	if got.Spec.TopicName != "" {
		t.Errorf("valid name: spec.topicName=%q, expected empty", got.Spec.TopicName)
	}
	if got.Spec.Partitions != 3 {
		t.Errorf("partitions=%d, want 3", got.Spec.Partitions)
	}

	// Re-create same valid name → AlreadyExists.
	if err := w.CreateTopic(ctx, "events", 3, nil); !errors.Is(err, handlers.ErrTopicAlreadyExists) {
		t.Errorf("re-create: err=%v, want ErrTopicAlreadyExists", err)
	}

	// Streams-style name forces the synthesised path.
	const streamsName = "KSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition"
	if err := w.CreateTopic(ctx, streamsName, 1, nil); err != nil {
		t.Fatalf("CreateTopic streams-style: %v", err)
	}
	syntheticMeta, _ := nameForCR(streamsName)
	if err := cli.Get(ctx, client.ObjectKey{Namespace: "skafka", Name: syntheticMeta}, &got); err != nil {
		t.Fatalf("Get synthetic: %v", err)
	}
	if got.Spec.TopicName != streamsName {
		t.Errorf("spec.topicName=%q, want %q", got.Spec.TopicName, streamsName)
	}
	if got.EffectiveTopicName() != streamsName {
		t.Errorf("EffectiveTopicName=%q, want %q", got.EffectiveTopicName(), streamsName)
	}

	// Delete via Kafka name resolves to the same synthetic CR.
	if err := w.DeleteTopic(ctx, streamsName); err != nil {
		t.Fatalf("DeleteTopic streams-style: %v", err)
	}
	err := cli.Get(ctx, client.ObjectKey{Namespace: "skafka", Name: syntheticMeta}, &got)
	if err == nil {
		t.Errorf("CR still present after delete via Kafka name")
	}

	// Delete missing → NotFound surfaces.
	if err := w.DeleteTopic(ctx, "never-existed"); !errors.Is(err, handlers.ErrTopicNotFound) {
		t.Errorf("delete missing: err=%v, want ErrTopicNotFound", err)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// TestTranslateConfigsKnownKeys is the gh #33 unit-level mapping:
// every Kafka-wire config name skafka understands gets stamped into
// the typed KafkaTopic CR Config field. Catches a typo in the key
// name (e.g. "cleanup_policy" vs "cleanup.policy" — Apache uses
// dotted) faster than a full integration round-trip.
func TestTranslateConfigsKnownKeys(t *testing.T) {
	cfg := translateConfigs(map[string]string{
		"cleanup.policy":         "compact",
		"retention.ms":           "604800000",
		"retention.bytes":        "1073741824",
		"segment.bytes":          "1048576",
		"min.compaction.lag.ms":  "60000",
		"delete.retention.ms":    "86400000",
	})
	if cfg.CleanupPolicy != "compact" {
		t.Errorf("CleanupPolicy=%q, want compact", cfg.CleanupPolicy)
	}
	if cfg.RetentionMs == nil || *cfg.RetentionMs != 604800000 {
		t.Errorf("RetentionMs=%v, want 604800000", cfg.RetentionMs)
	}
	if cfg.RetentionBytes == nil || *cfg.RetentionBytes != 1073741824 {
		t.Errorf("RetentionBytes=%v, want 1073741824", cfg.RetentionBytes)
	}
	if cfg.SegmentBytes == nil || *cfg.SegmentBytes != 1048576 {
		t.Errorf("SegmentBytes=%v, want 1048576", cfg.SegmentBytes)
	}
	if cfg.MinCompactionLagMs == nil || *cfg.MinCompactionLagMs != 60000 {
		t.Errorf("MinCompactionLagMs=%v, want 60000", cfg.MinCompactionLagMs)
	}
	if cfg.DeleteRetentionMs == nil || *cfg.DeleteRetentionMs != 86400000 {
		t.Errorf("DeleteRetentionMs=%v, want 86400000", cfg.DeleteRetentionMs)
	}
}

// TestTranslateConfigsUnknownKeysSilentlyDropped: a Streams client
// sends configs skafka doesn't understand (compression.type,
// message.format.version, etc.). The translation must silently
// ignore them — rejecting on unknown would break Streams' setUp,
// the exact gh #33 symptom we're closing.
func TestTranslateConfigsUnknownKeysSilentlyDropped(t *testing.T) {
	cfg := translateConfigs(map[string]string{
		"compression.type":         "lz4",
		"message.format.version":   "3.0",
		"unclean.leader.election.enable": "false",
		"cleanup.policy":           "delete", // recognised; should still land
	})
	if cfg.CleanupPolicy != "delete" {
		t.Errorf("CleanupPolicy=%q, want delete (recognised key didn't land)", cfg.CleanupPolicy)
	}
	// All other fields stay zero-valued — defense in depth, the
	// test should fail loudly if a refactor accidentally writes
	// unknown keys into stub fields.
	if cfg.RetentionMs != nil || cfg.SegmentBytes != nil {
		t.Errorf("unexpected non-nil stub fields: %+v", cfg)
	}
}

// TestTranslateConfigsMalformedNumericSkipsField: a malformed int
// value (truncated, non-numeric) shouldn't reject the create —
// just skip the field. Same liberal-acceptance reasoning as
// unknown keys.
func TestTranslateConfigsMalformedNumericSkipsField(t *testing.T) {
	cfg := translateConfigs(map[string]string{
		"retention.ms":  "not-a-number",
		"segment.bytes": "1048576",
	})
	if cfg.RetentionMs != nil {
		t.Errorf("RetentionMs=%v, want nil for malformed value", cfg.RetentionMs)
	}
	if cfg.SegmentBytes == nil || *cfg.SegmentBytes != 1048576 {
		t.Errorf("SegmentBytes=%v, want 1048576 (parallel valid key shouldn't be affected)", cfg.SegmentBytes)
	}
}

// TestTopicCRWriter_CreateWithConfigsLandsOnCR: end-to-end
// integration via the fake apiserver — configs passed to
// CreateTopic actually land on the CR's Spec.Config field. Catches
// the wiring gap between handler.translateConfigs and
// w.client.Create that the unit-level translateConfigs test alone
// can't cover.
func TestTopicCRWriter_CreateWithConfigsLandsOnCR(t *testing.T) {
	ctx := context.Background()
	cli := newFakeClient(t)
	w := NewTopicCRWriter(cli, "skafka")

	configs := map[string]string{
		"cleanup.policy": "compact",
		"segment.bytes":  "524288",
	}
	if err := w.CreateTopic(ctx, "compact-topic", 1, configs); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	var got v1alpha1.KafkaTopic
	if err := cli.Get(ctx, client.ObjectKey{Namespace: "skafka", Name: "compact-topic"}, &got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Spec.Config.CleanupPolicy != "compact" {
		t.Errorf("CR CleanupPolicy=%q, want compact", got.Spec.Config.CleanupPolicy)
	}
	if got.Spec.Config.SegmentBytes == nil || *got.Spec.Config.SegmentBytes != 524288 {
		t.Errorf("CR SegmentBytes=%v, want 524288", got.Spec.Config.SegmentBytes)
	}
}
