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
	if err := w.CreateTopic(ctx, "events", 3); err != nil {
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
	if err := w.CreateTopic(ctx, "events", 3); !errors.Is(err, handlers.ErrTopicAlreadyExists) {
		t.Errorf("re-create: err=%v, want ErrTopicAlreadyExists", err)
	}

	// Streams-style name forces the synthesised path.
	const streamsName = "KSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition"
	if err := w.CreateTopic(ctx, streamsName, 1); err != nil {
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
