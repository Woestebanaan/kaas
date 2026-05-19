package k8s

import (
	"context"
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/woestebanaan/skafka/internal/protocol/handlers"
	v1alpha1 "github.com/woestebanaan/skafka/operator/api/v1alpha1"
)

// TestTopicCRWriter_ExpandTopic_GrowsPartitionCount pins gh #52:
// ExpandTopic on a 2-partition topic with newCount=5 must patch
// spec.partitions to 5 (operator's reconciler then creates the
// additional dirs).
func TestTopicCRWriter_ExpandTopic_GrowsPartitionCount(t *testing.T) {
	c := newFakeClient(t)
	w := NewTopicCRWriter(c, "skafka", ArgoCDConfig{})

	ctx := context.Background()
	if err := w.CreateTopic(ctx, "events", 2, nil); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if err := w.ExpandTopic(ctx, "events", 5); err != nil {
		t.Fatalf("ExpandTopic: %v", err)
	}

	var got v1alpha1.KafkaTopic
	if err := c.Get(ctx, types.NamespacedName{Namespace: "skafka", Name: "events"}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Partitions != 5 {
		t.Errorf("partitions=%d, want 5", got.Spec.Partitions)
	}
}

// TestTopicCRWriter_ExpandTopic_RejectsShrink pins the KIP-195
// contract: count can only grow. Apache surfaces the equal-or-smaller
// case as INVALID_PARTITIONS (37); we wrap it as
// handlers.ErrInvalidPartitionCount.
func TestTopicCRWriter_ExpandTopic_RejectsShrink(t *testing.T) {
	c := newFakeClient(t)
	w := NewTopicCRWriter(c, "skafka", ArgoCDConfig{})

	ctx := context.Background()
	if err := w.CreateTopic(ctx, "events", 4, nil); err != nil {
		t.Fatal(err)
	}
	cases := []int32{4, 3, 1, 0, -1}
	for _, n := range cases {
		err := w.ExpandTopic(ctx, "events", n)
		if !errors.Is(err, handlers.ErrInvalidPartitionCount) {
			t.Errorf("ExpandTopic(events, %d): err=%v, want ErrInvalidPartitionCount", n, err)
		}
	}

	// Sanity: the CR's count is unchanged at 4.
	var got v1alpha1.KafkaTopic
	if err := c.Get(ctx, types.NamespacedName{Namespace: "skafka", Name: "events"}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Partitions != 4 {
		t.Errorf("partitions=%d after rejected shrinks, want 4", got.Spec.Partitions)
	}
}

// TestTopicCRWriter_ExpandTopic_UnknownTopic pins the not-found
// path: ExpandTopic on a CR that doesn't exist returns
// handlers.ErrTopicNotFound (handler maps to UNKNOWN_TOPIC_OR_PARTITION).
func TestTopicCRWriter_ExpandTopic_UnknownTopic(t *testing.T) {
	c := newFakeClient(t)
	w := NewTopicCRWriter(c, "skafka", ArgoCDConfig{})

	err := w.ExpandTopic(context.Background(), "ghost", 5)
	if !errors.Is(err, handlers.ErrTopicNotFound) {
		t.Errorf("err=%v, want ErrTopicNotFound", err)
	}
}

// TestTopicCRWriter_ExpandTopic_SyntheticName covers the gh #86
// path: a topic with an RFC-1123-invalid Kafka name has a
// synthesised metadata.name; ExpandTopic must still resolve via
// nameForCR.
func TestTopicCRWriter_ExpandTopic_SyntheticName(t *testing.T) {
	c := newFakeClient(t)
	w := NewTopicCRWriter(c, "skafka", ArgoCDConfig{})

	const kafkaName = "MY_STREAMS_TOPIC" // uppercase + underscores → synthesised
	if err := w.CreateTopic(context.Background(), kafkaName, 2, nil); err != nil {
		t.Fatal(err)
	}
	if err := w.ExpandTopic(context.Background(), kafkaName, 6); err != nil {
		t.Fatalf("ExpandTopic synthetic-name: %v", err)
	}

	// Verify by listing — we don't know the synthetic name a priori
	// (it's a sha1 prefix), but there should be exactly one CR with
	// the matching topicName.
	var list v1alpha1.KafkaTopicList
	if err := c.List(context.Background(), &list); err != nil {
		t.Fatal(err)
	}
	var found *v1alpha1.KafkaTopic
	for i := range list.Items {
		t := &list.Items[i]
		if t.Spec.TopicName == kafkaName {
			found = t
			break
		}
	}
	if found == nil {
		t.Fatalf("no CR with spec.topicName=%q after ExpandTopic", kafkaName)
	}
	if found.Spec.Partitions != 6 {
		t.Errorf("partitions=%d, want 6", found.Spec.Partitions)
	}

	// Compile-time guard against metadata reflection regressions:
	// using the metav1 import keeps the linter happy in case the
	// production code paths above are inlined out.
	_ = metav1.ObjectMeta{}
}
