package k8s

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/woestebanaan/skafka/internal/protocol/handlers"
	v1alpha1 "github.com/woestebanaan/skafka/operator/api/v1alpha1"
)

// TopicCRWriter implements handlers.TopicCRWriter against the
// KafkaTopic CR (gh #51). The broker's CreateTopics / DeleteTopics
// handlers call into this so admin-protocol topic ops persist as the
// same source of truth that GitOps / `kubectl apply -f` writes — the
// operator reconciles the CR into partition directories on the shared
// PVC, and every broker's TopicWatcher fires Added/Deleted, so a
// Metadata refresh from any peer sees the change immediately.
//
// Without this writer wired into the handlers (kafka-compat tests, dev
// mode without an apiserver), CreateTopics is best-effort local —
// writes the in-memory TopicRegistry on the broker that received the
// request and nothing else, which is fine for single-broker tests but
// invisible to peers in multi-broker production.
type TopicCRWriter struct {
	client    client.Client
	namespace string
}

// NewTopicCRWriter builds a writer bound to the given controller-
// runtime client and namespace. The Scheme on the client must have
// v1alpha1 registered.
func NewTopicCRWriter(c client.Client, namespace string) *TopicCRWriter {
	return &TopicCRWriter{client: c, namespace: namespace}
}

// CreateTopic creates a new KafkaTopic CR. Wraps apierrors.IsAlreadyExists
// in handlers.ErrTopicAlreadyExists so the handler can surface
// TOPIC_ALREADY_EXISTS to the client.
func (w *TopicCRWriter) CreateTopic(ctx context.Context, name string, partitions int32) error {
	t := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: w.namespace,
		},
		Spec: v1alpha1.KafkaTopicSpec{
			Partitions: partitions,
		},
	}
	if err := w.client.Create(ctx, t); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("%w: %s", handlers.ErrTopicAlreadyExists, name)
		}
		return fmt.Errorf("create KafkaTopic %s: %w", name, err)
	}
	return nil
}

// DeleteTopic removes a KafkaTopic CR by name. Wraps apierrors.IsNotFound
// in handlers.ErrTopicNotFound so the handler can surface
// UNKNOWN_TOPIC_OR_PARTITION.
func (w *TopicCRWriter) DeleteTopic(ctx context.Context, name string) error {
	t := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: w.namespace,
		},
	}
	if err := w.client.Delete(ctx, t); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("%w: %s", handlers.ErrTopicNotFound, name)
		}
		return fmt.Errorf("delete KafkaTopic %s: %w", name, err)
	}
	return nil
}
