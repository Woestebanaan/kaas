package controllers

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/woestebanaan/skafka/internal/storage"
	v1alpha1 "github.com/woestebanaan/skafka/operator/api/v1alpha1"
)

const topicFinalizer = "skafka.io/topic-cleanup"

// KafkaTopicReconciler creates and removes partition directories on the shared PVC.
type KafkaTopicReconciler struct {
	client.Client
	DataDir string
}

func NewKafkaTopicReconciler(c client.Client, dataDir string) *KafkaTopicReconciler {
	return &KafkaTopicReconciler{Client: c, DataDir: dataDir}
}

func (r *KafkaTopicReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.KafkaTopic{}).
		Complete(r)
}

func (r *KafkaTopicReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var topic v1alpha1.KafkaTopic
	if err := r.Get(ctx, req.NamespacedName, &topic); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Deletion path.
	if !topic.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&topic, topicFinalizer) {
			topicDir := filepath.Join(r.DataDir, topic.Name)
			if err := os.RemoveAll(topicDir); err != nil && !os.IsNotExist(err) {
				return ctrl.Result{}, fmt.Errorf("remove topic dir: %w", err)
			}
			controllerutil.RemoveFinalizer(&topic, topicFinalizer)
			if err := r.Update(ctx, &topic); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// Ensure finalizer.
	if !controllerutil.ContainsFinalizer(&topic, topicFinalizer) {
		controllerutil.AddFinalizer(&topic, topicFinalizer)
		if err := r.Update(ctx, &topic); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Reject partition count decreases.
	if topic.Status.PartitionCount > 0 && topic.Spec.Partitions < topic.Status.PartitionCount {
		setCondition(&topic.Status.Conditions, metav1.Condition{
			Type:    "Ready",
			Status:  metav1.ConditionFalse,
			Reason:  "InvalidPartitionCount",
			Message: "reducing partition count is not supported",
		})
		return ctrl.Result{}, r.Status().Update(ctx, &topic)
	}

	// Create partition directories (idempotent).
	for p := int32(0); p < topic.Spec.Partitions; p++ {
		dir := filepath.Join(r.DataDir, topic.Name, strconv.Itoa(int(p)))
		if err := os.MkdirAll(dir, 0755); err != nil {
			return ctrl.Result{}, fmt.Errorf("mkdir partition %d: %w", p, err)
		}
	}

	// Write per-topic config to /data/<topic>/.config.json so the broker
	// can pick up retentionBytes / retentionMs / segmentBytes etc. on next
	// partition open. Currently only retentionBytes is enforced by the
	// cleaner (gh #47); other fields are accepted but ignored.
	if err := storage.WriteTopicConfig(filepath.Join(r.DataDir, topic.Name), &storage.TopicConfigFile{
		RetentionMs:        topic.Spec.Config.RetentionMs,
		RetentionBytes:     topic.Spec.Config.RetentionBytes,
		SegmentBytes:       topic.Spec.Config.SegmentBytes,
		CleanupPolicy:      topic.Spec.Config.CleanupPolicy,
		MinCompactionLagMs: topic.Spec.Config.MinCompactionLagMs,
		DeleteRetentionMs:  topic.Spec.Config.DeleteRetentionMs,
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("write topic config: %w", err)
	}

	// Update status.
	topic.Status.PartitionCount = topic.Spec.Partitions
	setCondition(&topic.Status.Conditions, metav1.Condition{
		Type:    "Ready",
		Status:  metav1.ConditionTrue,
		Reason:  "PartitionsCreated",
		Message: fmt.Sprintf("%d partition directories created", topic.Spec.Partitions),
	})
	return ctrl.Result{}, r.Status().Update(ctx, &topic)
}

func setCondition(conditions *[]metav1.Condition, cond metav1.Condition) {
	cond.LastTransitionTime = metav1.Now()
	meta.SetStatusCondition(conditions, cond)
}
