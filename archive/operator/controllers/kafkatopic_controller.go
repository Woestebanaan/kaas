package controllers

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/woestebanaan/skafka/internal/storage"
	v1alpha1 "github.com/woestebanaan/skafka/operator/api/v1alpha1"
)

// generateTopicUUID returns a canonical hyphenated v4-style UUID
// string for gh #105 KafkaTopic.Status.TopicID. Cryptographically
// random; matches Apache's KIP-516 contract that re-created topics
// get distinct IDs.
func generateTopicUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	// Set the version (4) and variant bits per RFC 4122.
	b[6] = (b[6] & 0x0f) | 0x40 // v4
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant
	hexS := hex.EncodeToString(b[:])
	return fmt.Sprintf("%s-%s-%s-%s-%s", hexS[0:8], hexS[8:12], hexS[12:16], hexS[16:20], hexS[20:32])
}

// clusterFilesDir is the reserved subdirectory under DataDir for cluster-wide
// files (assignment.json, credentials.json, acls.json). The topic sweep must
// never touch it.
const clusterFilesDir = "__cluster"

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
		Complete(Observed("KafkaTopic", r))
}

func (r *KafkaTopicReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var topic v1alpha1.KafkaTopic
	err := r.Get(ctx, req.NamespacedName, &topic)
	if apierrors.IsNotFound(err) {
		// CR is gone. Best-effort: drop the topic dir on the PVC. We
		// only have req.Name (metadata.name) — for the common case
		// where metadata.name == Kafka name (RFC-1123-valid topic),
		// this is the right path and cleanup succeeds. For synthetic-
		// metadata-name CRs (gh #86 — Kafka name uses uppercase /
		// underscores / etc) the real dir is at the Kafka name, which
		// req.Name doesn't carry; RemoveAll(req.Name) no-ops on a
		// non-existent dir and SweepTopics at the next operator
		// startup catches the orphan via EffectiveTopicName lookups.
		topicDir := filepath.Join(r.DataDir, req.Name)
		if e := os.RemoveAll(topicDir); e != nil && !os.IsNotExist(e) {
			return ctrl.Result{}, fmt.Errorf("remove topic dir: %w", e)
		}
		return ctrl.Result{}, nil
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	// A deletionTimestamp is set when something else (a stray finalizer,
	// foreground propagation policy, etc.) is keeping the object alive.
	// Without our own finalizer there is nothing for us to do here — the
	// NotFound branch above handles dir cleanup once the CR is fully gone.
	if !topic.DeletionTimestamp.IsZero() {
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

	// Partition dir paths use the on-wire Kafka topic name, not the
	// CR's metadata.name (gh #86 — admin-protocol topic creation
	// synthesises a hash-derived metadata.name and stores the literal
	// Kafka name in spec.topicName). EffectiveTopicName falls back to
	// metadata.name when spec.topicName is unset, so old CRs and
	// hand-written ones with valid names still resolve to the same
	// path as before.
	topicName := topic.EffectiveTopicName()

	// Create partition directories (idempotent).
	for p := int32(0); p < topic.Spec.Partitions; p++ {
		dir := filepath.Join(r.DataDir, topicName, strconv.Itoa(int(p)))
		if err := os.MkdirAll(dir, 0o775); err != nil {
			return ctrl.Result{}, fmt.Errorf("mkdir partition %d: %w", p, err)
		}
	}

	// Write per-topic config to /data/<topic>/.config.json so the broker
	// can pick up retentionBytes / retentionMs / segmentBytes etc. on next
	// partition open. Currently only retentionBytes is enforced by the
	// cleaner (gh #47); other fields are accepted but ignored.
	if err := storage.WriteTopicConfig(filepath.Join(r.DataDir, topicName), &storage.TopicConfigFile{
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
	// gh #105: assign a stable TopicID on first reconcile. Apache's
	// KIP-516 contract: random UUID per topic, distinct across
	// re-creations. Operator never rotates it once set; subsequent
	// reconciles preserve.
	if topic.Status.TopicID == "" {
		topic.Status.TopicID = generateTopicUUID()
	}
	setCondition(&topic.Status.Conditions, metav1.Condition{
		Type:    "Ready",
		Status:  metav1.ConditionTrue,
		Reason:  "PartitionsCreated",
		Message: fmt.Sprintf("%d partition directories created", topic.Spec.Partitions),
	})
	return ctrl.Result{}, r.Status().Update(ctx, &topic)
}

// SweepTopics removes /data/<topic>/ dirs that have no corresponding
// KafkaTopic CR. Called once at operator startup so dirs orphaned while the
// operator was down get cleaned up. Returns the names that were removed for
// logging; non-fatal errors are returned as a multi-error joined by errors.Join.
func SweepTopics(ctx context.Context, c client.Client, namespace, dataDir string) ([]string, error) {
	var topics v1alpha1.KafkaTopicList
	if err := c.List(ctx, &topics, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list KafkaTopics: %w", err)
	}
	keep := map[string]bool{clusterFilesDir: true}
	for _, t := range topics.Items {
		keep[t.EffectiveTopicName()] = true
	}

	entries, err := os.ReadDir(dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read data dir: %w", err)
	}

	var removed []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		if keep[name] || strings.HasPrefix(name, ".") {
			continue
		}
		path := filepath.Join(dataDir, name)
		if err := os.RemoveAll(path); err != nil && !os.IsNotExist(err) {
			return removed, fmt.Errorf("remove %s: %w", path, err)
		}
		removed = append(removed, name)
	}
	return removed, nil
}

func setCondition(conditions *[]metav1.Condition, cond metav1.Condition) {
	cond.LastTransitionTime = metav1.Now()
	meta.SetStatusCondition(conditions, cond)
}
