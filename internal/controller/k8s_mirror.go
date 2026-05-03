package controller

import (
	"context"
	"fmt"
	"log/slog"
	"sort"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/woestebanaan/skafka/operator/api/v1alpha1"
	"github.com/woestebanaan/skafka/pkg/kafkaapi"
)

// DefaultMaxCRPartitions caps the partition count rendered into the
// KafkaClusterAssignments CR. Each PartitionAssignment serialises to
// roughly 100 bytes; 8000 partitions × 100 bytes ≈ 800KB, well under the
// Kubernetes 1MB object size cap with headroom for brokers/groups/metadata.
//
// Beyond this count, K8sMirror keeps the most-recently-changed partitions
// and sets status.truncated=true. Plan §"What about the
// KafkaClusterAssignments CR?": "the controller truncates the CR to the
// most relevant N partitions, sorted by recently changed."
const DefaultMaxCRPartitions = 8000

// K8sMirror implements CRMirror against a controller-runtime client. It
// writes the per-cluster KafkaClusterAssignments CR's Status with a
// snapshot of the assignment after every successful AssignmentStore.Write.
//
// Plan §"What about the KafkaClusterAssignments CR?": fire-and-forget.
// The file on the PVC is the source of truth; the CR is a kubectl-facing
// debugging convenience. Failures here are logged and ignored — they do
// not propagate back into the AssignmentLoop.
//
// One CR per cluster, sharing the KafkaCluster's name and namespace.
// Spec is empty; everything goes in Status. Brokers do NOT watch this CR.
//
// Truncation (size > 1MB Kubernetes object cap) lands in step 2; this
// skeleton always writes the full assignment.
type K8sMirror struct {
	client         client.Client
	namespace      string
	clusterName    string
	maxPartitions  int
}

// NewK8sMirror builds a mirror for the given (namespace, clusterName).
// clusterName must match the KafkaCluster CR's metadata.name so the
// KafkaClusterAssignments CR shares the same key — Plan §"What about
// the KafkaClusterAssignments CR?" line 388.
func NewK8sMirror(c client.Client, namespace, clusterName string) *K8sMirror {
	return &K8sMirror{
		client:         c,
		namespace:      namespace,
		clusterName:    clusterName,
		maxPartitions:  DefaultMaxCRPartitions,
	}
}

// WithMaxPartitions overrides the partition-count threshold above which
// the CR is truncated. Mostly a test hook; production should keep the
// default.
func (m *K8sMirror) WithMaxPartitions(n int) *K8sMirror {
	m.maxPartitions = n
	return m
}

// Mirror updates the CR's Status. Best-effort:
//   - Get the CR; if missing, log + skip (the operator owns creation —
//     see Phase 6 step 4). We don't create here because that risks the
//     mirror writing a CR without the proper ownerReferences back to the
//     KafkaCluster.
//   - On any other error: log + skip.
//   - On success: Status is replaced atomically. Partition list is
//     truncated to maxPartitions when over threshold; status.truncated
//     flags this for the kubectl debugger.
func (m *K8sMirror) Mirror(ctx context.Context, a *kafkaapi.Assignment) {
	if a == nil {
		return
	}

	key := types.NamespacedName{Namespace: m.namespace, Name: m.clusterName}
	var cr v1alpha1.KafkaClusterAssignments
	if err := m.client.Get(ctx, key, &cr); err != nil {
		if apierrors.IsNotFound(err) {
			slog.Debug("crmirror: assignments CR not found; operator should create it",
				"namespace", m.namespace, "name", m.clusterName)
			return
		}
		slog.Warn("crmirror: get failed",
			"namespace", m.namespace, "name", m.clusterName, "err", err)
		return
	}

	// Build "previous epochs" map from the existing CR Status so we can
	// prioritise partitions whose epoch changed since the last write.
	prevEpochs := make(map[string]int64, len(cr.Status.Partitions))
	for _, p := range cr.Status.Partitions {
		prevEpochs[partitionKey(p.Topic, p.Partition)] = p.Epoch
	}

	cr.Status = buildStatusTruncated(a, prevEpochs, m.maxPartitions)
	if err := m.client.Status().Update(ctx, &cr); err != nil {
		slog.Warn("crmirror: status update failed",
			"namespace", m.namespace, "name", m.clusterName,
			"controllerEpoch", a.ControllerEpoch,
			"assignmentVersion", a.AssignmentVersion,
			"err", err)
		return
	}
}

// buildStatus is the no-truncation path used when the assignment fits
// comfortably under the size cap. Pure function — trivially testable.
func buildStatus(a *kafkaapi.Assignment) v1alpha1.KafkaClusterAssignmentsStatus {
	return buildStatusTruncated(a, nil, 0)
}

// buildStatusTruncated translates a kafkaapi.Assignment into the CR
// Status. When max > 0 and len(a.Partitions) > max, the partition list
// is sorted with most-recently-changed first (using prevEpochs from the
// previously-mirrored CR) and truncated; status.truncated flags this.
//
// "Recently changed" = partition's epoch differs from prevEpochs (or
// the partition is absent from prevEpochs entirely — newly assigned).
// Within the changed/unchanged buckets, ordering is stable by
// (topic, partition) so two recompute passes with identical inputs
// produce identical CR content.
func buildStatusTruncated(
	a *kafkaapi.Assignment,
	prevEpochs map[string]int64,
	max int,
) v1alpha1.KafkaClusterAssignmentsStatus {
	status := v1alpha1.KafkaClusterAssignmentsStatus{
		ControllerEpoch:   a.ControllerEpoch,
		AssignmentVersion: a.AssignmentVersion,
		GeneratedAt:       a.GeneratedAt.UTC().Format("2006-01-02T15:04:05Z07:00"),
		Controller:        a.Controller,
	}

	status.Brokers = make([]v1alpha1.KafkaClusterBroker, 0, len(a.Brokers))
	for _, b := range a.Brokers {
		status.Brokers = append(status.Brokers, v1alpha1.KafkaClusterBroker{
			ID:       b.ID,
			Health:   string(b.Health),
			LastSeen: b.LastSeen.UTC().Format("2006-01-02T15:04:05Z07:00"),
		})
	}

	// Render every partition first so we can decide truncation against
	// the full list.
	rendered := make([]v1alpha1.KafkaClusterPartitionAssign, 0, len(a.Partitions))
	for _, p := range a.Partitions {
		rendered = append(rendered, v1alpha1.KafkaClusterPartitionAssign{
			Topic:     p.Topic,
			Partition: p.Partition,
			Broker:    p.Broker,
			Epoch:     int64(p.Epoch),
			Role:      string(p.Role),
		})
	}

	if max > 0 && len(rendered) > max {
		// Sort by (changed-first, topic asc, partition asc).
		sort.SliceStable(rendered, func(i, j int) bool {
			ci := isChanged(rendered[i], prevEpochs)
			cj := isChanged(rendered[j], prevEpochs)
			if ci != cj {
				return ci // changed entries come first
			}
			if rendered[i].Topic != rendered[j].Topic {
				return rendered[i].Topic < rendered[j].Topic
			}
			return rendered[i].Partition < rendered[j].Partition
		})
		rendered = rendered[:max]
		status.Truncated = true
	}
	status.Partitions = rendered

	status.ConsumerGroups = make([]v1alpha1.KafkaClusterConsumerGroupAssign, 0, len(a.ConsumerGroups))
	for _, g := range a.ConsumerGroups {
		status.ConsumerGroups = append(status.ConsumerGroups, v1alpha1.KafkaClusterConsumerGroupAssign{
			GroupID: g.GroupID,
			Broker:  g.Broker,
			Epoch:   int64(g.Epoch),
		})
	}

	return status
}

// isChanged returns true when p's epoch differs from the previously
// mirrored value (or wasn't previously mirrored at all). Used to
// prioritise partitions in the truncation-ordering tiebreak.
func isChanged(p v1alpha1.KafkaClusterPartitionAssign, prev map[string]int64) bool {
	if prev == nil {
		// No prior mirror data — every partition counts as "changed"
		// (initial population). Truncation falls back to plain
		// (topic, partition) ordering.
		return false
	}
	prevEpoch, ok := prev[partitionKey(p.Topic, p.Partition)]
	if !ok {
		return true
	}
	return prevEpoch != p.Epoch
}

// guard against the zero CR being treated as "exists but empty" by the
// fake client — also a sanity check that v1alpha1 imports stay aligned.
var _ = metav1.ObjectMeta{}

func (m *K8sMirror) String() string {
	return fmt.Sprintf("K8sMirror{ns=%s name=%s}", m.namespace, m.clusterName)
}

// Compile-time assertion that K8sMirror satisfies the CRMirror contract.
var _ CRMirror = (*K8sMirror)(nil)
