package k8s

import (
	"context"
	"errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/woestebanaan/skafka/internal/auth"
	"github.com/woestebanaan/skafka/internal/observability"
	v1alpha1 "github.com/woestebanaan/skafka/operator/api/v1alpha1"
)

// ErrKafkaUserNotFound is returned by KafkaUserWriter.UpdateQuotas when
// the AlterClientQuotas request names a user without a corresponding
// KafkaUser CR. AdminClient surfaces this through the wire-level error
// code the handler picks. Skafka does not auto-create KafkaUser CRs on
// quota set — that would mask typos and bypass the operator's
// credential-issuance pipeline.
var ErrKafkaUserNotFound = errors.New("kafka user CR not found")

// KafkaUserWriter implements handlers.KafkaUserWriter against the
// KafkaUser CR (gh #103 phase 2). The broker's AlterClientQuotas
// handler calls into this so admin-protocol quota mutations persist as
// the same source of truth that GitOps / `kubectl apply -f` writes —
// the operator's reconciler then materialises spec.quotas into
// credentials.json on the shared PVC, and every broker's
// CredentialLoader picks up the change via inotify hot-reload.
//
// Two-writer reconciliation: when an operator runs both `kubectl edit
// kafkauser/alice` and `kafka-configs.sh --alter ... --entity-name
// alice`, the latter wins because it's the most recent write to
// .spec.quotas. ArgoCD will report drift on the admin-protocol write;
// that's the intentional trade-off for letting the admin protocol
// reach the canonical store. Operators who don't want drift should
// stick to the CR.
//
// Annotations: argocd.argoproj.io/compare-options=IgnoreExtraneous is
// already stamped on KafkaUser CRs by the operator; this writer doesn't
// add or remove annotations. The runtime mutation path simply patches
// .spec.quotas in place.
type KafkaUserWriter struct {
	client    client.Client
	namespace string
}

// NewKafkaUserWriter binds a writer to the controller-runtime client
// and the namespace where KafkaUser CRs live (typically `skafka`,
// matching the broker's pod namespace).
func NewKafkaUserWriter(c client.Client, namespace string) *KafkaUserWriter {
	return &KafkaUserWriter{client: c, namespace: namespace}
}

// UpdateQuotas patches .spec.quotas on the KafkaUser CR named username.
// Pass q==nil to clear the quotas block entirely (revert to no quota).
// Wraps apierrors.IsNotFound as ErrKafkaUserNotFound so the handler can
// surface UNKNOWN_SERVER_ERROR rather than leaking a typed K8s error.
//
// Uses Get + Update rather than a JSON patch because the typed
// .spec.quotas field is a pointer; clearing it via patch would require
// a JSON-merge-patch with null which controller-runtime doesn't expose
// natively. The Get-then-Update sequence is racy under concurrent
// admin-protocol writes — last-write-wins, mirroring Apache's behavior.
func (w *KafkaUserWriter) UpdateQuotas(ctx context.Context, username string, q *auth.Quotas) error {
	var u v1alpha1.KafkaUser
	if err := observability.RecordK8sCall(ctx, "Get", "KafkaUser", func() error {
		return w.client.Get(ctx, types.NamespacedName{Namespace: w.namespace, Name: username}, &u)
	}); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("%w: %s", ErrKafkaUserNotFound, username)
		}
		return fmt.Errorf("get KafkaUser %s/%s: %w", w.namespace, username, err)
	}

	u.Spec.Quotas = translateQuotasToCR(q)
	if err := observability.RecordK8sCall(ctx, "Update", "KafkaUser", func() error {
		return w.client.Update(ctx, &u)
	}); err != nil {
		return fmt.Errorf("update KafkaUser %s/%s: %w", w.namespace, username, err)
	}
	return nil
}

// translateQuotasToCR mirrors auth.Quotas onto the v1alpha1 schema.
// Returns nil when every field is nil so a "remove all keys" alter
// clears the CR's quotas block instead of leaving a phantom empty
// struct that diff'd against the original spec.
func translateQuotasToCR(q *auth.Quotas) *v1alpha1.KafkaUserQuotas {
	if q == nil {
		return nil
	}
	if q.ProducerMaxByteRatePerBroker == nil && q.ConsumerMaxByteRatePerBroker == nil && q.RequestPercentage == nil {
		return nil
	}
	return &v1alpha1.KafkaUserQuotas{
		ProducerMaxByteRatePerBroker: q.ProducerMaxByteRatePerBroker,
		ConsumerMaxByteRatePerBroker: q.ConsumerMaxByteRatePerBroker,
		RequestPercentage:            q.RequestPercentage,
	}
}
