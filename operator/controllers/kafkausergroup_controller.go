package controllers

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/woestebanaan/skafka/operator/api/v1alpha1"
)

// KafkaUserGroupReconciler expands group membership into ACL entries in acls.json.
type KafkaUserGroupReconciler struct {
	client.Client
	DataDir   string
	Namespace string
}

func NewKafkaUserGroupReconciler(c client.Client, dataDir, namespace string) *KafkaUserGroupReconciler {
	return &KafkaUserGroupReconciler{Client: c, DataDir: dataDir, Namespace: namespace}
}

func (r *KafkaUserGroupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.KafkaUserGroup{}).
		Complete(r)
}

func (r *KafkaUserGroupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var group v1alpha1.KafkaUserGroup
	err := r.Get(ctx, req.NamespacedName, &group)
	if apierrors.IsNotFound(err) {
		// CR is gone — rebuild acls.json. The rebuild iterates all
		// current CRs, so this group's rules drop out naturally.
		return ctrl.Result{}, reconcileACLs(ctx, r.Client, r.Namespace, r.DataDir)
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	if !group.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// Rebuild acls.json from all current objects.
	if err := reconcileACLs(ctx, r.Client, r.Namespace, r.DataDir); err != nil {
		setCondition(&group.Status.Conditions, metav1.Condition{
			Type:    "Ready",
			Status:  metav1.ConditionFalse,
			Reason:  "ACLWriteError",
			Message: err.Error(),
		})
		_ = r.Status().Update(ctx, &group)
		return ctrl.Result{}, err
	}

	group.Status.MemberCount = int32(len(group.Spec.Members))
	setCondition(&group.Status.Conditions, metav1.Condition{
		Type:    "Ready",
		Status:  metav1.ConditionTrue,
		Reason:  "ACLsWritten",
		Message: fmt.Sprintf("%d members, %d rules", len(group.Spec.Members), len(group.Spec.Rules)),
	})
	return ctrl.Result{}, r.Status().Update(ctx, &group)
}
