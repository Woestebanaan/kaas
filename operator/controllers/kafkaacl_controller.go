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

// KafkaAclReconciler merges ACL rules into acls.json on the shared PVC.
type KafkaAclReconciler struct {
	client.Client
	DataDir   string
	Namespace string
}

func NewKafkaAclReconciler(c client.Client, dataDir, namespace string) *KafkaAclReconciler {
	return &KafkaAclReconciler{Client: c, DataDir: dataDir, Namespace: namespace}
}

func (r *KafkaAclReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.KafkaAcl{}).
		Complete(Observed("KafkaAcl", r))
}

func (r *KafkaAclReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var acl v1alpha1.KafkaAcl
	err := r.Get(ctx, req.NamespacedName, &acl)
	if apierrors.IsNotFound(err) {
		// CR is gone — rebuild acls.json without its entries. The rebuild
		// already iterates all current CRs, so the absence of this one
		// removes its rules naturally.
		return ctrl.Result{}, reconcileACLs(ctx, r.Client, r.Namespace, r.DataDir)
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	if !acl.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// Rebuild acls.json from all current objects.
	if err := reconcileACLs(ctx, r.Client, r.Namespace, r.DataDir); err != nil {
		setCondition(&acl.Status.Conditions, metav1.Condition{
			Type:    "Ready",
			Status:  metav1.ConditionFalse,
			Reason:  "ACLWriteError",
			Message: err.Error(),
		})
		_ = r.Status().Update(ctx, &acl)
		return ctrl.Result{}, err
	}

	acl.Status.AclCount = int32(len(acl.Spec.Rules))
	setCondition(&acl.Status.Conditions, metav1.Condition{
		Type:    "Ready",
		Status:  metav1.ConditionTrue,
		Reason:  "ACLsWritten",
		Message: fmt.Sprintf("%d rules for %s:%s", len(acl.Spec.Rules), acl.Spec.Principal.Kind, acl.Spec.Principal.Name),
	})
	return ctrl.Result{}, r.Status().Update(ctx, &acl)
}
