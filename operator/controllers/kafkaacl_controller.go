package controllers

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	v1alpha1 "github.com/woestebanaan/skafka/operator/api/v1alpha1"
)

const aclFinalizer = "skafka.io/acl-cleanup"

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
		Complete(r)
}

func (r *KafkaAclReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var acl v1alpha1.KafkaAcl
	if err := r.Get(ctx, req.NamespacedName, &acl); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Deletion path — rebuild ACLs without this object's entries.
	if !acl.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&acl, aclFinalizer) {
			if err := reconcileACLs(ctx, r.Client, r.Namespace, r.DataDir); err != nil {
				return ctrl.Result{}, err
			}
			controllerutil.RemoveFinalizer(&acl, aclFinalizer)
			if err := r.Update(ctx, &acl); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// Ensure finalizer.
	if !controllerutil.ContainsFinalizer(&acl, aclFinalizer) {
		controllerutil.AddFinalizer(&acl, aclFinalizer)
		if err := r.Update(ctx, &acl); err != nil {
			return ctrl.Result{}, err
		}
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
