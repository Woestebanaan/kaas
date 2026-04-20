package controllers

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	v1alpha1 "github.com/woestebanaan/skafka/operator/api/v1alpha1"
)

const kafkaClusterFinalizer = "skafka.io/kafkacluster-cleanup"

// External API group versions used by the reconciler via unstructured clients.
var (
	certificateGVK = schema.GroupVersionKind{
		Group:   "cert-manager.io",
		Version: "v1",
		Kind:    "Certificate",
	}
	tlsRouteGVK = schema.GroupVersionKind{
		Group:   "gateway.networking.k8s.io",
		Version: "v1alpha2",
		Kind:    "TLSRoute",
	}
)

// KafkaClusterReconciler reconciles the KafkaCluster CRD into the external
// listener resources: one Certificate, N per-broker Services, N per-broker TLSRoutes.
// The broker StatefulSet itself is managed by Helm (Phase 8); the reconciler only
// owns external-access plumbing that depends on runtime config (broker count, etc.).
type KafkaClusterReconciler struct {
	client.Client
	Namespace string
}

func NewKafkaClusterReconciler(c client.Client, namespace string) *KafkaClusterReconciler {
	return &KafkaClusterReconciler{Client: c, Namespace: namespace}
}

func (r *KafkaClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.KafkaCluster{}).
		Complete(r)
}

func (r *KafkaClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var cluster v1alpha1.KafkaCluster
	if err := r.Get(ctx, req.NamespacedName, &cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !cluster.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&cluster, kafkaClusterFinalizer) {
			if err := r.deleteExternalResources(ctx, &cluster); err != nil {
				return ctrl.Result{}, err
			}
			controllerutil.RemoveFinalizer(&cluster, kafkaClusterFinalizer)
			if err := r.Update(ctx, &cluster); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&cluster, kafkaClusterFinalizer) {
		controllerutil.AddFinalizer(&cluster, kafkaClusterFinalizer)
		if err := r.Update(ctx, &cluster); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	ext := cluster.Spec.Listeners.External
	if !ext.Enabled {
		// External listener disabled — tear down any previously-created resources.
		if err := r.deleteExternalResources(ctx, &cluster); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.updateReadyCondition(ctx, &cluster, metav1.ConditionTrue, "ExternalDisabled", "external listener disabled")
	}

	// 1. Certificate (cert-manager).
	if ext.TLS.CertManager.Enabled {
		if err := r.reconcileCertificate(ctx, &cluster); err != nil {
			_ = r.updateReadyCondition(ctx, &cluster, metav1.ConditionFalse, "CertificateError", err.Error())
			return ctrl.Result{}, err
		}
	}

	// 2. Per-broker Services (selecting by pod-name).
	for i := int32(0); i < cluster.Spec.Replicas; i++ {
		if err := r.reconcileBrokerService(ctx, &cluster, i); err != nil {
			_ = r.updateReadyCondition(ctx, &cluster, metav1.ConditionFalse, "ServiceError", err.Error())
			return ctrl.Result{}, err
		}
	}

	// 3. Per-broker TLSRoutes (matched by SNI hostname).
	if ext.Gateway.Enabled {
		for i := int32(0); i < cluster.Spec.Replicas; i++ {
			if err := r.reconcileBrokerTLSRoute(ctx, &cluster, i); err != nil {
				_ = r.updateReadyCondition(ctx, &cluster, metav1.ConditionFalse, "TLSRouteError", err.Error())
				return ctrl.Result{}, err
			}
		}
	}

	// 4. Status: bootstrap server list.
	cluster.Status.BootstrapServers = r.buildBootstrapServers(&cluster)
	if err := r.Status().Update(ctx, &cluster); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, r.updateReadyCondition(ctx, &cluster, metav1.ConditionTrue, "ExternalListenerReady",
		fmt.Sprintf("%d brokers advertised via external listener", cluster.Spec.Replicas))
}

func (r *KafkaClusterReconciler) reconcileCertificate(ctx context.Context, cluster *v1alpha1.KafkaCluster) error {
	name := cluster.Name + "-broker-tls"
	dnsNames := r.brokerHostnames(cluster)
	if h := cluster.Spec.Listeners.External.BootstrapHostname; h != "" {
		dnsNames = append(dnsNames, h)
	}

	cert := &unstructured.Unstructured{}
	cert.SetGroupVersionKind(certificateGVK)
	cert.SetName(name)
	cert.SetNamespace(cluster.Namespace)

	spec := map[string]interface{}{
		"secretName": name,
		"dnsNames":   toIfaceSlice(dnsNames),
		"issuerRef": map[string]interface{}{
			"name": cluster.Spec.Listeners.External.TLS.CertManager.IssuerRef.Name,
			"kind": cluster.Spec.Listeners.External.TLS.CertManager.IssuerRef.Kind,
		},
	}

	return r.applyUnstructured(ctx, cluster, cert, spec)
}

func (r *KafkaClusterReconciler) reconcileBrokerService(ctx context.Context, cluster *v1alpha1.KafkaCluster, ordinal int32) error {
	name := fmt.Sprintf("%s-broker-%d", cluster.Name, ordinal)
	port := cluster.Spec.Listeners.External.Port
	if port == 0 {
		port = 9093
	}

	desired := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeClusterIP,
			Selector: map[string]string{
				"app.kubernetes.io/name":                 "skafka",
				"app.kubernetes.io/instance":             cluster.Name,
				"statefulset.kubernetes.io/pod-name":     fmt.Sprintf("%s-%d", cluster.Name, ordinal),
			},
			Ports: []corev1.ServicePort{{
				Name:       "kafka-tls",
				Port:       port,
				TargetPort: intstr.FromString("kafka-tls"),
			}},
		},
	}

	var existing corev1.Service
	err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, &existing)
	if errors.IsNotFound(err) {
		_ = controllerutil.SetControllerReference(cluster, desired, r.Scheme())
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	existing.Spec.Selector = desired.Spec.Selector
	existing.Spec.Ports = desired.Spec.Ports
	return r.Update(ctx, &existing)
}

func (r *KafkaClusterReconciler) reconcileBrokerTLSRoute(ctx context.Context, cluster *v1alpha1.KafkaCluster, ordinal int32) error {
	name := fmt.Sprintf("%s-broker-%d", cluster.Name, ordinal)
	hostname := fmt.Sprintf(cluster.Spec.Listeners.External.HostnamePattern, ordinal)
	port := cluster.Spec.Listeners.External.Port
	if port == 0 {
		port = 9093
	}

	route := &unstructured.Unstructured{}
	route.SetGroupVersionKind(tlsRouteGVK)
	route.SetName(name)
	route.SetNamespace(cluster.Namespace)

	parentRef := map[string]interface{}{
		"name": cluster.Spec.Listeners.External.Gateway.GatewayRef.Name,
	}
	if ns := cluster.Spec.Listeners.External.Gateway.GatewayRef.Namespace; ns != "" {
		parentRef["namespace"] = ns
	}

	spec := map[string]interface{}{
		"hostnames": []interface{}{hostname},
		"parentRefs": []interface{}{parentRef},
		"rules": []interface{}{
			map[string]interface{}{
				"backendRefs": []interface{}{
					map[string]interface{}{
						"name": name,
						"port": int64(port),
					},
				},
			},
		},
	}

	return r.applyUnstructured(ctx, cluster, route, spec)
}

// applyUnstructured creates or updates an unstructured object and sets the given spec.
// Setting a controller reference on unstructured objects is skipped to avoid a direct
// cert-manager / Gateway API scheme dependency; ownership is tracked via labels instead.
func (r *KafkaClusterReconciler) applyUnstructured(ctx context.Context, cluster *v1alpha1.KafkaCluster, obj *unstructured.Unstructured, spec map[string]interface{}) error {
	// Tag with owner labels for cleanup tracking.
	labels := obj.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels["app.kubernetes.io/managed-by"] = "skafka-operator"
	labels["skafka.io/cluster"] = cluster.Name
	obj.SetLabels(labels)
	_ = unstructured.SetNestedMap(obj.Object, spec, "spec")

	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(obj.GroupVersionKind())
	err := r.Get(ctx, types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}, existing)
	if errors.IsNotFound(err) {
		return r.Create(ctx, obj)
	}
	if err != nil {
		return err
	}
	existing.Object["spec"] = obj.Object["spec"]
	existing.SetLabels(labels)
	return r.Update(ctx, existing)
}

// deleteExternalResources removes the Certificate, per-broker Services, and TLSRoutes
// that were previously created for this cluster.
func (r *KafkaClusterReconciler) deleteExternalResources(ctx context.Context, cluster *v1alpha1.KafkaCluster) error {
	// Certificate.
	cert := &unstructured.Unstructured{}
	cert.SetGroupVersionKind(certificateGVK)
	cert.SetName(cluster.Name + "-broker-tls")
	cert.SetNamespace(cluster.Namespace)
	_ = r.Delete(ctx, cert)

	// Services + TLSRoutes for each broker ordinal. Loop up to a reasonable upper
	// bound to catch cases where replica count was reduced mid-lifecycle.
	upperBound := cluster.Spec.Replicas
	if upperBound < 10 {
		upperBound = 10
	}
	for i := int32(0); i < upperBound; i++ {
		name := fmt.Sprintf("%s-broker-%d", cluster.Name, i)

		svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace}}
		_ = r.Delete(ctx, svc)

		route := &unstructured.Unstructured{}
		route.SetGroupVersionKind(tlsRouteGVK)
		route.SetName(name)
		route.SetNamespace(cluster.Namespace)
		_ = r.Delete(ctx, route)
	}
	return nil
}

func (r *KafkaClusterReconciler) brokerHostnames(cluster *v1alpha1.KafkaCluster) []string {
	pattern := cluster.Spec.Listeners.External.HostnamePattern
	hosts := make([]string, 0, cluster.Spec.Replicas)
	for i := int32(0); i < cluster.Spec.Replicas; i++ {
		hosts = append(hosts, fmt.Sprintf(pattern, i))
	}
	return hosts
}

func (r *KafkaClusterReconciler) buildBootstrapServers(cluster *v1alpha1.KafkaCluster) []string {
	port := cluster.Spec.Listeners.External.Port
	if port == 0 {
		port = 9093
	}
	hosts := r.brokerHostnames(cluster)
	out := make([]string, 0, len(hosts))
	for _, h := range hosts {
		out = append(out, fmt.Sprintf("%s:%d", h, port))
	}
	return out
}

func (r *KafkaClusterReconciler) updateReadyCondition(ctx context.Context, cluster *v1alpha1.KafkaCluster, status metav1.ConditionStatus, reason, message string) error {
	setCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:    "Ready",
		Status:  status,
		Reason:  reason,
		Message: message,
	})
	return r.Status().Update(ctx, cluster)
}

func toIfaceSlice(s []string) []interface{} {
	out := make([]interface{}, len(s))
	for i, v := range s {
		out[i] = v
	}
	return out
}
