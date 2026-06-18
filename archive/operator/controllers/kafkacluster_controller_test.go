package controllers

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/woestebanaan/skafka/operator/api/v1alpha1"
)

func newClusterScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(s)
	_ = corev1.AddToScheme(s)
	return s
}

func baseCluster() *v1alpha1.KafkaCluster {
	return &v1alpha1.KafkaCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "skafka", Namespace: "kafka"},
		Spec: v1alpha1.KafkaClusterSpec{
			Replicas: 3,
			Listeners: v1alpha1.KafkaClusterListeners{
				External: v1alpha1.KafkaClusterExternalListener{
					Enabled:         true,
					Port:            9093,
					HostnamePattern: "broker-%d.kafka.example.com",
					TLS: v1alpha1.KafkaClusterTLSConfig{
						CertManager: v1alpha1.KafkaClusterCertManagerConfig{
							Enabled: true,
							IssuerRef: v1alpha1.KafkaClusterIssuerRef{
								Name: "letsencrypt-prod",
								Kind: "ClusterIssuer",
							},
						},
					},
					Gateway: v1alpha1.KafkaClusterGatewayConfig{
						Enabled: true,
						GatewayRef: v1alpha1.KafkaClusterGatewayRef{
							Name:      "skafka-gateway",
							Namespace: "kafka",
						},
					},
				},
			},
		},
	}
}

// reconcileOnce drives the reconciler a single tick. The cluster reconciler
// no longer uses a finalizer (cleanup is handled by K8s GC via owner
// references), so the body runs on the first call.
func reconcileOnce(t *testing.T, r *KafkaClusterReconciler, ns, name string) {
	t.Helper()
	ctx := context.Background()
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
}

func TestKafkaClusterReconcileCreatesExternalResources(t *testing.T) {
	cluster := baseCluster()
	c := fake.NewClientBuilder().WithScheme(newClusterScheme()).WithObjects(cluster).WithStatusSubresource(cluster).Build()
	r := NewKafkaClusterReconciler(c, "kafka")

	reconcileOnce(t, r, "kafka", "skafka")

	// Certificate was created.
	cert := &unstructured.Unstructured{}
	cert.SetGroupVersionKind(certificateGVK)
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "kafka", Name: "skafka-broker-tls"}, cert); err != nil {
		t.Fatalf("Certificate not created: %v", err)
	}
	dnsNames, found, err := unstructured.NestedStringSlice(cert.Object, "spec", "dnsNames")
	if !found || err != nil {
		t.Fatalf("dnsNames missing: %v", err)
	}
	want := []string{"broker-0.kafka.example.com", "broker-1.kafka.example.com", "broker-2.kafka.example.com"}
	if len(dnsNames) != len(want) {
		t.Fatalf("dnsNames: got %v, want %v", dnsNames, want)
	}
	for i := range want {
		if dnsNames[i] != want[i] {
			t.Errorf("dnsNames[%d]=%q, want %q", i, dnsNames[i], want[i])
		}
	}

	// Per-broker Services were created.
	for i := 0; i < 3; i++ {
		name := "skafka-broker-" + string(rune('0'+i))
		var svc corev1.Service
		if err := c.Get(context.Background(), types.NamespacedName{Namespace: "kafka", Name: name}, &svc); err != nil {
			t.Errorf("Service %s: %v", name, err)
		}
		if svc.Spec.Selector["statefulset.kubernetes.io/pod-name"] != "skafka-"+string(rune('0'+i)) {
			t.Errorf("Service %s: selector mismatch: %v", name, svc.Spec.Selector)
		}
	}

	// Per-broker TLSRoutes were created.
	for i := 0; i < 3; i++ {
		route := &unstructured.Unstructured{}
		route.SetGroupVersionKind(tlsRouteGVK)
		name := "skafka-broker-" + string(rune('0'+i))
		if err := c.Get(context.Background(), types.NamespacedName{Namespace: "kafka", Name: name}, route); err != nil {
			t.Errorf("TLSRoute %s: %v", name, err)
			continue
		}
		hostnames, _, _ := unstructured.NestedStringSlice(route.Object, "spec", "hostnames")
		expected := []string{"broker-" + string(rune('0'+i)) + ".kafka.example.com"}
		if len(hostnames) != 1 || hostnames[0] != expected[0] {
			t.Errorf("TLSRoute %s: hostnames=%v, want %v", name, hostnames, expected)
		}
	}

	// Bootstrap servers status was populated.
	var updated v1alpha1.KafkaCluster
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "kafka", Name: "skafka"}, &updated); err != nil {
		t.Fatal(err)
	}
	if len(updated.Status.BootstrapServers) != 3 {
		t.Errorf("BootstrapServers=%v, want 3 entries", updated.Status.BootstrapServers)
	}
	if updated.Status.BootstrapServers[0] != "broker-0.kafka.example.com:9093" {
		t.Errorf("BootstrapServers[0]=%q", updated.Status.BootstrapServers[0])
	}
}

func TestKafkaClusterReconcileExternalDisabled(t *testing.T) {
	cluster := baseCluster()
	cluster.Spec.Listeners.External.Enabled = false
	c := fake.NewClientBuilder().WithScheme(newClusterScheme()).WithObjects(cluster).WithStatusSubresource(cluster).Build()
	r := NewKafkaClusterReconciler(c, "kafka")

	reconcileOnce(t, r, "kafka", "skafka")

	// No Certificate created.
	cert := &unstructured.Unstructured{}
	cert.SetGroupVersionKind(certificateGVK)
	err := c.Get(context.Background(), types.NamespacedName{Namespace: "kafka", Name: "skafka-broker-tls"}, cert)
	if !errors.IsNotFound(err) {
		t.Errorf("Certificate should not exist when external disabled: err=%v", err)
	}

	// Ready=True with ExternalDisabled reason.
	var updated v1alpha1.KafkaCluster
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "kafka", Name: "skafka"}, &updated)
	if len(updated.Status.Conditions) == 0 {
		t.Fatal("expected Ready condition")
	}
	cond := updated.Status.Conditions[0]
	if cond.Type != "Ready" || cond.Status != metav1.ConditionTrue || cond.Reason != "ExternalDisabled" {
		t.Errorf("condition: %+v", cond)
	}
}

func TestKafkaClusterBootstrapServers(t *testing.T) {
	cluster := baseCluster()
	cluster.Spec.Replicas = 5
	r := &KafkaClusterReconciler{}
	servers := r.buildBootstrapServers(cluster)
	if len(servers) != 5 {
		t.Fatalf("servers=%v", servers)
	}
	for i := 0; i < 5; i++ {
		want := "broker-" + string(rune('0'+i)) + ".kafka.example.com:9093"
		if servers[i] != want {
			t.Errorf("server[%d]=%q, want %q", i, servers[i], want)
		}
	}
}

func TestKafkaClusterBrokerHostnames(t *testing.T) {
	cluster := baseCluster()
	r := &KafkaClusterReconciler{}
	hosts := r.brokerHostnames(cluster)
	want := []string{"broker-0.kafka.example.com", "broker-1.kafka.example.com", "broker-2.kafka.example.com"}
	if len(hosts) != len(want) {
		t.Fatalf("hosts=%v want %v", hosts, want)
	}
	for i := range want {
		if hosts[i] != want[i] {
			t.Errorf("hosts[%d]=%q", i, hosts[i])
		}
	}
}

// Ensure the reconciler idempotently updates (second reconcile does not error).
func TestKafkaClusterReconcileIdempotent(t *testing.T) {
	cluster := baseCluster()
	c := fake.NewClientBuilder().WithScheme(newClusterScheme()).WithObjects(cluster).WithStatusSubresource(cluster).Build()
	r := NewKafkaClusterReconciler(c, "kafka")

	reconcileOnce(t, r, "kafka", "skafka")
	// Third reconcile should still succeed.
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "kafka", Name: "skafka"}}); err != nil {
		t.Errorf("third reconcile: %v", err)
	}
}

// Owned external resources (Certificate, broker Services, TLSRoutes) must
// each carry an ownerReference back to the KafkaCluster so K8s GC removes
// them when the cluster CR is deleted. Without this, deleting the cluster
// would leak those objects — the operator no longer runs a finalizer-driven
// cleanup pass on its own.
func TestKafkaClusterOwnedResourcesHaveOwnerRefs(t *testing.T) {
	cluster := baseCluster()
	cluster.UID = "cluster-uid-123"
	c := fake.NewClientBuilder().WithScheme(newClusterScheme()).WithObjects(cluster).WithStatusSubresource(cluster).Build()
	r := NewKafkaClusterReconciler(c, "kafka")

	reconcileOnce(t, r, "kafka", "skafka")

	hasOwner := func(refs []metav1.OwnerReference) bool {
		for _, ref := range refs {
			if ref.UID == cluster.UID && ref.Kind == "KafkaCluster" && ref.Controller != nil && *ref.Controller {
				return true
			}
		}
		return false
	}

	// Per-broker Services.
	for i := 0; i < 3; i++ {
		name := "skafka-broker-" + string(rune('0'+i))
		var svc corev1.Service
		if err := c.Get(context.Background(), types.NamespacedName{Namespace: "kafka", Name: name}, &svc); err != nil {
			t.Fatalf("get service %s: %v", name, err)
		}
		if !hasOwner(svc.OwnerReferences) {
			t.Errorf("service %s missing controller ownerRef on KafkaCluster", name)
		}
	}

	// Certificate (unstructured).
	cert := &unstructured.Unstructured{}
	cert.SetGroupVersionKind(certificateGVK)
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "kafka", Name: "skafka-broker-tls"}, cert); err != nil {
		t.Fatalf("get certificate: %v", err)
	}
	if !hasOwner(cert.GetOwnerReferences()) {
		t.Error("Certificate missing controller ownerRef on KafkaCluster")
	}

	// Per-broker TLSRoutes (unstructured).
	for i := 0; i < 3; i++ {
		route := &unstructured.Unstructured{}
		route.SetGroupVersionKind(tlsRouteGVK)
		name := "skafka-broker-" + string(rune('0'+i))
		if err := c.Get(context.Background(), types.NamespacedName{Namespace: "kafka", Name: name}, route); err != nil {
			t.Fatalf("get TLSRoute %s: %v", name, err)
		}
		if !hasOwner(route.GetOwnerReferences()) {
			t.Errorf("TLSRoute %s missing controller ownerRef on KafkaCluster", name)
		}
	}
}

var _ = client.Object((*v1alpha1.KafkaCluster)(nil)) // compile-time interface check

// TestKafkaClusterCreatesAssignmentsCR — Phase 6 step 4 — verifies the
// reconciler creates a matching KafkaClusterAssignments CR with an
// ownerReference back to the KafkaCluster, so deletion of the cluster
// cascades to the assignments CR.
func TestKafkaClusterCreatesAssignmentsCR(t *testing.T) {
	cluster := baseCluster()
	// Disable the external listener so the rest of the reconciler exits
	// early — this test focuses solely on the assignments CR creation.
	cluster.Spec.Listeners.External.Enabled = false

	c := fake.NewClientBuilder().
		WithScheme(newClusterScheme()).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	r := NewKafkaClusterReconciler(c, "kafka")

	reconcileOnce(t, r, "kafka", "skafka")

	var got v1alpha1.KafkaClusterAssignments
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "kafka", Name: "skafka"}, &got); err != nil {
		t.Fatalf("KafkaClusterAssignments CR not created: %v", err)
	}

	// Ownership: the KafkaCluster must be the controlling owner.
	if len(got.OwnerReferences) == 0 {
		t.Fatal("KafkaClusterAssignments CR has no ownerReferences")
	}
	owner := got.OwnerReferences[0]
	if owner.Kind != "KafkaCluster" || owner.Name != "skafka" {
		t.Errorf("ownerReference: got %s/%s, want KafkaCluster/skafka", owner.Kind, owner.Name)
	}
	if owner.Controller == nil || !*owner.Controller {
		t.Error("ownerReference.controller should be true (cascade delete)")
	}

	// Status starts empty — the elected broker controller fills it.
	if got.Status.AssignmentVersion != 0 {
		t.Errorf("Status should start empty; got AssignmentVersion=%d", got.Status.AssignmentVersion)
	}

	// Idempotent: a second reconcile shouldn't error or create a duplicate.
	reconcileOnce(t, r, "kafka", "skafka")
	var crList v1alpha1.KafkaClusterAssignmentsList
	if err := c.List(context.Background(), &crList, client.InNamespace("kafka")); err != nil {
		t.Fatal(err)
	}
	if len(crList.Items) != 1 {
		t.Errorf("idempotency: got %d KafkaClusterAssignments CRs, want 1", len(crList.Items))
	}
}
