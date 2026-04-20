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

// reconcileTwice: first call adds the finalizer, second call runs the reconciliation body.
func reconcileTwice(t *testing.T, r *KafkaClusterReconciler, ns, name string) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}}); err != nil {
			t.Fatalf("reconcile %d: %v", i, err)
		}
	}
}

func TestKafkaClusterReconcileCreatesExternalResources(t *testing.T) {
	cluster := baseCluster()
	c := fake.NewClientBuilder().WithScheme(newClusterScheme()).WithObjects(cluster).WithStatusSubresource(cluster).Build()
	r := NewKafkaClusterReconciler(c, "kafka")

	reconcileTwice(t, r, "kafka", "skafka")

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

	reconcileTwice(t, r, "kafka", "skafka")

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

	reconcileTwice(t, r, "kafka", "skafka")
	// Third reconcile should still succeed.
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "kafka", Name: "skafka"}}); err != nil {
		t.Errorf("third reconcile: %v", err)
	}
}

// Ensure deletion clears external resources and removes the finalizer.
func TestKafkaClusterReconcileDeletion(t *testing.T) {
	cluster := baseCluster()
	now := metav1.Now()
	cluster.DeletionTimestamp = &now
	cluster.Finalizers = []string{kafkaClusterFinalizer}

	c := fake.NewClientBuilder().WithScheme(newClusterScheme()).WithObjects(cluster).WithStatusSubresource(cluster).Build()
	r := NewKafkaClusterReconciler(c, "kafka")
	ctx := context.Background()

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "kafka", Name: "skafka"}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	// Finalizer removed.
	var updated v1alpha1.KafkaCluster
	err := c.Get(ctx, types.NamespacedName{Namespace: "kafka", Name: "skafka"}, &updated)
	if err != nil && !errors.IsNotFound(err) {
		t.Fatalf("get after delete: %v", err)
	}
	if err == nil {
		for _, f := range updated.Finalizers {
			if f == kafkaClusterFinalizer {
				t.Error("finalizer should be removed after delete")
			}
		}
	}
}

var _ = client.Object((*v1alpha1.KafkaCluster)(nil)) // compile-time interface check
