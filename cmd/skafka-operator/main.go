package main

import (
	"context"
	"flag"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/woestebanaan/skafka/internal/observability"
	v1alpha1 "github.com/woestebanaan/skafka/operator/api/v1alpha1"
	"github.com/woestebanaan/skafka/operator/controllers"
)

var scheme = runtime.NewScheme()

func init() {
	_ = corev1.AddToScheme(scheme)
	_ = v1alpha1.AddToScheme(scheme)
}

func main() {
	var metricsAddr string
	var probeAddr string
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "metrics endpoint address")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "health probe address")
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	observability.InstallLogger()

	bootstrapCtx, bootstrapCancel := context.WithCancel(context.Background())
	defer bootstrapCancel()
	obs, err := observability.Bootstrap(bootstrapCtx, "skafka-operator")
	if err != nil {
		ctrl.Log.Error(err, "observability bootstrap failed")
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = obs.Shutdown(shutdownCtx)
	}()

	dataDir := envOr("SKAFKA_DATA_DIR", "/data")
	namespace := envOr("SKAFKA_NAMESPACE", "default")

	// gh #117: restrict controller-runtime's cache to the operator's own
	// namespace. Without this, the cache starts cluster-scope Reflectors
	// for every type any reconciler touches (corev1.Secret, corev1.Service,
	// the CRDs) which fail RBAC because the operator's Role is
	// namespace-scoped by design. The reflectors then retry every ~30s
	// and spam the log with "Failed to watch *v1.Secret ... forbidden".
	//
	// Every resource the reconcilers touch lives in the operator's own
	// namespace: KafkaUser/Topic/ACL/UserGroup CRs (skafka namespace),
	// the credential Secrets they emit (same namespace as the owning
	// KafkaUser CR), and the per-broker Services KafkaClusterReconciler
	// creates (same namespace as the KafkaCluster CR). So a single-
	// namespace cache is correct AND minimal-privilege.
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		Cache: cache.Options{
			DefaultNamespaces: map[string]cache.Config{
				namespace: {},
			},
		},
	})
	if err != nil {
		ctrl.Log.Error(err, "unable to create manager")
		os.Exit(1)
	}

	if err := controllers.NewKafkaTopicReconciler(mgr.GetClient(), dataDir).SetupWithManager(mgr); err != nil {
		ctrl.Log.Error(err, "unable to set up KafkaTopic controller")
		os.Exit(1)
	}
	if err := controllers.NewKafkaUserReconciler(mgr.GetClient(), dataDir, namespace).SetupWithManager(mgr); err != nil {
		ctrl.Log.Error(err, "unable to set up KafkaUser controller")
		os.Exit(1)
	}
	if err := controllers.NewKafkaUserGroupReconciler(mgr.GetClient(), dataDir, namespace).SetupWithManager(mgr); err != nil {
		ctrl.Log.Error(err, "unable to set up KafkaUserGroup controller")
		os.Exit(1)
	}
	if err := controllers.NewKafkaClusterReconciler(mgr.GetClient(), namespace).SetupWithManager(mgr); err != nil {
		ctrl.Log.Error(err, "unable to set up KafkaCluster controller")
		os.Exit(1)
	}
	if err := controllers.NewKafkaAclReconciler(mgr.GetClient(), dataDir, namespace).SetupWithManager(mgr); err != nil {
		ctrl.Log.Error(err, "unable to set up KafkaAcl controller")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		ctrl.Log.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		ctrl.Log.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	// Once the manager's cache is in sync, run a one-shot GC pass for state
	// the (now finalizer-less) reconcilers can't reach: directories on the
	// PVC and entries in credentials.json that have no surviving CR. This
	// catches deletions that happened while the operator was down. ACL
	// state is rebuilt by the per-CR reconciles that fire as the cache
	// populates, so no separate sweep is needed there.
	if err := mgr.Add(startupSweepRunnable{
		client:    mgr.GetClient(),
		dataDir:   dataDir,
		namespace: namespace,
	}); err != nil {
		ctrl.Log.Error(err, "unable to register startup sweep")
		os.Exit(1)
	}

	ctrl.Log.Info("starting operator", "dataDir", dataDir, "namespace", namespace)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		ctrl.Log.Error(err, "manager exited with error")
		os.Exit(1)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// startupSweepRunnable runs once after the manager's cache is in sync,
// reconciling on-disk state with the surviving CRs: dropping topic
// directories on the PVC whose KafkaTopic CR is gone, and pruning
// credentials.json entries whose KafkaUser CR is gone. Implements
// controller-runtime's manager.Runnable so it gets the post-cache-sync
// timing for free; NeedLeaderElection ensures only one operator pod sweeps.
type startupSweepRunnable struct {
	client    ctrlclient.Client
	dataDir   string
	namespace string
}

func (s startupSweepRunnable) NeedLeaderElection() bool { return true }

func (s startupSweepRunnable) Start(ctx context.Context) error {
	log := ctrl.Log.WithName("startup-sweep")
	if removed, err := controllers.SweepTopics(ctx, s.client, s.namespace, s.dataDir); err != nil {
		log.Error(err, "topic sweep failed")
	} else if len(removed) > 0 {
		log.Info("removed orphan topic dirs", "names", removed)
	}
	if removed, err := controllers.SweepCredentials(ctx, s.client, s.namespace, s.dataDir); err != nil {
		log.Error(err, "credentials sweep failed")
	} else if len(removed) > 0 {
		log.Info("removed orphan credentials", "users", removed)
	}
	return nil
}
