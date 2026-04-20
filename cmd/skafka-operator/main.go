package main

import (
	"flag"
	"os"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

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

	dataDir := envOr("SKAFKA_DATA_DIR", "/data")
	namespace := envOr("SKAFKA_NAMESPACE", "default")

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
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
