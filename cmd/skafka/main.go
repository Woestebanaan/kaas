package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	sigs_client "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/woestebanaan/skafka/internal/auth"
	"github.com/woestebanaan/skafka/internal/broker"
	"github.com/woestebanaan/skafka/internal/coordinator"
	k8spkg "github.com/woestebanaan/skafka/internal/k8s"
	"github.com/woestebanaan/skafka/internal/lease"
	"github.com/woestebanaan/skafka/internal/lock"
	"github.com/woestebanaan/skafka/internal/protocol"
	"github.com/woestebanaan/skafka/internal/protocol/handlers"
	"github.com/woestebanaan/skafka/internal/storage"
	operatorv1 "github.com/woestebanaan/skafka/operator/api/v1alpha1"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	if len(os.Args) > 1 && os.Args[1] == "--init" {
		runInit(ctx)
		return
	}
	runBroker(ctx)
}

func runBroker(ctx context.Context) {
	host := envOr("SKAFKA_HOST", "0.0.0.0")
	port := envOr("SKAFKA_PORT", "9092")
	portNum := int32(9092)
	if p, err := strconv.Atoi(port); err == nil {
		portNum = int32(p)
	}
	clusterID := envOr("SKAFKA_CLUSTER_ID", "skafka-local")
	dataDir := os.Getenv("SKAFKA_DATA_DIR")
	namespace := envOr("SKAFKA_NAMESPACE", "default")
	headlessSvc := envOr("SKAFKA_HEADLESS_SVC", "skafka-headless")

	var (
		leaseManager lease.LeaseManager
		partLock     lock.PartitionLock
		brokerSource handlers.BrokerSource
		brokerID     int32
		k8sClient    kubernetes.Interface
		brokerReg    *k8spkg.BrokerRegistry
		coordMgr     *coordinator.Manager
	)

	k8sMode := os.Getenv("MY_POD_NAME") != ""
	if k8sMode {
		var err error
		k8sClient, err = buildK8sClient()
		if err != nil {
			slog.Error("build k8s client", "err", err)
			os.Exit(1)
		}

		identity, err := k8spkg.NewBrokerIdentity(namespace, headlessSvc, portNum)
		if err != nil {
			slog.Error("broker identity", "err", err)
			os.Exit(1)
		}
		brokerID = identity.Ordinal

		self := k8spkg.BrokerEndpoint{NodeID: identity.Ordinal, Host: identity.Host, Port: portNum, Ready: true}
		brokerReg = k8spkg.NewBrokerRegistry(self, nil)
		go func() {
			if err := brokerReg.Watch(ctx, k8sClient, namespace, headlessSvc); err != nil && ctx.Err() == nil {
				slog.Error("endpoint watcher stopped", "err", err)
			}
		}()
		src := broker.NewK8sBrokerSource(brokerReg)
		if pattern := os.Getenv("EXTERNAL_HOSTNAME_PATTERN"); pattern != "" {
			src.ExtHostPattern = pattern
			extPort := int32(9093)
			if p, err := strconv.Atoi(envOr("SKAFKA_TLS_PORT", "9093")); err == nil {
				extPort = int32(p)
			}
			src.ExtPort = extPort
		}
		brokerSource = src
		partLock = lock.NewFlockLock(dataDir)

		// Callbacks are wired after DiskStorageEngine is created; use placeholders for now.
		leaseManager = lease.NewKubernetesLeaseManager(k8sClient, namespace, identity.PodName, nil, nil)

		slog.Info("kubernetes mode",
			"pod", identity.PodName, "ordinal", identity.Ordinal, "namespace", namespace)
	} else {
		brokerID = 0
		leaseManager = broker.NewLocalLeaseManager()
		partLock = broker.NewLocalPartitionLock()
		brokerSource = handlers.BrokerInfo{NodeID: 0, Host: host, Port: portNum, ClusterID: clusterID}
		slog.Info("local-dev mode (single broker)")
	}

	// --- Storage ---
	var store storage.StorageEngine
	if dataDir != "" {
		flockLock := lock.NewFlockLock(dataDir)
		engine, err := storage.NewDiskStorageEngine(dataDir, leaseManager, flockLock, storage.DefaultConfig())
		if err != nil {
			slog.Error("open disk storage", "dir", dataDir, "err", err)
			os.Exit(1)
		}

		// Wire storage callbacks into the k8s lease manager now that the engine exists.
		if km, ok := leaseManager.(*lease.KubernetesLeaseManager); ok {
			km.SetOnStartedLeading(func(topic string, partition int32, epoch int64) {
				if err := engine.TakeoverPartition(topic, partition, epoch); err != nil {
					slog.Error("takeover partition", "topic", topic, "partition", partition, "err", err)
				}
			})
			km.SetOnStoppedLeading(func(topic string, partition int32) {
				engine.RelinquishPartition(topic, partition)
			})
		}

		if k8sMode {
			numBrokers := brokerReg.Count()
			acquireK8sPartitions(ctx, k8sClient, namespace, leaseManager, engine, brokerID, numBrokers)
		}

		// Build coordinator manager.
		if k8sMode {
			lookupBroker := func(ordinal int32) (string, int32, bool) {
				for _, ep := range brokerReg.All() {
					if ep.NodeID == ordinal {
						return ep.Host, ep.Port, true
					}
				}
				return "", 0, false
			}
			offsetStore := coordinator.NewOffsetStore(dataDir)
			coordMgr = coordinator.NewManager(ctx, leaseManager.(*lease.KubernetesLeaseManager), lookupBroker, offsetStore)
		} else {
			// Local-dev: single broker is always coordinator.
			localLeases := leaseManager.(*broker.LocalLeaseManager)
			lookupBroker := func(_ int32) (string, int32, bool) { return host, portNum, true }
			offsetStore := coordinator.NewOffsetStore(dataDir)
			coordMgr = coordinator.NewManager(ctx, localLeases, lookupBroker, offsetStore)
		}

		store = engine
		slog.Info("using disk storage", "dir", dataDir)
	} else {
		store = broker.NewMemoryStorage()
		slog.Info("using in-memory storage")

		// In-memory mode: wire coordinator with local leases (local-dev only).
		localLeases := leaseManager.(*broker.LocalLeaseManager)
		lookupBroker := func(_ int32) (string, int32, bool) { return host, portNum, true }
		offsetStore := coordinator.NewOffsetStore("")
		coordMgr = coordinator.NewManager(ctx, localLeases, lookupBroker, offsetStore)
	}

	// --- Auth engine ---
	var authEngine auth.AuthEngine = broker.NewAllowAllAuthEngine()
	if dataDir != "" {
		real, err := auth.NewRealAuthEngine(dataDir, k8sClient)
		if err != nil {
			slog.Warn("auth engine init failed, falling back to AllowAll", "err", err)
		} else {
			authEngine = real
			// Wire ClusterFileWatcher to hot-reload credentials and ACLs.
			watcher := storage.NewClusterFileWatcher(
				filepath.Join(dataDir, "__cluster", "acls.json"),
				filepath.Join(dataDir, "__cluster", "credentials.json"),
				func(_ string) { real.Reload() },
				func(_ string) { real.Reload() },
			)
			go func() {
				done := make(chan struct{})
				go func() { <-ctx.Done(); close(done) }()
				_ = watcher.Run(done)
			}()
		}
	}

	b := broker.NewWithBrokerSource(
		broker.Config{BrokerID: brokerID, Host: host, Port: portNum, ClusterID: clusterID},
		store,
		leaseManager,
		partLock,
		authEngine,
		brokerSource,
		coordMgr,
	)

	d := protocol.NewDispatcher()
	d.RequireSASL = os.Getenv("SKAFKA_REQUIRE_SASL") == "true"
	b.RegisterHandlers(d)

	srvCfg := protocol.Config{ListenAddr: host + ":" + port}
	if certFile := os.Getenv("SKAFKA_TLS_CERT_FILE"); certFile != "" {
		keyFile := os.Getenv("SKAFKA_TLS_KEY_FILE")
		tlsPort := envOr("SKAFKA_TLS_PORT", "9093")
		tlsCfg, err := protocol.WatchingCertificate(certFile, keyFile)
		if err != nil {
			slog.Error("load TLS cert", "err", err)
			os.Exit(1)
		}
		srvCfg.TLSListenAddr = host + ":" + tlsPort
		srvCfg.TLSConfig = tlsCfg
		slog.Info("TLS listener configured", "addr", srvCfg.TLSListenAddr, "cert", certFile)
	}
	srv := protocol.NewServer(srvCfg, d)
	srv.SetAuthEngine(authEngine)
	if err := srv.Start(ctx); err != nil {
		slog.Error("start server", "err", err)
		os.Exit(1)
	}

	// Health probe HTTP server (Kubernetes livenessProbe/readinessProbe).
	healthAddr := envOr("SKAFKA_HEALTH_ADDR", ":8080")
	startHealthServer(ctx, healthAddr, func() bool {
		return srv.Addr() != ""
	})

	slog.Info("skafka broker ready", "host", host, "port", port, "cluster_id", clusterID)
	<-ctx.Done()
	slog.Info("shutting down")
	srv.Wait()
}

// acquireK8sPartitions enumerates KafkaTopic CRDs and starts Lease acquisition for each partition.
func acquireK8sPartitions(ctx context.Context, k8sClient kubernetes.Interface, namespace string,
	lm lease.LeaseManager, engine *storage.DiskStorageEngine, selfOrdinal int32, numBrokers int) {

	scheme := runtime.NewScheme()
	if err := operatorv1.AddToScheme(scheme); err != nil {
		slog.Warn("acquireK8sPartitions: register scheme", "err", err)
		return
	}
	crClient, err := sigs_client.New(mustRestConfig(), sigs_client.Options{Scheme: scheme})
	if err != nil {
		slog.Warn("acquireK8sPartitions: build CRD client", "err", err)
		return
	}

	var topicList operatorv1.KafkaTopicList
	if err := crClient.List(ctx, &topicList, &sigs_client.ListOptions{Namespace: namespace}); err != nil {
		slog.Warn("acquireK8sPartitions: list KafkaTopics", "err", err)
		return
	}

	for _, topic := range topicList.Items {
		for p := int32(0); p < topic.Spec.Partitions; p++ {
			_ = engine.CreatePartition(topic.Name, p)
			if k8spkg.Preferred(topic.Name, p, selfOrdinal, numBrokers) {
				_ = lm.Acquire(ctx, topic.Name, p)
			}
		}
	}
	// Second pass: try to acquire any partition this broker doesn't yet hold.
	for _, topic := range topicList.Items {
		for p := int32(0); p < topic.Spec.Partitions; p++ {
			if !lm.IsLeader(topic.Name, p) {
				_ = lm.Acquire(ctx, topic.Name, p)
			}
		}
	}
}

// runInit creates partition directories for all KafkaTopics on the PVC and exits.
func runInit(ctx context.Context) {
	dataDir := os.Getenv("SKAFKA_DATA_DIR")
	if dataDir == "" {
		slog.Error("init: SKAFKA_DATA_DIR not set")
		os.Exit(1)
	}
	namespace := envOr("SKAFKA_NAMESPACE", "default")

	scheme := runtime.NewScheme()
	_ = operatorv1.AddToScheme(scheme)
	crClient, err := sigs_client.New(mustRestConfig(), sigs_client.Options{Scheme: scheme})
	if err != nil {
		slog.Error("init: build k8s client", "err", err)
		os.Exit(1)
	}

	var topicList operatorv1.KafkaTopicList
	if err := crClient.List(ctx, &topicList, &sigs_client.ListOptions{Namespace: namespace}); err != nil {
		slog.Error("init: list KafkaTopics", "err", err)
		os.Exit(1)
	}

	for _, topic := range topicList.Items {
		for p := int32(0); p < topic.Spec.Partitions; p++ {
			dir := filepath.Join(dataDir, topic.Name, strconv.Itoa(int(p)))
			if err := os.MkdirAll(dir, 0755); err != nil {
				slog.Error("init: mkdir", "dir", dir, "err", err)
				os.Exit(1)
			}
		}
	}
	slog.Info("init complete", "topics", len(topicList.Items))
}

func buildK8sClient() (kubernetes.Interface, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		cfg, err = clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
		if err != nil {
			return nil, err
		}
	}
	return kubernetes.NewForConfig(cfg)
}

func mustRestConfig() *rest.Config {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		cfg, err = clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
		if err != nil {
			panic("cannot build k8s rest config: " + err.Error())
		}
	}
	return cfg
}

// startHealthServer runs an HTTP server on addr with /healthz and /readyz endpoints.
// ready is called on each /readyz hit; it should return true when the broker is
// accepting client connections.
func startHealthServer(ctx context.Context, addr string, ready func() bool) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Warn("health server exited", "err", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	slog.Info("health server listening", "addr", addr)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
