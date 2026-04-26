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
	"github.com/woestebanaan/skafka/internal/observability"
	"github.com/woestebanaan/skafka/internal/storage"
	operatorv1 "github.com/woestebanaan/skafka/operator/api/v1alpha1"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	observability.InstallLogger()

	if len(os.Args) > 1 && os.Args[1] == "--init" {
		runInit(ctx)
		return
	}

	obs, err := observability.Bootstrap(ctx, "skafka")
	if err != nil {
		slog.Error("observability bootstrap", "err", err)
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = obs.Shutdown(shutdownCtx)
	}()
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
		k8sTopics    []topicSpec
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
		brokerSource = handlers.BrokerInfo{NodeID: 0, Host: host, Port: portNum, ClusterID: clusterID}
		// Disk-backed local-dev still needs a real flock so the engine and the
		// produce handler share the same lock state.
		if dataDir != "" {
			partLock = lock.NewFlockLock(dataDir)
		} else {
			partLock = broker.NewLocalPartitionLock()
		}
		slog.Info("local-dev mode (single broker)")
	}

	// --- Storage ---
	var store storage.StorageEngine
	var engine *storage.DiskStorageEngine
	if dataDir != "" {
		// Reuse partLock so engine.TakeoverPartition and handler.IsLocked share
		// the same in-memory `held` map. Two FlockLock instances would each
		// track their own state and the produce handler would always see the
		// partition as unlocked.
		var err error
		engine, err = storage.NewDiskStorageEngine(dataDir, leaseManager, partLock, storage.DefaultConfig())
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
			k8sTopics = acquireK8sPartitions(ctx, k8sClient, namespace, leaseManager, engine, brokerID, numBrokers)

			// Satisfy the StatefulSet's skafka.io/PartitionsReady gate now that the
			// initial partition acquisition pass has run. Without this patch the
			// pod's Ready condition stays False forever and it never joins the
			// headless service.
			ru := k8spkg.NewReadinessUpdater(k8sClient, os.Getenv("MY_POD_NAME"), namespace)
			if err := ru.SetReady(ctx, true); err != nil {
				slog.Warn("readiness gate patch failed", "err", err)
			}
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
	authDisabled := os.Getenv("SKAFKA_AUTH_DISABLED") == "true"
	if dataDir != "" && !authDisabled {
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

	// Register topics discovered during k8s startup so Metadata responses and
	// produce/fetch dispatch resolve them.
	for _, t := range k8sTopics {
		b.AddTopic(t.Name, t.Partitions)
	}

	// Watch KafkaTopic CRs so topics created after startup become visible
	// without a broker restart, and partition expansions are picked up.
	if k8sMode && dataDir != "" {
		startTopicWatcher(ctx, namespace, b, engine, leaseManager, brokerReg, brokerID, k8sTopics)
	}

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

// topicSpec is a minimal projection of operatorv1.KafkaTopic for in-process passing.
type topicSpec struct {
	Name       string
	Partitions int32
}

// acquireK8sPartitions enumerates KafkaTopic CRDs, creates partition dirs, kicks off
// Lease acquisition, and returns the discovered topics so callers can register them
// with the in-memory TopicRegistry.
func acquireK8sPartitions(ctx context.Context, k8sClient kubernetes.Interface, namespace string,
	lm lease.LeaseManager, engine *storage.DiskStorageEngine, selfOrdinal int32, numBrokers int) []topicSpec {

	scheme := runtime.NewScheme()
	if err := operatorv1.AddToScheme(scheme); err != nil {
		slog.Warn("acquireK8sPartitions: register scheme", "err", err)
		return nil
	}
	crClient, err := sigs_client.New(mustRestConfig(), sigs_client.Options{Scheme: scheme})
	if err != nil {
		slog.Warn("acquireK8sPartitions: build CRD client", "err", err)
		return nil
	}

	var topicList operatorv1.KafkaTopicList
	if err := crClient.List(ctx, &topicList, &sigs_client.ListOptions{Namespace: namespace}); err != nil {
		slog.Warn("acquireK8sPartitions: list KafkaTopics", "err", err)
		return nil
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

	out := make([]topicSpec, 0, len(topicList.Items))
	for _, topic := range topicList.Items {
		out = append(out, topicSpec{Name: topic.Name, Partitions: topic.Spec.Partitions})
	}
	return out
}

// startTopicWatcher launches a KafkaTopic CR watcher that mirrors changes into
// the broker's TopicRegistry, creates partition directories on the shared PVC,
// and acquires preferred-partition leases for newly observed partitions.
//
// The watcher is primed with topics already discovered by acquireK8sPartitions
// so the watch-restart re-list does not re-fire callbacks for them.
func startTopicWatcher(
	ctx context.Context,
	namespace string,
	b *broker.Broker,
	engine *storage.DiskStorageEngine,
	lm lease.LeaseManager,
	brokerReg *k8spkg.BrokerRegistry,
	selfOrdinal int32,
	primed []topicSpec,
) {
	onEvent := func(ev k8spkg.TopicEvent) {
		switch ev.Type {
		case k8spkg.TopicAdded:
			slog.Info("kafkatopic added", "topic", ev.Name, "partitions", ev.Partitions)
			numBrokers := brokerReg.Count()
			for p := int32(0); p < ev.Partitions; p++ {
				if err := engine.CreatePartition(ev.Name, p); err != nil {
					slog.Warn("topic watcher: create partition", "topic", ev.Name, "partition", p, "err", err)
				}
				if k8spkg.Preferred(ev.Name, p, selfOrdinal, numBrokers) {
					_ = lm.Acquire(ctx, ev.Name, p)
				}
			}
			for p := int32(0); p < ev.Partitions; p++ {
				if !lm.IsLeader(ev.Name, p) {
					_ = lm.Acquire(ctx, ev.Name, p)
				}
			}
			b.AddTopic(ev.Name, ev.Partitions)

		case k8spkg.TopicModified:
			slog.Info("kafkatopic expanded", "topic", ev.Name, "old", ev.OldPartitions, "new", ev.Partitions)
			numBrokers := brokerReg.Count()
			for p := ev.OldPartitions; p < ev.Partitions; p++ {
				if err := engine.CreatePartition(ev.Name, p); err != nil {
					slog.Warn("topic watcher: create partition", "topic", ev.Name, "partition", p, "err", err)
				}
				if k8spkg.Preferred(ev.Name, p, selfOrdinal, numBrokers) {
					_ = lm.Acquire(ctx, ev.Name, p)
				}
			}
			for p := ev.OldPartitions; p < ev.Partitions; p++ {
				if !lm.IsLeader(ev.Name, p) {
					_ = lm.Acquire(ctx, ev.Name, p)
				}
			}
			b.AddTopic(ev.Name, ev.Partitions)

		case k8spkg.TopicDeleted:
			slog.Info("kafkatopic deleted", "topic", ev.Name, "partitions", ev.Partitions)
			b.RemoveTopic(ev.Name)
			for p := int32(0); p < ev.Partitions; p++ {
				_ = lm.Release(ev.Name, p)
			}
		}
	}

	w, err := k8spkg.NewTopicWatcher(mustRestConfig(), namespace, onEvent)
	if err != nil {
		slog.Warn("topic watcher: init failed", "err", err)
		return
	}
	for _, t := range primed {
		w.Prime(t.Name, t.Partitions)
	}
	go func() {
		if err := w.Run(ctx); err != nil && ctx.Err() == nil {
			slog.Error("topic watcher stopped", "err", err)
		}
	}()
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
