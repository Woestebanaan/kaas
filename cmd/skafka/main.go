package main

import (
	"context"
	"fmt"
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
	"github.com/woestebanaan/skafka/internal/protocol"
	"github.com/woestebanaan/skafka/internal/protocol/handlers"
	"github.com/woestebanaan/skafka/internal/observability"
	"github.com/woestebanaan/skafka/internal/storage"
	operatorv1 "github.com/woestebanaan/skafka/operator/api/v1alpha1"
	"github.com/woestebanaan/skafka/pkg/kafkaapi"
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

		// Callbacks are wired after DiskStorageEngine is created; use placeholders for now.
		leaseManager = lease.NewKubernetesLeaseManager(k8sClient, namespace, identity.PodName, nil, nil)

		slog.Info("kubernetes mode",
			"pod", identity.PodName, "ordinal", identity.Ordinal, "namespace", namespace)
	} else {
		brokerID = 0
		leaseManager = broker.NewLocalLeaseManager()
		brokerSource = handlers.BrokerInfo{NodeID: 0, Host: host, Port: portNum, ClusterID: clusterID}
		slog.Info("local-dev mode (single broker)")
	}

	// --- Storage ---
	var store storage.StorageEngine
	var engine *storage.DiskStorageEngine
	if dataDir != "" {
		// Phase 4 dropped the flock parameter — single-writer enforcement is
		// now BrokerCoordinator.Owns + epoch-prefixed segment filenames.
		var err error
		engine, err = storage.NewDiskStorageEngine(dataDir, leaseManager, storage.DefaultConfig())
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
			k8sTopics = acquireK8sPartitions(ctx, k8sClient, namespace, leaseManager, engine, brokerReg, brokerID)

			// Satisfy the StatefulSet's skafka.io/PartitionsReady gate now that the
			// initial partition acquisition pass has run. Without this patch the
			// pod's Ready condition stays False forever and it never joins the
			// headless service.
			ru := k8spkg.NewReadinessUpdater(k8sClient, os.Getenv("MY_POD_NAME"), namespace)
			if err := ru.SetReady(ctx, true); err != nil {
				slog.Warn("readiness gate patch failed", "err", err)
			}
		}

		// Build coordinator manager. Phase 5: GroupAssignmentSource replaces
		// the v2.6 per-group Lease wiring. Until the runtime
		// internal/broker.Coordinator is end-to-end wired into main.go (a
		// follow-up to phase4 step 5/6), use LocalGroupSource — single
		// broker is always coordinator. k8s-mode lookupBroker walks the
		// brokerRegistry by parsing the trailing ordinal from the
		// "skafka-N" identifier convention.
		brokerIDStr := fmt.Sprintf("skafka-%d", brokerID)
		groupSrc := broker.NewLocalGroupSource(brokerIDStr)
		var lookupBroker coordinator.BrokerLookup
		if k8sMode {
			lookupBroker = func(id string) (int32, string, int32, bool) {
				ord := lease.ParseOrdinalFromIdentity(id)
				for _, ep := range brokerReg.All() {
					if ep.NodeID == ord {
						return ep.NodeID, ep.Host, ep.Port, true
					}
				}
				return 0, "", 0, false
			}
		} else {
			lookupBroker = func(_ string) (int32, string, int32, bool) {
				return brokerID, host, portNum, true
			}
		}
		offsetStore := coordinator.NewOffsetStore(dataDir)
		coordMgr = coordinator.NewManager(ctx, groupSrc, lookupBroker, offsetStore)

		store = engine
		slog.Info("using disk storage", "dir", dataDir)
	} else {
		store = broker.NewMemoryStorage()
		slog.Info("using in-memory storage")

		// In-memory mode: same LocalGroupSource pattern as the disk-backed
		// path above. Phase 5: GroupAssignmentSource replaces v2.6 per-group
		// Lease.
		brokerIDStr := fmt.Sprintf("skafka-%d", brokerID)
		groupSrc := broker.NewLocalGroupSource(brokerIDStr)
		lookupBroker := func(_ string) (int32, string, int32, bool) {
			return brokerID, host, portNum, true
		}
		offsetStore := coordinator.NewOffsetStore("")
		coordMgr = coordinator.NewManager(ctx, groupSrc, lookupBroker, offsetStore)
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
		authEngine,
		brokerSource,
		coordMgr,
	)

	// Register topics discovered during k8s startup so Metadata responses and
	// produce/fetch dispatch resolve them.
	for _, t := range k8sTopics {
		b.AddTopic(t.Name, t.Partitions)
	}

	// v3 cluster runtime must boot BEFORE RegisterHandlers so the
	// BrokerCoordinator is available for the produce handler to pick up
	// via WithCoordinator. In single-broker dev mode (no k8sClient or
	// no dataDir) the runtime isn't started and the broker stays on
	// the legacy lease+lock fallback path.
	//
	// It also boots before the topic watcher so the watcher's onEvent
	// callback can notify the controller's AssignmentLoop on KafkaTopic
	// adds/modifies/deletes — fixes the gap where new topics never made
	// it into assignment.json (gh #74).
	var rt *clusterRuntime
	if k8sMode && dataDir != "" && k8sClient != nil && engine != nil {
		brokerIDStr := fmt.Sprintf("skafka-%d", brokerID)
		heartbeatAddr := envOr("SKAFKA_CONTROLLER_HEARTBEAT_ADDR", "0.0.0.0:9094")
		peerPort := int32(9094)
		if p, err := strconv.Atoi(envOr("SKAFKA_PEER_HEARTBEAT_PORT", "9094")); err == nil {
			peerPort = int32(p)
		}

		// Build a controller-runtime client for the KafkaClusterAssignments
		// CR mirror (Phase 6). Failures are non-fatal — the file on the
		// PVC is the source of truth, the CR is kubectl-debugging convenience.
		var crClient sigs_client.Client
		crScheme := runtime.NewScheme()
		if err := operatorv1.AddToScheme(crScheme); err == nil {
			if cl, err := sigs_client.New(mustRestConfig(), sigs_client.Options{Scheme: crScheme}); err == nil {
				crClient = cl
			} else {
				slog.Warn("crmirror: build controller-runtime client", "err", err)
			}
		}

		// Cluster name maps the broker back to its KafkaCluster CR. The Helm
		// chart populates SKAFKA_CLUSTER_NAME via release-name templating; if
		// unset, fall back to SKAFKA_CLUSTER_ID (the value we already pass for
		// Metadata responses) so old deploys without the new env var still work.
		clusterName := envOr("SKAFKA_CLUSTER_NAME", clusterID)

		// Controller Lease tuning. Zero (or unparseable) → cluster_runtime
		// falls back to controller.New defaults (15s/10s/2s). The Helm
		// chart's broker.controllerLease.* block is the production source.
		leaseDuration := envSecondsOr("SKAFKA_CONTROLLER_LEASE_DURATION_SECONDS", 0)
		renewDeadline := envSecondsOr("SKAFKA_CONTROLLER_RENEW_DEADLINE_SECONDS", 0)
		retryPeriod := envSecondsOr("SKAFKA_CONTROLLER_RETRY_PERIOD_SECONDS", 0)

		rt = startClusterRuntime(ctx, clusterRuntimeConfig{
			k8sClient:         k8sClient,
			namespace:         namespace,
			brokerIDStr:       brokerIDStr,
			dataDir:           dataDir,
			engine:            engine,
			coordMgr:          coordMgr,
			topicRegistry:     b.Topics(),
			brokerReg:         brokerReg,
			heartbeatAddr:     heartbeatAddr,
			peerHeartbeatPort: peerPort,
			crClient:          crClient,
			clusterName:       clusterName,
			leaseDuration:     leaseDuration,
			renewDeadline:     renewDeadline,
			retryPeriod:       retryPeriod,
		})
		b.UseCoordinator(rt.coord)
		// Phase 10 Gap #3c: feed runtime state to the ObservableGauge
		// callback registered by observability.Bootstrap. Until this
		// runs, gauges report zero.
		observability.SetGaugeSource(&runtimeGaugeSource{rt: rt})
	}

	// Watch KafkaTopic CRs so topics created after startup become visible
	// without a broker restart, partition expansions are picked up, and
	// the controller (when this broker holds the Lease) recomputes the
	// assignment for the new topic shape.
	if k8sMode && dataDir != "" {
		startTopicWatcher(ctx, namespace, b, engine, leaseManager, brokerReg, brokerID, k8sTopics, rt)
	}

	d := protocol.NewDispatcher()
	d.RequireSASL = os.Getenv("SKAFKA_REQUIRE_SASL") == "true"
	b.RegisterHandlers(d)

	srvCfg := protocol.Config{ListenAddr: host + ":" + port}
	if certFile := os.Getenv("SKAFKA_TLS_CERT_FILE"); certFile != "" {
		keyFile := os.Getenv("SKAFKA_TLS_KEY_FILE")
		tlsPort := envOr("SKAFKA_TLS_PORT", "9093")

		// mTLS: when SKAFKA_TLS_CLIENT_CA_FILE is set, the listener
		// requires every client to present a cert signed by one of the
		// CAs in the bundle. Without it, TLS is opportunistic — clients
		// can connect cert-less and authenticate via SASL.
		var tlsOpts []protocol.TLSOption
		if caFile := os.Getenv("SKAFKA_TLS_CLIENT_CA_FILE"); caFile != "" {
			tlsOpts = append(tlsOpts, protocol.WithRequireClientCert(caFile))
			slog.Info("TLS: client cert verification enabled", "ca_bundle", caFile)
		}

		tlsCfg, err := protocol.WatchingCertificate(certFile, keyFile, tlsOpts...)
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
	// /healthz returns the v3 runtime state (Phase 10 plan schema);
	// /readyz gates on the listener being bound.
	healthAddr := envOr("SKAFKA_HEALTH_ADDR", ":8080")
	listeners := []string{"internal"}
	var tlsInfo *observability.TLSInfo
	if srvCfg.TLSListenAddr != "" {
		listeners = append(listeners, "external")
		tlsInfo = &observability.TLSInfo{
			Enabled:      true,
			ExternalHost: os.Getenv("EXTERNAL_HOSTNAME_PATTERN"),
		}
	}
	var healthSource observability.RuntimeState
	if rt != nil {
		healthSource = &healthRuntimeState{rt: rt}
	}
	healthBrokerID := ""
	if k8sMode {
		healthBrokerID = fmt.Sprintf("skafka-%d", brokerID)
	}
	startHealthServer(ctx, healthServerConfig{
		addr:      healthAddr,
		brokerID:  healthBrokerID,
		listeners: listeners,
		tls:       tlsInfo,
		source:    healthSource,
		ready:     func() bool { return srv.Addr() != "" },
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

// acquireK8sPartitions enumerates KafkaTopic CRDs and creates partition
// directories on the shared PVC. Returns the discovered topics so
// callers can register them with the in-memory TopicRegistry.
//
// Per-partition Lease acquisition was removed in the gh #75 architectural
// cleanup: leadership is now driven entirely by assignment.json (the
// controller's single source of truth) via *broker.Coordinator. The old
// path acquired ~50 Leases × 3 brokers and saturated the K8s API client
// QPS budget by ~15×; without it, broker startup is much quieter and the
// Lease-vs-CR split-brain on freshly-added topics is gone.
func acquireK8sPartitions(ctx context.Context, k8sClient kubernetes.Interface, namespace string,
	lm lease.LeaseManager, engine *storage.DiskStorageEngine, brokerReg *k8spkg.BrokerRegistry,
	selfOrdinal int32) []topicSpec {

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
	rt *clusterRuntime,
) {
	onEvent := func(ev k8spkg.TopicEvent) {
		switch ev.Type {
		case k8spkg.TopicAdded:
			slog.Info("kafkatopic added", "topic", ev.Name, "partitions", ev.Partitions)
			// Just create partition directories on the PVC. Leadership
			// is decided by the controller's balancer and surfaces via
			// assignment.json — no per-partition Lease acquisition here
			// (gh #75 cleanup).
			for p := int32(0); p < ev.Partitions; p++ {
				if err := engine.CreatePartition(ev.Name, p); err != nil {
					slog.Warn("topic watcher: create partition", "topic", ev.Name, "partition", p, "err", err)
				}
			}
			b.AddTopic(ev.Name, ev.Partitions)
			// Triggers an assignment recompute on whichever broker is
			// currently controller. No-op on non-controller brokers and
			// when the v3 runtime is disabled. Without this, new topics
			// don't appear in assignment.json — gh #74.
			rt.NotifyTopicChange(ctx, kafkaapi.AssignmentReasonTopicCreated, ev.Name)

		case k8spkg.TopicModified:
			slog.Info("kafkatopic expanded", "topic", ev.Name, "old", ev.OldPartitions, "new", ev.Partitions)
			for p := ev.OldPartitions; p < ev.Partitions; p++ {
				if err := engine.CreatePartition(ev.Name, p); err != nil {
					slog.Warn("topic watcher: create partition", "topic", ev.Name, "partition", p, "err", err)
				}
			}
			b.AddTopic(ev.Name, ev.Partitions)
			rt.NotifyTopicChange(ctx, kafkaapi.AssignmentReasonTopicResized, ev.Name)

		case k8spkg.TopicDeleted:
			slog.Info("kafkatopic deleted", "topic", ev.Name, "partitions", ev.Partitions)
			// Close the partition log file handles BEFORE we drop the
			// topic from the registry. On NFS, an open file under a
			// directory the operator wants to unlinkat turns into a
			// .nfsXXXX silly-rename that EBUSYs the parent unlink
			// indefinitely (gh #76). The topic_watcher fires this
			// event the moment the CR's deletionTimestamp is set, so
			// we close ahead of the operator's finalizer reconcile.
			for p := int32(0); p < ev.Partitions; p++ {
				if err := engine.ClosePartition(ev.Name, p); err != nil {
					slog.Warn("topic watcher: close partition", "topic", ev.Name, "partition", p, "err", err)
				}
				_ = lm.Release(ev.Name, p)
			}
			b.RemoveTopic(ev.Name)
			rt.NotifyTopicChange(ctx, kafkaapi.AssignmentReasonTopicDeleted, ev.Name)
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

// healthServerConfig is the inputs to startHealthServer. brokerID and
// listeners label the response; tls is the external-listener summary;
// source is the v3 RuntimeState (nil in local-dev). ready gates /readyz.
type healthServerConfig struct {
	addr      string
	brokerID  string
	listeners []string
	tls       *observability.TLSInfo
	source    observability.RuntimeState
	ready     func() bool
}

// startHealthServer runs an HTTP server with the plan's /healthz JSON
// schema and a /readyz that flips on cfg.ready(). The handler is a
// thin wrapper around observability.HealthHandler — keeps the schema
// definition in one place, with unit-test coverage there.
func startHealthServer(ctx context.Context, cfg healthServerConfig) {
	mux := http.NewServeMux()
	mux.Handle("/healthz", observability.HealthHandler(cfg.brokerID, cfg.listeners, cfg.tls, cfg.source))
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !cfg.ready() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	srv := &http.Server{Addr: cfg.addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
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
	slog.Info("health server listening", "addr", cfg.addr)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// envSecondsOr reads an env var as an integer count of seconds and
// returns it as a time.Duration. Empty / unparseable returns def. Used
// for the Phase 8 controllerLease.* knobs; passing 0 to the cluster
// runtime falls back to controller.New's hardcoded defaults.
func envSecondsOr(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return def
	}
	return time.Duration(n) * time.Second
}
