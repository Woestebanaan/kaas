package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"encoding/json"
	"net/http"
	// gh #132: opt-in pprof on the health server's mux. Registered
	// directly (not via http.DefaultServeMux) so we don't share state
	// with anything else in-process that might inadvertently mutate
	// the default mux.
	"net/http/pprof"
	goruntime "runtime"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"go.opentelemetry.io/otel"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	sigs_client "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/woestebanaan/skafka/internal/auth"
	"github.com/woestebanaan/skafka/internal/broker"
	"github.com/woestebanaan/skafka/internal/connstate"
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

// brokerConfig collects the env-derived configuration consumed by runBroker.
// One struct here = one place to audit "what does this binary read from env?".
// Pure-derivable from os.Environ() via loadBrokerConfig(); no side effects.
type brokerConfig struct {
	Host        string // SKAFKA_HOST          (default 0.0.0.0)
	Port        string // SKAFKA_PORT          (default 9092 — string form, kept for logs)
	PortNum     int32  // numeric form of Port
	ClusterID   string // SKAFKA_CLUSTER_ID    (default skafka-local)
	DataDir     string // SKAFKA_DATA_DIR      (empty = in-memory storage / dev mode)
	Namespace   string // SKAFKA_NAMESPACE     (default "default")
	HeadlessSvc string // SKAFKA_HEADLESS_SVC  (default skafka-headless)
	K8sMode     bool   // true iff MY_POD_NAME is set (StatefulSet pod identity present)
}

// loadBrokerConfig reads broker config from the environment. Pure function:
// inputs = current process env, output = brokerConfig, no I/O beyond env access.
func loadBrokerConfig() brokerConfig {
	portStr := envOr("SKAFKA_PORT", "9092")
	portNum := int32(9092)
	if p, err := strconv.Atoi(portStr); err == nil {
		portNum = int32(p)
	}
	return brokerConfig{
		Host:        envOr("SKAFKA_HOST", "0.0.0.0"),
		Port:        portStr,
		PortNum:     portNum,
		ClusterID:   envOr("SKAFKA_CLUSTER_ID", "skafka-local"),
		DataDir:     os.Getenv("SKAFKA_DATA_DIR"),
		Namespace:   envOr("SKAFKA_NAMESPACE", "default"),
		HeadlessSvc: envOr("SKAFKA_HEADLESS_SVC", "skafka-headless"),
		K8sMode:     os.Getenv("MY_POD_NAME") != "",
	}
}

// brokerIdentity gathers the per-mode wiring runBroker needs after it has
// decided whether this process is a Kubernetes StatefulSet pod or a
// local-dev singleton. K8sClient and BrokerReg are nil in local-dev mode;
// LeaseManager is always non-nil (a stub in local-dev).
//
// LeaseManager's onStarted/onStopped callbacks are placeholders here —
// runBroker wires them in after the storage engine exists (KubernetesLeaseManager
// wraps engine.TakeoverPartition / engine.RelinquishPartition).
type brokerIdentity struct {
	BrokerID     int32
	K8sClient    kubernetes.Interface
	BrokerReg    *k8spkg.BrokerRegistry
	BrokerSource handlers.BrokerSource
	LeaseManager lease.LeaseManager
}

// setupBrokerIdentity dispatches to the K8s or local-dev path based on
// cfg.K8sMode. Fatal env / API errors call os.Exit(1) here (same as the
// inline code did before extraction) — runBroker can't proceed without
// identity in K8s mode.
func setupBrokerIdentity(ctx context.Context, cfg brokerConfig) brokerIdentity {
	if !cfg.K8sMode {
		slog.Info("local-dev mode (single broker)")
		return brokerIdentity{
			BrokerID:     0,
			LeaseManager: broker.NewLocalLeaseManager(),
			BrokerSource: handlers.BrokerInfo{NodeID: 0, Host: cfg.Host, Port: cfg.PortNum, ClusterID: cfg.ClusterID},
		}
	}

	k8sClient, err := buildK8sClient()
	if err != nil {
		slog.Error("build k8s client", "err", err)
		os.Exit(1)
	}
	identity, err := k8spkg.NewBrokerIdentity(cfg.Namespace, cfg.HeadlessSvc, cfg.PortNum)
	if err != nil {
		slog.Error("broker identity", "err", err)
		os.Exit(1)
	}

	self := k8spkg.BrokerEndpoint{NodeID: identity.Ordinal, Host: identity.Host, Port: cfg.PortNum, Ready: true}
	brokerReg := k8spkg.NewBrokerRegistry(self, identity.DNS, nil)
	go func() {
		if err := brokerReg.Watch(ctx, k8sClient, cfg.Namespace, cfg.HeadlessSvc); err != nil && ctx.Err() == nil {
			slog.Error("endpoint watcher: stopped before ctx cancellation (broker has lost EndpointSlice watch and will not see peer broker join/leave events until restart; assignment-based ownership decisions still work because the controller writes assignment.json independently)",
				"err", err)
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

	slog.Info("kubernetes mode",
		"pod", identity.PodName, "ordinal", identity.Ordinal, "namespace", cfg.Namespace)

	return brokerIdentity{
		BrokerID:     identity.Ordinal,
		K8sClient:    k8sClient,
		BrokerReg:    brokerReg,
		BrokerSource: src,
		LeaseManager: lease.NewKubernetesLeaseManager(k8sClient, cfg.Namespace, identity.PodName, nil, nil),
	}
}

// storageStack collects the storage + coordinator wiring produced by
// setupStorageStack. Engine is nil when dataDir == "" (memory-storage
// dev mode); Store is always non-nil. CoordMgr is always non-nil.
// K8sTopics is populated only in K8s mode after acquireK8sPartitions runs.
type storageStack struct {
	Store     storage.StorageEngine
	Engine    *storage.DiskStorageEngine
	CoordMgr  *coordinator.Manager
	K8sTopics []topicSpec
}

// setupStorageStack builds the on-disk storage engine + reaper + consumer-group
// coordinator (or the in-memory fallback when SKAFKA_DATA_DIR is unset). Wires
// the KubernetesLeaseManager's onStarted/onStopped callbacks to the engine's
// takeover/relinquish methods now that the engine exists; in K8s mode it also
// drives the initial acquireK8sPartitions sweep and clears the
// PartitionsReady readiness gate so the Pod can join the headless service.
//
// Fatal storage / RBAC errors call os.Exit(1) — runBroker can't proceed
// without a working storage backend.
func setupStorageStack(ctx context.Context, cfg brokerConfig, id brokerIdentity) storageStack {
	if cfg.DataDir == "" {
		// Memory-storage dev mode. Same LocalGroupSource pattern as the
		// disk path — single broker is always coordinator.
		slog.Info("using in-memory storage")
		brokerIDStr := fmt.Sprintf("skafka-%d", id.BrokerID)
		groupSrc := broker.NewLocalGroupSource(brokerIDStr)
		lookupBroker := func(_ string) (int32, string, int32, bool) {
			return id.BrokerID, cfg.Host, cfg.PortNum, true
		}
		offsetStore := coordinator.NewOffsetStore("")
		return storageStack{
			Store:    broker.NewMemoryStorage(),
			CoordMgr: coordinator.NewManager(ctx, groupSrc, lookupBroker, offsetStore),
		}
	}

	// Disk-backed storage.
	// Phase 4 dropped the flock parameter — single-writer enforcement is
	// now BrokerCoordinator.Owns + epoch-prefixed segment filenames.
	storageCfg := applyStorageEnv(storage.DefaultConfig())
	slog.Info("storage config",
		"flushIntervalMessages", storageCfg.FlushIntervalMessages,
		"segmentBytes", storageCfg.SegmentBytes,
		"retentionMs", storageCfg.RetentionMs,
		"fsyncMaxLatency", storageCfg.FsyncMaxLatency.String())
	engine, err := storage.NewDiskStorageEngine(cfg.DataDir, id.LeaseManager, storageCfg)
	if err != nil {
		slog.Error("open disk storage", "dir", cfg.DataDir, "err", err)
		os.Exit(1)
	}

	attachReaper(ctx, engine)
	wireLeaseCallbacks(id.LeaseManager, engine)

	var k8sTopics []topicSpec
	if cfg.K8sMode {
		k8sTopics = acquireK8sPartitions(ctx, cfg.Namespace, engine)
		// Clear the StatefulSet's skafka.io/PartitionsReady readiness gate
		// now that the initial sweep has run. Without this the Pod's Ready
		// condition stays False forever and it never joins the headless
		// service.
		ru := k8spkg.NewReadinessUpdater(id.K8sClient, os.Getenv("MY_POD_NAME"), cfg.Namespace)
		if err := ru.SetReady(ctx, true); err != nil {
			slog.Warn("readiness: patching the broker's own Pod readiness gate failed (Pod won't transition to Ready and so won't join the headless service; clients can't reach this broker through DNS until the gate is set; check RBAC: the broker ServiceAccount needs patch on pods/status)",
				"err", err)
		}
	}

	// Phase 5: GroupAssignmentSource replaces the v2.6 per-group Lease
	// wiring. LocalGroupSource is the stub — single broker is always
	// coordinator until cluster_runtime hot-swaps in broker.Coordinator.
	// k8s-mode lookupBroker walks brokerRegistry by parsing the trailing
	// ordinal from the "skafka-N" identifier convention.
	brokerIDStr := fmt.Sprintf("skafka-%d", id.BrokerID)
	groupSrc := broker.NewLocalGroupSource(brokerIDStr)
	var lookupBroker coordinator.BrokerLookup
	if cfg.K8sMode {
		lookupBroker = func(idStr string) (int32, string, int32, bool) {
			ord := lease.ParseOrdinalFromIdentity(idStr)
			for _, ep := range id.BrokerReg.All() {
				if ep.NodeID == ord {
					return ep.NodeID, ep.Host, ep.Port, true
				}
			}
			return 0, "", 0, false
		}
	} else {
		lookupBroker = func(_ string) (int32, string, int32, bool) {
			return id.BrokerID, cfg.Host, cfg.PortNum, true
		}
	}
	offsetStore := coordinator.NewOffsetStore(cfg.DataDir)
	slog.Info("using disk storage", "dir", cfg.DataDir)
	return storageStack{
		Store:     engine,
		Engine:    engine,
		CoordMgr:  coordinator.NewManager(ctx, groupSrc, lookupBroker, offsetStore),
		K8sTopics: k8sTopics,
	}
}

// attachReaper wires the gh #119 partition reaper to the engine. The
// reaper rate-limits ClosePartition's slow close+RemoveAll work so it
// can't compete with active Produce/Fetch on shared NFS. CR-existence
// recheck is wired separately from cluster_runtime once topic-watcher
// boots.
func attachReaper(ctx context.Context, engine *storage.DiskStorageEngine) {
	reaperRate := 5.0
	if v := os.Getenv("SKAFKA_DELETION_RATE_PER_SEC"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			reaperRate = f
		}
	}
	reaper := storage.NewPartitionReaper(storage.ReaperConfig{RatePerSec: reaperRate})

	// Attach OTel instruments so reaper activity shows up in Grafana
	// alongside Produce/Fetch. observability.Bootstrap already called
	// otel.SetMeterProvider; pull a named meter off the global.
	reaperMeter := otel.Meter("skafka-reaper")
	enq, comp, abrt, rtry, gup, dur, merr := observability.NewReaperMetrics(reaperMeter)
	if merr == nil {
		reaper.WithMetrics(&storage.ReaperMetrics{
			Enqueued: enq, Completed: comp, Aborted: abrt,
			Retried: rtry, GivenUp: gup, Duration: dur,
		})
	} else {
		slog.Warn("reaper metrics disabled", "err", merr)
	}
	engine.WithReaper(reaper)
	go reaper.Run(ctx)
}

// wireLeaseCallbacks plugs the engine's takeover/relinquish methods into
// the lease manager's onStarted/onStopped hooks. No-op for the local
// (non-Kubernetes) lease manager.
func wireLeaseCallbacks(lm lease.LeaseManager, engine *storage.DiskStorageEngine) {
	km, ok := lm.(*lease.KubernetesLeaseManager)
	if !ok {
		return
	}
	km.SetOnStartedLeading(func(topic string, partition int32, epoch int64) {
		if err := engine.TakeoverPartition(topic, partition, epoch); err != nil {
			slog.Error("takeover: opening file handles + replaying recovery for partition we just won leadership of failed (this broker holds the lease but cannot serve Produce/Fetch for this partition; clients will see NOT_LEADER until the next assignment change or partition relinquish/retake)",
				"topic", topic, "partition", partition, "epoch", epoch, "err", err)
		}
	})
	km.SetOnStoppedLeading(func(topic string, partition int32) {
		engine.RelinquishPartition(topic, partition)
	})
}

// listenerSetup is what setupListeners returns: the bound listener
// configs, the per-listener auth engine map the dispatcher should use,
// and a flag indicating whether at least one TLS listener is up.
type listenerSetup struct {
	Configs   []protocol.ListenerConfig
	Engines   auth.PerListenerAuthEngine
	TLSActive bool
}

// setupListeners decides the set of TCP/TLS listeners to bind. Two paths:
//
//   - Preferred: chart-emitted SKAFKA_LISTENERS JSON (parsed by
//     parseListenersEnv). Each entry self-describes name / port / tls /
//     auth.type. buildListenerWireup turns them into protocol.ListenerConfig
//     + a per-listener auth-engine map + per-listener advertised ports
//     (stamped onto brokerSource for Metadata).
//   - Legacy: SKAFKA_PORT / SKAFKA_TLS_* / SKAFKA_AUTHED_LISTEN_ADDR
//     triplet. Kept for pre-v0.1.122 deployments.
//
// TLS material loads lazily — only when any listener requested it. Fatal
// TLS-config errors call os.Exit(1).
//
// Side effect: applyListenerPortsToBrokerSource(&brokerSource, ...) is
// called in the SKAFKA_LISTENERS path so Metadata responses point clients
// back to the listener they bootstrapped on (gh #125).
func setupListeners(cfg brokerConfig, authEngine auth.AuthEngine, allowAll *broker.AllowAllAuthEngine, initial auth.PerListenerAuthEngine, brokerSource *handlers.BrokerSource) listenerSetup {
	var sharedTLS *tls.Config
	loadTLSCfg := func() *tls.Config {
		if sharedTLS != nil {
			return sharedTLS
		}
		certFile := os.Getenv("SKAFKA_TLS_CERT_FILE")
		if certFile == "" {
			slog.Error("SKAFKA_TLS_CERT_FILE unset but a listener requested tls: true")
			os.Exit(1)
		}
		keyFile := os.Getenv("SKAFKA_TLS_KEY_FILE")
		var tlsOpts []protocol.TLSOption
		if caFile := os.Getenv("SKAFKA_TLS_CLIENT_CA_FILE"); caFile != "" {
			tlsOpts = append(tlsOpts, protocol.WithRequireClientCert(caFile))
			slog.Info("TLS: client cert verification enabled", "ca_bundle", caFile)
		}
		cfgTLS, err := protocol.WatchingCertificate(certFile, keyFile, tlsOpts...)
		if err != nil {
			slog.Error("load TLS cert", "err", err)
			os.Exit(1)
		}
		sharedTLS = cfgTLS
		return cfgTLS
	}

	specs, err := parseListenersEnv()
	if err != nil {
		slog.Error("SKAFKA_LISTENERS", "err", err)
		os.Exit(1)
	}
	if specs != nil {
		logListenerWireup(specs)
		for _, s := range specs {
			if s.TLS {
				_ = loadTLSCfg()
				break
			}
		}
		wire := buildListenerWireup(specs, cfg.Host, sharedTLS, authEngine, allowAll)
		applyListenerPortsToBrokerSource(brokerSource, wire.ListenerPorts)
		return listenerSetup{Configs: wire.Configs, Engines: wire.Engines, TLSActive: wire.TLSActive}
	}

	// Legacy env-var-per-listener path.
	configs := []protocol.ListenerConfig{{Name: "internal", Addr: cfg.Host + ":" + cfg.Port}}
	tlsActive := false
	if certFile := os.Getenv("SKAFKA_TLS_CERT_FILE"); certFile != "" {
		tlsCfg := loadTLSCfg()
		tlsPort := envOr("SKAFKA_TLS_PORT", "9093")
		configs = append(configs, protocol.ListenerConfig{
			Name:      "external",
			Addr:      cfg.Host + ":" + tlsPort,
			TLSConfig: tlsCfg,
		})
		tlsActive = true
		slog.Info("TLS listener configured", "addr", cfg.Host+":"+tlsPort, "cert", certFile)
	}
	// gh #139: optional SASL-required plaintext listener.
	if authedAddr := os.Getenv("SKAFKA_AUTHED_LISTEN_ADDR"); authedAddr != "" {
		configs = append(configs, protocol.ListenerConfig{Name: "authed", Addr: authedAddr})
		slog.Info("authed listener configured (SASL required)", "addr", authedAddr)
	}
	return listenerSetup{Configs: configs, Engines: initial, TLSActive: tlsActive}
}

// startBrokerHealth brings up the /healthz + /readyz HTTP server. The
// readiness function checks (a) the TCP listener is bound and (b) the
// optional storage reaper's queue isn't saturated (gh #118 — saturated
// readiness flips 503 so kube-proxy routes around this pod until the
// queue drains).
func startBrokerHealth(ctx context.Context, cfg brokerConfig, id brokerIdentity, srv *protocol.Server, store storage.StorageEngine, rt *clusterRuntime, tlsActive bool) {
	listeners := []string{"internal"}
	var tlsInfo *observability.TLSInfo
	if tlsActive {
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
	if cfg.K8sMode {
		healthBrokerID = fmt.Sprintf("skafka-%d", id.BrokerID)
	}
	const reaperBacklogReadyThreshold = 50
	readinessFn := func() readinessStatus {
		if srv.Addr() == "" {
			return readinessStatus{Ready: false, Reason: "tcp_listener_not_bound"}
		}
		// Storage engine may not expose a reaper (memory storage /
		// dev mode); only apply backpressure when one is wired.
		type reaperHolder interface {
			Reaper() *storage.PartitionReaper
		}
		if rh, ok := store.(reaperHolder); ok {
			if r := rh.Reaper(); r != nil {
				depth := r.QueueDepth()
				if depth > reaperBacklogReadyThreshold {
					return readinessStatus{
						Ready:            false,
						Reason:           "reaper_saturated",
						ReaperBacklog:    depth,
						BacklogThreshold: reaperBacklogReadyThreshold,
					}
				}
			}
		}
		return readinessStatus{Ready: true}
	}
	startHealthServer(ctx, healthServerConfig{
		addr:      envOr("SKAFKA_HEALTH_ADDR", ":8080"),
		brokerID:  healthBrokerID,
		listeners: listeners,
		tls:       tlsInfo,
		source:    healthSource,
		readiness: readinessFn,
	})
}

// setupClusterRuntime boots the v3 cluster runtime (controller election,
// assignment.json writer, heartbeat gRPC server, K8s CR mirror) and wires
// the broker.Coordinator into the produce handler. Returns nil when the
// preconditions aren't met (single-broker dev mode without K8s / dataDir
// / engine — the broker stays on the legacy lease+lock fallback).
//
// Must be called BEFORE RegisterHandlers (the produce handler picks up
// the BrokerCoordinator via WithCoordinator) and BEFORE the topic watcher
// (the watcher's onEvent callback notifies the controller's AssignmentLoop
// on KafkaTopic adds/modifies/deletes — gh #74).
func setupClusterRuntime(ctx context.Context, cfg brokerConfig, id brokerIdentity, engine *storage.DiskStorageEngine, coordMgr *coordinator.Manager, b *broker.Broker) *clusterRuntime {
	if !cfg.K8sMode || cfg.DataDir == "" || id.K8sClient == nil || engine == nil {
		return nil
	}
	brokerIDStr := fmt.Sprintf("skafka-%d", id.BrokerID)
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
	// unset, fall back to SKAFKA_CLUSTER_ID so old deploys without the new
	// env var still work.
	clusterName := envOr("SKAFKA_CLUSTER_NAME", cfg.ClusterID)

	// Controller Lease tuning. Zero (or unparseable) → cluster_runtime
	// falls back to controller.New defaults (15s/10s/2s). The Helm
	// chart's broker.controllerLease.* block is the production source.
	leaseDuration := envSecondsOr("SKAFKA_CONTROLLER_LEASE_DURATION_SECONDS", 0)
	renewDeadline := envSecondsOr("SKAFKA_CONTROLLER_RENEW_DEADLINE_SECONDS", 0)
	retryPeriod := envSecondsOr("SKAFKA_CONTROLLER_RETRY_PERIOD_SECONDS", 0)

	rt := startClusterRuntime(ctx, clusterRuntimeConfig{
		k8sClient:         id.K8sClient,
		namespace:         cfg.Namespace,
		brokerIDStr:       brokerIDStr,
		dataDir:           cfg.DataDir,
		engine:            engine,
		coordMgr:          coordMgr,
		topicRegistry:     b.Topics(),
		brokerReg:         id.BrokerReg,
		heartbeatAddr:     heartbeatAddr,
		peerHeartbeatPort: peerPort,
		crClient:          crClient,
		clusterName:       clusterName,
		leaseDuration:     leaseDuration,
		renewDeadline:     renewDeadline,
		retryPeriod:       retryPeriod,
	})
	b.UseCoordinator(rt.coord)
	// Feed runtime state to the ObservableGauge callback registered by
	// observability.Bootstrap. Until this runs, gauges report zero.
	observability.SetGaugeSource(&runtimeGaugeSource{rt: rt})
	return rt
}

// wireTopicCRWriter plugs admin-protocol CreateTopics/DeleteTopics into
// the operator: every API request writes a KafkaTopic CR; the operator's
// reconciler then materialises partition dirs on the shared PVC and every
// broker's TopicWatcher fires Added — so a Metadata refresh from any peer
// sees the new topic. Without this, admin-protocol topic ops are local
// to a single broker.
//
// No-op outside K8s mode (no API server to write CRs against).
func wireTopicCRWriter(b *broker.Broker, cfg brokerConfig) {
	if !cfg.K8sMode {
		return
	}
	topicCRScheme := runtime.NewScheme()
	if err := operatorv1.AddToScheme(topicCRScheme); err != nil {
		return
	}
	cl, err := sigs_client.New(mustRestConfig(), sigs_client.Options{Scheme: topicCRScheme})
	if err != nil {
		slog.Warn("admin: build CR writer failed; CreateTopics will be local-only", "err", err)
		return
	}
	// gh #106: optional ArgoCD integration. When ApplicationName is set,
	// admin-protocol-created CRs get tracking-id + compare-options
	// annotations so they coexist cleanly with git-managed CRs in the
	// same ArgoCD Application's resource tree.
	argoCfg := k8spkg.ArgoCDConfig{
		Enabled:         os.Getenv("SKAFKA_ARGOCD_ENABLED") == "true",
		ApplicationName: os.Getenv("SKAFKA_ARGOCD_APPLICATION_NAME"),
		CompareOptions:  os.Getenv("SKAFKA_ARGOCD_COMPARE_OPTIONS"),
		SyncOptions:     os.Getenv("SKAFKA_ARGOCD_SYNC_OPTIONS"),
	}
	// Default compare-options to IgnoreExtraneous when ArgoCD is on but
	// the env was not explicitly set. Empty stays empty when set to "".
	if argoCfg.Enabled {
		if _, set := os.LookupEnv("SKAFKA_ARGOCD_COMPARE_OPTIONS"); !set {
			argoCfg.CompareOptions = "IgnoreExtraneous"
		}
	}
	b.UseTopicCRWriter(k8spkg.NewTopicCRWriter(cl, cfg.Namespace, argoCfg))
	// gh #103 phase 2 + gh #104: the K8s KafkaUserWriter implements
	// both the handlers.KafkaUserWriter (AlterClientQuotas) and the
	// handlers.SCRAMCredentialWriter (AlterUserScramCredentials)
	// interfaces — same struct, disjoint methods, shared controller-
	// runtime client + scheme.
	userWriter := k8spkg.NewKafkaUserWriter(cl, cfg.Namespace)
	b.UseKafkaUserCRWriter(userWriter)
	b.UseSCRAMCredentialCRWriter(userWriter)
	// gh #107: ACL admin-protocol writes patch the same KafkaUser CRs
	// via their inline spec.authorization.acls list (gh #135).
	b.UseACLCRWriter(k8spkg.NewKafkaUserACLWriter(cl, cfg.Namespace))
}

// setupDispatcher builds the request dispatcher: per-listener auth-engine
// map, cluster-wide authorizer (gh #126), request-level observability +
// tracing middleware, then registers all the Kafka API handlers. Returns
// the dispatcher and the per-listener engine map that setupListeners may
// later override.
//
// Order matters: middleware Use() calls must happen BEFORE RegisterHandlers
// (the chain is applied at Register time).
func setupDispatcher(ctx context.Context, authEngine auth.AuthEngine, allowAll *broker.AllowAllAuthEngine, b *broker.Broker) (*protocol.Dispatcher, auth.PerListenerAuthEngine) {
	d := protocol.NewDispatcher()
	// gh #124: per-listener engine selector. Until SKAFKA_LISTENERS is
	// parsed (setupListeners), build a 3-entry map matching the legacy
	// internal/external/authed triplet:
	//   - internal: AllowAll unless SKAFKA_REQUIRE_SASL forces real
	//   - external: same as internal (TLS is independent of auth)
	//   - authed:   always real engine — SASL-required listener
	// The "" fallback uses the plain pick so untagged connections
	// (test harnesses) preserve the pre-#124 behaviour.
	globalRequireSASL := os.Getenv("SKAFKA_REQUIRE_SASL") == "true"
	pickPlain := func() auth.AuthEngine {
		if globalRequireSASL {
			return authEngine
		}
		return allowAll
	}
	pickAuthed := func() auth.AuthEngine {
		// Authed listener always requires SASL. Falls back to allowAll
		// only when no real engine is wired (auth.enabled: false), in
		// which case the listener doesn't carry meaningful policy
		// anyway — every conn becomes ANONYMOUS regardless.
		if _, isReal := authEngine.(*auth.RealAuthEngine); isReal {
			return authEngine
		}
		return allowAll
	}
	listenerEngines := auth.PerListenerAuthEngine{
		string(connstate.ListenerName("internal")): pickPlain(),
		string(connstate.ListenerName("external")): pickPlain(),
		string(connstate.ListenerName("authed")):   pickAuthed(),
		"": pickPlain(),
	}
	d.SetAuthEngines(listenerEngines)
	if b != nil {
		b.SetAuthEngineSelector(listenerEngines)
		applyClusterAuthorizer(b, authEngine)
	}
	// gh #121: request-level observability is a uniform middleware so
	// every API key gets a latency histogram. Must Use() before
	// RegisterHandlers — the chain is applied at Register time.
	d.Use(protocol.RequestObservability())
	// gh #121 PR4: per-request OTel span around every handler. Tracing
	// is the inner ring — RequestObservability wraps around it so the
	// latency histogram captures the tracing-middleware overhead too.
	d.Use(protocol.RequestTracing())
	// gh #14: broker-side max.message.bytes enforcement. Defaults to
	// Apache's 1048588 (1MiB + 12 bytes batch overhead) when the env
	// var is unset; the produce handler treats <= 0 as "use default".
	if n, err := strconv.Atoi(os.Getenv("SKAFKA_MAX_MESSAGE_BYTES")); err == nil && n > 0 {
		b.SetMaxMessageBytes(int32(n))
	}
	b.RegisterHandlers(d)
	// gh #108 phase 2: cross-broker producer-fence broadcast.
	// No-op in dev-mode (memory storage).
	b.StartFenceWatcher(ctx)
	return d, listenerEngines
}

// applyClusterAuthorizer wires the gh #126 cluster-wide authorizer when
// SKAFKA_AUTHORIZATION_TYPE=simple is set AND the broker has a real auth
// engine (auth.enabled: true). SKAFKA_SUPER_USERS wraps the simple
// authorizer with an early-allow set — Strimzi's `authorization.superUsers`
// semantic.
func applyClusterAuthorizer(b *broker.Broker, authEngine auth.AuthEngine) {
	if os.Getenv("SKAFKA_AUTHORIZATION_TYPE") != "simple" {
		return
	}
	real, ok := authEngine.(*auth.RealAuthEngine)
	if !ok {
		slog.Warn("SKAFKA_AUTHORIZATION_TYPE=simple set but no RealAuthEngine wired — falling back to AllowAll. Set auth.enabled: true in the chart to load credentials.json / acls.json.")
		return
	}
	var authz auth.Authorizer = real
	if raw := os.Getenv("SKAFKA_SUPER_USERS"); raw != "" {
		var supers []string
		for _, s := range strings.Split(raw, ",") {
			if s = strings.TrimSpace(s); s != "" {
				supers = append(supers, s)
			}
		}
		if len(supers) > 0 {
			authz = auth.NewSuperUserAuthorizer(supers, real)
			slog.Info("authorization configured", "type", "simple", "super_users", supers)
		}
	}
	b.SetAuthorizer(authz)
	slog.Info("authorization configured (cluster-wide)", "type", "simple")
}

// setupAuthEngine returns the auth.AuthEngine for the broker. Default is
// AllowAll (anonymous). When SKAFKA_DATA_DIR is set and SKAFKA_AUTH_DISABLED
// is not "true", attempts to load RealAuthEngine from /data/__cluster/...
// and starts a ClusterFileWatcher goroutine that calls Reload() on
// credentials.json / acls.json changes. Falls back to AllowAll if the real
// engine can't initialize.
func setupAuthEngine(ctx context.Context, cfg brokerConfig, k8sClient kubernetes.Interface) auth.AuthEngine {
	if cfg.DataDir == "" || os.Getenv("SKAFKA_AUTH_DISABLED") == "true" {
		return broker.NewAllowAllAuthEngine()
	}
	real, err := auth.NewRealAuthEngine(cfg.DataDir, k8sClient)
	if err != nil {
		slog.Warn("auth engine init failed, falling back to AllowAll", "err", err)
		return broker.NewAllowAllAuthEngine()
	}
	// Hot-reload credentials and ACLs on file change.
	watcher := storage.NewClusterFileWatcher(
		filepath.Join(cfg.DataDir, "__cluster", "acls.json"),
		filepath.Join(cfg.DataDir, "__cluster", "credentials.json"),
		func(_ string) { real.Reload() },
		func(_ string) { real.Reload() },
	)
	go func() {
		done := make(chan struct{})
		go func() { <-ctx.Done(); close(done) }()
		_ = watcher.Run(done)
	}()
	return real
}

func runBroker(ctx context.Context) {
	cfg := loadBrokerConfig()
	host := cfg.Host
	port := cfg.Port
	portNum := cfg.PortNum
	clusterID := cfg.ClusterID
	dataDir := cfg.DataDir
	namespace := cfg.Namespace
	_ = cfg.HeadlessSvc // consumed inside setupBrokerIdentity

	id := setupBrokerIdentity(ctx, cfg)
	brokerID := id.BrokerID
	k8sClient := id.K8sClient
	brokerSource := id.BrokerSource
	leaseManager := id.LeaseManager
	k8sMode := cfg.K8sMode

	stack := setupStorageStack(ctx, cfg, id)
	store := stack.Store
	engine := stack.Engine
	coordMgr := stack.CoordMgr
	k8sTopics := stack.K8sTopics

	authEngine := setupAuthEngine(ctx, cfg, k8sClient)

	b := broker.NewWithBrokerSource(
		broker.Config{BrokerID: brokerID, Host: host, Port: portNum, ClusterID: clusterID},
		store,
		leaseManager,
		authEngine,
		brokerSource,
		coordMgr,
	)

	// gh #119: wire the partition reaper's CR-existence recheck so
	// it consults the broker's TopicRegistry (the live view the
	// topic-watcher maintains) before each reap. Idempotent — no-op
	// when the engine has no reaper (memory-storage / dev mode).
	b.WireReaperCRCheck()

	// Register topics discovered during k8s startup so Metadata responses and
	// produce/fetch dispatch resolve them.
	for _, t := range k8sTopics {
		b.AddTopic(t.Name, t.Partitions)
	}

	// gh #48: combined retention (gh #47) + compaction cleaner.
	// Per-topic cleanup.policy decides which path each partition
	// takes. Policy source is the broker's TopicRegistry — the
	// topic-watcher (started below) pushes CR-driven values into
	// it via b.SetTopicCleanupPolicy on every observation. The
	// adapter shaves the typed broker.CleanupPolicy down to the
	// string the storage-side interface expects (avoids a typed
	// dependency from internal/storage onto internal/broker).
	if engine != nil {
		cleaner := storage.NewRetentionCleaner(engine, leaseManager, 0).
			WithPolicySource(cleanupPolicyAdapter{b.Topics()})
		go cleaner.Run(ctx)
		slog.Info("retention + compaction cleaner started")
	}

	rt := setupClusterRuntime(ctx, cfg, id, engine, coordMgr, b)

	// Watch KafkaTopic CRs so topics created after startup become visible
	// without a broker restart, partition expansions are picked up, and
	// the controller (when this broker holds the Lease) recomputes the
	// assignment for the new topic shape.
	if k8sMode && dataDir != "" {
		startTopicWatcher(ctx, namespace, b, engine, leaseManager, k8sTopics, rt)
	}

	wireTopicCRWriter(b, cfg)

	allowAll := broker.NewAllowAllAuthEngine()
	d, listenerEngines := setupDispatcher(ctx, authEngine, allowAll, b)

	ls := setupListeners(cfg, authEngine, allowAll, listenerEngines, &brokerSource)
	listenerCfgs := ls.Configs
	listenerEngines = ls.Engines
	tlsListenerActive := ls.TLSActive

	srvCfg := protocol.Config{Listeners: listenerCfgs}
	srv := protocol.NewServer(srvCfg, d)
	srv.SetAuthEngine(authEngine)
	// gh #43: ssl.principal.mapping.rules (KIP-371). Empty → CN-only
	// behaviour preserved. Parse failure logs a warning and skips
	// (rather than failing broker startup over a chart-config typo).
	if rules := os.Getenv("SKAFKA_SSL_PRINCIPAL_MAPPING_RULES"); rules != "" {
		if mapper, err := auth.NewPrincipalMapper(rules); err == nil {
			srv.SetPrincipalMapper(mapper)
		} else {
			slog.Warn("ssl.principal.mapping.rules parse failed; falling back to CN-only mTLS principal extraction",
				"rules", rules, "err", err)
		}
	}
	if err := srv.Start(ctx); err != nil {
		slog.Error("start server", "err", err)
		os.Exit(1)
	}

	startBrokerHealth(ctx, cfg, id, srv, store, rt, tlsListenerActive)

	slog.Info("skafka broker ready", "host", host, "port", port, "cluster_id", clusterID)
	<-ctx.Done()
	slog.Info("shutting down")
	// gh #139: flush manifests on shutdown so the next broker open
	// reads accurate HighWatermark values. Without this, the
	// lazy-manifest persistence (only on segment roll / cleaner /
	// takeover / Relinquish) leaves the manifest stale and the new
	// broker reports HWM=0 to clients on first OffsetFetch.
	if engine != nil {
		if err := engine.FlushManifests(); err != nil {
			slog.Warn("shutdown: FlushManifests failed (next start may read stale HWM)", "err", err)
		}
	}
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
func acquireK8sPartitions(ctx context.Context, namespace string, engine *storage.DiskStorageEngine) []topicSpec {
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
	primed []topicSpec,
	rt *clusterRuntime,
) {
	onEvent := func(ev k8spkg.TopicEvent) {
		switch ev.Type {
		case k8spkg.TopicAdded:
			slog.Info("kafkatopic added", "topic", ev.Name, "partitions", ev.Partitions, "cleanupPolicy", ev.CleanupPolicy)
			// Just create partition directories on the PVC. Leadership
			// is decided by the controller's balancer and surfaces via
			// assignment.json — no per-partition Lease acquisition here
			// (gh #75 cleanup).
			for p := int32(0); p < ev.Partitions; p++ {
				if err := engine.CreatePartition(ev.Name, p); err != nil {
					slog.Warn("topic watcher: creating partition directory on the PVC failed (subsequent Produce to this partition will hit the open-handles path on a missing dir and return UNKNOWN_SERVER_ERROR; mkdir is idempotent and will be retried on next CR reconcile)",
						"topic", ev.Name, "partition", p, "err", err)
				}
			}
			b.AddTopic(ev.Name, ev.Partitions)
			// gh #93: push the full CR Spec.Config so DescribeConfigs
			// surfaces effective per-topic values (cleanup.policy,
			// retention.ms, segment.bytes, …). Supersedes the
			// gh #48 SetTopicCleanupPolicy push — Cleanup is part of
			// Config now.
			b.SetTopicConfig(ev.Name, ev.Config)
			// Triggers an assignment recompute on whichever broker is
			// currently controller. No-op on non-controller brokers and
			// when the v3 runtime is disabled. Without this, new topics
			// don't appear in assignment.json — gh #74.
			rt.NotifyTopicChange(ctx, kafkaapi.AssignmentReasonTopicCreated, ev.Name)

		case k8spkg.TopicModified:
			slog.Info("kafkatopic modified", "topic", ev.Name, "oldPartitions", ev.OldPartitions, "newPartitions", ev.Partitions, "cleanupPolicy", ev.CleanupPolicy)
			for p := ev.OldPartitions; p < ev.Partitions; p++ {
				if err := engine.CreatePartition(ev.Name, p); err != nil {
					slog.Warn("topic watcher: creating partition directory on the PVC failed (subsequent Produce to this partition will hit the open-handles path on a missing dir and return UNKNOWN_SERVER_ERROR; mkdir is idempotent and will be retried on next CR reconcile)",
						"topic", ev.Name, "partition", p, "err", err)
				}
			}
			b.AddTopic(ev.Name, ev.Partitions)
			b.SetTopicConfig(ev.Name, ev.Config)
			if ev.OldPartitions != ev.Partitions {
				rt.NotifyTopicChange(ctx, kafkaapi.AssignmentReasonTopicResized, ev.Name)
			}

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
					slog.Warn("topic watcher: closing partition fds on the leader broker failed before the operator's unlinkat (NFS may silly-rename the open files to .nfsXXXX entries that EBUSY the operator's directory removal forever — gh #76); the operator's reconcile will retry but may stall until this broker's fds are forced closed",
						"topic", ev.Name, "partition", p, "err", err)
				}
				_ = lm.Release(ev.Name, p)
			}
			b.RemoveTopic(ev.Name)
			rt.NotifyTopicChange(ctx, kafkaapi.AssignmentReasonTopicDeleted, ev.Name)
		}
	}

	w, err := k8spkg.NewTopicWatcher(mustRestConfig(), namespace, onEvent)
	if err != nil {
		slog.Warn("topic watcher: initialization failed (broker proceeds with topics it knows about from the startup CR list, but will not see new-topic-creation, modification, or delete events — operator-driven CR changes won't propagate until the broker restarts)",
			"err", err)
		return
	}
	for _, t := range primed {
		w.Prime(t.Name, t.Partitions)
	}
	go func() {
		if err := w.Run(ctx); err != nil && ctx.Err() == nil {
			slog.Error("topic watcher: stopped before ctx cancellation (same impact as init-failed: broker is blind to subsequent KafkaTopic CR changes; the running broker keeps serving but operator-driven changes — new topics, partition expansion, deletes — won't be honoured until restart)",
				"err", err)
		}
	}()
}

// runInit creates partition directories for all KafkaTopics on the PVC and exits.
//
// The partition-init initContainer runs as root (uid 0) so that
// ensureDataDirPerms can chown -R the data dir to the broker's uid/gid even
// when the PVC came back from the provisioner as root-owned and
// fsGroupChangePolicy=OnRootMismatch silently skipped the recursive perm fix
// (skafka#110). Mirrors the Strimzi `volume-mount-hack` pattern.
func runInit(ctx context.Context) {
	dataDir := os.Getenv("SKAFKA_DATA_DIR")
	if dataDir == "" {
		slog.Error("init: SKAFKA_DATA_DIR not set")
		os.Exit(1)
	}
	namespace := envOr("SKAFKA_NAMESPACE", "default")
	brokerUID := envOrInt("SKAFKA_BROKER_UID", 65532)
	brokerGID := envOrInt("SKAFKA_BROKER_GID", 65532)

	if err := os.MkdirAll(dataDir, 0o775); err != nil {
		slog.Error("init: mkdir data dir", "dir", dataDir, "err", err)
		os.Exit(1)
	}

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
			if err := os.MkdirAll(dir, 0o775); err != nil {
				slog.Error("init: mkdir", "dir", dir, "err", err)
				os.Exit(1)
			}
		}
	}

	if err := ensureDataDirPerms(dataDir, brokerUID, brokerGID); err != nil {
		slog.Error("init: ensure data dir perms", "err", err)
		os.Exit(1)
	}
	slog.Info("init complete", "topics", len(topicList.Items))
}

// ensureDataDirPerms is layer B of the gh #110 defence-in-depth
// stack: kubelet's fsGroup (layer A) might silently fail on
// non-cooperating CSI drivers; this initContainer runs as root and
// makes the data dir writable by the broker process (uid:gid).
// Layer C (the storage engine's MkdirAll modes) gives every
// runtime-created subdirectory the same shape so even a missing
// init container can't cause a future cross-pod-write failure.
//
// Walk semantics:
//   - chown every entry to (uid, gid) so anything pre-existing
//     under root or another owner ends up owned by the broker.
//   - chmod every DIRECTORY to 0o775 (setgid + 0775). The setgid
//     bit makes new children inherit the dir's group, so files
//     created later by the broker (gid=0 via runAsGroup) keep
//     that group regardless of the broker's umask. Files keep
//     their existing modes — no need to chmod them.
//
// In dev mode (running as a normal user) chown is skipped — dataDir
// is then already owned by us, and chmod returns EPERM which we
// tolerate. Layer A + C carry the cluster on cooperating CSI;
// layer B + C carry it on non-cooperating CSI; only the absence of
// ALL THREE breaks topic creation.
func ensureDataDirPerms(dataDir string, uid, gid int) error {
	if err := os.Chmod(dataDir, 0o775); err != nil && !os.IsPermission(err) {
		return fmt.Errorf("chmod %s: %w", dataDir, err)
	}
	if os.Geteuid() != 0 {
		return nil
	}
	return filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if err := os.Chown(path, uid, gid); err != nil {
			return err
		}
		if info.IsDir() {
			if err := os.Chmod(path, 0o775); err != nil {
				return err
			}
		}
		return nil
	})
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
// source is the v3 RuntimeState (nil in local-dev). readiness returns
// the structured "why am I unready" state for /readyz.
type healthServerConfig struct {
	addr      string
	brokerID  string
	listeners []string
	tls       *observability.TLSInfo
	source    observability.RuntimeState
	readiness func() readinessStatus
}

// readinessStatus is the gh #118 structured /readyz response so
// operators see WHY the broker is unready (not just a 503 with empty
// body). Marshaled to JSON either way (200 or 503).
type readinessStatus struct {
	Ready          bool   `json:"ready"`
	Reason         string `json:"reason,omitempty"`
	ReaperBacklog  int    `json:"reaper_backlog,omitempty"`
	BacklogThreshold int  `json:"backlog_threshold,omitempty"`
}

// startHealthServer runs an HTTP server with the plan's /healthz JSON
// schema and a /readyz that flips on cfg.ready(). The handler is a
// thin wrapper around observability.HealthHandler — keeps the schema
// definition in one place, with unit-test coverage there.
func startHealthServer(ctx context.Context, cfg healthServerConfig) {
	mux := http.NewServeMux()
	mux.Handle("/healthz", observability.HealthHandler(cfg.brokerID, cfg.listeners, cfg.tls, cfg.source))
	// gh #132: opt-in pprof for perf investigations. Block + mutex
	// profilers are off by default in Go (rate 0); enable them
	// explicitly so /debug/pprof/{block,mutex} actually return data.
	// Block rate 1 = sample every block event; mutex fraction 1 = every
	// contention event. Both add observable overhead — only flip
	// SKAFKA_PPROF=true when you're about to profile.
	if os.Getenv("SKAFKA_PPROF") == "true" {
		goruntime.SetBlockProfileRate(1)
		goruntime.SetMutexProfileFraction(1)
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		slog.Info("pprof enabled on health server", "path", "/debug/pprof/")
	}
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		st := cfg.readiness()
		w.Header().Set("Content-Type", "application/json")
		if !st.Ready {
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			w.WriteHeader(http.StatusOK)
		}
		body, _ := json.Marshal(st)
		_, _ = w.Write(body)
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

// cleanupPolicyAdapter satisfies storage.CleanupPolicySource by shaving
// the typed broker.CleanupPolicy down to a plain string. Keeps
// internal/storage from having to import internal/broker (which would
// form a cycle: broker imports storage).
type cleanupPolicyAdapter struct {
	r *broker.TopicRegistry
}

func (a cleanupPolicyAdapter) CleanupPolicy(topic string) string {
	return string(a.r.CleanupPolicy(topic))
}
