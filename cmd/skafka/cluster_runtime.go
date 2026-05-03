package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"google.golang.org/grpc"
	"k8s.io/client-go/kubernetes"
	sigs_client "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/woestebanaan/skafka/internal/assignment"
	"github.com/woestebanaan/skafka/internal/broker"
	"github.com/woestebanaan/skafka/internal/controller"
	"github.com/woestebanaan/skafka/internal/coordinator"
	k8spkg "github.com/woestebanaan/skafka/internal/k8s"
	"github.com/woestebanaan/skafka/internal/lease"
	"github.com/woestebanaan/skafka/internal/storage"
	"github.com/woestebanaan/skafka/pkg/heartbeatpb"
)

// clusterRuntimeConfig captures the inputs the v3 runtime needs from main.go.
type clusterRuntimeConfig struct {
	k8sClient         kubernetes.Interface
	namespace         string
	brokerIDStr       string // matches kafkaapi.ConsumerGroupAssignment.Broker, e.g. "skafka-0"
	dataDir           string // shared PVC mount
	engine            *storage.DiskStorageEngine
	coordMgr          *coordinator.Manager
	topicRegistry     *broker.TopicRegistry
	brokerReg         *k8spkg.BrokerRegistry
	heartbeatAddr     string // host:port the controller's heartbeat gRPC server binds to
	peerHeartbeatPort int32  // port to dial when reaching another broker's heartbeat server (default 9094)
	// crClient + clusterName drive the KafkaClusterAssignments CR mirror.
	// nil crClient → NoopMirror (the v2.6 path) so dev/test setups don't
	// need a working controller-runtime client.
	crClient    sigs_client.Client
	clusterName string
	// Controller Lease tuning. Zero values fall back to controller.New
	// defaults (15s/10s/2s); production overrides come from the Helm
	// chart's broker.controllerLease.* block via env vars in main.go.
	leaseDuration time.Duration
	renewDeadline time.Duration
	retryPeriod   time.Duration
}

// clusterRuntime owns the v3 distributed-coordination goroutines: the
// always-on broker side (BrokerCoordinator + ControllerWatch +
// AssignmentStore) plus the on-demand controller side (Election +
// AssignmentLoop + heartbeat gRPC server) that activates only when this
// broker holds the cluster Lease.
//
// Today the produce hot path stays on the legacy lease+lock fallback.
// Switching it onto Coordinator is a follow-up — once metrics confirm
// the runtime is happy in production, change ProduceHandler.coord via
// the WithCoordinator builder. The broker's fsnotify+poll watcher on
// /data/__cluster/assignment.json catches assignment changes within ~1s,
// so the heartbeat-pushed ASSIGNMENT_CHANGED fast path is also a
// follow-up; the broker doesn't dial the controller's heartbeat server
// from this initial cut.
type clusterRuntime struct {
	coord     *broker.Coordinator
	ctrlWatch *broker.ControllerWatch
	heart     *broker.HeartbeatClient
	brokerID  string
}

// startClusterRuntime boots every v3 goroutine and returns a handle that
// the rest of main.go can pull the BrokerCoordinator from. It only runs
// when all dependencies are present (k8s client, dataDir for the
// AssignmentStore); single-broker dev mode without those falls back to
// the v2.6 path naturally because this function isn't called.
func startClusterRuntime(ctx context.Context, cfg clusterRuntimeConfig) *clusterRuntime {
	// --- Broker-side, always-on ---

	// AssignmentStore writes to /data/__cluster/assignment.json on the shared
	// PVC. Bootstrap orphan-tmp cleanup once at start.
	store := assignment.NewFileStore(cfg.dataDir)
	store.CleanupOrphanTmp()

	// ControllerWatch polls the singleton skafka-controller Lease — the
	// source of truth for which broker is currently controller and which
	// leaseTransitions epoch they hold. The Coordinator's epoch fence
	// rejects assignment.json files whose controllerEpoch is behind.
	ctrlWatch := broker.NewControllerWatch(cfg.k8sClient, cfg.namespace)
	go func() { _ = ctrlWatch.Run(ctx) }()

	// Heartbeat client. The target follows the controller via
	// ctrlWatch.CurrentHolder + brokerReg lookup: when the Lease moves
	// to a different broker, the resolver returns the new dial target
	// and the next reconnect cycle picks it up. Empty return → backoff.
	peerPort := cfg.peerHeartbeatPort
	if peerPort == 0 {
		peerPort = 9094
	}
	heart := broker.NewHeartbeatClient("", cfg.brokerIDStr).WithTargetFunc(func() string {
		holder := ctrlWatch.CurrentHolder()
		if holder == "" {
			return ""
		}
		// holder is the pod name like "skafka-N"; look up its host via
		// the EndpointSlice-driven BrokerRegistry.
		ord := lease.ParseOrdinalFromIdentity(holder)
		for _, ep := range cfg.brokerReg.All() {
			if ep.NodeID == ord {
				return fmt.Sprintf("%s:%d", ep.Host, peerPort)
			}
		}
		return ""
	})
	go func() { _ = heart.Run(ctx) }()

	coord := broker.NewCoordinator(cfg.brokerIDStr, store, ctrlWatch, heart)
	go func() { _ = coord.Start(ctx) }()

	// Periodic upstream BrokerStatus tick: every heartbeatInterval, send
	// the broker's current view of the cluster — last-applied
	// assignmentVersion, partition statuses, active groups. The
	// controller aggregates these into ActiveGroups (Phase 5 GroupSource)
	// and broker liveness.
	go runHeartbeatPump(ctx, heart, coord, cfg.coordMgr)

	// TakeoverDriver moves the storage engine through TakeOver/Relinquish
	// when partition assignments change. GroupTakeoverDriver does the same
	// for consumer group state via coordinator.Manager.
	coord.OnAssignmentChange(broker.NewTakeoverDriver(cfg.engine, cfg.brokerIDStr).OnAssignmentChange)
	if cfg.coordMgr != nil {
		coord.OnAssignmentChange(broker.NewGroupTakeoverDriver(cfg.coordMgr, cfg.brokerIDStr).OnAssignmentChange)
	}

	// --- Controller-side, only when this broker holds the Lease ---

	// onAcquired fires when this broker wins the controller Lease. We spin
	// up the heartbeat gRPC server, the AssignmentLoop, and let them run
	// until the leader context is cancelled (loss of lease or process exit).
	onAcquired := func(leaderCtx context.Context, epoch int64) {
		slog.Info("controller acquired lease", "broker", cfg.brokerIDStr, "epoch", epoch)

		heartSrv := controller.NewHeartbeatServer()
		grpcSrv := grpc.NewServer()
		heartbeatpb.RegisterControllerHeartbeatServer(grpcSrv, heartSrv)

		lis, err := net.Listen("tcp", cfg.heartbeatAddr)
		if err != nil {
			slog.Error("controller: bind heartbeat addr", "addr", cfg.heartbeatAddr, "err", err)
			return
		}
		go func() {
			if err := grpcSrv.Serve(lis); err != nil && leaderCtx.Err() == nil {
				slog.Error("controller: grpc server exited", "err", err)
			}
		}()
		defer func() {
			grpcSrv.GracefulStop()
			_ = lis.Close()
		}()

		topicSrc := &topicSourceAdapter{r: cfg.topicRegistry}
		brokerSrc := &brokerSourceAdapter{reg: cfg.brokerReg, heart: heartSrv}

		var mirror controller.CRMirror = controller.NewNoopMirror()
		if cfg.crClient != nil && cfg.clusterName != "" {
			mirror = controller.NewK8sMirror(cfg.crClient, cfg.namespace, cfg.clusterName)
		}

		loop := controller.NewAssignmentLoop(
			store, heartSrv, mirror,
			topicSrc, brokerSrc, cfg.brokerIDStr,
		).WithGroupSource(heartSrv)

		if err := loop.Start(leaderCtx, epoch); err != nil {
			slog.Error("controller: assignment loop exited", "err", err)
		}
	}
	onLost := func() {
		slog.Info("controller lost lease", "broker", cfg.brokerIDStr)
	}

	election := controller.New(cfg.k8sClient, cfg.namespace, cfg.brokerIDStr, onAcquired, onLost)
	if cfg.leaseDuration > 0 && cfg.renewDeadline > 0 && cfg.retryPeriod > 0 {
		election = election.WithTimings(cfg.leaseDuration, cfg.renewDeadline, cfg.retryPeriod)
	}
	go func() { _ = election.Run(ctx) }()

	slog.Info("v3 cluster runtime started",
		"broker", cfg.brokerIDStr,
		"namespace", cfg.namespace,
		"heartbeat_addr", cfg.heartbeatAddr,
	)

	return &clusterRuntime{
		coord:     coord,
		ctrlWatch: ctrlWatch,
		heart:     heart,
		brokerID:  cfg.brokerIDStr,
	}
}

// healthRuntimeState adapts clusterRuntime to observability.RuntimeState
// for the /healthz handler. Lives here so cmd/skafka stays the only
// place that imports both packages — observability has no broker
// dependency.
type healthRuntimeState struct{ rt *clusterRuntime }

func (s *healthRuntimeState) IsController() bool {
	if s.rt == nil || s.rt.ctrlWatch == nil {
		return false
	}
	return s.rt.ctrlWatch.CurrentHolder() == s.rt.brokerID
}

func (s *healthRuntimeState) ControllerID() string {
	if s.rt == nil || s.rt.ctrlWatch == nil {
		return ""
	}
	return s.rt.ctrlWatch.CurrentHolder()
}

func (s *healthRuntimeState) ControllerEpoch() int64 {
	if s.rt == nil || s.rt.ctrlWatch == nil {
		return 0
	}
	return s.rt.ctrlWatch.CurrentEpoch()
}

// HeartbeatRTTMs is wired in Phase 10 Gap #3b — needs the heartbeat
// protocol to echo a send-time timestamp back. -1 → /healthz omits.
func (s *healthRuntimeState) HeartbeatRTTMs() int64 { return -1 }

func (s *healthRuntimeState) HeartbeatAgeMs() int64 {
	if s.rt == nil || s.rt.heart == nil {
		return -1
	}
	last := s.rt.heart.LastReceived()
	if last.IsZero() {
		return -1
	}
	return time.Since(last).Milliseconds()
}

func (s *healthRuntimeState) AssignmentVersion() uint64 {
	if s.rt == nil || s.rt.coord == nil {
		return 0
	}
	snap := s.rt.coord.Snapshot()
	if snap == nil {
		return 0
	}
	return uint64(snap.AssignmentVersion)
}

func (s *healthRuntimeState) AssignmentAgeMs() int64 {
	if s.rt == nil || s.rt.coord == nil {
		return -1
	}
	snap := s.rt.coord.Snapshot()
	if snap == nil || snap.GeneratedAt.IsZero() {
		return -1
	}
	return time.Since(snap.GeneratedAt).Milliseconds()
}

func (s *healthRuntimeState) PartitionsLed() int {
	if s.rt == nil || s.rt.coord == nil {
		return 0
	}
	snap := s.rt.coord.Snapshot()
	if snap == nil {
		return 0
	}
	n := 0
	for _, p := range snap.Partitions {
		if p.Broker == s.rt.brokerID && s.rt.coord.Owns(p.Topic, p.Partition) {
			n++
		}
	}
	return n
}

func (s *healthRuntimeState) PartitionsAssigned() int {
	if s.rt == nil || s.rt.coord == nil {
		return 0
	}
	snap := s.rt.coord.Snapshot()
	if snap == nil {
		return 0
	}
	n := 0
	for _, p := range snap.Partitions {
		if p.Broker == s.rt.brokerID {
			n++
		}
	}
	return n
}

// PartitionsRecovering is wired in Phase 10 Gap #3 — needs takeover
// instrumentation in TakeoverDriver. Until then, the broker reports
// 0 here, which is correct in steady state.
func (s *healthRuntimeState) PartitionsRecovering() int { return 0 }

// topicSourceAdapter wraps *broker.TopicRegistry as controller.TopicSource.
// Lives in cmd/skafka rather than internal/broker because internal/controller's
// own tests import internal/broker, so a broker→controller import would form
// a build-time cycle.
type topicSourceAdapter struct{ r *broker.TopicRegistry }

func (a *topicSourceAdapter) Topics() []controller.TopicSpec {
	entries := a.r.All()
	out := make([]controller.TopicSpec, len(entries))
	for i, e := range entries {
		out[i] = controller.TopicSpec{Name: e.Name, PartitionCount: e.Partitions}
	}
	return out
}

// brokerSourceAdapter combines two views of "alive brokers": the k8s
// EndpointSlice-driven BrokerRegistry (gives us pods that exist + are
// reachable) intersected with the heartbeat server's ConnectedBrokers
// list (gives us pods that have actually established a heartbeat). The
// intersection is the conservative answer the assignment loop needs —
// a pod that exists but never heartbeats shouldn't get partitions.
//
// Until the heartbeat client is wired (follow-up), heart.ConnectedBrokers
// only ever contains the controller broker itself once it self-streams,
// or remains empty. Fall back to the BrokerRegistry list in that case so
// at least the partitions get distributed.
type brokerSourceAdapter struct {
	reg   *k8spkg.BrokerRegistry
	heart *controller.HeartbeatServer
}

func (a *brokerSourceAdapter) AliveBrokers() []string {
	connected := map[string]struct{}{}
	for _, id := range a.heart.ConnectedBrokers() {
		connected[id] = struct{}{}
	}

	out := make([]string, 0)
	for _, ep := range a.reg.All() {
		id := fmt.Sprintf("skafka-%d", ep.NodeID)
		// If we have ANY heartbeat data, prefer the intersection.
		// Otherwise (cold-start, no heartbeat yet) trust the registry
		// alone so partitions still get distributed.
		if len(connected) > 0 {
			if _, ok := connected[id]; !ok {
				continue
			}
		}
		out = append(out, id)
	}
	return out
}

// runHeartbeatPump sends a periodic upstream BrokerStatus to the
// controller every heartbeatInterval. The status carries:
//   - lastSeenAssignmentVersion (so the controller can detect brokers
//     that missed an ASSIGNMENT_CHANGED push and re-deliver),
//   - active_groups (Phase 5 GroupSource — the controller takes the
//     union across brokers and treats it as the live group catalog).
//
// First Send is fire-and-forget; if the heartbeat stream isn't connected
// yet (target empty, dial failing, etc.) the helper backs off silently.
// This is the periodic "I'm alive" signal — controller-side recvLoop
// updates lastSeen on each message.
func runHeartbeatPump(
	ctx context.Context,
	heart *broker.HeartbeatClient,
	coord *broker.Coordinator,
	mgr *coordinator.Manager,
) {
	tick := time.NewTicker(1 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			status := &heartbeatpb.BrokerStatus{
				TimestampMs: time.Now().UnixMilli(),
			}
			if snap := coord.Snapshot(); snap != nil {
				status.LastSeenAssignmentVersion = uint64(snap.AssignmentVersion)
			}
			if mgr != nil {
				status.ActiveGroups = mgr.LocalGroups()
			}
			_ = heart.Send(status)
		}
	}
}
