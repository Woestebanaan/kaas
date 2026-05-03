package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"google.golang.org/grpc"
	"k8s.io/client-go/kubernetes"

	"github.com/woestebanaan/skafka/internal/assignment"
	"github.com/woestebanaan/skafka/internal/broker"
	"github.com/woestebanaan/skafka/internal/controller"
	"github.com/woestebanaan/skafka/internal/coordinator"
	k8spkg "github.com/woestebanaan/skafka/internal/k8s"
	"github.com/woestebanaan/skafka/internal/storage"
	"github.com/woestebanaan/skafka/pkg/heartbeatpb"
)

// clusterRuntimeConfig captures the inputs the v3 runtime needs from main.go.
type clusterRuntimeConfig struct {
	k8sClient      kubernetes.Interface
	namespace      string
	brokerIDStr    string // matches kafkaapi.ConsumerGroupAssignment.Broker, e.g. "skafka-0"
	dataDir        string // shared PVC mount
	engine         *storage.DiskStorageEngine
	coordMgr       *coordinator.Manager
	topicRegistry  *broker.TopicRegistry
	brokerReg      *k8spkg.BrokerRegistry
	heartbeatAddr  string // host:port the controller's heartbeat gRPC server binds to
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
	coord *broker.Coordinator
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

	// Heartbeat client deferred — see comment on clusterRuntime above.
	coord := broker.NewCoordinator(cfg.brokerIDStr, store, ctrlWatch, nil /* heartbeat client */)
	go func() { _ = coord.Start(ctx) }()

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

		loop := controller.NewAssignmentLoop(
			store, heartSrv, controller.NewNoopMirror(),
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
	go func() { _ = election.Run(ctx) }()

	slog.Info("v3 cluster runtime started",
		"broker", cfg.brokerIDStr,
		"namespace", cfg.namespace,
		"heartbeat_addr", cfg.heartbeatAddr,
	)

	return &clusterRuntime{coord: coord}
}

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
		// Otherwise (cold-start, no heartbeat client wired yet) trust
		// the registry alone.
		if len(connected) > 0 {
			if _, ok := connected[id]; !ok {
				continue
			}
		}
		out = append(out, id)
	}
	return out
}
