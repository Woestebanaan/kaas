package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
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
	"github.com/woestebanaan/skafka/internal/observability"
	"github.com/woestebanaan/skafka/internal/storage"
	"github.com/woestebanaan/skafka/pkg/heartbeatpb"
	"github.com/woestebanaan/skafka/pkg/kafkaapi"
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
	// dataDir + brokerReg + engine are kept for the gauge source — none
	// are owned by the runtime, just borrowed pointers.
	dataDir   string
	brokerReg *k8spkg.BrokerRegistry
	engine    *storage.DiskStorageEngine

	// activeLoop is the AssignmentLoop owned by onAcquired while this
	// broker holds the controller Lease. NotifyTopicChange uses it to
	// trigger a recompute when the topic_watcher fires (gh #74). Set
	// before loop.Start, cleared after it returns. Stored behind a
	// minimal interface so cluster_runtime_test.go can substitute a
	// fake sink without spinning up the real assignment loop.
	mu         sync.Mutex
	activeLoop assignmentSink
}

// assignmentSink is the slice of *controller.AssignmentLoop that
// NotifyTopicChange depends on. *controller.AssignmentLoop satisfies
// this interface implicitly.
type assignmentSink interface {
	UpdateAssignment(ctx context.Context, change kafkaapi.AssignmentChange) error
}

// NotifyTopicChange asks the controller's AssignmentLoop to recompute
// because a KafkaTopic CR was added / modified / deleted. No-op on
// non-controller brokers (activeLoop is nil) and on a nil receiver
// (single-broker dev mode where the runtime never starts). Safe to call
// from any goroutine.
func (r *clusterRuntime) NotifyTopicChange(ctx context.Context, reason kafkaapi.AssignmentChangeReason, topic string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	loop := r.activeLoop
	r.mu.Unlock()
	if loop == nil {
		return
	}
	_ = loop.UpdateAssignment(ctx, kafkaapi.AssignmentChange{Reason: reason, Topic: topic})
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

	// Build the runtime handle early so the onAcquired closure below can
	// reach back into it (to publish/withdraw the active AssignmentLoop
	// pointer used by NotifyTopicChange).
	rt := &clusterRuntime{
		coord:     coord,
		ctrlWatch: ctrlWatch,
		heart:     heart,
		brokerID:  cfg.brokerIDStr,
		dataDir:   cfg.dataDir,
		brokerReg: cfg.brokerReg,
		engine:    cfg.engine,
	}

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
		observability.Global().ControllerFailovers.Add(leaderCtx, 1)

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

		// Publish the loop pointer so NotifyTopicChange (called from the
		// topic-watcher goroutine in main.go) can queue recomputes while
		// we hold the Lease. Withdraw it on the way out.
		rt.mu.Lock()
		rt.activeLoop = loop
		rt.mu.Unlock()
		defer func() {
			rt.mu.Lock()
			rt.activeLoop = nil
			rt.mu.Unlock()
		}()

		// Watch for broker join/leave so we recompute when the cluster
		// shape changes — without this, killing a broker leaves its
		// partitions assigned to a dead pod and producers hang on
		// retries until the StatefulSet recreates the broker (gh #77).
		// Polls AliveBrokers cheaply (in-memory), diffs against the
		// previous snapshot, queues an UpdateAssignment when the set
		// changes. Coalescing in the loop collapses bursts (e.g. all
		// brokers turning over during a rollout) into one recompute.
		go watchBrokerSet(leaderCtx, brokerSrc, loop)

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

	return rt
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

// runtimeGaugeSource adapts clusterRuntime to observability.GaugeSource
// so the Phase 10 ObservableGauges (is_controller, assignment_version,
// per-partition leader/epoch/HWM, broker counts, file size) sample from
// the live runtime on every Prometheus scrape.
type runtimeGaugeSource struct{ rt *clusterRuntime }

func (g *runtimeGaugeSource) IsController() int64 {
	if g.rt == nil || g.rt.ctrlWatch == nil {
		return 0
	}
	if g.rt.ctrlWatch.CurrentHolder() == g.rt.brokerID {
		return 1
	}
	return 0
}

func (g *runtimeGaugeSource) AssignmentVersion() int64 {
	if g.rt == nil || g.rt.coord == nil {
		return 0
	}
	snap := g.rt.coord.Snapshot()
	if snap == nil {
		return 0
	}
	return snap.AssignmentVersion
}

func (g *runtimeGaugeSource) BrokerCountAlive() int64 {
	if g.rt == nil || g.rt.brokerReg == nil {
		return 0
	}
	return int64(len(g.rt.brokerReg.All()))
}

func (g *runtimeGaugeSource) BrokerCountAssigned() int64 {
	if g.rt == nil || g.rt.coord == nil {
		return 0
	}
	snap := g.rt.coord.Snapshot()
	if snap == nil {
		return 0
	}
	seen := map[string]struct{}{}
	for _, p := range snap.Partitions {
		seen[p.Broker] = struct{}{}
	}
	return int64(len(seen))
}

func (g *runtimeGaugeSource) AssignmentFileSizeBytes() int64 {
	if g.rt == nil || g.rt.dataDir == "" {
		return 0
	}
	path := filepath.Join(g.rt.dataDir, "__cluster", "assignment.json")
	st, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return st.Size()
}

func (g *runtimeGaugeSource) Partitions() []observability.PartitionGauge {
	if g.rt == nil || g.rt.coord == nil {
		return nil
	}
	snap := g.rt.coord.Snapshot()
	if snap == nil {
		return nil
	}
	out := make([]observability.PartitionGauge, 0, len(snap.Partitions))
	for _, p := range snap.Partitions {
		row := observability.PartitionGauge{
			Topic:     p.Topic,
			Partition: p.Partition,
			LeaderID:  parsedOrdinal(p.Broker),
			Epoch:     int64(p.Epoch),
		}
		// HighWatermark only meaningful on the broker that leads the
		// partition; reading from the storage engine on a non-leader
		// would either fail or return a stale value.
		if g.rt.engine != nil && p.Broker == g.rt.brokerID {
			if hwm, err := g.rt.engine.HighWatermark(p.Topic, p.Partition); err == nil {
				row.HighWatermark = hwm
			}
		}
		out = append(out, row)
	}
	return out
}

// parsedOrdinal extracts the trailing integer from a "skafka-N"
// identifier. Returns -1 on a malformed value so the gauge clearly
// flags the bug rather than silently mapping to broker 0.
func parsedOrdinal(id string) int64 {
	ord := lease.ParseOrdinalFromIdentity(id)
	if ord < 0 {
		return -1
	}
	return int64(ord)
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

// watchBrokerSet polls the BrokerSource at a fixed cadence and queues
// an UpdateAssignment whenever the alive-broker set changes. Used by
// the controller side of the cluster runtime to detect broker joins
// and deaths without relying on a callback from the heartbeat server
// or the Endpoint slice watcher (the loop's recompute reads fresh
// inputs from BrokerSource anyway, so polling is sufficient and avoids
// a coordination contract that didn't exist before — gh #77).
//
// Reason is set to BrokerJoined / BrokerDead based on the diff
// direction; both ultimately drop into the same recompute path. The
// watcher lives for as long as this broker holds the controller Lease
// (ctx is the leaderCtx).
func watchBrokerSet(ctx context.Context, src controller.BrokerSource, loop assignmentSink) {
	const pollInterval = 2 * time.Second
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	prev := setOf(src.AliveBrokers())
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cur := setOf(src.AliveBrokers())
			added, removed := diffSets(prev, cur)
			if len(added) == 0 && len(removed) == 0 {
				continue
			}
			reason := kafkaapi.AssignmentReasonBrokerJoined
			brokerID := pickAny(added)
			if len(removed) > 0 {
				reason = kafkaapi.AssignmentReasonBrokerDead
				brokerID = pickAny(removed)
			}
			slog.Info("broker set changed; triggering recompute",
				"added", added, "removed", removed)
			_ = loop.UpdateAssignment(ctx, kafkaapi.AssignmentChange{
				Reason:   reason,
				BrokerID: brokerID,
			})
			prev = cur
		}
	}
}

func setOf(ids []string) map[string]struct{} {
	out := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		out[id] = struct{}{}
	}
	return out
}

func diffSets(prev, cur map[string]struct{}) (added, removed []string) {
	for k := range cur {
		if _, ok := prev[k]; !ok {
			added = append(added, k)
		}
	}
	for k := range prev {
		if _, ok := cur[k]; !ok {
			removed = append(removed, k)
		}
	}
	return added, removed
}

func pickAny(ids []string) string {
	if len(ids) == 0 {
		return ""
	}
	return ids[0]
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
