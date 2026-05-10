// Package broker wires the protocol layer to the storage, lease, and auth
// interfaces. Phase 4 dropped the flock parameter from all constructors —
// single-writer enforcement is now BrokerCoordinator.Owns + epoch-prefixed
// segment filenames; see phase4-breakdown.md.
package broker

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/woestebanaan/skafka/internal/auth"
	"github.com/woestebanaan/skafka/internal/coordinator"
	k8sbroker "github.com/woestebanaan/skafka/internal/k8s"
	"github.com/woestebanaan/skafka/internal/lease"
	"github.com/woestebanaan/skafka/internal/protocol"
	"github.com/woestebanaan/skafka/internal/protocol/handlers"
	"github.com/woestebanaan/skafka/internal/storage"
	"github.com/woestebanaan/skafka/pkg/kafkaapi"
)

func sprintfOrdinal(pattern string, ordinal int32) string {
	return fmt.Sprintf(pattern, ordinal)
}

// envBool reads an env var and returns the boolean value, defaulting
// to def when unset or unparseable. "true"/"1" → true; "false"/"0" →
// false; everything else → def with a warn log.
func envBool(key string, def bool) bool {
	v := os.Getenv(key)
	switch strings.ToLower(v) {
	case "":
		return def
	case "true", "1":
		return true
	case "false", "0":
		return false
	default:
		slog.Warn("ignoring unparseable bool env var", "key", key, "value", v, "default", def)
		return def
	}
}

// envOrIntBroker is the broker-package mirror of cmd/skafka.envOrInt
// (kept private here to avoid importing the cmd package). Reads an
// env var and returns its int value, defaulting to def when unset or
// unparseable.
func envOrIntBroker(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		slog.Warn("ignoring unparseable int env var", "key", key, "value", v, "default", def, "err", err)
		return def
	}
	return n
}

// broadcastingFencer wraps a local ProducerEpochFencer with a
// FenceLog so every fence call (a) advances the local engine's
// per-partition producer-state cache and (b) writes to this
// broker's outbound fence log on the shared PVC. Peer brokers'
// FenceWatcher polls the directory and applies the entries to
// their local engines (gh #108 phase 2).
//
// Errors writing to the log are logged and swallowed — local
// fencing already happened, so the in-flight zombie is fenced on
// partitions led by this broker; the broadcast is a best-effort
// extension to peers.
type broadcastingFencer struct {
	local handlers.ProducerEpochFencer
	log   *coordinator.FenceLog
}

func (b *broadcastingFencer) FenceProducerEpoch(pid int64, epoch int16) {
	b.local.FenceProducerEpoch(pid, epoch)
	if err := b.log.Append(pid, epoch); err != nil {
		slog.Warn("fence log append failed; peer brokers won't see this fence until next bump",
			"pid", pid, "epoch", epoch, "err", err)
	}
}

// StartFenceWatcher starts the gh #108 phase 2 fence-watcher
// goroutine. Caller (cmd/skafka) supplies a context tied to the
// broker lifecycle. No-op if the watcher wasn't wired (dev-mode
// memory storage).
func (b *Broker) StartFenceWatcher(ctx context.Context) {
	if b.fenceWatcher == nil {
		return
	}
	go b.fenceWatcher.Run(ctx)
}

// Config holds broker identity and static configuration.
type Config struct {
	BrokerID  int32
	Host      string
	Port      int32
	ClusterID string
}

// Broker holds all runtime dependencies and registers handlers with the dispatcher.
type Broker struct {
	cfg     Config
	store   storage.StorageEngine
	leases  lease.LeaseManager
	auth    auth.AuthEngine
	topics  *TopicRegistry
	brokers handlers.BrokerSource
	coord   *coordinator.Manager // nil in local-dev mode (consumer-group manager)

	// fenceWatcher applies peer brokers' FenceLog entries to this
	// broker's storage engine. Set in RegisterHandlers when the
	// engine implements ProducerEpochFencer; nil in dev-mode
	// (memory storage) where there's no per-partition state to
	// fence. Started by StartFenceWatcher (gh #108 phase 2).
	fenceWatcher *FenceWatcher
	// brokerCoord is the v3 BrokerCoordinator that produces partition
	// ownership decisions on the produce hot path. UseCoordinator wires
	// it; RegisterHandlers picks it up via WithCoordinator on the
	// ProduceHandler. nil → legacy lease+lock fallback.
	brokerCoord kafkaapi.BrokerCoordinator

	// topicCRWriter persists CreateTopics / DeleteTopics admin-protocol
	// calls as KafkaTopic CR mutations (gh #51). UseTopicCRWriter wires
	// it; RegisterHandlers picks it up via WithCRWriter on the
	// CreateTopicsHandler / DeleteTopicsHandler. nil → admin protocol
	// updates only the local TopicRegistry, which is invisible to
	// peer brokers (broken in multi-broker production).
	topicCRWriter handlers.TopicCRWriter
}

func New(
	cfg Config,
	store storage.StorageEngine,
	leases lease.LeaseManager,
	authEng auth.AuthEngine,
) *Broker {
	info := handlers.BrokerInfo{
		NodeID:    cfg.BrokerID,
		Host:      cfg.Host,
		Port:      cfg.Port,
		ClusterID: cfg.ClusterID,
	}
	return &Broker{
		cfg:     cfg,
		store:   store,
		leases:  leases,
		auth:    authEng,
		topics:  NewTopicRegistry(),
		brokers: info,
	}
}

// NewWithBrokerSource creates a Broker with a dynamic multi-broker source and
// an optional coordinator (nil disables consumer group coordination).
func NewWithBrokerSource(
	cfg Config,
	store storage.StorageEngine,
	leases lease.LeaseManager,
	authEng auth.AuthEngine,
	brokers handlers.BrokerSource,
	coord *coordinator.Manager,
) *Broker {
	return &Broker{
		cfg:     cfg,
		store:   store,
		leases:  leases,
		auth:    authEng,
		topics:  NewTopicRegistry(),
		brokers: brokers,
		coord:   coord,
	}
}

// AddTopic registers a topic in the local registry so Metadata and produce/fetch work immediately.
func (b *Broker) AddTopic(name string, partitions int32) {
	b.topics.Add(name, partitions)
}

// SetTopicCleanupPolicy is the gh #48 hook: cmd/skafka calls this
// from the topic-watcher onEvent so the broker's cleaner knows
// whether to dispatch a partition through retention-only or the
// compactor. Empty policy is fine — the registry treats it as the
// default (delete).
func (b *Broker) SetTopicCleanupPolicy(name, policy string) {
	b.topics.SetCleanupPolicy(name, CleanupPolicy(policy))
}

// SetTopicConfig is the gh #93 hook: pipes the watcher-resolved
// KafkaTopic CR Spec.Config into the registry so DescribeConfigs
// returns effective per-topic values instead of broker defaults.
// Supersedes SetTopicCleanupPolicy at the cmd/skafka onEvent
// callsite — Cleanup gets propagated through Config.CleanupPolicy.
func (b *Broker) SetTopicConfig(name string, cfg handlers.TopicConfig) {
	b.topics.SetTopicConfig(name, cfg)
}

// Topics returns the underlying topic registry. Phase 4+5: cmd/skafka
// wraps this as the controller's TopicSource so the AssignmentLoop sees
// every topic the broker knows about.
func (b *Broker) Topics() *TopicRegistry {
	return b.topics
}

// UseTopicCRWriter wires the optional KafkaTopic-CR writer (gh #51).
// Must be called BEFORE RegisterHandlers so the admin handlers can
// pick it up via WithCRWriter. Without this, admin-protocol
// CreateTopics / DeleteTopics is local-broker-only (in-memory
// TopicRegistry), which is fine for tests / dev but invisible to
// peer brokers in production.
func (b *Broker) UseTopicCRWriter(w handlers.TopicCRWriter) {
	b.topicCRWriter = w
}

// UseCoordinator wires the v3 BrokerCoordinator into the broker. Must be
// called BEFORE RegisterHandlers so the ProduceHandler can pick it up
// via WithCoordinator at registration time. Calling this with nil leaves
// the broker on the legacy lease+lock fallback (the v2.6 path) — that's
// the single-broker dev mode default.
func (b *Broker) UseCoordinator(c kafkaapi.BrokerCoordinator) {
	b.brokerCoord = c
}

// RemoveTopic deregisters a topic from the local registry. Storage and lease
// cleanup happen elsewhere (lease TTL expiry; operator finalizer for dirs).
func (b *Broker) RemoveTopic(name string) {
	b.topics.Remove(name)
}

// RegisterHandlers wires all API key handlers into d and returns d.
func (b *Broker) RegisterHandlers(d *protocol.Dispatcher) *protocol.Dispatcher {
	produceHandler := handlers.NewProduceHandler(b.store, b.leases, b.auth)
	if b.brokerCoord != nil {
		produceHandler = produceHandler.WithCoordinator(b.brokerCoord)
	}
	d.Register(0, 3, 9, produceHandler)
	d.Register(1, 4, 12, handlers.NewFetchHandler(b.store, b.leases, b.auth))
	d.Register(2, 1, 7, handlers.NewListOffsetsHandler(b.store, b.leases))
	// Metadata: cap at v10. v11 removed IncludeClusterAuthorizedOperations,
	// but the Java AdminClient happily selects our advertised max and then
	// fails serialisation on its own side when the flag is set
	// ("Attempted to write a non-default includeClusterAuthorizedOperations
	// at version 12") — observed from kafbat-ui's brokers page. Capping at
	// v10 keeps the flag available, which is what callers actually want.
	// The only thing we give up is v11/v12 UUID-based topic IDs (a KRaft
	// transition feature skafka does not need).
	// Metadata's leader source: prefer the v3 BrokerCoordinator
	// (assignment.json-driven) when wired, fall back to the legacy
	// per-partition LeaseManager. The Lease path can disagree with the
	// controller's CR for freshly-added topics or during failover —
	// using assignment.json eliminates that split-brain (gh #75).
	var leaderSrc handlers.PartitionLeaderSource = b.leases
	if b.brokerCoord != nil {
		leaderSrc = b.brokerCoord
	}
	// gh #109: read auto.create.topics.enable + num.partitions from
	// env. Defaults match Apache 3.7 (true / 1). Only the CR-writer
	// path drives auto-create — without TopicCRWriter wired (dev mode
	// memory-only) the branch stays disabled even with env=true.
	autoCreate := handlers.AutoCreateTopicsConfig{
		Enabled:       envBool("SKAFKA_AUTO_CREATE_TOPICS_ENABLE", true),
		NumPartitions: int32(envOrIntBroker("SKAFKA_NUM_PARTITIONS", 1)),
	}
	metadataHandler := handlers.NewMetadataHandlerWithSource(b.brokers, b.cfg.ClusterID, b.topics, leaderSrc)
	if b.topicCRWriter != nil {
		metadataHandler = metadataHandler.WithAutoCreate(autoCreate, b.topicCRWriter)
	}
	d.Register(3, 1, 10, metadataHandler)
	d.Register(8, 2, 8, handlers.NewOffsetCommitHandler(b.coord))
	d.Register(9, 1, 8, handlers.NewOffsetFetchHandler(b.coord))
	d.Register(10, 0, 4, handlers.NewFindCoordinatorHandler(b.coord))
	d.Register(11, 2, 9, handlers.NewJoinGroupHandler(b.coord))
	d.Register(12, 0, 4, handlers.NewHeartbeatHandler(b.coord))
	d.Register(13, 0, 4, handlers.NewLeaveGroupHandler(b.coord))
	d.Register(14, 0, 5, handlers.NewSyncGroupHandler(b.coord))
	d.Register(15, 0, 5, handlers.NewDescribeGroupsHandler(b.coord))
	d.Register(16, 0, 4, handlers.NewListGroupsHandler(b.coord))
	// DeleteGroups (gh #89): supports kafka-consumer-groups.sh --delete
	// and AdminClient.deleteConsumerGroups(). v3+ adds member-level
	// deletion (per-member instead of per-group); skafka caps at v2
	// until that path is wired.
	d.Register(42, 0, 2, handlers.NewDeleteGroupsHandler(b.coord, b.auth))
	d.Register(17, 0, 1, handlers.NewSaslHandshakeHandler())
	// CreateTopics is capped at v6: v7 added the topic_id UUID
	// (KIP-516) to CreatableTopicResult, which our encoder doesn't
	// write — modern Java admin clients hit BufferUnderflowException
	// reading the missing 16 bytes (gh #73). Same shape as the
	// Metadata v10 cap above. Real fix is to encode topic_id and
	// raise back to v7+.
	createTopicsHandler := handlers.NewCreateTopicsHandler(b.topics)
	if b.topicCRWriter != nil {
		createTopicsHandler = createTopicsHandler.WithCRWriter(b.topicCRWriter)
	}
	d.Register(19, 0, 6, createTopicsHandler)
	// DeleteTopics capped at v5: v6+ changed `topic_names: [STRING]` to
	// `topics: [DeleteTopicState]` (name COMPACT_NULLABLE_STRING +
	// topic_id UUID — KRaft topic-id KIP-516). The codec still expects
	// the v0–v5 flat name array; advertising v6 made franz-go's
	// kmsg.DeleteTopicsRequest send the new struct shape and skafka
	// errored with "unexpected null compact string". Capping at v5
	// keeps name-based deletes working for kafka-topics.sh, kafbat-ui,
	// and Java AdminClient. v6 topic-id support is a separate parity
	// task.
	deleteTopicsHandler := handlers.NewDeleteTopicsHandler(b.topics)
	if b.topicCRWriter != nil {
		deleteTopicsHandler = deleteTopicsHandler.WithCRWriter(b.topicCRWriter)
	}
	d.Register(20, 0, 5, deleteTopicsHandler)
	deleteRecordsHandler := handlers.NewDeleteRecordsHandler(b.store)
	if b.brokerCoord != nil {
		deleteRecordsHandler = deleteRecordsHandler.WithCoordinator(b.brokerCoord)
	}
	d.Register(21, 0, 2, deleteRecordsHandler)
	// gh #12 stage A: hand out a fresh PID/epoch so idempotent producers
	// (default since Kafka 3.0) can complete their startup handshake.
	// Sequence-number enforcement in Produce is stage B.
	// gh #22: layer the TxnStateStore on top so non-empty
	// transactional.id rejoins bump the epoch (and the storage-
	// layer fence rejects the previous instance's writes).
	initPIDHandler := handlers.NewInitProducerIdHandler()
	// Only wire the TxnStateStore when DataDir is a real on-disk
	// path. MemoryStorage (local-dev / unit tests) returns
	// "memory://" — joining "__cluster" onto it would create a
	// stray "memory:/__cluster" directory in cwd. Production
	// DiskStorageEngine returns an absolute path so this just
	// skips the dev path.
	if dataDir := b.store.DataDir(); strings.HasPrefix(dataDir, "/") {
		clusterDir := filepath.Join(dataDir, "__cluster")
		// numSlots is decoupled from the StatefulSet replica count
		// (gh #108 follow-up): pinning to a fixed cluster-wide
		// constant — Apache's transaction.state.log.num.partitions=50
		// default — keeps the storage layout stable across scale
		// operations. Override via SKAFKA_TXN_NUM_SLOTS if 50 is
		// wrong for the cluster (set once at bootstrap; changes
		// trigger a re-shard pass on every broker startup).
		numSlots := 0 // 0 → DefaultNumSlots (50)
		if v := os.Getenv("SKAFKA_TXN_NUM_SLOTS"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				numSlots = n
			}
		}
		if txnStore, err := coordinator.NewTxnStateStore(clusterDir, numSlots); err == nil {
			initPIDHandler = initPIDHandler.WithTxnStateStore(txnStore)
		} else {
			slog.Warn("InitProducerId: TxnStateStore disabled (gh #22 epoch fence inactive)",
				"clusterDir", clusterDir, "err", err)
		}
	}
	// gh #30: cross-partition fence on every rejoin bump. Only
	// DiskStorageEngine implements the fence interface; MemoryStorage
	// has no per-partition producerStates to walk, so the cast just
	// fails and the fence stays a no-op.
	//
	// gh #108 phase 2: wrap the local fencer with a broadcasting
	// fencer that also writes to this broker's outbound fence log
	// under <dataDir>/__cluster/producer_fences/from-skafka-N.json.
	// Peer brokers' FenceWatcher (started in StartFenceWatcher)
	// poll the directory and apply each entry locally — closing
	// the cross-broker zombie window without a new gRPC RPC.
	if fencer, ok := b.store.(handlers.ProducerEpochFencer); ok {
		fencer := fencer // shadow for closure capture below
		if dataDir := b.store.DataDir(); strings.HasPrefix(dataDir, "/") {
			clusterDir := filepath.Join(dataDir, "__cluster")
			fenceDir := coordinator.FenceLogDir(clusterDir)
			brokerIDStr := fmt.Sprintf("skafka-%d", b.cfg.BrokerID)
			if log, err := coordinator.NewFenceLog(fenceDir, brokerIDStr); err == nil {
				initPIDHandler = initPIDHandler.WithFencer(&broadcastingFencer{local: fencer, log: log})
				selfFile := fmt.Sprintf("from-%s.json", brokerIDStr)
				b.fenceWatcher = NewFenceWatcher(fenceDir, selfFile, fencer)
			} else {
				slog.Warn("InitProducerId: fence broadcast disabled (gh #108 phase 2 inactive)",
					"fenceDir", fenceDir, "err", err)
				initPIDHandler = initPIDHandler.WithFencer(fencer)
			}
		} else {
			initPIDHandler = initPIDHandler.WithFencer(fencer)
		}
	}
	// gh #91: route InitProducerId for non-empty transactional.id to
	// the txn-coordinator broker (hash of txnID into the StatefulSet
	// broker set). Wiring is opt-in; in dev mode brokerCoord is the
	// LocalLeaseManager-backed stub which does not implement
	// TxnOwnership, so the cast fails and the gate stays disabled —
	// exactly the same back-compat shape as WithFencer above.
	if ownership, ok := b.brokerCoord.(handlers.TxnOwnership); ok {
		initPIDHandler = initPIDHandler.WithTxnOwnership(ownership)
	}
	d.Register(22, 0, 4, initPIDHandler)
	d.Register(29, 0, 3, handlers.NewDescribeAclsHandler())
	d.Register(30, 0, 3, handlers.NewCreateAclsHandler())
	d.Register(31, 0, 3, handlers.NewDeleteAclsHandler())
	// gh #109: advertise the live broker config so kafka-configs.sh /
	// kafbat-ui render the actual auto-create + num-partitions values.
	d.Register(32, 0, 3, handlers.NewDescribeConfigsHandler(b.topics, b.brokers).
		WithBrokerConfig(autoCreate.Enabled, autoCreate.NumPartitions))
	d.Register(35, 0, 1, handlers.NewDescribeLogDirsHandler(b.store, b.topics))
	d.Register(36, 0, 2, handlers.NewSaslAuthenticateHandler(b.auth))
	// gh #102: DescribeCluster (key 60). AdminClient.describeCluster()
	// and `kafka-cluster.sh --describe` need this; without it they
	// return empty / "no controller". ControllerID falls back to
	// Self().NodeID (every broker reports itself as controller),
	// matching the existing Metadata response (gh #85). Accurate
	// controller-id reporting via ControllerWatch.CurrentHolder is
	// a future enhancement once a pod-name → NodeID resolver is in
	// place.
	d.Register(60, 0, 1, handlers.NewDescribeClusterHandler(b.brokers, b.cfg.ClusterID))

	supported := d.SupportedVersions()
	supported[18] = [2]int16{0, 4}
	d.Register(18, 0, 4, handlers.NewAPIVersionsHandler(supported))

	return d
}

// K8sBrokerSource adapts a *k8s.BrokerRegistry to handlers.BrokerSource.
// ExtHostPattern + ExtPort optionally add per-broker external hostnames
// (broker-{ordinal}.kafka.example.com:9093) to each BrokerEndpoint.
type K8sBrokerSource struct {
	reg            *k8sbroker.BrokerRegistry
	ExtHostPattern string // fmt-style pattern, e.g. "broker-%d.kafka.example.com"
	ExtPort        int32
}

func NewK8sBrokerSource(reg *k8sbroker.BrokerRegistry) *K8sBrokerSource {
	return &K8sBrokerSource{reg: reg}
}

func (a *K8sBrokerSource) Self() handlers.BrokerEndpoint {
	e := a.reg.Self()
	ep := handlers.BrokerEndpoint{NodeID: e.NodeID, Host: e.Host, Port: e.Port}
	a.fillExternal(&ep)
	return ep
}

func (a *K8sBrokerSource) All() []handlers.BrokerEndpoint {
	all := a.reg.All()
	out := make([]handlers.BrokerEndpoint, 0, len(all))
	for _, e := range all {
		ep := handlers.BrokerEndpoint{NodeID: e.NodeID, Host: e.Host, Port: e.Port}
		a.fillExternal(&ep)
		out = append(out, ep)
	}
	return out
}

func (a *K8sBrokerSource) fillExternal(ep *handlers.BrokerEndpoint) {
	if a.ExtHostPattern == "" {
		return
	}
	ep.ExternalHost = fmtExternalHost(a.ExtHostPattern, ep.NodeID)
	ep.ExternalPort = a.ExtPort
}

// fmtExternalHost substitutes the broker ordinal into the fmt-style hostname pattern.
func fmtExternalHost(pattern string, ordinal int32) string {
	// Use Sprintf so the caller can use %d or other verbs.
	return sprintfOrdinal(pattern, ordinal)
}

var _ handlers.BrokerSource = (*K8sBrokerSource)(nil)
