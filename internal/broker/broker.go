// Package broker wires the protocol layer to the storage, lease, and auth
// interfaces. Phase 4 dropped the flock parameter from all constructors —
// single-writer enforcement is now BrokerCoordinator.Owns + epoch-prefixed
// segment filenames; see phase4-breakdown.md.
package broker

import (
	"context"
	"errors"
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
	// auth is the per-listener engine selector (gh #124). The legacy
	// "single engine wired everywhere" path stays available via
	// auth.NewSingleAuthEngine wrapping a RealAuthEngine or
	// AllowAllAuthEngine, so callers (broker.New, tests) keep working
	// without knowing about the selector.
	auth    auth.AuthEngineSelector
	// gh #126: authorization is cluster-wide (not per-listener). The
	// dispatcher's pre-SASL gate still reads RequiresPreAuth() off the
	// per-listener engine, but every Authorize call goes through this
	// shared authorizer regardless of which listener accepted the
	// connection.
	authorizer auth.Authorizer
	// gh #126: quotas are also cluster-wide. Per-user throughput caps
	// don't depend on which listener a producer connected through.
	quotas auth.QuotaChecker
	// gh #103: runtime quota mutation surface (KIP-546). Wired by
	// main.go when a *auth.QuotaEnforcer is available; nil otherwise.
	quotaMgr handlers.QuotaManager
	topics   *TopicRegistry
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
	// kafkaUserCRWriter persists AlterClientQuotas mutations to
	// KafkaUser CRs (gh #103 phase 2). Wired by UseKafkaUserCRWriter
	// from main.go; nil leaves AlterClientQuotas in-memory-only.
	kafkaUserCRWriter handlers.KafkaUserWriter
	// gh #104: SCRAM-credential describe + alter store/writer (KIP-554).
	// scramStore is *auth.CredentialLoader via SCRAMCredentialStore;
	// scramCRWriter shares the same impl as kafkaUserCRWriter
	// (different methods on internal/k8s.KafkaUserWriter).
	scramStore    handlers.SCRAMCredentialStore
	scramCRWriter handlers.SCRAMCredentialWriter

	// gh #107: ACL writer threads admin-protocol CreateAcls /
	// DeleteAcls / DescribeAcls into KafkaUser.spec.authorization.acls
	// (per gh #135 — the dedicated KafkaACL CR is gone). Wired by
	// UseACLCRWriter from main.go; nil leaves the ACL handlers as
	// per-entry success stubs (kafka-compat / dev-mode behavior).
	aclCRWriter handlers.ACLCRWriter

	// txnStore is the per-broker TxnStateStore wired into both
	// InitProducerId (gh #22 epoch fence) and AddPartitionsToTxn
	// (gh #23 per-txn partition tracking). Nil in dev-mode or when
	// the dataDir is in-memory.
	txnStore *coordinator.TxnStateStore
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
		// Wrap the single AuthEngine in a SingleAuthEngine selector so
		// existing callers (tests, dev-mode boot) keep working unchanged
		// while handler code reads through AuthEngineSelector (gh #124).
		// Production callers wire a PerListenerAuthEngine directly via
		// SetAuthEngineSelector below.
		auth: auth.NewSingleAuthEngine(authEng),
		// gh #126: defaults to permissive cluster-wide authz + no
		// quotas. main.go replaces both via SetAuthorizer /
		// SetQuotaChecker when a RealAuthEngine is wired AND the
		// cluster config sets `authorization.type: simple`. Tests
		// that need authz checks call the setters themselves.
		authorizer: pickDefaultAuthorizer(authEng),
		quotas:     pickDefaultQuotaChecker(authEng),
		quotaMgr:   pickDefaultQuotaManager(authEng),
		scramStore: pickDefaultSCRAMStore(authEng),
		topics:     NewTopicRegistry(),
		brokers:    info,
	}
}

// pickDefaultAuthorizer returns AllowAllAuthorizer regardless of the
// supplied AuthEngine. gh #126: defaulting to "no authorization" at
// the Broker layer preserves pre-v0.1.125 behavior — production
// callers (cmd/skafka/main.go) opt INTO real ACL enforcement via
// SetAuthorizer when SKAFKA_AUTHORIZATION_TYPE=simple is set, which
// matches Strimzi's "authorization property missing = no restrictions"
// semantic. Tests that need ACL enforcement wire their own via
// SetAuthorizer.
func pickDefaultAuthorizer(_ auth.AuthEngine) auth.Authorizer {
	return auth.NewAllowAllAuthorizer()
}

// pickDefaultQuotaChecker is independent of authorization — quotas
// are per-principal byte-rate limits and run whenever a RealAuthEngine
// is wired (it implements QuotaChecker via its embedded QuotaEnforcer).
// Test stubs without QuotaChecker fall back to NoQuotaChecker.
func pickDefaultQuotaChecker(eng auth.AuthEngine) auth.QuotaChecker {
	if q, ok := eng.(auth.QuotaChecker); ok {
		return q
	}
	return auth.NewNoQuotaChecker()
}

// pickDefaultQuotaManager wires the KIP-546 runtime-mutation surface
// (gh #103) for any AuthEngine that exposes its QuotaEnforcer. Today
// that's *RealAuthEngine via its Quotas() accessor. AllowAll / test
// stubs return nil so the two quota admin handlers degrade gracefully.
func pickDefaultQuotaManager(eng auth.AuthEngine) handlers.QuotaManager {
	type quotaAccessor interface {
		Quotas() *auth.QuotaEnforcer
	}
	if qa, ok := eng.(quotaAccessor); ok {
		if qe := qa.Quotas(); qe != nil {
			return qe
		}
	}
	return nil
}

// pickDefaultSCRAMStore exposes the read-side of KIP-554
// DescribeUserScramCredentials (gh #104). Same shape as
// pickDefaultQuotaManager but pulls the CredentialLoader instead.
// AllowAll / dev mode returns nil and the handler stays unregistered.
func pickDefaultSCRAMStore(eng auth.AuthEngine) handlers.SCRAMCredentialStore {
	type credsAccessor interface {
		Creds() *auth.CredentialLoader
	}
	if ca, ok := eng.(credsAccessor); ok {
		if c := ca.Creds(); c != nil {
			return c
		}
	}
	return nil
}

// SetAuthorizer overrides the default cluster-wide authorizer (gh #126).
// Production main.go uses this to wire a SuperUserAuthorizer{simple ACL}
// when `spec.kafka.authorization.type: simple`. Tests can wire fakes.
func (b *Broker) SetAuthorizer(a auth.Authorizer) { b.authorizer = a }

// SetQuotaChecker overrides the default cluster-wide quota checker (gh #126).
func (b *Broker) SetQuotaChecker(q auth.QuotaChecker) { b.quotas = q }

// SetQuotaManager wires the runtime-mutation surface for KIP-546
// (DescribeClientQuotas / AlterClientQuotas, gh #103). Nil leaves
// the broker without the admin-quota APIs — the two handlers will
// degrade to "no entries" on describe and INVALID_REQUEST on alter.
// Production main.go passes the same *auth.QuotaEnforcer that
// SetQuotaChecker received.
func (b *Broker) SetQuotaManager(q handlers.QuotaManager) { b.quotaMgr = q }

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
		cfg:        cfg,
		store:      store,
		leases:     leases,
		auth:       auth.NewSingleAuthEngine(authEng),
		authorizer: pickDefaultAuthorizer(authEng),
		quotas:     pickDefaultQuotaChecker(authEng),
		quotaMgr:   pickDefaultQuotaManager(authEng),
		scramStore: pickDefaultSCRAMStore(authEng),
		topics:     NewTopicRegistry(),
		brokers:    brokers,
		coord:      coord,
	}
}

// SetAuthEngineSelector replaces the SingleAuthEngine wrapper installed
// in New/NewWithBrokerSource with a real per-listener selector
// (gh #124). Production main.go calls this after constructing the
// PerListenerAuthEngine map from the parsed listener list. Must be
// called BEFORE RegisterHandlers so the handlers pick up the
// per-listener engine on the first request.
func (b *Broker) SetAuthEngineSelector(sel auth.AuthEngineSelector) { b.auth = sel }

// WireReaperCRCheck installs the gh #119 CR-existence callback on
// the storage engine's PartitionReaper. The reaper consults this
// before doing each reap; if the topic CR has come back during the
// reap window (recreate-with-same-name), the reap is aborted and
// the partition data is preserved.
//
// Wired from cmd/skafka after both the broker AND the engine (with
// its reaper) are up. Idempotent — no-op when the engine doesn't
// have a reaper attached (memory storage / dev mode).
func (b *Broker) WireReaperCRCheck() {
	type reaperHolder interface {
		Reaper() *storage.PartitionReaper
	}
	rh, ok := b.store.(reaperHolder)
	if !ok || rh.Reaper() == nil {
		return
	}
	// The topic registry IS the broker's view of the
	// "currently-valid topics" — a topic missing from it has been
	// deleted (or is being deleted; the TopicWatcher already
	// removes it on deletionTimestamp != nil).
	rh.Reaper().WithTopicExists(func(topic string) bool {
		_, ok := b.topics.Get(topic)
		return ok
	})
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

// UseKafkaUserCRWriter wires the optional KafkaUser-CR writer for the
// AlterClientQuotas handler (gh #103 phase 2). Must be called BEFORE
// RegisterHandlers so the admin handler can pick it up via WithCRWriter.
// Without this, AlterClientQuotas mutations live in QuotaEnforcer's
// in-memory override map and die on broker restart.
func (b *Broker) UseKafkaUserCRWriter(w handlers.KafkaUserWriter) {
	b.kafkaUserCRWriter = w
}

// UseSCRAMCredentialStore wires the read side of KIP-554
// DescribeUserScramCredentials (gh #104). Pass *auth.CredentialLoader
// in production; tests can substitute fakes. Nil disables the handler
// entirely so an ApiVersions probe doesn't advertise key 50.
func (b *Broker) UseSCRAMCredentialStore(s handlers.SCRAMCredentialStore) {
	b.scramStore = s
}

// UseSCRAMCredentialCRWriter wires the write side of KIP-554
// AlterUserScramCredentials (gh #104). Pass the same KafkaUserWriter
// that UseKafkaUserCRWriter received; the K8s-package writer
// implements both interfaces with disjoint methods.
func (b *Broker) UseSCRAMCredentialCRWriter(w handlers.SCRAMCredentialWriter) {
	b.scramCRWriter = w
}

// UseACLCRWriter wires the optional ACL writer for the
// CreateAcls / DeleteAcls / DescribeAcls handlers (gh #107). Must be
// called BEFORE RegisterHandlers so registerAclHandlers can pick it up.
// Without this, the ACL handlers degrade to per-entry success stubs —
// fine for kafka-compat tests without an apiserver, broken in
// production because admin-protocol ACL writes silently disappear.
func (b *Broker) UseACLCRWriter(w handlers.ACLCRWriter) {
	b.aclCRWriter = w
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
	autoCreate := b.autoCreateConfig()
	b.registerDataHandlers(d, autoCreate)
	b.registerConsumerGroupHandlers(d)
	b.registerTopicAdminHandlers(d)
	b.registerSaslHandlers(d)
	b.registerProducerIDHandlers(d)
	b.registerTxnHandlers(d)
	b.registerAclHandlers(d)
	b.registerClusterAdminHandlers(d, autoCreate)
	b.registerAPIVersionsHandler(d)
	return d
}

// autoCreateConfig reads auto-create topic env config (gh #109). Defaults
// match Apache 3.7 (true / 1). Used by both Metadata and DescribeConfigs.
func (b *Broker) autoCreateConfig() handlers.AutoCreateTopicsConfig {
	return handlers.AutoCreateTopicsConfig{
		Enabled:       envBool("SKAFKA_AUTO_CREATE_TOPICS_ENABLE", true),
		NumPartitions: int32(envOrIntBroker("SKAFKA_NUM_PARTITIONS", 1)),
	}
}

// registerDataHandlers: Produce (0), Fetch (1), ListOffsets (2),
// Metadata (3), DeleteRecords (21) — the data-plane APIs.
func (b *Broker) registerDataHandlers(d *protocol.Dispatcher, autoCreate handlers.AutoCreateTopicsConfig) {
	produceHandler := handlers.NewProduceHandler(b.store, b.leases, b.authorizer, b.quotas)
	if b.brokerCoord != nil {
		produceHandler = produceHandler.WithCoordinator(b.brokerCoord)
	}
	d.Register(0, 3, 9, produceHandler)
	d.Register(1, 4, 12, handlers.NewFetchHandler(b.store, b.leases, b.authorizer, b.quotas))
	d.Register(2, 1, 7, handlers.NewListOffsetsHandler(b.store, b.leases))
	// OffsetForLeaderEpoch (key 23, v0–v4) — KIP-101 epoch-aware
	// truncation lookup (gh #101). The handler defers to the storage
	// engine which walks epoch-prefixed segments. Skafka is RF=1 so
	// the underlying failure mode this API was designed for can't
	// happen, but the lookup is still served so Java consumers'
	// epoch-aware paths don't stall on UNSUPPORTED_VERSION.
	d.Register(23, 0, 4, handlers.NewOffsetForLeaderEpochHandler(b.store))

	// Metadata: cap at v10. v11 removed IncludeClusterAuthorizedOperations
	// but the Java AdminClient happily selects our advertised max and
	// then fails serialisation on its own side when the flag is set
	// ("Attempted to write a non-default includeClusterAuthorizedOperations
	// at version 12") — observed from kafbat-ui's brokers page. Capping
	// at v10 keeps the flag available, which is what callers actually want.
	// We give up only v11/v12 UUID-based topic IDs (a KRaft transition
	// feature skafka does not need).
	//
	// Metadata's leader source: prefer the v3 BrokerCoordinator
	// (assignment.json-driven) when wired, fall back to the legacy
	// per-partition LeaseManager. The Lease path can disagree with the
	// controller's CR for freshly-added topics or during failover —
	// using assignment.json eliminates that split-brain (gh #75).
	var leaderSrc handlers.PartitionLeaderSource = b.leases
	if b.brokerCoord != nil {
		leaderSrc = b.brokerCoord
	}
	metadataHandler := handlers.NewMetadataHandlerWithSource(b.brokers, b.cfg.ClusterID, b.topics, leaderSrc)
	if b.topicCRWriter != nil {
		metadataHandler = metadataHandler.WithAutoCreate(autoCreate, b.topicCRWriter)
	}
	d.Register(3, 1, 10, metadataHandler)

	deleteRecordsHandler := handlers.NewDeleteRecordsHandler(b.store)
	if b.brokerCoord != nil {
		deleteRecordsHandler = deleteRecordsHandler.WithCoordinator(b.brokerCoord)
	}
	d.Register(21, 0, 2, deleteRecordsHandler)
}

// registerConsumerGroupHandlers: OffsetCommit (8), OffsetFetch (9),
// FindCoordinator (10), JoinGroup (11), Heartbeat (12), LeaveGroup (13),
// SyncGroup (14), DescribeGroups (15), ListGroups (16), DeleteGroups (42).
func (b *Broker) registerConsumerGroupHandlers(d *protocol.Dispatcher) {
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
	d.Register(42, 0, 2, handlers.NewDeleteGroupsHandler(b.coord, b.authorizer))

	// OffsetDelete (key 47, v0) — kafka-consumer-groups --delete-offsets
	// and AdminClient.deleteConsumerGroupOffsets(). Per-partition variant
	// of DeleteGroups; same coordinator + Empty/Dead state guard. Apache
	// Kafka 3.7 only ships v0 (non-flexible). gh #100.
	d.Register(47, 0, 0, handlers.NewOffsetDeleteHandler(b.coord, b.authorizer))
}

// registerTopicAdminHandlers: CreateTopics (19), DeleteTopics (20).
// Both caps at v6/v5 because newer versions require KIP-516 topic-id
// (UUID) support that skafka's codec doesn't write yet.
func (b *Broker) registerTopicAdminHandlers(d *protocol.Dispatcher) {
	// CreateTopics capped at v6: v7 added the topic_id UUID (KIP-516)
	// to CreatableTopicResult, which our encoder doesn't write —
	// modern Java admin clients hit BufferUnderflowException reading
	// the missing 16 bytes (gh #73).
	createTopicsHandler := handlers.NewCreateTopicsHandler(b.topics)
	if b.topicCRWriter != nil {
		createTopicsHandler = createTopicsHandler.WithCRWriter(b.topicCRWriter)
	}
	d.Register(19, 0, 6, createTopicsHandler)

	// DeleteTopics capped at v5: v6+ changed `topic_names: [STRING]`
	// to `topics: [DeleteTopicState]` (name COMPACT_NULLABLE_STRING +
	// topic_id UUID — KRaft topic-id KIP-516). The codec still expects
	// the v0–v5 flat name array; advertising v6 made franz-go's
	// kmsg.DeleteTopicsRequest send the new struct shape and skafka
	// errored with "unexpected null compact string". Capping at v5
	// keeps name-based deletes working for kafka-topics.sh, kafbat-ui,
	// and Java AdminClient.
	deleteTopicsHandler := handlers.NewDeleteTopicsHandler(b.topics)
	if b.topicCRWriter != nil {
		deleteTopicsHandler = deleteTopicsHandler.WithCRWriter(b.topicCRWriter)
	}
	d.Register(20, 0, 5, deleteTopicsHandler)
}

// registerSaslHandlers: SaslHandshake (17), SaslAuthenticate (36).
func (b *Broker) registerSaslHandlers(d *protocol.Dispatcher) {
	d.Register(17, 0, 1, handlers.NewSaslHandshakeHandler())
	d.Register(36, 0, 2, handlers.NewSaslAuthenticateHandler(b.auth))
}

// registerProducerIDHandlers wires InitProducerId (22) plus all its
// dependencies (TxnStateStore, FenceWatcher, broadcasting fencer,
// txn-coordinator ownership). SIDE EFFECTS: sets b.txnStore and
// b.fenceWatcher when their preconditions are met.
func (b *Broker) registerProducerIDHandlers(d *protocol.Dispatcher) {
	// gh #12 stage A: hand out a fresh PID/epoch so idempotent producers
	// (default since Kafka 3.0) can complete their startup handshake.
	// Sequence-number enforcement in Produce is stage B.
	// gh #22: layer the TxnStateStore on top so non-empty transactional.id
	// rejoins bump the epoch (and the storage-layer fence rejects the
	// previous instance's writes).
	initPIDHandler := handlers.NewInitProducerIdHandler()

	// Only wire the TxnStateStore when DataDir is a real on-disk path.
	// MemoryStorage (local-dev / unit tests) returns "memory://" —
	// joining "__cluster" onto it would create a stray "memory:/__cluster"
	// directory in cwd. Production DiskStorageEngine returns an absolute
	// path so this just skips the dev path.
	if dataDir := b.store.DataDir(); strings.HasPrefix(dataDir, "/") {
		clusterDir := filepath.Join(dataDir, "__cluster")
		// numSlots decoupled from StatefulSet replica count (gh #108
		// follow-up): pinning to Apache's transaction.state.log.num.
		// partitions=50 default keeps the storage layout stable across
		// scale operations. Override via SKAFKA_TXN_NUM_SLOTS if 50 is
		// wrong for the cluster.
		numSlots := 0 // 0 → DefaultNumSlots (50)
		if v := os.Getenv("SKAFKA_TXN_NUM_SLOTS"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				numSlots = n
			}
		}
		if store, err := coordinator.NewTxnStateStore(clusterDir, numSlots); err == nil {
			initPIDHandler = initPIDHandler.WithTxnStateStore(store)
			b.txnStore = store
		} else {
			slog.Warn("InitProducerId: TxnStateStore disabled (gh #22 epoch fence inactive)",
				"clusterDir", clusterDir, "err", err)
		}
	}

	// gh #30: cross-partition fence on every rejoin bump. Only
	// DiskStorageEngine implements the fence interface; MemoryStorage
	// has no per-partition producerStates to walk, so the cast fails
	// and the fence stays a no-op.
	//
	// gh #108 phase 2: wrap the local fencer with a broadcasting fencer
	// that also writes to this broker's outbound fence log. Peer
	// brokers' FenceWatcher polls the directory and applies each entry
	// locally — closing the cross-broker zombie window without a new
	// gRPC RPC.
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
	// broker set). Opt-in: in dev mode brokerCoord is the
	// LocalLeaseManager-backed stub which does not implement TxnOwnership.
	if ownership, ok := b.brokerCoord.(handlers.TxnOwnership); ok {
		initPIDHandler = initPIDHandler.WithTxnOwnership(ownership)
	}
	d.Register(22, 0, 4, initPIDHandler)
}

// registerTxnHandlers: AddPartitionsToTxn (24), AddOffsetsToTxn (25),
// EndTxn (26), WriteTxnMarkers (27), TxnOffsetCommit (28). Most are
// gated on b.txnStore being non-nil; clients otherwise see
// UNSUPPORTED_VERSION via ApiVersions.
func (b *Broker) registerTxnHandlers(d *protocol.Dispatcher) {
	// AddPartitions / EndTxn / AddOffsets all require the txn store.
	if b.txnStore != nil {
		// gh #23: AddPartitionsToTxn (key 24) v0–v3. Handler reuses
		// the gh #91 OwnsTxn gate for routing.
		addPartHandler := handlers.NewAddPartitionsToTxnHandler(&txnPartitionStoreAdapter{store: b.txnStore})
		if ownership, ok := b.brokerCoord.(handlers.TxnOwnership); ok {
			addPartHandler = addPartHandler.WithTxnOwnership(ownership)
		}
		d.Register(24, 0, 3, addPartHandler)

		// gh #25/#26: EndTxn (key 26) v0–v3 — state-machine only.
		// Marker control-batch writes land with WriteTxnMarkers (#27
		// follow-up) + read-committed isolation (#31). Until then
		// EndTxn transitions the txn state and the Java producer's
		// commitTransaction() / abortTransaction() returns success.
		endTxnHandler := handlers.NewEndTxnHandler(&txnEndStoreAdapter{store: b.txnStore})
		if ownership, ok := b.brokerCoord.(handlers.TxnOwnership); ok {
			endTxnHandler = endTxnHandler.WithTxnOwnership(ownership)
		}
		d.Register(26, 0, 3, endTxnHandler)

		// gh #24: AddOffsetsToTxn (key 25) v0–v3. The recorded group
		// association drives pending-offset commit/abort on EndTxn
		// (via txnOffsetHook wired below).
		addOffsetsHandler := handlers.NewAddOffsetsToTxnHandler(&txnGroupStoreAdapter{store: b.txnStore})
		if ownership, ok := b.brokerCoord.(handlers.TxnOwnership); ok {
			addOffsetsHandler = addOffsetsHandler.WithTxnOwnership(ownership)
		}
		d.Register(25, 0, 3, addOffsetsHandler)
	}

	// gh #27: TxnOffsetCommit (key 28) v0–v3 — routes through the
	// GROUP coordinator (not the txn coordinator). The handler stages
	// offsets in the offset store's pending layer; they become visible
	// to OffsetFetch when EndTxn(commit) fires via the TxnStateStore's
	// txnOffsetHook.
	if b.coord != nil {
		d.Register(28, 0, 3, handlers.NewTxnOffsetCommitHandler(b.coord))
		// Wire the gh #24/#27 hook: EndTxn(commit) materialises any
		// pending offsets staged by TxnOffsetCommit, EndTxn(abort)
		// discards them. Same-broker only — cross-broker dispatch
		// lands with gh #114.
		if b.txnStore != nil {
			b.coord.WireTxnOffsetHook(b.txnStore)
		}
	}

	// gh #114: WriteTxnMarkers (key 27) v0–v1. Receives marker
	// dispatch requests from txn coordinators after EndTxn; writes
	// the COMMIT/ABORT control batch on the partition leader.
	wtmHandler := handlers.NewWriteTxnMarkersHandler(b.store)
	if owns, ok := b.brokerCoord.(handlers.WriteTxnMarkersOwnership); ok {
		wtmHandler = wtmHandler.WithOwnership(owns)
	}
	d.Register(27, 0, 1, wtmHandler)
}

// registerAclHandlers: DescribeAcls (29), CreateAcls (30), DeleteAcls (31).
// gh #107: handlers translate the AdminClient wire shape to/from the
// KafkaUser.spec.authorization.acls inline list (gh #135) via the
// ACLCRWriter; without one wired they degrade to no-op stubs that
// preserve pre-gh #107 kafka-compat-test behavior.
func (b *Broker) registerAclHandlers(d *protocol.Dispatcher) {
	describe := handlers.NewDescribeAclsHandler()
	create := handlers.NewCreateAclsHandler()
	delete_ := handlers.NewDeleteAclsHandler()
	if b.aclCRWriter != nil {
		describe = describe.WithCRWriter(b.aclCRWriter)
		create = create.WithCRWriter(b.aclCRWriter)
		delete_ = delete_.WithCRWriter(b.aclCRWriter)
	}
	d.Register(29, 0, 3, describe)
	d.Register(30, 0, 3, create)
	d.Register(31, 0, 3, delete_)
}

// registerClusterAdminHandlers: DescribeConfigs (32), DescribeLogDirs (35),
// DescribeCluster (60). The AdminClient + kafka-configs.sh / kafbat-ui /
// kafka-cluster.sh probes hit these.
func (b *Broker) registerClusterAdminHandlers(d *protocol.Dispatcher, autoCreate handlers.AutoCreateTopicsConfig) {
	// gh #109: advertise the live broker config so kafka-configs.sh /
	// kafbat-ui render the actual auto-create + num-partitions values.
	d.Register(32, 0, 3, handlers.NewDescribeConfigsHandler(b.topics, b.brokers).
		WithBrokerConfig(autoCreate.Enabled, autoCreate.NumPartitions))
	d.Register(35, 0, 1, handlers.NewDescribeLogDirsHandler(b.store, b.topics))
	// gh #102: DescribeCluster (key 60). AdminClient.describeCluster()
	// and `kafka-cluster.sh --describe` need this. ControllerID falls
	// back to Self().NodeID (every broker reports itself as controller),
	// matching the existing Metadata response (gh #85). Accurate
	// controller-id reporting via ControllerWatch.CurrentHolder is a
	// future enhancement once a pod-name → NodeID resolver is in place.
	d.Register(60, 0, 1, handlers.NewDescribeClusterHandler(b.brokers, b.cfg.ClusterID))

	// KIP-546 dynamic quota management (gh #103). v0 non-flexible, v1
	// flexible. Mutations land in QuotaEnforcer's in-memory override
	// map; broker restart drops them. CR-write-back (phase 2) keeps
	// operator-set quotas durable across restart.
	d.Register(48, 0, 1, handlers.NewDescribeClientQuotasHandler(b.quotaMgr, b.authorizer))
	alterClientQuotas := handlers.NewAlterClientQuotasHandler(b.quotaMgr, b.authorizer)
	if b.kafkaUserCRWriter != nil {
		alterClientQuotas = alterClientQuotas.WithCRWriter(b.kafkaUserCRWriter)
	}
	d.Register(49, 0, 1, alterClientQuotas)

	// KIP-554 SCRAM-credential rotation (gh #104). v0 flexible. Describe
	// reads from the CredentialLoader (auth.SCRAMCredentialStore);
	// Alter writes to the KafkaUser CR via the same writer used for
	// quotas — different methods on the same impl.
	if b.scramStore != nil {
		d.Register(50, 0, 0, handlers.NewDescribeUserScramCredentialsHandler(b.scramStore, b.authorizer))
	}
	alterScram := handlers.NewAlterUserScramCredentialsHandler(b.authorizer)
	if b.scramCRWriter != nil {
		alterScram = alterScram.WithCRWriter(b.scramCRWriter)
	}
	d.Register(51, 0, 0, alterScram)
}

// registerAPIVersionsHandler: ApiVersions (18). Must be registered LAST
// because it snapshots d.SupportedVersions() and adjusts its own cap.
func (b *Broker) registerAPIVersionsHandler(d *protocol.Dispatcher) {
	supported := d.SupportedVersions()
	supported[18] = [2]int16{0, 4}
	d.Register(18, 0, 4, handlers.NewAPIVersionsHandler(supported))
}

// K8sBrokerSource adapts a *k8s.BrokerRegistry to handlers.BrokerSource.
// ExtHostPattern + ExtPort optionally add per-broker external hostnames
// (broker-{ordinal}.kafka.example.com:9093) to each BrokerEndpoint.
//
// ListenerPorts (gh #125) carries the per-listener advertised port
// map (e.g. {"internal": 9092, "authed": 9095}) — copied onto every
// BrokerEndpoint so MetadataResponse can route the client back to
// the same listener it bootstrapped on. Same map for every broker
// in the cluster because brokers run identical config.
type K8sBrokerSource struct {
	reg            *k8sbroker.BrokerRegistry
	ExtHostPattern string // fmt-style pattern, e.g. "broker-%d.kafka.example.com"
	ExtPort        int32
	ListenerPorts  map[string]int32
}

func NewK8sBrokerSource(reg *k8sbroker.BrokerRegistry) *K8sBrokerSource {
	return &K8sBrokerSource{reg: reg}
}

func (a *K8sBrokerSource) Self() handlers.BrokerEndpoint {
	e := a.reg.Self()
	ep := handlers.BrokerEndpoint{NodeID: e.NodeID, Host: e.Host, Port: e.Port}
	a.fillExternal(&ep)
	ep.ListenerPorts = a.ListenerPorts
	return ep
}

func (a *K8sBrokerSource) All() []handlers.BrokerEndpoint {
	all := a.reg.All()
	out := make([]handlers.BrokerEndpoint, 0, len(all))
	for _, e := range all {
		ep := handlers.BrokerEndpoint{NodeID: e.NodeID, Host: e.Host, Port: e.Port}
		a.fillExternal(&ep)
		ep.ListenerPorts = a.ListenerPorts
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

// txnGroupStoreAdapter wraps coordinator.TxnStateStore so it
// satisfies handlers.TxnGroupStore (AddOffsetsToTxn). Mirrors
// txnPartitionStoreAdapter from gh #23. gh #24.
type txnGroupStoreAdapter struct {
	store *coordinator.TxnStateStore
}

func (a *txnGroupStoreAdapter) AddOffsetsToTxn(txnID string, pid int64, epoch int16, groupID string) error {
	if err := a.store.AddOffsetsToTxn(txnID, pid, epoch, groupID); err != nil {
		switch {
		case errors.Is(err, coordinator.ErrEmptyTxnID):
			return handlers.ErrTxnGroupEmptyID
		case errors.Is(err, coordinator.ErrTxnUnknownProducer):
			return handlers.ErrTxnGroupUnknownProducer
		case errors.Is(err, coordinator.ErrTxnEpochFenced):
			return handlers.ErrTxnGroupEpochFenced
		case errors.Is(err, coordinator.ErrTxnConcurrent):
			return handlers.ErrTxnGroupConcurrent
		case errors.Is(err, coordinator.ErrTxnInvalidState):
			return handlers.ErrTxnGroupInvalidState
		default:
			return err
		}
	}
	return nil
}

// txnEndStoreAdapter wraps coordinator.TxnStateStore so it
// satisfies handlers.TxnEndStore. Translates coordinator's
// sentinels to handler-package sentinels (avoiding the
// handlers→coordinator import cycle). gh #25/#26.
type txnEndStoreAdapter struct {
	store *coordinator.TxnStateStore
}

func (a *txnEndStoreAdapter) EndTxn(txnID string, pid int64, epoch int16, commit bool) error {
	if err := a.store.EndTxn(txnID, pid, epoch, commit); err != nil {
		switch {
		case errors.Is(err, coordinator.ErrEmptyTxnID):
			return handlers.ErrTxnEndEmptyID
		case errors.Is(err, coordinator.ErrTxnUnknownProducer):
			return handlers.ErrTxnEndUnknownProducer
		case errors.Is(err, coordinator.ErrTxnEpochFenced):
			return handlers.ErrTxnEndEpochFenced
		case errors.Is(err, coordinator.ErrTxnConcurrent):
			return handlers.ErrTxnEndConcurrent
		case errors.Is(err, coordinator.ErrTxnInvalidState):
			return handlers.ErrTxnEndInvalidState
		default:
			return err
		}
	}
	return nil
}

// txnPartitionStoreAdapter wraps coordinator.TxnStateStore so it
// satisfies handlers.TxnPartitionStore. The two interfaces are
// near-identical; the adapter exists only to translate
// coordinator's sentinel errors to the handler-package sentinels
// (which avoids a handlers→coordinator import cycle), and to
// convert the codec-aware [] handlers.TxnPartitionAddition into
// [] coordinator.TxnTopic. gh #23.
type txnPartitionStoreAdapter struct {
	store *coordinator.TxnStateStore
}

func (a *txnPartitionStoreAdapter) AddPartitions(txnID string, pid int64, epoch int16, additions []handlers.TxnPartitionAddition) error {
	tt := make([]coordinator.TxnTopic, 0, len(additions))
	for _, add := range additions {
		tt = append(tt, coordinator.TxnTopic{
			Topic:      add.Topic,
			Partitions: append([]int32(nil), add.Partitions...),
		})
	}
	if err := a.store.AddPartitions(txnID, pid, epoch, tt); err != nil {
		switch {
		case errors.Is(err, coordinator.ErrEmptyTxnID):
			return handlers.ErrTxnPartitionEmptyID
		case errors.Is(err, coordinator.ErrTxnUnknownProducer):
			return handlers.ErrTxnPartitionUnknownProducer
		case errors.Is(err, coordinator.ErrTxnEpochFenced):
			return handlers.ErrTxnPartitionEpochFenced
		default:
			return err
		}
	}
	return nil
}
