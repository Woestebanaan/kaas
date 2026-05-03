// Package broker wires the protocol layer to the storage, lease, and auth
// interfaces. Phase 4 dropped the flock parameter from all constructors —
// single-writer enforcement is now BrokerCoordinator.Owns + epoch-prefixed
// segment filenames; see phase4-breakdown.md.
package broker

import (
	"fmt"

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
	// brokerCoord is the v3 BrokerCoordinator that produces partition
	// ownership decisions on the produce hot path. UseCoordinator wires
	// it; RegisterHandlers picks it up via WithCoordinator on the
	// ProduceHandler. nil → legacy lease+lock fallback.
	brokerCoord kafkaapi.BrokerCoordinator
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

// Topics returns the underlying topic registry. Phase 4+5: cmd/skafka
// wraps this as the controller's TopicSource so the AssignmentLoop sees
// every topic the broker knows about.
func (b *Broker) Topics() *TopicRegistry {
	return b.topics
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
	d.Register(3, 1, 10, handlers.NewMetadataHandlerWithSource(b.brokers, b.cfg.ClusterID, b.topics, b.leases))
	d.Register(8, 2, 8, handlers.NewOffsetCommitHandler(b.coord))
	d.Register(9, 1, 8, handlers.NewOffsetFetchHandler(b.coord))
	d.Register(10, 0, 4, handlers.NewFindCoordinatorHandler(b.coord))
	d.Register(11, 2, 9, handlers.NewJoinGroupHandler(b.coord))
	d.Register(12, 0, 4, handlers.NewHeartbeatHandler(b.coord))
	d.Register(13, 0, 4, handlers.NewLeaveGroupHandler(b.coord))
	d.Register(14, 0, 5, handlers.NewSyncGroupHandler(b.coord))
	d.Register(15, 0, 5, handlers.NewDescribeGroupsHandler(b.coord))
	d.Register(16, 0, 4, handlers.NewListGroupsHandler(b.coord))
	d.Register(17, 0, 1, handlers.NewSaslHandshakeHandler())
	d.Register(19, 0, 7, handlers.NewCreateTopicsHandler(b.topics))
	d.Register(20, 0, 6, handlers.NewDeleteTopicsHandler(b.topics))
	d.Register(29, 0, 3, handlers.NewDescribeAclsHandler())
	d.Register(30, 0, 3, handlers.NewCreateAclsHandler())
	d.Register(31, 0, 3, handlers.NewDeleteAclsHandler())
	d.Register(32, 0, 3, handlers.NewDescribeConfigsHandler(b.topics, b.brokers))
	d.Register(35, 0, 1, handlers.NewDescribeLogDirsHandler(b.store, b.topics))
	d.Register(36, 0, 2, handlers.NewSaslAuthenticateHandler(b.auth))

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
