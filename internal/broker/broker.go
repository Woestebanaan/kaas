// Package broker wires the protocol layer to the storage, lease, lock, and auth interfaces.
package broker

import (
	"github.com/woestebanaan/skafka/internal/auth"
	k8sbroker "github.com/woestebanaan/skafka/internal/k8s"
	"github.com/woestebanaan/skafka/internal/lease"
	"github.com/woestebanaan/skafka/internal/lock"
	"github.com/woestebanaan/skafka/internal/protocol"
	"github.com/woestebanaan/skafka/internal/protocol/handlers"
	"github.com/woestebanaan/skafka/internal/storage"
)

// Config holds broker identity and static configuration.
type Config struct {
	BrokerID   int32
	Host       string
	Port       int32
	ClusterID  string
}

// Broker holds all runtime dependencies and registers handlers with the dispatcher.
type Broker struct {
	cfg     Config
	store   storage.StorageEngine
	leases  lease.LeaseManager
	locks   lock.PartitionLock
	auth    auth.AuthEngine
	topics  *TopicRegistry
	brokers handlers.BrokerSource
}

func New(
	cfg Config,
	store storage.StorageEngine,
	leases lease.LeaseManager,
	locks lock.PartitionLock,
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
		locks:   locks,
		auth:    authEng,
		topics:  NewTopicRegistry(),
		brokers: info,
	}
}

// NewWithBrokerSource creates a Broker using a dynamic multi-broker source.
// Used in Kubernetes mode.
func NewWithBrokerSource(
	cfg Config,
	store storage.StorageEngine,
	leases lease.LeaseManager,
	locks lock.PartitionLock,
	authEng auth.AuthEngine,
	brokers handlers.BrokerSource,
) *Broker {
	return &Broker{
		cfg:     cfg,
		store:   store,
		leases:  leases,
		locks:   locks,
		auth:    authEng,
		topics:  NewTopicRegistry(),
		brokers: brokers,
	}
}

// AddTopic registers a topic in the local registry so Metadata and produce/fetch work immediately.
func (b *Broker) AddTopic(name string, partitions int32) {
	b.topics.Add(name, partitions)
}

// RegisterHandlers wires all API key handlers into d and returns d.
func (b *Broker) RegisterHandlers(d *protocol.Dispatcher) *protocol.Dispatcher {
	// ApiVersions (18) — registered last so it can read the dispatcher's own version table.
	// Placeholder; re-registered below after all others are added.
	d.Register(0, 3, 9, handlers.NewProduceHandler(b.store, b.leases, b.locks, b.auth))
	d.Register(1, 4, 12, handlers.NewFetchHandler(b.store, b.leases, b.auth)) // v13 switches to UUID topics
	d.Register(2, 1, 7, handlers.NewListOffsetsHandler(b.store, b.leases))
	d.Register(3, 1, 12, handlers.NewMetadataHandlerWithSource(b.brokers, b.cfg.ClusterID, b.topics, b.leases))
	d.Register(8, 2, 8, handlers.NewOffsetCommitHandler())
	d.Register(9, 1, 8, handlers.NewOffsetFetchHandler())
	d.Register(10, 0, 4, handlers.NewFindCoordinatorHandler())
	d.Register(11, 2, 9, handlers.NewJoinGroupHandler())
	d.Register(12, 0, 4, handlers.NewHeartbeatHandler())
	d.Register(13, 0, 4, handlers.NewLeaveGroupHandler())
	d.Register(14, 0, 5, handlers.NewSyncGroupHandler())
	d.Register(15, 0, 5, handlers.NewDescribeGroupsHandler())
	d.Register(16, 0, 4, handlers.NewListGroupsHandler())
	d.Register(17, 0, 1, handlers.NewSaslHandshakeHandler())
	d.Register(19, 0, 7, handlers.NewCreateTopicsHandler(b.topics))
	d.Register(20, 0, 6, handlers.NewDeleteTopicsHandler(b.topics))
	d.Register(29, 0, 3, handlers.NewDescribeAclsHandler())
	d.Register(30, 0, 3, handlers.NewCreateAclsHandler())
	d.Register(31, 0, 3, handlers.NewDeleteAclsHandler())
	d.Register(36, 0, 2, handlers.NewSaslAuthenticateHandler(b.auth))

	// ApiVersions last — builds version table from all registered handlers.
	supported := d.SupportedVersions()
	supported[18] = [2]int16{0, 4}
	d.Register(18, 0, 4, handlers.NewAPIVersionsHandler(supported))

	return d
}

// K8sBrokerSource adapts a *k8s.BrokerRegistry to handlers.BrokerSource.
// Lives here to avoid import cycles between k8s ↔ handlers.
type K8sBrokerSource struct {
	reg *k8sbroker.BrokerRegistry
}

func NewK8sBrokerSource(reg *k8sbroker.BrokerRegistry) *K8sBrokerSource {
	return &K8sBrokerSource{reg: reg}
}

func (a *K8sBrokerSource) Self() handlers.BrokerEndpoint {
	e := a.reg.Self()
	return handlers.BrokerEndpoint{NodeID: e.NodeID, Host: e.Host, Port: e.Port}
}

func (a *K8sBrokerSource) All() []handlers.BrokerEndpoint {
	all := a.reg.All()
	out := make([]handlers.BrokerEndpoint, 0, len(all))
	for _, e := range all {
		out = append(out, handlers.BrokerEndpoint{NodeID: e.NodeID, Host: e.Host, Port: e.Port})
	}
	return out
}

var _ handlers.BrokerSource = (*K8sBrokerSource)(nil)
