package handlers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

// PartitionLeaderSource resolves which broker leads (topic, partition).
// Returns -1 when the partition is unknown. Both *broker.Coordinator
// (assignment.json-driven, v3 path) and lease.LeaseManager (legacy
// per-partition Lease, dev/fallback) satisfy this interface implicitly,
// so callers can swap implementations without touching the metadata
// handler — gh #75 architectural cleanup.
type PartitionLeaderSource interface {
	LeaderFor(topic string, partition int32) int32
}

// BrokerEndpoint is one broker in the cluster as seen by the Metadata handler.
// Host/Port are the internal (in-cluster) address. ExternalHost/ExternalPort are
// the externally-routable address used for the TLS listener; if ExternalHost is
// empty, the internal Host is used instead.
//
// gh #125: ListenerPorts maps listener name → port on this broker for
// per-listener Metadata advertisement (Apache Kafka's
// `advertised.listeners` semantic). When a client connects on the
// "authed" listener (port 9095) and asks for Metadata, the response
// must redirect future connections to the same listener — otherwise
// the client follows back to the anon listener (:9092) and the SCRAM
// handshake breaks because the engine there is AllowAll. The map is
// populated from SKAFKA_LISTENERS at broker boot.
type BrokerEndpoint struct {
	NodeID       int32
	Host         string
	Port         int32
	ExternalHost string           // per-broker external hostname (empty if no external listener)
	ExternalPort int32
	// ListenerPorts: listener name → port. "external" is still
	// handled by ExternalHost/ExternalPort (per-broker FQDN); every
	// other listener uses the same Host with the listener's specific
	// port. Missing entries fall back to (Host, Port).
	ListenerPorts map[string]int32
}

// addressFor returns the host/port to advertise for the given listener.
func (b BrokerEndpoint) addressFor(listener connstate.ListenerName) (string, int32) {
	listenerStr := string(listener)
	if listenerStr == "external" && b.ExternalHost != "" {
		return b.ExternalHost, b.ExternalPort
	}
	if p, ok := b.ListenerPorts[listenerStr]; ok {
		return b.Host, p
	}
	return b.Host, b.Port
}

// BrokerSource provides the live broker list for Metadata responses.
type BrokerSource interface {
	Self() BrokerEndpoint
	All() []BrokerEndpoint
}

// TopicSource provides the set of known topics and their partition counts.
type TopicSource interface {
	Get(name string) (partitions int32, ok bool)
	All() []TopicEntry
}

// TopicConfig is the per-topic configuration the broker tracks for
// each KafkaTopic CR. Mirrors the recognised subset of
// `cleanup.policy`, `retention.ms`, `retention.bytes`, `segment.bytes`,
// `min.compaction.lag.ms`, and `delete.retention.ms` — the same set
// CreateTopics persists into the CR via TopicCRWriter.translateConfigs.
//
// Pointer fields are nil when the CR didn't set the corresponding
// override; the DescribeConfigs handler then surfaces the broker
// default for that key (with ConfigSource=DEFAULT_CONFIG), and the
// CR-set fields surface as ConfigSource=TOPIC_CONFIG.
type TopicConfig struct {
	CleanupPolicy      string // "delete" | "compact" | "compact,delete"; "" means broker default
	RetentionMs        *int64
	RetentionBytes     *int64
	SegmentBytes       *int64
	MinCompactionLagMs *int64
	DeleteRetentionMs  *int64
}

// TopicConfigSource is an optional supplement to TopicSource: when a
// production TopicSource also implements TopicConfigSource the
// DescribeConfigs handler consults it for per-topic overrides
// (gh #93). Test stubs that only implement TopicSource get the old
// "broker defaults for everything" behaviour, which matches what
// they exercised before this contract existed.
type TopicConfigSource interface {
	TopicConfig(name string) (TopicConfig, bool)
}

// TopicEntry is a single topic visible to the metadata handler.
type TopicEntry struct {
	Name       string
	Partitions int32
}

// BrokerInfo is a static single-broker implementation of BrokerSource.
// Used in local-dev and tests; replaced by a dynamic registry in Kubernetes mode.
type BrokerInfo struct {
	NodeID       int32
	Host         string
	Port         int32
	ClusterID    string
	ExternalHost string // advertised on the TLS listener; empty = reuse Host
	ExternalPort int32
	// ListenerPorts mirrors BrokerEndpoint.ListenerPorts (gh #125):
	// per-listener advertised port for non-external listeners.
	ListenerPorts map[string]int32
}

func (b BrokerInfo) Self() BrokerEndpoint {
	return BrokerEndpoint{
		NodeID:        b.NodeID,
		Host:          b.Host,
		Port:          b.Port,
		ExternalHost:  b.ExternalHost,
		ExternalPort:  b.ExternalPort,
		ListenerPorts: b.ListenerPorts,
	}
}
func (b BrokerInfo) All() []BrokerEndpoint { return []BrokerEndpoint{b.Self()} }

// AutoCreateTopicsConfig is the gh #109 auto.create.topics.enable
// surface the metadata handler consults. When Enabled is true and the
// caller supplies a Creator, MetadataRequest(AllowAutoTopicCreation=
// true) for an unknown non-internal topic triggers a CreateTopic via
// the production CR writer. Mirrors Apache Kafka's
// `auto.create.topics.enable` + `num.partitions` broker configs.
type AutoCreateTopicsConfig struct {
	Enabled       bool
	NumPartitions int32 // default for client-driven auto-create; >0 required when Enabled
}

// AutoTopicCreator is the slim contract MetadataHandler needs to
// implement gh #109. Production wires the same TopicCRWriter the
// CreateTopicsHandler uses (writes a KafkaTopic CR; operator
// reconciles into partition dirs). Tests can substitute a fake.
//
// Returning ErrTopicAlreadyExists is treated as "someone created it
// concurrently" — the metadata handler still returns
// LEADER_NOT_AVAILABLE so the client retries and the next refresh
// finds the leader.
type AutoTopicCreator interface {
	CreateTopic(ctx context.Context, name string, partitions int32, configs map[string]string) error
}

type MetadataHandler struct {
	brokers   BrokerSource
	clusterID string
	topics    TopicSource
	leaders   PartitionLeaderSource

	autoCreate AutoCreateTopicsConfig
	creator    AutoTopicCreator
	inflight   sync.Map // string → struct{}: topics currently being created
}

func NewMetadataHandler(self BrokerInfo, topics TopicSource, leaders PartitionLeaderSource) *MetadataHandler {
	return &MetadataHandler{
		brokers:   self,
		clusterID: self.ClusterID,
		topics:    topics,
		leaders:   leaders,
	}
}

// NewMetadataHandlerWithSource creates a MetadataHandler with a dynamic broker source.
// Used in Kubernetes mode where multiple brokers are known via EndpointSlice.
func NewMetadataHandlerWithSource(brokers BrokerSource, clusterID string, topics TopicSource, leaders PartitionLeaderSource) *MetadataHandler {
	return &MetadataHandler{brokers: brokers, clusterID: clusterID, topics: topics, leaders: leaders}
}

// WithLeaderSource swaps the partition-leader source after construction.
// cmd/skafka uses this to point Metadata at the v3 BrokerCoordinator
// once the cluster runtime starts; until then (and in single-broker
// dev mode) the original lease-backed source is fine.
func (h *MetadataHandler) WithLeaderSource(s PartitionLeaderSource) *MetadataHandler {
	h.leaders = s
	return h
}

// WithAutoCreate enables the gh #109 auto-topic-creation branch.
// Both cfg.Enabled and a non-nil creator are required for the branch
// to fire; without WithAutoCreate the handler keeps the legacy
// "unknown topic → UNKNOWN_TOPIC_OR_PARTITION" behaviour.
func (h *MetadataHandler) WithAutoCreate(cfg AutoCreateTopicsConfig, creator AutoTopicCreator) *MetadataHandler {
	if cfg.NumPartitions < 1 {
		cfg.NumPartitions = 1
	}
	h.autoCreate = cfg
	h.creator = creator
	return h
}

// autoCreateTopic is the gh #109 unknown-topic branch: writes a
// KafkaTopic CR via the configured creator and returns a
// LEADER_NOT_AVAILABLE response so the client's metadata-retry
// loop refetches and finds the topic with leader info on the next
// round. Mirrors Apache's AutoTopicCreationManager which remaps
// TOPIC_ALREADY_EXISTS / REQUEST_TIMED_OUT to LEADER_NOT_AVAILABLE
// for the same reason — the topic now exists; client retries.
//
// Per-handler in-flight dedup via sync.Map: many producers
// concurrently sending Metadata for the same unknown topic submit
// the create exactly once; subsequent callers see the in-flight
// marker and return LEADER_NOT_AVAILABLE without re-entering the
// creator.
func (h *MetadataHandler) autoCreateTopic(_ *connstate.ConnState, name string) api.MetadataTopic {
	leaderNotAvail := api.MetadataTopic{
		ErrorCode: int16(codec.ErrLeaderNotAvailable),
		Name:      name,
	}
	if _, loaded := h.inflight.LoadOrStore(name, struct{}{}); loaded {
		return leaderNotAvail
	}
	defer h.inflight.Delete(name)

	err := h.creator.CreateTopic(context.Background(), name, h.autoCreate.NumPartitions, nil)
	switch {
	case err == nil, errors.Is(err, ErrTopicAlreadyExists):
		// Either path: the topic now exists. Return retriable.
		return leaderNotAvail
	default:
		// Real failure (e.g. K8s API down, RBAC denied). Surface
		// UNKNOWN_TOPIC_OR_PARTITION so the client doesn't tight-
		// loop on a broker that can't satisfy the create — matches
		// the pre-#109 behaviour for every operationally-broken
		// state.
		slog.Warn("auto-create topic failed; falling back to UNKNOWN_TOPIC_OR_PARTITION",
			"topic", name, "err", err)
		return api.MetadataTopic{
			ErrorCode: int16(codec.ErrUnknownTopicOrPartition),
			Name:      name,
		}
	}
}

// isInternalTopic mirrors Apache's `Topic.isInternal`:
// `__consumer_offsets` and `__transaction_state` are the canonical
// internal topics in 3.7. The `__` prefix rule is defensive — skafka
// has no internal topics today (consumer-group state lives in
// per-group files; txn state in /data/__cluster/txn_state slot
// files), but reserving the prefix matches Apache convention and
// keeps clients from auto-creating something that conflicts with a
// future internal topic.
func isInternalTopic(name string) bool {
	return strings.HasPrefix(name, "__")
}

func (h *MetadataHandler) Handle(conn *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeMetadataRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("metadata decode: %w", err)
	}

	// Pick per-listener advertised host. External listener uses per-broker
	// hostnames so clients can route directly to the correct leader on retry.
	listener := connstate.ListenerName("internal")
	if conn != nil && conn.Listener != "" {
		listener = conn.Listener
	}

	allBrokers := h.brokers.All()
	resp := &api.MetadataResponse{
		ClusterID:    h.clusterID,
		ControllerID: h.brokers.Self().NodeID,
	}
	for _, b := range allBrokers {
		host, port := b.addressFor(listener)
		resp.Brokers = append(resp.Brokers, api.MetadataBroker{
			NodeID: b.NodeID,
			Host:   host,
			Port:   port,
		})
	}

	var entries []TopicEntry
	isAllTopics := len(req.Topics) == 0
	if isAllTopics {
		entries = h.topics.All()
	} else {
		for _, name := range req.Topics {
			if partitions, ok := h.topics.Get(name); ok {
				entries = append(entries, TopicEntry{Name: name, Partitions: partitions})
				continue
			}
			// gh #109: auto.create.topics.enable. Apache requires
			// BOTH the request flag AND the broker config to be on,
			// AND the request not to be the "list everything" form
			// (Streams' periodic full refresh would otherwise spam-
			// create). Internal-topic names are reserved.
			if h.autoCreate.Enabled && req.AllowAutoTopicCreation && h.creator != nil &&
				!isAllTopics && !isInternalTopic(name) {
				resp.Topics = append(resp.Topics, h.autoCreateTopic(conn, name))
				continue
			}
			resp.Topics = append(resp.Topics, api.MetadataTopic{
				ErrorCode: int16(codec.ErrUnknownTopicOrPartition),
				Name:      name,
			})
		}
	}

	for _, entry := range entries {
		topic := api.MetadataTopic{Name: entry.Name, ErrorCode: 0}
		for p := int32(0); p < entry.Partitions; p++ {
			leaderID := h.leaders.LeaderFor(entry.Name, p)
			// Replicas/ISR must include the leader, otherwise modern
			// admin/consumer clients refuse to send listOffsets etc. to
			// it ("Timed out waiting for a node assignment"). Skafka has
			// no replication, so the replica set is just the leader.
			// When leadership is unknown (controller hasn't recomputed
			// since topic add) fall back to self so the response stays
			// well-formed; the client will retry on its next refresh.
			replicaID := leaderID
			if replicaID < 0 {
				replicaID = h.brokers.Self().NodeID
			}
			topic.Partitions = append(topic.Partitions, api.MetadataPartition{
				PartitionIndex:  p,
				LeaderID:        leaderID,
				LeaderEpoch:     0,
				ReplicaNodes:    []int32{replicaID},
				IsrNodes:        []int32{replicaID},
				OfflineReplicas: []int32{},
			})
		}
		resp.Topics = append(resp.Topics, topic)
	}

	w := codec.NewWriter()
	api.EncodeMetadataResponse(w, resp, version)
	return w.Bytes(), nil
}
