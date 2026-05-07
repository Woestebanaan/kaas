package handlers

import (
	"fmt"

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
type BrokerEndpoint struct {
	NodeID       int32
	Host         string
	Port         int32
	ExternalHost string // per-broker external hostname (empty if no external listener)
	ExternalPort int32
}

// addressFor returns the host/port to advertise for the given listener.
func (b BrokerEndpoint) addressFor(listener connstate.ListenerName) (string, int32) {
	if listener == connstate.ListenerExternal && b.ExternalHost != "" {
		return b.ExternalHost, b.ExternalPort
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
}

func (b BrokerInfo) Self() BrokerEndpoint {
	return BrokerEndpoint{
		NodeID:       b.NodeID,
		Host:         b.Host,
		Port:         b.Port,
		ExternalHost: b.ExternalHost,
		ExternalPort: b.ExternalPort,
	}
}
func (b BrokerInfo) All() []BrokerEndpoint { return []BrokerEndpoint{b.Self()} }

type MetadataHandler struct {
	brokers   BrokerSource
	clusterID string
	topics    TopicSource
	leaders   PartitionLeaderSource
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

func (h *MetadataHandler) Handle(conn *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeMetadataRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("metadata decode: %w", err)
	}

	// Pick per-listener advertised host. External listener uses per-broker
	// hostnames so clients can route directly to the correct leader on retry.
	listener := connstate.ListenerInternal
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
	if len(req.Topics) == 0 {
		entries = h.topics.All()
	} else {
		for _, name := range req.Topics {
			if partitions, ok := h.topics.Get(name); ok {
				entries = append(entries, TopicEntry{Name: name, Partitions: partitions})
			} else {
				resp.Topics = append(resp.Topics, api.MetadataTopic{
					ErrorCode: int16(codec.ErrUnknownTopicOrPartition),
					Name:      name,
				})
			}
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
