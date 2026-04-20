package handlers

import (
	"fmt"

	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/lease"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

// BrokerEndpoint is one broker in the cluster as seen by the Metadata handler.
type BrokerEndpoint struct {
	NodeID int32
	Host   string
	Port   int32
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

// TopicEntry is a single topic visible to the metadata handler.
type TopicEntry struct {
	Name       string
	Partitions int32
}

// BrokerInfo is a static single-broker implementation of BrokerSource.
// Used in local-dev and tests; replaced by a dynamic registry in Kubernetes mode.
type BrokerInfo struct {
	NodeID    int32
	Host      string
	Port      int32
	ClusterID string
}

func (b BrokerInfo) Self() BrokerEndpoint { return BrokerEndpoint{NodeID: b.NodeID, Host: b.Host, Port: b.Port} }
func (b BrokerInfo) All() []BrokerEndpoint { return []BrokerEndpoint{b.Self()} }

type MetadataHandler struct {
	brokers   BrokerSource
	clusterID string
	topics    TopicSource
	leases    lease.LeaseManager
}

func NewMetadataHandler(self BrokerInfo, topics TopicSource, leases lease.LeaseManager) *MetadataHandler {
	return &MetadataHandler{
		brokers:   self,
		clusterID: self.ClusterID,
		topics:    topics,
		leases:    leases,
	}
}

// NewMetadataHandlerWithSource creates a MetadataHandler with a dynamic broker source.
// Used in Kubernetes mode where multiple brokers are known via EndpointSlice.
func NewMetadataHandlerWithSource(brokers BrokerSource, clusterID string, topics TopicSource, leases lease.LeaseManager) *MetadataHandler {
	return &MetadataHandler{brokers: brokers, clusterID: clusterID, topics: topics, leases: leases}
}

func (h *MetadataHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeMetadataRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("metadata decode: %w", err)
	}

	allBrokers := h.brokers.All()
	resp := &api.MetadataResponse{
		ClusterID:    h.clusterID,
		ControllerID: h.brokers.Self().NodeID,
	}
	for _, b := range allBrokers {
		resp.Brokers = append(resp.Brokers, api.MetadataBroker{
			NodeID: b.NodeID,
			Host:   b.Host,
			Port:   b.Port,
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
			leaderID := h.leases.LeaderFor(entry.Name, p)
			topic.Partitions = append(topic.Partitions, api.MetadataPartition{
				PartitionIndex:  p,
				LeaderID:        leaderID,
				LeaderEpoch:     0,
				ReplicaNodes:    []int32{h.brokers.Self().NodeID},
				IsrNodes:        []int32{h.brokers.Self().NodeID},
				OfflineReplicas: []int32{},
			})
		}
		resp.Topics = append(resp.Topics, topic)
	}

	w := codec.NewWriter()
	api.EncodeMetadataResponse(w, resp, version)
	return w.Bytes(), nil
}
