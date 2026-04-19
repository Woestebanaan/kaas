package handlers

import (
	"fmt"

	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/lease"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

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

// BrokerInfo is static identity for this broker instance.
type BrokerInfo struct {
	NodeID    int32
	Host      string
	Port      int32
	ClusterID string
}

type MetadataHandler struct {
	self   BrokerInfo
	topics TopicSource
	leases lease.LeaseManager
}

func NewMetadataHandler(self BrokerInfo, topics TopicSource, leases lease.LeaseManager) *MetadataHandler {
	return &MetadataHandler{self: self, topics: topics, leases: leases}
}

func (h *MetadataHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeMetadataRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("metadata decode: %w", err)
	}

	broker := api.MetadataBroker{
		NodeID: h.self.NodeID,
		Host:   h.self.Host,
		Port:   h.self.Port,
	}

	resp := &api.MetadataResponse{
		Brokers:      []api.MetadataBroker{broker},
		ClusterID:    h.self.ClusterID,
		ControllerID: h.self.NodeID,
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
			leaderID := int32(-1)
			if h.leases.IsLeader(entry.Name, p) {
				leaderID = h.self.NodeID
			}
			topic.Partitions = append(topic.Partitions, api.MetadataPartition{
				PartitionIndex:  p,
				LeaderID:        leaderID,
				LeaderEpoch:     0,
				ReplicaNodes:    []int32{h.self.NodeID},
				IsrNodes:        []int32{h.self.NodeID},
				OfflineReplicas: []int32{},
			})
		}
		resp.Topics = append(resp.Topics, topic)
	}

	w := codec.NewWriter()
	api.EncodeMetadataResponse(w, resp, version)
	return w.Bytes(), nil
}
