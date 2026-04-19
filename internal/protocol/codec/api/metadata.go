package api

import "github.com/woestebanaan/skafka/internal/protocol/codec"

// MetadataRequest (key 3, v1–v12).
type MetadataRequest struct {
	Topics                             []string // nil = all topics
	AllowAutoTopicCreation             bool     // v4+
	IncludeClusterAuthorizedOperations bool     // v8+
	IncludeTopicAuthorizedOperations   bool     // v8+
}

type MetadataBroker struct {
	NodeID int32
	Host   string
	Port   int32
	Rack   string // v1+, empty string if absent
}

type MetadataPartition struct {
	ErrorCode       int16
	PartitionIndex  int32
	LeaderID        int32
	LeaderEpoch     int32   // v7+
	ReplicaNodes    []int32
	IsrNodes        []int32
	OfflineReplicas []int32 // v5+
}

type MetadataTopic struct {
	ErrorCode                 int16
	Name                      string
	IsInternal                bool    // v1+
	Partitions                []MetadataPartition
	TopicAuthorizedOperations int32   // v8+
}

// MetadataResponse (key 3, v1–v12).
type MetadataResponse struct {
	ThrottleTimeMs              int32  // v3+
	Brokers                     []MetadataBroker
	ClusterID                   string // v2+, nullable
	ControllerID                int32  // v1+
	Topics                      []MetadataTopic
	ClusterAuthorizedOperations int32  // v8+
}

func DecodeMetadataRequest(r *codec.Reader, version int16) (*MetadataRequest, error) {
	req := &MetadataRequest{}
	flexible := version >= 9

	var topicCount int
	if flexible {
		if err := r.ReadCompactArray(func() error {
			var name string
			if version >= 10 {
				// v10+: topic_id (UUID, 16 raw bytes) then nullable topic name.
				if _, err := r.ReadRaw(16); err != nil {
					return err
				}
				n, _, err := r.ReadCompactNullableString()
				if err != nil {
					return err
				}
				name = n
			} else {
				// v9: non-nullable compact string.
				n, err := r.ReadCompactString()
				if err != nil {
					return err
				}
				name = n
			}
			if err := r.ReadTaggedFields(); err != nil {
				return err
			}
			if name != "" {
				req.Topics = append(req.Topics, name)
			}
			topicCount++
			return nil
		}); err != nil {
			return nil, err
		}
	} else {
		if err := r.ReadArray(func() error {
			name, err := r.ReadString()
			if err != nil {
				return err
			}
			req.Topics = append(req.Topics, name)
			return nil
		}); err != nil {
			return nil, err
		}
	}
	_ = topicCount

	if version >= 4 {
		v, err := r.ReadInt8()
		if err != nil {
			return nil, err
		}
		req.AllowAutoTopicCreation = v != 0
	}
	if version >= 8 && version <= 10 {
		// IncludeClusterAuthorizedOperations was removed in v11 (Kafka 2.8+).
		v, err := r.ReadInt8()
		if err != nil {
			return nil, err
		}
		req.IncludeClusterAuthorizedOperations = v != 0
	}
	if version >= 8 {
		v, err := r.ReadInt8()
		if err != nil {
			return nil, err
		}
		req.IncludeTopicAuthorizedOperations = v != 0
	}
	if flexible {
		if err := r.ReadTaggedFields(); err != nil {
			return nil, err
		}
	}
	return req, nil
}

func EncodeMetadataResponse(w *codec.Writer, resp *MetadataResponse, version int16) {
	flexible := version >= 9

	if version >= 3 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}

	writeBrokers := func() {
		for _, b := range resp.Brokers {
			w.WriteInt32(b.NodeID)
			if flexible {
				w.WriteCompactString(b.Host)
			} else {
				w.WriteString(b.Host)
			}
			w.WriteInt32(b.Port)
			if version >= 1 {
				if flexible {
					w.WriteCompactNullableString(b.Rack, b.Rack == "")
				} else {
					w.WriteNullableString(b.Rack, b.Rack == "")
				}
			}
			if flexible {
				w.WriteEmptyTaggedFields()
			}
		}
	}
	if flexible {
		w.WriteCompactArray(len(resp.Brokers), writeBrokers)
	} else {
		w.WriteArray(len(resp.Brokers), writeBrokers)
	}

	if version >= 2 {
		if flexible {
			w.WriteCompactNullableString(resp.ClusterID, resp.ClusterID == "")
		} else {
			w.WriteNullableString(resp.ClusterID, resp.ClusterID == "")
		}
	}
	if version >= 1 {
		w.WriteInt32(resp.ControllerID)
	}

	writeTopics := func() {
		for _, t := range resp.Topics {
			w.WriteInt16(t.ErrorCode)
			// v12+: topic name is compact nullable string.
			// v9-v11 flexible: compact string (non-nullable).
			// non-flexible: legacy string.
			if flexible && version >= 12 {
				w.WriteCompactNullableString(t.Name, false)
			} else if flexible {
				w.WriteCompactString(t.Name)
			} else {
				w.WriteString(t.Name)
			}
			if version >= 10 {
				// TopicID: 16-byte UUID. We use all-zero for now.
				w.WriteRawBytes(make([]byte, 16))
			}
			if version >= 1 {
				if t.IsInternal {
					w.WriteInt8(1)
				} else {
					w.WriteInt8(0)
				}
			}

			writePartitions := func() {
				for _, p := range t.Partitions {
					w.WriteInt16(p.ErrorCode)
					w.WriteInt32(p.PartitionIndex)
					w.WriteInt32(p.LeaderID)
					if version >= 7 {
						w.WriteInt32(p.LeaderEpoch)
					}
					writeInt32Slice := func(s []int32) {
						if flexible {
							w.WriteCompactArray(len(s), func() {
								for _, v := range s {
									w.WriteInt32(v)
								}
							})
						} else {
							w.WriteArray(len(s), func() {
								for _, v := range s {
									w.WriteInt32(v)
								}
							})
						}
					}
					writeInt32Slice(p.ReplicaNodes)
					writeInt32Slice(p.IsrNodes)
					if version >= 5 {
						writeInt32Slice(p.OfflineReplicas)
					}
					if flexible {
						w.WriteEmptyTaggedFields()
					}
				}
			}
			if flexible {
				w.WriteCompactArray(len(t.Partitions), writePartitions)
			} else {
				w.WriteArray(len(t.Partitions), writePartitions)
			}

			if version >= 8 {
				w.WriteInt32(t.TopicAuthorizedOperations)
			}
			if flexible {
				w.WriteEmptyTaggedFields()
			}
		}
	}
	if flexible {
		w.WriteCompactArray(len(resp.Topics), writeTopics)
	} else {
		w.WriteArray(len(resp.Topics), writeTopics)
	}

	if version >= 8 && version <= 10 {
		// ClusterAuthorizedOperations removed in v11 (Kafka 2.8+).
		w.WriteInt32(resp.ClusterAuthorizedOperations)
	}
	if flexible {
		w.WriteEmptyTaggedFields()
	}
}
