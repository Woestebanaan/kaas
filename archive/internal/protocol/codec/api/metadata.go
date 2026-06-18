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
	// TopicIDBytes is the 16-byte KIP-516 UUID (gh #105). Empty → the
	// encoder writes the all-zero UUID (pre-#105 fallback). Length is
	// either 0 or 16; anything else is a bug. Surfaced on v10+.
	TopicIDBytes              []byte
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
				// gh #105: surface the per-topic UUID when the operator
				// has assigned one. Empty / wrong-length falls back to
				// the all-zero sentinel for legacy CRs that predate
				// Status.TopicID.
				if len(t.TopicIDBytes) == 16 {
					w.WriteRawBytes(t.TopicIDBytes)
				} else {
					w.WriteRawBytes(make([]byte, 16))
				}
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

// DecodeMetadataResponse is the symmetric partner of EncodeMetadataResponse.
// Used by tests and by any future caller that needs to inspect a Metadata response
// (for example, operator-side validation). Handles all supported versions 1–12.
func DecodeMetadataResponse(r *codec.Reader, version int16) (*MetadataResponse, error) {
	resp := &MetadataResponse{}
	flexible := version >= 9

	var err error
	if version >= 3 {
		if resp.ThrottleTimeMs, err = r.ReadInt32(); err != nil {
			return nil, err
		}
	}

	readBrokers := func() error {
		var b MetadataBroker
		if b.NodeID, err = r.ReadInt32(); err != nil {
			return err
		}
		if flexible {
			if b.Host, err = r.ReadCompactString(); err != nil {
				return err
			}
		} else {
			if b.Host, err = r.ReadString(); err != nil {
				return err
			}
		}
		if b.Port, err = r.ReadInt32(); err != nil {
			return err
		}
		if version >= 1 {
			// Reader returns "" on null and the actual string otherwise,
			// so discard the bool — historical handling of it was
			// inverted (named "null" but actually "isNonNull").
			if flexible {
				b.Rack, _, err = r.ReadCompactNullableString()
			} else {
				b.Rack, _, err = r.ReadNullableString()
			}
			if err != nil {
				return err
			}
		}
		if flexible {
			if err := r.ReadTaggedFields(); err != nil {
				return err
			}
		}
		resp.Brokers = append(resp.Brokers, b)
		return nil
	}
	if flexible {
		if err := r.ReadCompactArray(readBrokers); err != nil {
			return nil, err
		}
	} else {
		if err := r.ReadArray(readBrokers); err != nil {
			return nil, err
		}
	}

	if version >= 2 {
		if flexible {
			resp.ClusterID, _, err = r.ReadCompactNullableString()
		} else {
			resp.ClusterID, _, err = r.ReadNullableString()
		}
		if err != nil {
			return nil, err
		}
	}
	if version >= 1 {
		if resp.ControllerID, err = r.ReadInt32(); err != nil {
			return nil, err
		}
	}

	readTopics := func() error {
		var t MetadataTopic
		if t.ErrorCode, err = r.ReadInt16(); err != nil {
			return err
		}
		if flexible && version >= 12 {
			t.Name, _, err = r.ReadCompactNullableString()
		} else if flexible {
			t.Name, err = r.ReadCompactString()
		} else {
			t.Name, err = r.ReadString()
		}
		if err != nil {
			return err
		}
		if version >= 10 {
			// Skip the 16-byte TopicID UUID.
			if _, err := r.ReadRaw(16); err != nil {
				return err
			}
		}
		if version >= 1 {
			v, err := r.ReadInt8()
			if err != nil {
				return err
			}
			t.IsInternal = v != 0
		}

		readPartitions := func() error {
			var p MetadataPartition
			if p.ErrorCode, err = r.ReadInt16(); err != nil {
				return err
			}
			if p.PartitionIndex, err = r.ReadInt32(); err != nil {
				return err
			}
			if p.LeaderID, err = r.ReadInt32(); err != nil {
				return err
			}
			if version >= 7 {
				if p.LeaderEpoch, err = r.ReadInt32(); err != nil {
					return err
				}
			}
			readInt32Slice := func(dst *[]int32) error {
				reader := func() error {
					v, err := r.ReadInt32()
					if err != nil {
						return err
					}
					*dst = append(*dst, v)
					return nil
				}
				if flexible {
					return r.ReadCompactArray(reader)
				}
				return r.ReadArray(reader)
			}
			if err := readInt32Slice(&p.ReplicaNodes); err != nil {
				return err
			}
			if err := readInt32Slice(&p.IsrNodes); err != nil {
				return err
			}
			if version >= 5 {
				if err := readInt32Slice(&p.OfflineReplicas); err != nil {
					return err
				}
			}
			if flexible {
				if err := r.ReadTaggedFields(); err != nil {
					return err
				}
			}
			t.Partitions = append(t.Partitions, p)
			return nil
		}
		if flexible {
			if err := r.ReadCompactArray(readPartitions); err != nil {
				return err
			}
		} else {
			if err := r.ReadArray(readPartitions); err != nil {
				return err
			}
		}

		if version >= 8 {
			if t.TopicAuthorizedOperations, err = r.ReadInt32(); err != nil {
				return err
			}
		}
		if flexible {
			if err := r.ReadTaggedFields(); err != nil {
				return err
			}
		}
		resp.Topics = append(resp.Topics, t)
		return nil
	}
	if flexible {
		if err := r.ReadCompactArray(readTopics); err != nil {
			return nil, err
		}
	} else {
		if err := r.ReadArray(readTopics); err != nil {
			return nil, err
		}
	}

	if version >= 8 && version <= 10 {
		if resp.ClusterAuthorizedOperations, err = r.ReadInt32(); err != nil {
			return nil, err
		}
	}
	if flexible {
		if err := r.ReadTaggedFields(); err != nil {
			return nil, err
		}
	}
	return resp, nil
}
