package api

import "github.com/woestebanaan/skafka/internal/protocol/codec"

// FetchRequest (key 1, v4–v12). v13 introduced UUID topic IDs in place of
// topic names; that schema change is not implemented yet — clients
// negotiating to v13 fall back to v12 via the ApiVersions handshake.
type FetchRequest struct {
	ReplicaID       int32  // always -1 for clients
	MaxWaitMs       int32
	MinBytes        int32
	MaxBytes        int32  // v3+
	IsolationLevel  int8   // v4+: 0=read_uncommitted, 1=read_committed
	SessionID       int32  // v7+
	SessionEpoch    int32  // v7+
	Topics          []FetchTopic
	ForgottenTopics []ForgottenTopic // v7+
	RackID          string           // v11+
}

type FetchTopic struct {
	Name       string
	Partitions []FetchPartition
}

type FetchPartition struct {
	PartitionIndex     int32
	CurrentLeaderEpoch int32  // v9+
	FetchOffset        int64
	LastFetchedEpoch   int32  // v12+
	LogStartOffset     int64  // v5+
	PartitionMaxBytes  int32
}

type ForgottenTopic struct {
	Name       string
	Partitions []int32
}

// FetchResponse (key 1, v4–v13).
type FetchResponse struct {
	ThrottleTimeMs int32  // v1+
	ErrorCode      int16  // v7+
	SessionID      int32  // v7+
	Responses      []FetchTopicResponse
}

type FetchTopicResponse struct {
	Name       string
	Partitions []FetchPartitionResponse
}

type AbortedTransaction struct {
	ProducerID  int64
	FirstOffset int64
}

type FetchPartitionResponse struct {
	PartitionIndex        int32
	ErrorCode             int16
	HighWatermark         int64
	LastStableOffset      int64                // v4+; -1 if not available
	LogStartOffset        int64                // v5+
	AbortedTransactions   []AbortedTransaction // v4+
	PreferredReadReplica  int32                // v11+; -1 if none
	Records               []byte               // nullable bytes
}

func DecodeFetchRequest(r *codec.Reader, version int16) (*FetchRequest, error) {
	req := &FetchRequest{}
	flexible := version >= 12

	var err error
	if req.ReplicaID, err = r.ReadInt32(); err != nil {
		return nil, err
	}
	if req.MaxWaitMs, err = r.ReadInt32(); err != nil {
		return nil, err
	}
	if req.MinBytes, err = r.ReadInt32(); err != nil {
		return nil, err
	}
	if version >= 3 {
		if req.MaxBytes, err = r.ReadInt32(); err != nil {
			return nil, err
		}
	}
	if version >= 4 {
		v, err := r.ReadInt8()
		if err != nil {
			return nil, err
		}
		req.IsolationLevel = v
	}
	if version >= 7 {
		if req.SessionID, err = r.ReadInt32(); err != nil {
			return nil, err
		}
		if req.SessionEpoch, err = r.ReadInt32(); err != nil {
			return nil, err
		}
	}

	readTopics := func() error {
		var t FetchTopic
		var err error
		if t.Name, err = readString(r, flexible); err != nil {
			return err
		}
		readPartitions := func() error {
			var p FetchPartition
			var err error
			if p.PartitionIndex, err = r.ReadInt32(); err != nil {
				return err
			}
			if version >= 9 {
				if p.CurrentLeaderEpoch, err = r.ReadInt32(); err != nil {
					return err
				}
			}
			if p.FetchOffset, err = r.ReadInt64(); err != nil {
				return err
			}
			if version >= 12 {
				if p.LastFetchedEpoch, err = r.ReadInt32(); err != nil {
					return err
				}
			}
			if version >= 5 {
				if p.LogStartOffset, err = r.ReadInt64(); err != nil {
					return err
				}
			}
			if p.PartitionMaxBytes, err = r.ReadInt32(); err != nil {
				return err
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
			err = r.ReadCompactArray(readPartitions)
		} else {
			err = r.ReadArray(readPartitions)
		}
		if err != nil {
			return err
		}
		if flexible {
			if err := r.ReadTaggedFields(); err != nil {
				return err
			}
		}
		req.Topics = append(req.Topics, t)
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

	if version >= 7 {
		readForgotten := func() error {
			var ft ForgottenTopic
			var err error
			if ft.Name, err = readString(r, flexible); err != nil {
				return err
			}
			readParts := func() error {
				v, err := r.ReadInt32()
				ft.Partitions = append(ft.Partitions, v)
				return err
			}
			if flexible {
				err = r.ReadCompactArray(readParts)
			} else {
				err = r.ReadArray(readParts)
			}
			if err != nil {
				return err
			}
			if flexible {
				if err := r.ReadTaggedFields(); err != nil {
					return err
				}
			}
			req.ForgottenTopics = append(req.ForgottenTopics, ft)
			return nil
		}
		if flexible {
			if err := r.ReadCompactArray(readForgotten); err != nil {
				return nil, err
			}
		} else {
			if err := r.ReadArray(readForgotten); err != nil {
				return nil, err
			}
		}
	}

	if version >= 11 {
		var err error
		if req.RackID, err = readString(r, flexible); err != nil {
			return nil, err
		}
	}
	if flexible {
		return req, r.ReadTaggedFields()
	}
	return req, nil
}

func EncodeFetchResponse(w *codec.Writer, resp *FetchResponse, version int16) {
	flexible := version >= 12

	if version >= 1 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}
	if version >= 7 {
		w.WriteInt16(resp.ErrorCode)
		w.WriteInt32(resp.SessionID)
	}

	writeResponses := func() {
		for _, t := range resp.Responses {
			writeString(w, t.Name, flexible)
			writePartitions := func() {
				for _, p := range t.Partitions {
					w.WriteInt32(p.PartitionIndex)
					w.WriteInt16(p.ErrorCode)
					w.WriteInt64(p.HighWatermark)
					if version >= 4 {
						w.WriteInt64(p.LastStableOffset)
					}
					if version >= 5 {
						w.WriteInt64(p.LogStartOffset)
					}
					if version >= 4 {
						writeAborted := func() {
							for _, a := range p.AbortedTransactions {
								w.WriteInt64(a.ProducerID)
								w.WriteInt64(a.FirstOffset)
								if flexible {
									w.WriteEmptyTaggedFields()
								}
							}
						}
						if flexible {
							w.WriteCompactArray(len(p.AbortedTransactions), writeAborted)
						} else {
							w.WriteArray(len(p.AbortedTransactions), writeAborted)
						}
					}
					if version >= 11 {
						w.WriteInt32(p.PreferredReadReplica)
					}
					if flexible {
						w.WriteCompactNullableBytes(p.Records)
					} else {
						w.WriteNullableBytes(p.Records)
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
			if flexible {
				w.WriteEmptyTaggedFields()
			}
		}
	}
	if flexible {
		w.WriteCompactArray(len(resp.Responses), writeResponses)
	} else {
		w.WriteArray(len(resp.Responses), writeResponses)
	}

	if flexible {
		w.WriteEmptyTaggedFields()
	}
}
