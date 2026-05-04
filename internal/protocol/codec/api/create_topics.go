package api

import "github.com/woestebanaan/skafka/internal/protocol/codec"

// CreateTopicsRequest (key 19, v0–v6).
type CreateTopicsRequest struct {
	Topics    []CreatableTopic
	TimeoutMs int32
	ValidateOnly bool // v1+
}

type CreatableTopic struct {
	Name              string
	NumPartitions     int32
	ReplicationFactor int16
	Assignments       []CreatableTopicAssignment
	Configs           []CreateableTopicConfig
}

type CreatableTopicAssignment struct {
	PartitionIndex int32
	BrokerIDs      []int32
}

type CreateableTopicConfig struct {
	Name  string
	Value string // nullable
}

// CreateTopicsResponse (key 19, v0–v6).
type CreateTopicsResponse struct {
	ThrottleTimeMs int32 // v2+
	Topics         []CreatableTopicResult
}

type CreatableTopicResult struct {
	Name              string
	ErrorCode         int16
	ErrorMessage      string // v1+, nullable
	NumPartitions     int32  // v5+
	ReplicationFactor int16  // v5+
}

func DecodeCreateTopicsRequest(r *codec.Reader, version int16) (*CreateTopicsRequest, error) {
	req := &CreateTopicsRequest{}
	flexible := version >= 5

	readTopic := func() error {
		var t CreatableTopic
		var err error
		if t.Name, err = readString(r, flexible); err != nil {
			return err
		}
		if t.NumPartitions, err = r.ReadInt32(); err != nil {
			return err
		}
		if t.ReplicationFactor, err = r.ReadInt16(); err != nil {
			return err
		}
		readAssignment := func() error {
			var a CreatableTopicAssignment
			var err error
			if a.PartitionIndex, err = r.ReadInt32(); err != nil {
				return err
			}
			readBroker := func() error {
				id, err := r.ReadInt32()
				a.BrokerIDs = append(a.BrokerIDs, id)
				return err
			}
			if flexible {
				err = r.ReadCompactArray(readBroker)
			} else {
				err = r.ReadArray(readBroker)
			}
			if err != nil {
				return err
			}
			if flexible {
				if err := r.ReadTaggedFields(); err != nil {
					return err
				}
			}
			t.Assignments = append(t.Assignments, a)
			return nil
		}
		if flexible {
			if err := r.ReadCompactArray(readAssignment); err != nil {
				return err
			}
		} else {
			if err := r.ReadArray(readAssignment); err != nil {
				return err
			}
		}
		readConfig := func() error {
			var c CreateableTopicConfig
			var err error
			if c.Name, err = readString(r, flexible); err != nil {
				return err
			}
			val, _, err := nullableString(r, flexible)
			if err != nil {
				return err
			}
			c.Value = val
			if flexible {
				if err := r.ReadTaggedFields(); err != nil {
					return err
				}
			}
			t.Configs = append(t.Configs, c)
			return nil
		}
		if flexible {
			if err := r.ReadCompactArray(readConfig); err != nil {
				return err
			}
			if err := r.ReadTaggedFields(); err != nil {
				return err
			}
		} else {
			if err := r.ReadArray(readConfig); err != nil {
				return err
			}
		}
		req.Topics = append(req.Topics, t)
		return nil
	}
	if flexible {
		if err := r.ReadCompactArray(readTopic); err != nil {
			return nil, err
		}
	} else {
		if err := r.ReadArray(readTopic); err != nil {
			return nil, err
		}
	}
	var err error
	if req.TimeoutMs, err = r.ReadInt32(); err != nil {
		return nil, err
	}
	if version >= 1 {
		v, err := r.ReadInt8()
		if err != nil {
			return nil, err
		}
		req.ValidateOnly = v != 0
	}
	if flexible {
		return req, r.ReadTaggedFields()
	}
	return req, nil
}

func EncodeCreateTopicsResponse(w *codec.Writer, resp *CreateTopicsResponse, version int16) {
	flexible := version >= 5
	if version >= 2 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}
	writeTopics := func() {
		for _, t := range resp.Topics {
			writeString(w, t.Name, flexible)
			w.WriteInt16(t.ErrorCode)
			if version >= 1 {
				if flexible {
					w.WriteCompactNullableString(t.ErrorMessage, t.ErrorMessage == "")
				} else {
					w.WriteNullableString(t.ErrorMessage, t.ErrorMessage == "")
				}
			}
			if version >= 5 {
				w.WriteInt32(t.NumPartitions)
				w.WriteInt16(t.ReplicationFactor)
				// configs omitted in response for v5+ (empty compact array)
				if flexible {
					w.WriteCompactArray(0, func() {})
				}
			}
			if flexible {
				w.WriteEmptyTaggedFields()
			}
		}
	}
	if flexible {
		w.WriteCompactArray(len(resp.Topics), writeTopics)
		w.WriteEmptyTaggedFields()
	} else {
		w.WriteArray(len(resp.Topics), writeTopics)
	}
}
