package api

import "github.com/woestebanaan/skafka/internal/protocol/codec"

// DeleteRecordsRequest (key 21, v0–v2). Used by Kafka admin clients
// (e.g. kafka-delete-records.sh, Kafbat's "Purge messages") to advance
// a partition's log start offset, making earlier records invisible to
// Fetch and eligible for retention cleanup. v2 introduces flexible
// (KIP-482 tagged-fields) framing.
type DeleteRecordsRequest struct {
	Topics    []DeleteRecordsTopic
	TimeoutMs int32
}

type DeleteRecordsTopic struct {
	Name       string
	Partitions []DeleteRecordsPartition
}

type DeleteRecordsPartition struct {
	PartitionIndex int32
	Offset         int64 // -1 = "all current records" (HWM)
}

// DeleteRecordsResponse (key 21, v0–v2).
type DeleteRecordsResponse struct {
	ThrottleTimeMs int32
	Topics         []DeleteRecordsTopicResult
}

type DeleteRecordsTopicResult struct {
	Name       string
	Partitions []DeleteRecordsPartitionResult
}

type DeleteRecordsPartitionResult struct {
	PartitionIndex int32
	LowWatermark   int64 // new log start offset after the truncation
	ErrorCode      int16
}

func DecodeDeleteRecordsRequest(r *codec.Reader, version int16) (*DeleteRecordsRequest, error) {
	req := &DeleteRecordsRequest{}
	flexible := version >= 2

	readTopics := func() error {
		var t DeleteRecordsTopic
		var err error
		if t.Name, err = readString(r, flexible); err != nil {
			return err
		}
		readPartitions := func() error {
			var p DeleteRecordsPartition
			var err error
			if p.PartitionIndex, err = r.ReadInt32(); err != nil {
				return err
			}
			if p.Offset, err = r.ReadInt64(); err != nil {
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

	var err error
	if flexible {
		if err = r.ReadCompactArray(readTopics); err != nil {
			return nil, err
		}
	} else {
		if err = r.ReadArray(readTopics); err != nil {
			return nil, err
		}
	}
	if req.TimeoutMs, err = r.ReadInt32(); err != nil {
		return nil, err
	}
	if flexible {
		return req, r.ReadTaggedFields()
	}
	return req, nil
}

func EncodeDeleteRecordsResponse(w *codec.Writer, resp *DeleteRecordsResponse, version int16) {
	flexible := version >= 2

	w.WriteInt32(resp.ThrottleTimeMs)

	writeTopics := func() {
		for _, t := range resp.Topics {
			writeString(w, t.Name, flexible)
			writePartitions := func() {
				for _, p := range t.Partitions {
					w.WriteInt32(p.PartitionIndex)
					w.WriteInt64(p.LowWatermark)
					w.WriteInt16(p.ErrorCode)
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
		w.WriteCompactArray(len(resp.Topics), writeTopics)
	} else {
		w.WriteArray(len(resp.Topics), writeTopics)
	}
	if flexible {
		w.WriteEmptyTaggedFields()
	}
}
