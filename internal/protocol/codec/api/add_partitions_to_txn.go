package api

import "github.com/woestebanaan/skafka/internal/protocol/codec"

// AddPartitionsToTxnRequest (API key 24, v0–v3 single-txn shape).
// v4 introduced multi-transaction batching (Transactions[]); skafka
// supports v0-v3 only — the single-txn form is what every Java/Go
// client actually sends.
//
// Apache schema (clients/.../AddPartitionsToTxnRequest.json):
//   - V3AndBelowTransactionalId: "0-3" string
//   - V3AndBelowProducerId:      "0-3" int64
//   - V3AndBelowProducerEpoch:   "0-3" int16
//   - V3AndBelowTopics:          "0-3" array
//   - flexibleVersions:          "3+"
type AddPartitionsToTxnRequest struct {
	TransactionalID string
	ProducerID      int64
	ProducerEpoch   int16
	Topics          []AddPartitionsToTxnTopic
}

// AddPartitionsToTxnTopic groups partitions by topic on the wire.
type AddPartitionsToTxnTopic struct {
	Name       string
	Partitions []int32
}

// AddPartitionsToTxnResponse (v0–v3). Per-partition error code is
// the only error surface in v0-3 (no top-level ErrorCode field —
// that's v4+).
type AddPartitionsToTxnResponse struct {
	ThrottleTimeMs int32
	Results        []AddPartitionsToTxnTopicResult
}

type AddPartitionsToTxnTopicResult struct {
	Name             string
	PartitionResults []AddPartitionsToTxnPartitionResult
}

type AddPartitionsToTxnPartitionResult struct {
	PartitionIndex int32
	ErrorCode      int16
}

func DecodeAddPartitionsToTxnRequest(r *codec.Reader, version int16) (*AddPartitionsToTxnRequest, error) {
	flexible := version >= 3
	req := &AddPartitionsToTxnRequest{}

	var (
		tid string
		err error
	)
	if flexible {
		tid, err = r.ReadCompactString()
	} else {
		tid, err = r.ReadString()
	}
	if err != nil {
		return nil, err
	}
	req.TransactionalID = tid

	pid, err := r.ReadInt64()
	if err != nil {
		return nil, err
	}
	req.ProducerID = pid

	epoch, err := r.ReadInt16()
	if err != nil {
		return nil, err
	}
	req.ProducerEpoch = epoch

	readTopic := func() error {
		var t AddPartitionsToTxnTopic
		var name string
		var err error
		if flexible {
			name, err = r.ReadCompactString()
		} else {
			name, err = r.ReadString()
		}
		if err != nil {
			return err
		}
		t.Name = name

		readPartition := func() error {
			p, err := r.ReadInt32()
			if err != nil {
				return err
			}
			t.Partitions = append(t.Partitions, p)
			return nil
		}
		if flexible {
			if err := r.ReadCompactArray(readPartition); err != nil {
				return err
			}
		} else {
			if err := r.ReadArray(readPartition); err != nil {
				return err
			}
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
		if err := r.ReadCompactArray(readTopic); err != nil {
			return nil, err
		}
	} else {
		if err := r.ReadArray(readTopic); err != nil {
			return nil, err
		}
	}
	if flexible {
		if err := r.ReadTaggedFields(); err != nil {
			return nil, err
		}
	}
	return req, nil
}

func EncodeAddPartitionsToTxnResponse(w *codec.Writer, resp *AddPartitionsToTxnResponse, version int16) {
	flexible := version >= 3

	w.WriteInt32(resp.ThrottleTimeMs)

	writeTopic := func(t AddPartitionsToTxnTopicResult) {
		if flexible {
			w.WriteCompactString(t.Name)
		} else {
			w.WriteString(t.Name)
		}
		writePartition := func() {
			for _, pr := range t.PartitionResults {
				w.WriteInt32(pr.PartitionIndex)
				w.WriteInt16(pr.ErrorCode)
				if flexible {
					w.WriteEmptyTaggedFields()
				}
			}
		}
		if flexible {
			w.WriteCompactArray(len(t.PartitionResults), writePartition)
			w.WriteEmptyTaggedFields()
		} else {
			w.WriteArray(len(t.PartitionResults), writePartition)
		}
	}

	if flexible {
		w.WriteCompactArray(len(resp.Results), func() {
			for _, t := range resp.Results {
				writeTopic(t)
			}
		})
		w.WriteEmptyTaggedFields()
	} else {
		w.WriteArray(len(resp.Results), func() {
			for _, t := range resp.Results {
				writeTopic(t)
			}
		})
	}
}
