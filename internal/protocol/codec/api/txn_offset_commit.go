package api

import "github.com/woestebanaan/skafka/internal/protocol/codec"

// TxnOffsetCommitRequest (API key 28, v0–v3). gh #27.
//
// Apache schema (clients/.../TxnOffsetCommitRequest.json). KIP-447
// added GenerationId/MemberId/GroupInstanceId at v3.
//
//	TransactionalId:    "0+" string
//	GroupId:            "0+" string
//	ProducerId:         "0+" int64
//	ProducerEpoch:      "0+" int16
//	GenerationId:       "3+" int32, default -1
//	MemberId:           "3+" string, default ""
//	GroupInstanceId:    "3+" nullable_string, default null
//	Topics:             "0+" []TxnOffsetCommitTopic
//	flexibleVersions:   "3+"
type TxnOffsetCommitRequest struct {
	TransactionalID string
	GroupID         string
	ProducerID      int64
	ProducerEpoch   int16
	GenerationID    int32   // v3+
	MemberID        string  // v3+
	GroupInstanceID *string // v3+, nullable
	Topics          []TxnOffsetCommitTopic
}

type TxnOffsetCommitTopic struct {
	Name       string
	Partitions []TxnOffsetCommitPartition
}

type TxnOffsetCommitPartition struct {
	PartitionIndex       int32
	CommittedOffset      int64
	CommittedLeaderEpoch int32 // v2+, default -1
	CommittedMetadata    *string
}

type TxnOffsetCommitResponse struct {
	ThrottleTimeMs int32
	Topics         []TxnOffsetCommitResponseTopic
}

type TxnOffsetCommitResponseTopic struct {
	Name       string
	Partitions []TxnOffsetCommitResponsePartition
}

type TxnOffsetCommitResponsePartition struct {
	PartitionIndex int32
	ErrorCode      int16
}

func DecodeTxnOffsetCommitRequest(r *codec.Reader, version int16) (*TxnOffsetCommitRequest, error) {
	flexible := version >= 3
	req := &TxnOffsetCommitRequest{GenerationID: -1}

	readStr := func() (string, error) {
		if flexible {
			return r.ReadCompactString()
		}
		return r.ReadString()
	}
	readNullableStr := func() (*string, error) {
		if flexible {
			s, isNull, err := r.ReadCompactNullableString()
			if err != nil {
				return nil, err
			}
			if isNull {
				return nil, nil
			}
			return &s, nil
		}
		s, isNull, err := r.ReadNullableString()
		if err != nil {
			return nil, err
		}
		if isNull {
			return nil, nil
		}
		return &s, nil
	}

	tid, err := readStr()
	if err != nil {
		return nil, err
	}
	req.TransactionalID = tid

	gid, err := readStr()
	if err != nil {
		return nil, err
	}
	req.GroupID = gid

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

	if version >= 3 {
		gen, err := r.ReadInt32()
		if err != nil {
			return nil, err
		}
		req.GenerationID = gen

		mid, err := readStr()
		if err != nil {
			return nil, err
		}
		req.MemberID = mid

		gins, err := readNullableStr()
		if err != nil {
			return nil, err
		}
		req.GroupInstanceID = gins
	}

	readPartition := func() (TxnOffsetCommitPartition, error) {
		var p TxnOffsetCommitPartition
		p.CommittedLeaderEpoch = -1
		var err error
		p.PartitionIndex, err = r.ReadInt32()
		if err != nil {
			return p, err
		}
		p.CommittedOffset, err = r.ReadInt64()
		if err != nil {
			return p, err
		}
		if version >= 2 {
			p.CommittedLeaderEpoch, err = r.ReadInt32()
			if err != nil {
				return p, err
			}
		}
		md, err := readNullableStr()
		if err != nil {
			return p, err
		}
		p.CommittedMetadata = md
		if flexible {
			if err := r.ReadTaggedFields(); err != nil {
				return p, err
			}
		}
		return p, nil
	}

	readTopic := func() error {
		var t TxnOffsetCommitTopic
		name, err := readStr()
		if err != nil {
			return err
		}
		t.Name = name
		readPart := func() error {
			p, err := readPartition()
			if err != nil {
				return err
			}
			t.Partitions = append(t.Partitions, p)
			return nil
		}
		if flexible {
			if err := r.ReadCompactArray(readPart); err != nil {
				return err
			}
		} else {
			if err := r.ReadArray(readPart); err != nil {
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

func EncodeTxnOffsetCommitResponse(w *codec.Writer, resp *TxnOffsetCommitResponse, version int16) {
	flexible := version >= 3
	w.WriteInt32(resp.ThrottleTimeMs)

	writeTopic := func(t TxnOffsetCommitResponseTopic) {
		if flexible {
			w.WriteCompactString(t.Name)
		} else {
			w.WriteString(t.Name)
		}
		writeParts := func() {
			for _, p := range t.Partitions {
				w.WriteInt32(p.PartitionIndex)
				w.WriteInt16(p.ErrorCode)
				if flexible {
					w.WriteEmptyTaggedFields()
				}
			}
		}
		if flexible {
			w.WriteCompactArray(len(t.Partitions), writeParts)
			w.WriteEmptyTaggedFields()
		} else {
			w.WriteArray(len(t.Partitions), writeParts)
		}
	}

	if flexible {
		w.WriteCompactArray(len(resp.Topics), func() {
			for _, t := range resp.Topics {
				writeTopic(t)
			}
		})
		w.WriteEmptyTaggedFields()
	} else {
		w.WriteArray(len(resp.Topics), func() {
			for _, t := range resp.Topics {
				writeTopic(t)
			}
		})
	}
}
