package api

import "github.com/woestebanaan/skafka/internal/protocol/codec"

// AddOffsetsToTxnRequest (API key 25, v0–v3). gh #24.
//
// Apache schema (clients/.../AddOffsetsToTxnRequest.json):
//
//	TransactionalId: "0+" string
//	ProducerId:      "0+" int64
//	ProducerEpoch:   "0+" int16
//	GroupId:         "0+" string
//	flexibleVersions: "3+"
//
// Sent by a transactional producer before TxnOffsetCommit to tell
// the txn coordinator "I'm going to commit offsets for consumer
// group G as part of this transaction". The coordinator records the
// group association so EndTxn can sweep pending offsets on
// commit/abort.
type AddOffsetsToTxnRequest struct {
	TransactionalID string
	ProducerID      int64
	ProducerEpoch   int16
	GroupID         string
}

type AddOffsetsToTxnResponse struct {
	ThrottleTimeMs int32
	ErrorCode      int16
}

func DecodeAddOffsetsToTxnRequest(r *codec.Reader, version int16) (*AddOffsetsToTxnRequest, error) {
	flexible := version >= 3
	req := &AddOffsetsToTxnRequest{}

	read := func() (string, error) {
		if flexible {
			return r.ReadCompactString()
		}
		return r.ReadString()
	}

	tid, err := read()
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

	gid, err := read()
	if err != nil {
		return nil, err
	}
	req.GroupID = gid

	if flexible {
		if err := r.ReadTaggedFields(); err != nil {
			return nil, err
		}
	}
	return req, nil
}

func EncodeAddOffsetsToTxnResponse(w *codec.Writer, resp *AddOffsetsToTxnResponse, version int16) {
	flexible := version >= 3
	w.WriteInt32(resp.ThrottleTimeMs)
	w.WriteInt16(resp.ErrorCode)
	if flexible {
		w.WriteEmptyTaggedFields()
	}
}
