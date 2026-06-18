package api

import "github.com/woestebanaan/skafka/internal/protocol/codec"

// EndTxnRequest (API key 26, v0–v3). Apache schema
// (clients/.../EndTxnRequest.json):
//
//	TransactionalId: "0+" string
//	ProducerId:      "0+" int64
//	ProducerEpoch:   "0+" int16
//	Committed:       "0+" bool
//	flexibleVersions: "3+"
type EndTxnRequest struct {
	TransactionalID string
	ProducerID      int64
	ProducerEpoch   int16
	Committed       bool
}

// EndTxnResponse (v0–v3).
//
//	ThrottleTimeMs: "0+" int32
//	ErrorCode:      "0+" int16
//	flexibleVersions: "3+"
type EndTxnResponse struct {
	ThrottleTimeMs int32
	ErrorCode      int16
}

func DecodeEndTxnRequest(r *codec.Reader, version int16) (*EndTxnRequest, error) {
	flexible := version >= 3
	req := &EndTxnRequest{}

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

	committed, err := r.ReadInt8()
	if err != nil {
		return nil, err
	}
	req.Committed = committed != 0

	if flexible {
		if err := r.ReadTaggedFields(); err != nil {
			return nil, err
		}
	}
	return req, nil
}

func EncodeEndTxnResponse(w *codec.Writer, resp *EndTxnResponse, version int16) {
	flexible := version >= 3
	w.WriteInt32(resp.ThrottleTimeMs)
	w.WriteInt16(resp.ErrorCode)
	if flexible {
		w.WriteEmptyTaggedFields()
	}
}
