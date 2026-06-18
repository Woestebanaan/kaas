package api

import "github.com/woestebanaan/skafka/internal/protocol/codec"

// InitProducerIdRequest (key 22, v0–v4). The Java client sends this on
// startup when enable.idempotence=true (the default since Kafka 3.0)
// to obtain a (producerId, epoch) pair used to tag every Produce batch
// for the lifetime of the producer.
//
// v3+ adds (ProducerID, ProducerEpoch) request fields per KIP-360 so a
// producer can ask the broker to renew its existing PID after a fatal
// error (instead of allocating a fresh one and risking duplicate
// records). skafka currently treats every InitProducerId as fresh —
// the request fields are accepted but ignored.
//
// Flexibility: v0/v1 use the legacy header (no tagged fields); v2+
// uses REQUEST_HEADER_V2 with tagged fields.
type InitProducerIdRequest struct {
	TransactionalID      string // v0+, nullable; "" + null=true ⇒ non-transactional producer
	TransactionTimeoutMs int32  // v0+
	ProducerID           int64  // v3+
	ProducerEpoch        int16  // v3+
}

// InitProducerIdResponse (key 22, v0–v4).
type InitProducerIdResponse struct {
	ThrottleTimeMs int32
	ErrorCode      int16
	ProducerID     int64
	ProducerEpoch  int16
}

func DecodeInitProducerIdRequest(r *codec.Reader, version int16) (*InitProducerIdRequest, error) {
	req := &InitProducerIdRequest{ProducerID: -1, ProducerEpoch: -1}
	flexible := version >= 2

	var err error
	req.TransactionalID, _, err = nullableString(r, flexible)
	if err != nil {
		return nil, err
	}
	if req.TransactionTimeoutMs, err = r.ReadInt32(); err != nil {
		return nil, err
	}
	if version >= 3 {
		if req.ProducerID, err = r.ReadInt64(); err != nil {
			return nil, err
		}
		if req.ProducerEpoch, err = r.ReadInt16(); err != nil {
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

func EncodeInitProducerIdResponse(w *codec.Writer, resp *InitProducerIdResponse, version int16) {
	flexible := version >= 2
	w.WriteInt32(resp.ThrottleTimeMs)
	w.WriteInt16(resp.ErrorCode)
	w.WriteInt64(resp.ProducerID)
	w.WriteInt16(resp.ProducerEpoch)
	if flexible {
		w.WriteEmptyTaggedFields()
	}
}
