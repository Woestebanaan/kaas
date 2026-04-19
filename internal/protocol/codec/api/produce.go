package api

import "github.com/woestebanaan/skafka/internal/protocol/codec"

// ProduceRequest (key 0, v3–v9).
type ProduceRequest struct {
	TransactionalID string  // v3+, nullable
	Acks            int16   // -1=all, 0=none, 1=leader
	TimeoutMs       int32
	TopicData       []ProduceTopicData
}

type ProduceTopicData struct {
	Name           string
	PartitionData  []ProducePartitionData
}

type ProducePartitionData struct {
	Index   int32
	Records []byte // raw RecordBatch bytes
}

// ProduceResponse (key 0, v3–v9).
type ProduceResponse struct {
	Responses    []ProduceTopicResponse
	ThrottleTime int32 // v1+
}

type ProduceTopicResponse struct {
	Name               string
	PartitionResponses []ProducePartitionResponse
}

type ProducePartitionResponse struct {
	Index          int32
	ErrorCode      int16
	BaseOffset     int64
	LogAppendTime  int64 // v2+; -1 if not available
	LogStartOffset int64 // v5+
}

func DecodeProduceRequest(r *codec.Reader, version int16) (*ProduceRequest, error) {
	req := &ProduceRequest{}
	flexible := version >= 9

	var err error
	if version >= 3 {
		var null bool
		req.TransactionalID, null, err = nullableString(r, flexible)
		if err != nil {
			return nil, err
		}
		if null {
			req.TransactionalID = ""
		}
	}

	if req.Acks, err = r.ReadInt16(); err != nil {
		return nil, err
	}
	if req.TimeoutMs, err = r.ReadInt32(); err != nil {
		return nil, err
	}

	readTopics := func() error {
		var td ProduceTopicData
		var err error
		if td.Name, err = readString(r, flexible); err != nil {
			return err
		}
		readPartitions := func() error {
			var pd ProducePartitionData
			var err error
			if pd.Index, err = r.ReadInt32(); err != nil {
				return err
			}
			if flexible {
				pd.Records, err = r.ReadCompactNullableBytes()
			} else {
				pd.Records, err = r.ReadNullableBytes()
			}
			if err != nil {
				return err
			}
			if flexible {
				if err := r.ReadTaggedFields(); err != nil {
					return err
				}
			}
			td.PartitionData = append(td.PartitionData, pd)
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
		req.TopicData = append(req.TopicData, td)
		return nil
	}

	if flexible {
		if err := r.ReadCompactArray(readTopics); err != nil {
			return nil, err
		}
		return req, r.ReadTaggedFields()
	}
	return req, r.ReadArray(readTopics)
}

func EncodeProduceResponse(w *codec.Writer, resp *ProduceResponse, version int16) {
	flexible := version >= 9

	writeResponses := func() {
		for _, t := range resp.Responses {
			writeString(w, t.Name, flexible)
			writePartitions := func() {
				for _, p := range t.PartitionResponses {
					w.WriteInt32(p.Index)
					w.WriteInt16(p.ErrorCode)
					w.WriteInt64(p.BaseOffset)
					if version >= 2 {
						w.WriteInt64(p.LogAppendTime)
					}
					if version >= 5 {
						w.WriteInt64(p.LogStartOffset)
					}
					if version >= 8 {
						// ErrorRecords: empty compact array (no per-record errors).
						if flexible {
							w.WriteCompactArray(0, func() {})
						} else {
							w.WriteArray(0, func() {})
						}
						// ErrorMessage: null.
						if flexible {
							w.WriteCompactNullableString("", true)
						} else {
							w.WriteNullableString("", true)
						}
					}
					if flexible {
						w.WriteEmptyTaggedFields()
					}
				}
			}
			if flexible {
				w.WriteCompactArray(len(t.PartitionResponses), writePartitions)
			} else {
				w.WriteArray(len(t.PartitionResponses), writePartitions)
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

	if version >= 1 {
		w.WriteInt32(resp.ThrottleTime)
	}
	if flexible {
		w.WriteEmptyTaggedFields()
	}
}

// helpers shared across API codecs

func readString(r *codec.Reader, flexible bool) (string, error) {
	if flexible {
		return r.ReadCompactString()
	}
	return r.ReadString()
}

func nullableString(r *codec.Reader, flexible bool) (string, bool, error) {
	if flexible {
		return r.ReadCompactNullableString()
	}
	return r.ReadNullableString()
}

func writeString(w *codec.Writer, s string, flexible bool) {
	if flexible {
		w.WriteCompactString(s)
	} else {
		w.WriteString(s)
	}
}
