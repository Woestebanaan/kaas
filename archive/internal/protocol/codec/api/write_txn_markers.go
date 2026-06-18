package api

import "github.com/woestebanaan/skafka/internal/protocol/codec"

// WriteTxnMarkersRequest (API key 27, v0–v1). gh #114.
//
// Apache schema (clients/.../WriteTxnMarkersRequest.json):
//
//	Markers: "0+" []WritableTxnMarker
//	  ProducerId:       "0+" int64
//	  ProducerEpoch:    "0+" int16
//	  TransactionResult: "0+" bool (false=ABORT, true=COMMIT)
//	  Topics:           "0+" []WritableTxnMarkerTopic
//	    Name:             "0+" string
//	    PartitionIndexes: "0+" []int32
//	  CoordinatorEpoch: "0+" int32
//	flexibleVersions: "1+"
//
// Sent by the txn coordinator to each partition's leader to write
// the COMMIT/ABORT control batch. The receiving broker validates
// that it leads the partition, builds a control batch, and appends
// it via the storage engine.
type WriteTxnMarkersRequest struct {
	Markers []WritableTxnMarker
}

type WritableTxnMarker struct {
	ProducerID        int64
	ProducerEpoch     int16
	TransactionResult bool // false=ABORT, true=COMMIT
	Topics            []WritableTxnMarkerTopic
	CoordinatorEpoch  int32
}

type WritableTxnMarkerTopic struct {
	Name             string
	PartitionIndexes []int32
}

type WriteTxnMarkersResponse struct {
	Markers []WritableTxnMarkerResult
}

type WritableTxnMarkerResult struct {
	ProducerID int64
	Topics     []WritableTxnMarkerTopicResult
}

type WritableTxnMarkerTopicResult struct {
	Name       string
	Partitions []WritableTxnMarkerPartitionResult
}

type WritableTxnMarkerPartitionResult struct {
	PartitionIndex int32
	ErrorCode      int16
}

func DecodeWriteTxnMarkersRequest(r *codec.Reader, version int16) (*WriteTxnMarkersRequest, error) {
	flexible := version >= 1
	req := &WriteTxnMarkersRequest{}

	readStr := func() (string, error) {
		if flexible {
			return r.ReadCompactString()
		}
		return r.ReadString()
	}
	readArray := func(fn func() error) error {
		if flexible {
			return r.ReadCompactArray(fn)
		}
		return r.ReadArray(fn)
	}

	readMarker := func() error {
		var m WritableTxnMarker
		var err error
		m.ProducerID, err = r.ReadInt64()
		if err != nil {
			return err
		}
		m.ProducerEpoch, err = r.ReadInt16()
		if err != nil {
			return err
		}
		tr, err := r.ReadInt8()
		if err != nil {
			return err
		}
		m.TransactionResult = tr != 0
		readTopic := func() error {
			var t WritableTxnMarkerTopic
			name, err := readStr()
			if err != nil {
				return err
			}
			t.Name = name
			readPart := func() error {
				p, err := r.ReadInt32()
				if err != nil {
					return err
				}
				t.PartitionIndexes = append(t.PartitionIndexes, p)
				return nil
			}
			if err := readArray(readPart); err != nil {
				return err
			}
			if flexible {
				if err := r.ReadTaggedFields(); err != nil {
					return err
				}
			}
			m.Topics = append(m.Topics, t)
			return nil
		}
		if err := readArray(readTopic); err != nil {
			return err
		}
		m.CoordinatorEpoch, err = r.ReadInt32()
		if err != nil {
			return err
		}
		if flexible {
			if err := r.ReadTaggedFields(); err != nil {
				return err
			}
		}
		req.Markers = append(req.Markers, m)
		return nil
	}
	if err := readArray(readMarker); err != nil {
		return nil, err
	}
	if flexible {
		if err := r.ReadTaggedFields(); err != nil {
			return nil, err
		}
	}
	return req, nil
}

func EncodeWriteTxnMarkersResponse(w *codec.Writer, resp *WriteTxnMarkersResponse, version int16) {
	flexible := version >= 1
	writeStr := func(s string) {
		if flexible {
			w.WriteCompactString(s)
		} else {
			w.WriteString(s)
		}
	}
	writeArr := func(n int, fn func()) {
		if flexible {
			w.WriteCompactArray(n, fn)
		} else {
			w.WriteArray(n, fn)
		}
	}

	writeArr(len(resp.Markers), func() {
		for _, m := range resp.Markers {
			w.WriteInt64(m.ProducerID)
			writeArr(len(m.Topics), func() {
				for _, t := range m.Topics {
					writeStr(t.Name)
					writeArr(len(t.Partitions), func() {
						for _, p := range t.Partitions {
							w.WriteInt32(p.PartitionIndex)
							w.WriteInt16(p.ErrorCode)
							if flexible {
								w.WriteEmptyTaggedFields()
							}
						}
					})
					if flexible {
						w.WriteEmptyTaggedFields()
					}
				}
			})
			if flexible {
				w.WriteEmptyTaggedFields()
			}
		}
	})
	if flexible {
		w.WriteEmptyTaggedFields()
	}
}
