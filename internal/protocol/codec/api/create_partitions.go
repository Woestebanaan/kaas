package api

import "github.com/woestebanaan/skafka/internal/protocol/codec"

// CreatePartitionsRequest (API key 37, v0–v3). KIP-195: grow a
// topic's partition count at runtime via AdminClient.createPartitions
// or `kafka-topics.sh --alter --partitions N`. Skafka previously
// required editing the KafkaTopic CR directly; gh #52 wires the
// admin protocol path.
//
// Apache schema (clients/.../CreatePartitionsRequest.json):
//   - Topics:        "0+"
//   - TimeoutMs:     "0+"  int32
//   - ValidateOnly:  "0+"  bool
//   - flexibleVersions: "2+"
type CreatePartitionsRequest struct {
	Topics        []CreatePartitionsTopic
	TimeoutMs     int32
	ValidateOnly  bool
}

type CreatePartitionsTopic struct {
	Name         string
	Count        int32                          // new total partition count (NOT delta)
	Assignments  []CreatePartitionsAssignment   // per-new-partition broker assignment; empty = let the broker pick
}

type CreatePartitionsAssignment struct {
	BrokerIDs []int32
}

// CreatePartitionsResponse (v0–v3). Per-topic error surface; the
// outer ErrorCode (currently always 0) is reserved for cluster-wide
// failures the wire spec doesn't yet expose.
type CreatePartitionsResponse struct {
	ThrottleTimeMs int32
	Results        []CreatePartitionsResult
}

type CreatePartitionsResult struct {
	Name         string
	ErrorCode    int16
	ErrorMessage string // nullable
}

func DecodeCreatePartitionsRequest(r *codec.Reader, version int16) (*CreatePartitionsRequest, error) {
	flexible := version >= 2
	req := &CreatePartitionsRequest{}

	readTopic := func() error {
		var t CreatePartitionsTopic
		name, err := readString(r, flexible)
		if err != nil {
			return err
		}
		t.Name = name
		if t.Count, err = r.ReadInt32(); err != nil {
			return err
		}
		readAssignment := func() error {
			var a CreatePartitionsAssignment
			readBroker := func() error {
				id, err := r.ReadInt32()
				if err != nil {
					return err
				}
				a.BrokerIDs = append(a.BrokerIDs, id)
				return nil
			}
			if flexible {
				if err := r.ReadCompactArray(readBroker); err != nil {
					return err
				}
				if err := r.ReadTaggedFields(); err != nil {
					return err
				}
			} else {
				if err := r.ReadArray(readBroker); err != nil {
					return err
				}
			}
			t.Assignments = append(t.Assignments, a)
			return nil
		}
		// AssignmentsNull (flex/non-flex both): -1 / 0 → no assignments.
		if flexible {
			if err := r.ReadCompactArray(readAssignment); err != nil {
				return err
			}
			if err := r.ReadTaggedFields(); err != nil {
				return err
			}
		} else {
			if err := r.ReadArray(readAssignment); err != nil {
				return err
			}
		}
		req.Topics = append(req.Topics, t)
		return nil
	}

	var err error
	if flexible {
		err = r.ReadCompactArray(readTopic)
	} else {
		err = r.ReadArray(readTopic)
	}
	if err != nil {
		return nil, err
	}
	if req.TimeoutMs, err = r.ReadInt32(); err != nil {
		return nil, err
	}
	vo, err := r.ReadInt8()
	if err != nil {
		return nil, err
	}
	req.ValidateOnly = vo != 0
	if flexible {
		if err := r.ReadTaggedFields(); err != nil {
			return nil, err
		}
	}
	return req, nil
}

func EncodeCreatePartitionsResponse(w *codec.Writer, resp *CreatePartitionsResponse, version int16) {
	flexible := version >= 2
	w.WriteInt32(resp.ThrottleTimeMs)
	writeResults := func() {
		for _, r := range resp.Results {
			writeString(w, r.Name, flexible)
			w.WriteInt16(r.ErrorCode)
			if flexible {
				w.WriteCompactNullableString(r.ErrorMessage, r.ErrorMessage == "")
				w.WriteEmptyTaggedFields()
			} else {
				w.WriteNullableString(r.ErrorMessage, r.ErrorMessage == "")
			}
		}
	}
	if flexible {
		w.WriteCompactArray(len(resp.Results), writeResults)
		w.WriteEmptyTaggedFields()
	} else {
		w.WriteArray(len(resp.Results), writeResults)
	}
}
