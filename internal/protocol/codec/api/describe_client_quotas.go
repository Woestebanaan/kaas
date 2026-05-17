package api

import "github.com/woestebanaan/skafka/internal/protocol/codec"

// DescribeClientQuotasRequest (key 48, v0–v1). KIP-546 added in Apache
// 2.6; backs `kafka-configs.sh --describe --entity-type users` and
// AdminClient.describeClientQuotas(). v1 makes the wire flexible
// (KIP-482 tagged-fields); v0 stays non-flexible.
//
// Components is a list of filter constraints applied conjunctively
// (when Strict is true) or disjunctively (when false). The handler
// implements the user-entity slice today; client-id and ip entities
// return empty results.
type DescribeClientQuotasRequest struct {
	Components []QuotaComponent
	Strict     bool
}

// QuotaComponent filters entries by (entity_type, match_type, match).
// match_type: 0=exact (match against MatchName), 1=default (only
// default values), 2=any (any value). Apache constants:
//
//	MATCH_TYPE_EXACT   = 0
//	MATCH_TYPE_DEFAULT = 1
//	MATCH_TYPE_ANY     = 2
type QuotaComponent struct {
	EntityType string
	MatchType  int8
	MatchName  string // empty when MatchType != EXACT (encoded as null)
}

// DescribeClientQuotasResponse (key 48, v0–v1).
type DescribeClientQuotasResponse struct {
	ThrottleTimeMs int32
	ErrorCode      int16
	ErrorMessage   string
	Entries        []QuotaEntry
}

type QuotaEntry struct {
	Entity []QuotaEntity
	Values []QuotaValue
}

type QuotaEntity struct {
	Type string
	Name string // empty == null on the wire (default entity)
}

type QuotaValue struct {
	Key   string  // e.g. "producer_byte_rate"
	Value float64 // bytes/sec (or request percentage, depending on Key)
}

func DecodeDescribeClientQuotasRequest(r *codec.Reader, version int16) (*DescribeClientQuotasRequest, error) {
	req := &DescribeClientQuotasRequest{}
	flexible := version >= 1

	readComp := func() error {
		var c QuotaComponent
		etype, err := readString(r, flexible)
		if err != nil {
			return err
		}
		c.EntityType = etype
		mt, err := r.ReadInt8()
		if err != nil {
			return err
		}
		c.MatchType = mt
		mn, _, err := nullableString(r, flexible)
		if err != nil {
			return err
		}
		c.MatchName = mn
		if flexible {
			if err := r.ReadTaggedFields(); err != nil {
				return err
			}
		}
		req.Components = append(req.Components, c)
		return nil
	}
	var err error
	if flexible {
		err = r.ReadCompactArray(readComp)
	} else {
		err = r.ReadArray(readComp)
	}
	if err != nil {
		return nil, err
	}
	strict, err := r.ReadInt8()
	if err != nil {
		return nil, err
	}
	req.Strict = strict != 0
	if flexible {
		if err := r.ReadTaggedFields(); err != nil {
			return nil, err
		}
	}
	return req, nil
}

func EncodeDescribeClientQuotasResponse(w *codec.Writer, resp *DescribeClientQuotasResponse, version int16) {
	flexible := version >= 1
	w.WriteInt32(resp.ThrottleTimeMs)
	w.WriteInt16(resp.ErrorCode)
	writeNullableString(w, resp.ErrorMessage, flexible)

	writeEntries := func() {
		for _, e := range resp.Entries {
			writeEntity := func() {
				for _, en := range e.Entity {
					writeString(w, en.Type, flexible)
					// EntityName is nullable; empty string serialises as null
					// (== the "default entity" wire encoding).
					writeNullableString(w, en.Name, flexible)
					if flexible {
						w.WriteEmptyTaggedFields()
					}
				}
			}
			if flexible {
				w.WriteCompactArray(len(e.Entity), writeEntity)
			} else {
				w.WriteArray(len(e.Entity), writeEntity)
			}
			writeValues := func() {
				for _, v := range e.Values {
					writeString(w, v.Key, flexible)
					w.WriteFloat64(v.Value)
					if flexible {
						w.WriteEmptyTaggedFields()
					}
				}
			}
			if flexible {
				w.WriteCompactArray(len(e.Values), writeValues)
			} else {
				w.WriteArray(len(e.Values), writeValues)
			}
			if flexible {
				w.WriteEmptyTaggedFields()
			}
		}
	}
	if flexible {
		w.WriteCompactArray(len(resp.Entries), writeEntries)
		w.WriteEmptyTaggedFields()
	} else {
		w.WriteArray(len(resp.Entries), writeEntries)
	}
}
