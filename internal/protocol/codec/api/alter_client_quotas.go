package api

import "github.com/woestebanaan/skafka/internal/protocol/codec"

// AlterClientQuotasRequest (key 49, v0–v1). KIP-546 dynamic quota
// mutation: backs `kafka-configs.sh --alter --entity-type users` and
// AdminClient.alterClientQuotas(). v1 is flexible (KIP-482).
//
// Each entry is one (entity, list-of-ops) tuple. Each op is one
// (key, value, remove) — Apache uses Remove=true to unset a key
// (revert to default/inherit), Remove=false to set Value.
type AlterClientQuotasRequest struct {
	Entries      []AlterQuotaEntry
	ValidateOnly bool // dry-run: no state change, just per-op validation
}

type AlterQuotaEntry struct {
	Entity []QuotaEntity
	Ops    []AlterQuotaOp
}

type AlterQuotaOp struct {
	Key    string
	Value  float64
	Remove bool
}

// AlterClientQuotasResponse (key 49, v0–v1). One per-entry result;
// per-op results are NOT individually reported in v0/v1 (a single
// error fails the whole entry).
type AlterClientQuotasResponse struct {
	ThrottleTimeMs int32
	Entries        []AlterQuotaEntryResponse
}

type AlterQuotaEntryResponse struct {
	ErrorCode    int16
	ErrorMessage string
	Entity       []QuotaEntity
}

func DecodeAlterClientQuotasRequest(r *codec.Reader, version int16) (*AlterClientQuotasRequest, error) {
	req := &AlterClientQuotasRequest{}
	flexible := version >= 1

	readEntry := func() error {
		var e AlterQuotaEntry
		readEntity := func() error {
			var en QuotaEntity
			t, err := readString(r, flexible)
			if err != nil {
				return err
			}
			en.Type = t
			n, _, err := nullableString(r, flexible)
			if err != nil {
				return err
			}
			en.Name = n
			if flexible {
				if err := r.ReadTaggedFields(); err != nil {
					return err
				}
			}
			e.Entity = append(e.Entity, en)
			return nil
		}
		var err error
		if flexible {
			err = r.ReadCompactArray(readEntity)
		} else {
			err = r.ReadArray(readEntity)
		}
		if err != nil {
			return err
		}
		readOp := func() error {
			var op AlterQuotaOp
			k, err := readString(r, flexible)
			if err != nil {
				return err
			}
			op.Key = k
			v, err := r.ReadFloat64()
			if err != nil {
				return err
			}
			op.Value = v
			rb, err := r.ReadInt8()
			if err != nil {
				return err
			}
			op.Remove = rb != 0
			if flexible {
				if err := r.ReadTaggedFields(); err != nil {
					return err
				}
			}
			e.Ops = append(e.Ops, op)
			return nil
		}
		if flexible {
			err = r.ReadCompactArray(readOp)
		} else {
			err = r.ReadArray(readOp)
		}
		if err != nil {
			return err
		}
		if flexible {
			if err := r.ReadTaggedFields(); err != nil {
				return err
			}
		}
		req.Entries = append(req.Entries, e)
		return nil
	}
	var err error
	if flexible {
		err = r.ReadCompactArray(readEntry)
	} else {
		err = r.ReadArray(readEntry)
	}
	if err != nil {
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

func EncodeAlterClientQuotasResponse(w *codec.Writer, resp *AlterClientQuotasResponse, version int16) {
	flexible := version >= 1
	w.WriteInt32(resp.ThrottleTimeMs)
	writeEntries := func() {
		for _, e := range resp.Entries {
			w.WriteInt16(e.ErrorCode)
			writeNullableString(w, e.ErrorMessage, flexible)
			writeEntity := func() {
				for _, en := range e.Entity {
					writeString(w, en.Type, flexible)
					writeNullableString(w, en.Name, flexible)
					if flexible {
						w.WriteEmptyTaggedFields()
					}
				}
			}
			if flexible {
				w.WriteCompactArray(len(e.Entity), writeEntity)
				w.WriteEmptyTaggedFields()
			} else {
				w.WriteArray(len(e.Entity), writeEntity)
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
