package api

import (
	"testing"

	"github.com/woestebanaan/skafka/internal/protocol/codec"
)

// TestDescribeClientQuotasRequestDecodeV0 covers the non-flexible
// wire shape Apache 3.7 negotiates for v0:
//
//	components: int32-array of:
//	  entity_type: int16-string
//	  match_type:  int8
//	  match:       nullable int16-string
//	strict: int8 (0/1)
func TestDescribeClientQuotasRequestDecodeV0(t *testing.T) {
	w := codec.NewWriter()
	w.WriteArray(1, func() {
		w.WriteString("user")
		w.WriteInt8(0) // exact
		w.WriteNullableString("alice", false)
	})
	w.WriteInt8(1) // strict=true

	req, err := DecodeDescribeClientQuotasRequest(codec.NewReader(w.Bytes()), 0)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(req.Components) != 1 {
		t.Fatalf("components=%d, want 1", len(req.Components))
	}
	c := req.Components[0]
	if c.EntityType != "user" || c.MatchType != 0 || c.MatchName != "alice" {
		t.Errorf("component=%+v, want {user,0,alice}", c)
	}
	if !req.Strict {
		t.Errorf("strict=false, want true")
	}
}

// TestDescribeClientQuotasResponseRoundTripV0 pins the v0 layout that
// AdminClient expects. A regression that flips error_code vs throttle
// or drops a field would crash the Java decoder mid-record.
func TestDescribeClientQuotasResponseRoundTripV0(t *testing.T) {
	pProd := float64(1048576)
	pCons := float64(2097152)
	resp := &DescribeClientQuotasResponse{
		Entries: []QuotaEntry{
			{
				Entity: []QuotaEntity{{Type: "user", Name: "alice"}},
				Values: []QuotaValue{
					{Key: "producer_byte_rate", Value: pProd},
					{Key: "consumer_byte_rate", Value: pCons},
				},
			},
		},
	}
	w := codec.NewWriter()
	EncodeDescribeClientQuotasResponse(w, resp, 0)
	r := codec.NewReader(w.Bytes())

	tt, _ := r.ReadInt32()
	ec, _ := r.ReadInt16()
	em, _, _ := r.ReadNullableString()
	if tt != 0 || ec != 0 || em != "" {
		t.Errorf("(throttle, err, msg)=(%d, %d, %q), want (0,0,\"\")", tt, ec, em)
	}
	var entries int
	if err := r.ReadArray(func() error {
		entries++
		var entityN, valuesN int
		if err := r.ReadArray(func() error {
			entityN++
			et, _ := r.ReadString()
			en, _, _ := r.ReadNullableString()
			if et != "user" || en != "alice" {
				t.Errorf("entity=(%q,%q), want (user,alice)", et, en)
			}
			return nil
		}); err != nil {
			return err
		}
		if err := r.ReadArray(func() error {
			valuesN++
			k, _ := r.ReadString()
			v, _ := r.ReadFloat64()
			switch valuesN {
			case 1:
				if k != "producer_byte_rate" || v != pProd {
					t.Errorf("value 1=(%q,%v), want (producer_byte_rate,%v)", k, v, pProd)
				}
			case 2:
				if k != "consumer_byte_rate" || v != pCons {
					t.Errorf("value 2=(%q,%v), want (consumer_byte_rate,%v)", k, v, pCons)
				}
			}
			return nil
		}); err != nil {
			return err
		}
		if entityN != 1 || valuesN != 2 {
			t.Errorf("entry shape: entities=%d values=%d, want (1, 2)", entityN, valuesN)
		}
		return nil
	}); err != nil {
		t.Fatalf("read entries: %v", err)
	}
	if entries != 1 {
		t.Errorf("entries=%d, want 1", entries)
	}
}

// TestAlterClientQuotasRequestDecodeV0 verifies the request walker
// (entity → ops, with the Remove flag).
func TestAlterClientQuotasRequestDecodeV0(t *testing.T) {
	w := codec.NewWriter()
	w.WriteArray(1, func() {
		// entity = [{user, bob}]
		w.WriteArray(1, func() {
			w.WriteString("user")
			w.WriteNullableString("bob", false)
		})
		// ops = [(producer_byte_rate, 2.0e6, false), (consumer_byte_rate, 0, true)]
		w.WriteArray(2, func() {
			w.WriteString("producer_byte_rate")
			w.WriteFloat64(2e6)
			w.WriteInt8(0)
			w.WriteString("consumer_byte_rate")
			w.WriteFloat64(0)
			w.WriteInt8(1) // remove
		})
	})
	w.WriteInt8(0) // validate_only=false

	req, err := DecodeAlterClientQuotasRequest(codec.NewReader(w.Bytes()), 0)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(req.Entries) != 1 || len(req.Entries[0].Ops) != 2 {
		t.Fatalf("entries=%+v", req.Entries)
	}
	e := req.Entries[0]
	if e.Entity[0].Type != "user" || e.Entity[0].Name != "bob" {
		t.Errorf("entity=%+v", e.Entity)
	}
	if e.Ops[0].Key != "producer_byte_rate" || e.Ops[0].Value != 2e6 || e.Ops[0].Remove {
		t.Errorf("op 0=%+v", e.Ops[0])
	}
	if e.Ops[1].Key != "consumer_byte_rate" || !e.Ops[1].Remove {
		t.Errorf("op 1=%+v", e.Ops[1])
	}
}

// TestAlterClientQuotasResponseRoundTripV1 covers the flexible
// encoding path: compact arrays + tagged-fields per entity + tagged-
// fields per entry + top-level tagged-fields.
func TestAlterClientQuotasResponseRoundTripV1(t *testing.T) {
	resp := &AlterClientQuotasResponse{
		Entries: []AlterQuotaEntryResponse{
			{
				ErrorCode: 0,
				Entity:    []QuotaEntity{{Type: "user", Name: "bob"}},
			},
		},
	}
	w := codec.NewWriter()
	EncodeAlterClientQuotasResponse(w, resp, 1)
	got := w.Bytes()
	// v1 last byte is the response-level empty tagged-fields sentinel.
	if got[len(got)-1] != 0x00 {
		t.Errorf("v1 last byte = %#x, want 0x00 (response tagged-fields)", got[len(got)-1])
	}
}
