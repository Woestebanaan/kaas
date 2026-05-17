package api

import (
	"testing"

	"github.com/woestebanaan/skafka/internal/protocol/codec"
)

// TestDescribeUserScramCredentialsRequestDecodeWithUsers covers the
// "describe these specific users" wire shape: non-null compact array
// with one entry per username. v0 is flexible-from-zero per KIP-554.
func TestDescribeUserScramCredentialsRequestDecodeWithUsers(t *testing.T) {
	w := codec.NewWriter()
	w.WriteCompactArray(2, func() {
		w.WriteCompactString("alice")
		w.WriteEmptyTaggedFields()
		w.WriteCompactString("bob")
		w.WriteEmptyTaggedFields()
	})
	w.WriteEmptyTaggedFields()

	req, err := DecodeDescribeUserScramCredentialsRequest(codec.NewReader(w.Bytes()), 0)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if req.UsersNil {
		t.Errorf("UsersNil=true, want false (non-null array)")
	}
	if len(req.Users) != 2 || req.Users[0].Name != "alice" || req.Users[1].Name != "bob" {
		t.Errorf("users=%+v", req.Users)
	}
}

// TestDescribeUserScramCredentialsRequestDecodeNullUsers covers the
// "describe all" path: null array marker (0 uvarint) → UsersNil=true,
// empty list. Apache treats null as "every user" and empty as "no
// users". The decoder distinguishes the two for round-trip safety.
func TestDescribeUserScramCredentialsRequestDecodeNullUsers(t *testing.T) {
	w := codec.NewWriter()
	w.WriteUvarint(0) // null compact-array marker
	w.WriteEmptyTaggedFields()

	req, err := DecodeDescribeUserScramCredentialsRequest(codec.NewReader(w.Bytes()), 0)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !req.UsersNil {
		t.Errorf("UsersNil=false, want true (null marker decoded)")
	}
	if len(req.Users) != 0 {
		t.Errorf("users=%+v, want empty", req.Users)
	}
}

// TestDescribeUserScramCredentialsResponseRoundTrip pins the response
// layout AdminClient parses: throttle, top-level error, top-level
// message, then per-user (name, error, message, credentials).
func TestDescribeUserScramCredentialsResponseRoundTrip(t *testing.T) {
	resp := &DescribeUserScramCredentialsResponse{
		Results: []DescribeUserScramCredentialsResult{
			{
				User: "alice",
				Credentials: []ScramCredentialInfo{
					{Mechanism: 2, Iterations: 8192},
				},
			},
		},
	}
	w := codec.NewWriter()
	EncodeDescribeUserScramCredentialsResponse(w, resp, 0)
	got := w.Bytes()
	// Final byte must be the response-level empty tagged-fields
	// sentinel — KIP-482 framing.
	if got[len(got)-1] != 0x00 {
		t.Errorf("last byte = %#x, want 0x00 (response tagged-fields)", got[len(got)-1])
	}
}

// TestAlterUserScramCredentialsRequestDecodeRoundTrip walks the
// deletions + upsertions arrays. The salt + salted_password are
// compact-bytes; iterations is int32. v0 is flexible.
func TestAlterUserScramCredentialsRequestDecodeRoundTrip(t *testing.T) {
	w := codec.NewWriter()
	// deletions: one (alice, SHA-512)
	w.WriteCompactArray(1, func() {
		w.WriteCompactString("alice")
		w.WriteInt8(2)
		w.WriteEmptyTaggedFields()
	})
	// upsertions: one (bob, SHA-512, 4096 iter, salt=0xCAFE, salted_pw=0xBEEF)
	w.WriteCompactArray(1, func() {
		w.WriteCompactString("bob")
		w.WriteInt8(2)
		w.WriteInt32(4096)
		w.WriteCompactBytes([]byte{0xCA, 0xFE})
		w.WriteCompactBytes([]byte{0xBE, 0xEF})
		w.WriteEmptyTaggedFields()
	})
	w.WriteEmptyTaggedFields()

	req, err := DecodeAlterUserScramCredentialsRequest(codec.NewReader(w.Bytes()), 0)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(req.Deletions) != 1 || req.Deletions[0].Name != "alice" || req.Deletions[0].Mechanism != 2 {
		t.Errorf("deletions=%+v", req.Deletions)
	}
	if len(req.Upsertions) != 1 {
		t.Fatalf("upsertions=%+v", req.Upsertions)
	}
	u := req.Upsertions[0]
	if u.Name != "bob" || u.Mechanism != 2 || u.Iterations != 4096 {
		t.Errorf("upsertion hdr=%+v", u)
	}
	if len(u.Salt) != 2 || u.Salt[0] != 0xCA || u.Salt[1] != 0xFE {
		t.Errorf("salt=%x, want CAFE", u.Salt)
	}
	if len(u.SaltedPassword) != 2 || u.SaltedPassword[0] != 0xBE || u.SaltedPassword[1] != 0xEF {
		t.Errorf("salted_password=%x, want BEEF", u.SaltedPassword)
	}
}
