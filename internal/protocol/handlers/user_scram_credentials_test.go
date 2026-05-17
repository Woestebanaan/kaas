package handlers

import (
	"context"
	"testing"

	"github.com/woestebanaan/skafka/internal/auth"
	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
)

// fakeSCRAMStore is a tiny SCRAMCredentialStore for the describe path.
// Returns SHA-512 fixed-iteration credentials for the registered users.
type fakeSCRAMStore struct {
	users map[string]int // username → iterations
}

func (s *fakeSCRAMStore) LookupSCRAM(u string) ([]byte, []byte, []byte, int, bool) {
	if it, ok := s.users[u]; ok {
		return []byte("stored"), []byte("server"), []byte("salt"), it, true
	}
	return nil, nil, nil, 0, false
}
func (s *fakeSCRAMStore) ListAllSCRAMUsers() map[string]auth.SCRAMInfo {
	out := make(map[string]auth.SCRAMInfo, len(s.users))
	for u, it := range s.users {
		out[u] = auth.SCRAMInfo{Mechanism: "SCRAM-SHA-512", Iterations: it}
	}
	return out
}

// recordingSCRAMWriter captures Upsert / Delete calls for assertion.
type recordingSCRAMWriter struct {
	upserts []recordedUpsert
	deletes []string
	err     error
}

type recordedUpsert struct {
	username                    string
	salt, storedKey, serverKey  []byte
	iterations                  int
}

func (w *recordingSCRAMWriter) UpsertScramCredential(_ context.Context, username string, salt, storedKey, serverKey []byte, iterations int) error {
	w.upserts = append(w.upserts, recordedUpsert{username, salt, storedKey, serverKey, iterations})
	return w.err
}
func (w *recordingSCRAMWriter) DeleteScramCredential(_ context.Context, username string) error {
	w.deletes = append(w.deletes, username)
	return w.err
}

// TestDescribeUserScramCredentialsListsAllWhenUsersNull walks the
// "kafka-configs.sh --describe --entity-type users" all-users path:
// null user list → response carries every known SCRAM user with
// mechanism + iterations.
func TestDescribeUserScramCredentialsListsAllWhenUsersNull(t *testing.T) {
	store := &fakeSCRAMStore{users: map[string]int{"alice": 4096, "bob": 8192}}
	h := NewDescribeUserScramCredentialsHandler(store, allowAuth{})

	w := codec.NewWriter()
	w.WriteUvarint(0) // null compact array
	w.WriteEmptyTaggedFields()

	body, err := h.Handle(&connstate.ConnState{}, 0, w.Bytes())
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	r := codec.NewReader(body)
	_, _ = r.ReadInt32()
	if ec, _ := r.ReadInt16(); ec != 0 {
		t.Fatalf("top error=%d, want 0", ec)
	}
	_, _, _ = r.ReadCompactNullableString()
	users := map[string]int32{}
	r.ReadCompactArray(func() error {
		name, _ := r.ReadCompactString()
		ec, _ := r.ReadInt16()
		_, _, _ = r.ReadCompactNullableString()
		var iter int32
		r.ReadCompactArray(func() error {
			mech, _ := r.ReadInt8()
			it, _ := r.ReadInt32()
			if mech != 2 {
				t.Errorf("%s: mechanism=%d, want 2 (SHA-512)", name, mech)
			}
			iter = it
			r.ReadTaggedFields()
			return nil
		})
		r.ReadTaggedFields()
		if ec == 0 {
			users[name] = iter
		}
		return nil
	})
	if users["alice"] != 4096 || users["bob"] != 8192 {
		t.Errorf("users=%+v", users)
	}
}

// TestAlterUserScramCredentialsUpsertWritesCRWithDerivedKeys verifies
// the broker-side SCRAM derivation: given a salted_password, the
// handler computes (storedKey, serverKey) per RFC 5802 §3 and hands
// them to the writer alongside salt + iterations. The plaintext
// password never appears; the salted_password is not persisted.
func TestAlterUserScramCredentialsUpsertWritesCRWithDerivedKeys(t *testing.T) {
	writer := &recordingSCRAMWriter{}
	h := NewAlterUserScramCredentialsHandler(allowAuth{}).WithCRWriter(writer)

	salt := []byte{0x01, 0x02, 0x03, 0x04}
	saltedPw := []byte("64-byte salted password input goes here  padding......") // any non-empty bytes
	w := codec.NewWriter()
	w.WriteCompactArray(0, func() {}) // no deletions
	w.WriteCompactArray(1, func() {
		w.WriteCompactString("alice")
		w.WriteInt8(2)
		w.WriteInt32(8192)
		w.WriteCompactBytes(salt)
		w.WriteCompactBytes(saltedPw)
		w.WriteEmptyTaggedFields()
	})
	w.WriteEmptyTaggedFields()

	body, err := h.Handle(&connstate.ConnState{}, 0, w.Bytes())
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	r := codec.NewReader(body)
	_, _ = r.ReadInt32()
	r.ReadCompactArray(func() error {
		name, _ := r.ReadCompactString()
		ec, _ := r.ReadInt16()
		_, _, _ = r.ReadCompactNullableString()
		r.ReadTaggedFields()
		if name != "alice" || ec != 0 {
			t.Errorf("result=(%q,%d), want (alice,0)", name, ec)
		}
		return nil
	})

	if len(writer.upserts) != 1 {
		t.Fatalf("upserts=%+v", writer.upserts)
	}
	u := writer.upserts[0]
	if u.username != "alice" || u.iterations != 8192 {
		t.Errorf("upsert hdr=%+v", u)
	}
	// Both derived keys must be non-empty and DIFFERENT from each
	// other (HMAC with "Client Key" vs "Server Key"). And neither
	// should equal the salted password.
	if len(u.storedKey) == 0 || len(u.serverKey) == 0 {
		t.Fatalf("empty derived keys: stored=%x server=%x", u.storedKey, u.serverKey)
	}
	if string(u.storedKey) == string(u.serverKey) {
		t.Errorf("storedKey == serverKey — derivation collapsed")
	}
	if string(u.storedKey) == string(saltedPw) {
		t.Errorf("storedKey == saltedPassword — handler emitted raw wire bytes")
	}
}

// TestAlterUserScramCredentialsDeleteCallsWriter checks the deletion
// path: each deletion translates to one DeleteScramCredential call.
// Mixing deletions + upsertions in the same batch must hit both
// writer methods in order.
func TestAlterUserScramCredentialsDeleteCallsWriter(t *testing.T) {
	writer := &recordingSCRAMWriter{}
	h := NewAlterUserScramCredentialsHandler(allowAuth{}).WithCRWriter(writer)

	w := codec.NewWriter()
	w.WriteCompactArray(2, func() {
		w.WriteCompactString("alice")
		w.WriteInt8(2)
		w.WriteEmptyTaggedFields()
		w.WriteCompactString("bob")
		w.WriteInt8(2)
		w.WriteEmptyTaggedFields()
	})
	w.WriteCompactArray(0, func() {}) // no upsertions
	w.WriteEmptyTaggedFields()

	if _, err := h.Handle(&connstate.ConnState{}, 0, w.Bytes()); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(writer.deletes) != 2 || writer.deletes[0] != "alice" || writer.deletes[1] != "bob" {
		t.Errorf("deletes=%+v", writer.deletes)
	}
}

// TestAlterUserScramCredentialsRejectsSHA256 verifies that the
// SCRAM-SHA-256 mechanism (1) is rejected with
// UNSUPPORTED_SASL_MECHANISM. Skafka models SHA-512 only; surfacing
// the wire error gives the operator an actionable signal rather than
// silent acceptance.
func TestAlterUserScramCredentialsRejectsSHA256(t *testing.T) {
	writer := &recordingSCRAMWriter{}
	h := NewAlterUserScramCredentialsHandler(allowAuth{}).WithCRWriter(writer)

	w := codec.NewWriter()
	w.WriteCompactArray(0, func() {})
	w.WriteCompactArray(1, func() {
		w.WriteCompactString("alice")
		w.WriteInt8(1) // SHA-256
		w.WriteInt32(4096)
		w.WriteCompactBytes([]byte{0x01})
		w.WriteCompactBytes([]byte{0x02})
		w.WriteEmptyTaggedFields()
	})
	w.WriteEmptyTaggedFields()

	body, err := h.Handle(&connstate.ConnState{}, 0, w.Bytes())
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	r := codec.NewReader(body)
	_, _ = r.ReadInt32()
	r.ReadCompactArray(func() error {
		_, _ = r.ReadCompactString()
		ec, _ := r.ReadInt16()
		if ec != 33 { // UNSUPPORTED_SASL_MECHANISM
			t.Errorf("err=%d, want 33", ec)
		}
		_, _, _ = r.ReadCompactNullableString()
		r.ReadTaggedFields()
		return nil
	})
	if len(writer.upserts) != 0 {
		t.Errorf("writer received SHA-256 upsert: %+v", writer.upserts)
	}
}
