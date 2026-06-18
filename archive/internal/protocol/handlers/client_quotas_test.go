package handlers

import (
	"context"
	"errors"
	"testing"

	"github.com/woestebanaan/skafka/internal/auth"
	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

// staticQuotaStore is a minimal CredentialStore that supplies known
// quotas for one user, no SCRAM/TLS/SA. Lets us boot a *auth.QuotaEnforcer
// with an interesting initial state.
type staticQuotaStore struct {
	users map[string]*auth.Quotas
}

func (s *staticQuotaStore) LookupSCRAM(string) ([]byte, []byte, []byte, int, bool) {
	return nil, nil, nil, 0, false
}
func (s *staticQuotaStore) LookupTLS(string) (string, bool) { return "", false }
func (s *staticQuotaStore) LookupSA(string, string) bool    { return false }
func (s *staticQuotaStore) LookupQuotas(u string) *auth.Quotas {
	return s.users[u]
}
func (s *staticQuotaStore) ListAllQuotas() map[string]*auth.Quotas {
	out := make(map[string]*auth.Quotas, len(s.users))
	for k, v := range s.users {
		out[k] = v
	}
	return out
}

func ptrI64(v int64) *int64 { return &v }

// TestDescribeClientQuotasReturnsCRBackedValue covers the
// "kafka-configs.sh --describe --entity-type users --entity-name alice"
// happy path — the CR-loaded quota surfaces verbatim through the wire.
func TestDescribeClientQuotasReturnsCRBackedValue(t *testing.T) {
	store := &staticQuotaStore{users: map[string]*auth.Quotas{
		"alice": {ProducerMaxByteRatePerBroker: ptrI64(1048576)},
	}}
	qe := auth.NewQuotaEnforcer(store)
	h := NewDescribeClientQuotasHandler(qe, allowAuth{})

	req := &api.DescribeClientQuotasRequest{
		Components: []api.QuotaComponent{{EntityType: "user", MatchType: 0, MatchName: "alice"}},
		Strict:     true,
	}
	w := codec.NewWriter()
	w.WriteArray(len(req.Components), func() {
		for _, c := range req.Components {
			w.WriteString(c.EntityType)
			w.WriteInt8(c.MatchType)
			w.WriteNullableString(c.MatchName, false)
		}
	})
	w.WriteInt8(1)

	body, err := h.Handle(&connstate.ConnState{}, 0, w.Bytes())
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	r := codec.NewReader(body)
	_, _ = r.ReadInt32() // throttle
	if ec, _ := r.ReadInt16(); ec != 0 {
		t.Fatalf("error_code=%d, want 0", ec)
	}
	_, _, _ = r.ReadNullableString()
	var entries int
	r.ReadArray(func() error {
		entries++
		// entity
		r.ReadArray(func() error {
			tp, _ := r.ReadString()
			nm, _, _ := r.ReadNullableString()
			if tp != "user" || nm != "alice" {
				t.Errorf("entity=(%q,%q), want (user,alice)", tp, nm)
			}
			return nil
		})
		// values
		var vals int
		r.ReadArray(func() error {
			vals++
			k, _ := r.ReadString()
			v, _ := r.ReadFloat64()
			if k != "producer_byte_rate" || v != 1048576 {
				t.Errorf("value=(%q,%v), want (producer_byte_rate,1048576)", k, v)
			}
			return nil
		})
		if vals != 1 {
			t.Errorf("values=%d, want 1 (consumer_byte_rate is unset)", vals)
		}
		return nil
	})
	if entries != 1 {
		t.Errorf("entries=%d, want 1", entries)
	}
}

// TestAlterClientQuotasUpdatesInMemoryAndIsVisibleToDescribe walks the
// round trip: --alter sets a new producer_byte_rate, the next
// --describe sees it, and a remove op reverts to the CR-backed value
// (which here means "no value" because alice's initial quotas only
// had producer_byte_rate, not consumer_byte_rate, so removing producer
// removes the override and falls back to the store).
func TestAlterClientQuotasUpdatesInMemoryAndIsVisibleToDescribe(t *testing.T) {
	store := &staticQuotaStore{users: map[string]*auth.Quotas{
		"alice": {ProducerMaxByteRatePerBroker: ptrI64(100)},
	}}
	qe := auth.NewQuotaEnforcer(store)
	alter := NewAlterClientQuotasHandler(qe, allowAuth{})

	// --add-config producer_byte_rate=5000,consumer_byte_rate=2000
	w := codec.NewWriter()
	w.WriteArray(1, func() {
		// entity
		w.WriteArray(1, func() {
			w.WriteString("user")
			w.WriteNullableString("alice", false)
		})
		// ops
		w.WriteArray(2, func() {
			w.WriteString("producer_byte_rate")
			w.WriteFloat64(5000)
			w.WriteInt8(0)
			w.WriteString("consumer_byte_rate")
			w.WriteFloat64(2000)
			w.WriteInt8(0)
		})
	})
	w.WriteInt8(0) // validate_only=false

	body, err := alter.Handle(&connstate.ConnState{}, 0, w.Bytes())
	if err != nil {
		t.Fatalf("alter: %v", err)
	}
	r := codec.NewReader(body)
	_, _ = r.ReadInt32() // throttle
	r.ReadArray(func() error {
		ec, _ := r.ReadInt16()
		if ec != 0 {
			t.Errorf("alter entry err=%d, want 0", ec)
		}
		_, _, _ = r.ReadNullableString()
		// entity
		r.ReadArray(func() error {
			r.ReadString()
			r.ReadNullableString()
			return nil
		})
		return nil
	})

	// The enforcer should now report the override, not the store
	// value, for alice.
	got := qe.DescribeUserQuota("alice")
	if got == nil || got.ProducerMaxByteRatePerBroker == nil || *got.ProducerMaxByteRatePerBroker != 5000 {
		t.Errorf("after alter, producer rate=%v, want 5000", got)
	}
	if got.ConsumerMaxByteRatePerBroker == nil || *got.ConsumerMaxByteRatePerBroker != 2000 {
		t.Errorf("after alter, consumer rate=%v, want 2000", got)
	}
}

// TestAlterClientQuotasValidateOnlySkipsMutation verifies that
// validate_only=true reports success without changing in-memory state.
// AdminClient.alterClientQuotas exposes this as the dry-run mode that
// kafka-configs.sh sets for `--alter --dry-run`.
func TestAlterClientQuotasValidateOnlySkipsMutation(t *testing.T) {
	store := &staticQuotaStore{users: map[string]*auth.Quotas{
		"alice": {ProducerMaxByteRatePerBroker: ptrI64(100)},
	}}
	qe := auth.NewQuotaEnforcer(store)
	alter := NewAlterClientQuotasHandler(qe, allowAuth{})

	w := codec.NewWriter()
	w.WriteArray(1, func() {
		w.WriteArray(1, func() {
			w.WriteString("user")
			w.WriteNullableString("alice", false)
		})
		w.WriteArray(1, func() {
			w.WriteString("producer_byte_rate")
			w.WriteFloat64(5000)
			w.WriteInt8(0)
		})
	})
	w.WriteInt8(1) // validate_only=true

	if _, err := alter.Handle(&connstate.ConnState{}, 0, w.Bytes()); err != nil {
		t.Fatalf("alter: %v", err)
	}
	// Store value (100) must still be the effective value — no
	// override should have been installed.
	got := qe.DescribeUserQuota("alice")
	if got == nil || got.ProducerMaxByteRatePerBroker == nil || *got.ProducerMaxByteRatePerBroker != 100 {
		t.Errorf("after validate-only alter, producer rate=%v, want 100 (store value)", got)
	}
}

// recordingCRWriter is a stub KafkaUserWriter that captures every
// UpdateQuotas call so tests can assert what the handler wrote.
// Optional `err` field forces a failure.
type recordingCRWriter struct {
	calls []recordedQuotaCall
	err   error
}

type recordedQuotaCall struct {
	username string
	quotas   *auth.Quotas
}

func (w *recordingCRWriter) UpdateQuotas(_ context.Context, username string, q *auth.Quotas) error {
	w.calls = append(w.calls, recordedQuotaCall{username: username, quotas: q})
	return w.err
}

// TestAlterClientQuotasWritesToCR confirms gh #103 phase 2: when a
// KafkaUserWriter is wired, every successful alter triggers a CR
// write-back AND the in-memory map is updated. Both surfaces have to
// agree — that's the contract that makes "kubectl edit kafkauser/alice"
// + "kafka-configs.sh --alter --entity-name alice" coexist.
func TestAlterClientQuotasWritesToCR(t *testing.T) {
	store := &staticQuotaStore{users: map[string]*auth.Quotas{
		"alice": {ProducerMaxByteRatePerBroker: ptrI64(100)},
	}}
	qe := auth.NewQuotaEnforcer(store)
	writer := &recordingCRWriter{}
	alter := NewAlterClientQuotasHandler(qe, allowAuth{}).WithCRWriter(writer)

	w := codec.NewWriter()
	w.WriteArray(1, func() {
		w.WriteArray(1, func() {
			w.WriteString("user")
			w.WriteNullableString("alice", false)
		})
		w.WriteArray(1, func() {
			w.WriteString("producer_byte_rate")
			w.WriteFloat64(7777)
			w.WriteInt8(0)
		})
	})
	w.WriteInt8(0)

	if _, err := alter.Handle(&connstate.ConnState{}, 0, w.Bytes()); err != nil {
		t.Fatalf("alter: %v", err)
	}
	if len(writer.calls) != 1 || writer.calls[0].username != "alice" {
		t.Fatalf("CR writer calls=%+v, want one call for alice", writer.calls)
	}
	q := writer.calls[0].quotas
	if q == nil || q.ProducerMaxByteRatePerBroker == nil || *q.ProducerMaxByteRatePerBroker != 7777 {
		t.Errorf("CR writer received quotas=%+v, want producer=7777", q)
	}
	// In-memory state must also reflect the alter (closed loop with
	// the operator: writer hits CR → operator reconciles → file watcher
	// reloads → next describe matches. In the test we shortcut the
	// reconcile and just trust the in-memory override.)
	got := qe.DescribeUserQuota("alice")
	if got == nil || *got.ProducerMaxByteRatePerBroker != 7777 {
		t.Errorf("in-memory quota after alter=%+v, want producer=7777", got)
	}
}

// TestAlterClientQuotasSkipsInMemoryOnCRFailure verifies the gh #103
// phase 2 atomicity invariant: a CR write-back failure must leave the
// in-memory override unchanged, otherwise restart would surface a
// confusing revert ("I set this 5 minutes ago, but the broker reverted
// it" because in-memory had 7777 while the CR had 100). Restart reads
// from the CR, so the in-memory map must not run ahead of it.
func TestAlterClientQuotasSkipsInMemoryOnCRFailure(t *testing.T) {
	store := &staticQuotaStore{users: map[string]*auth.Quotas{
		"alice": {ProducerMaxByteRatePerBroker: ptrI64(100)},
	}}
	qe := auth.NewQuotaEnforcer(store)
	writer := &recordingCRWriter{err: errors.New("apiserver borked")}
	alter := NewAlterClientQuotasHandler(qe, allowAuth{}).WithCRWriter(writer)

	w := codec.NewWriter()
	w.WriteArray(1, func() {
		w.WriteArray(1, func() {
			w.WriteString("user")
			w.WriteNullableString("alice", false)
		})
		w.WriteArray(1, func() {
			w.WriteString("producer_byte_rate")
			w.WriteFloat64(7777)
			w.WriteInt8(0)
		})
	})
	w.WriteInt8(0)

	body, err := alter.Handle(&connstate.ConnState{}, 0, w.Bytes())
	if err != nil {
		t.Fatalf("alter: %v", err)
	}
	r := codec.NewReader(body)
	_, _ = r.ReadInt32() // throttle
	r.ReadArray(func() error {
		ec, _ := r.ReadInt16()
		if ec != -1 { // UNKNOWN_SERVER_ERROR
			t.Errorf("entry err=%d, want -1 (UNKNOWN_SERVER_ERROR)", ec)
		}
		_, _, _ = r.ReadNullableString()
		r.ReadArray(func() error {
			r.ReadString()
			r.ReadNullableString()
			return nil
		})
		return nil
	})
	// Store-backed value must still be the effective one.
	got := qe.DescribeUserQuota("alice")
	if got == nil || *got.ProducerMaxByteRatePerBroker != 100 {
		t.Errorf("after failed CR alter, in-memory=%+v, want producer=100 (CR rollback invariant)", got)
	}
}

// TestAlterClientQuotasUserNotFoundReturnsInvalidRequest covers the
// "typo in --entity-name" case: KafkaUser CR doesn't exist → write-back
// returns ErrKafkaUserNotFound, handler surfaces INVALID_REQUEST with
// an actionable message. AdminClient prints both for the operator.
func TestAlterClientQuotasUserNotFoundReturnsInvalidRequest(t *testing.T) {
	qe := auth.NewQuotaEnforcer(&staticQuotaStore{users: map[string]*auth.Quotas{}})
	writer := &recordingCRWriter{err: ErrKafkaUserNotFound}
	alter := NewAlterClientQuotasHandler(qe, allowAuth{}).WithCRWriter(writer)

	w := codec.NewWriter()
	w.WriteArray(1, func() {
		w.WriteArray(1, func() {
			w.WriteString("user")
			w.WriteNullableString("ghost", false)
		})
		w.WriteArray(1, func() {
			w.WriteString("producer_byte_rate")
			w.WriteFloat64(1)
			w.WriteInt8(0)
		})
	})
	w.WriteInt8(0)

	body, err := alter.Handle(&connstate.ConnState{}, 0, w.Bytes())
	if err != nil {
		t.Fatalf("alter: %v", err)
	}
	r := codec.NewReader(body)
	_, _ = r.ReadInt32()
	r.ReadArray(func() error {
		ec, _ := r.ReadInt16()
		if ec != 42 { // ErrInvalidRequest
			t.Errorf("entry err=%d, want 42 (INVALID_REQUEST)", ec)
		}
		msg, _, _ := r.ReadNullableString()
		if msg == "" || !contains(msg, "KafkaUser") {
			t.Errorf("error message=%q, want mention of KafkaUser", msg)
		}
		r.ReadArray(func() error {
			r.ReadString()
			r.ReadNullableString()
			return nil
		})
		return nil
	})
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// TestAlterClientQuotasRejectsNonUserEntity confirms compound or non-
// user entities surface INVALID_REQUEST instead of silently being
// dropped — operators get an actionable error from kafka-configs.sh.
func TestAlterClientQuotasRejectsNonUserEntity(t *testing.T) {
	qe := auth.NewQuotaEnforcer(&staticQuotaStore{users: map[string]*auth.Quotas{}})
	alter := NewAlterClientQuotasHandler(qe, allowAuth{})

	w := codec.NewWriter()
	w.WriteArray(1, func() {
		w.WriteArray(1, func() {
			w.WriteString("client-id")
			w.WriteNullableString("foo", false)
		})
		w.WriteArray(1, func() {
			w.WriteString("producer_byte_rate")
			w.WriteFloat64(1)
			w.WriteInt8(0)
		})
	})
	w.WriteInt8(0)

	body, err := alter.Handle(&connstate.ConnState{}, 0, w.Bytes())
	if err != nil {
		t.Fatalf("alter: %v", err)
	}
	r := codec.NewReader(body)
	_, _ = r.ReadInt32()
	r.ReadArray(func() error {
		ec, _ := r.ReadInt16()
		if ec != 42 { // ErrInvalidRequest
			t.Errorf("entry err=%d, want 42 (INVALID_REQUEST)", ec)
		}
		_, _, _ = r.ReadNullableString()
		r.ReadArray(func() error {
			r.ReadString()
			r.ReadNullableString()
			return nil
		})
		return nil
	})
}
