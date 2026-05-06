package broker

import (
	"encoding/binary"
	"testing"

	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/protocol"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
)

// TestInitProducerIdAdvertisedToClients guards gh #12 stage A:
// when ApiVersions is built from the dispatcher's SupportedVersions
// map, key 22 must be in the list with [0, 4]. This is the contract
// Java's KafkaProducer (idempotent by default since 3.0) and franz-go
// rely on to decide whether to attempt InitProducerId at all — if 22
// isn't advertised, the producer fails fast with
// "Producer has been disabled because [...]" instead of retrying.
func TestInitProducerIdAdvertisedToClients(t *testing.T) {
	cfg := Config{BrokerID: 0, Host: "localhost", Port: 9092, ClusterID: "test"}
	b := New(cfg, NewMemoryStorage(), NewLocalLeaseManager(), NewAllowAllAuthEngine())
	d := protocol.NewDispatcher()
	b.RegisterHandlers(d)

	versions := d.SupportedVersions()
	got, ok := versions[22]
	if !ok {
		t.Fatal("InitProducerId (key 22) not registered with the dispatcher")
	}
	want := [2]int16{0, 4}
	if got != want {
		t.Fatalf("InitProducerId version range = %v, want %v", got, want)
	}
}

// TestInitProducerIdEndToEndV4 walks a real Kafka 3.7 idempotent
// producer's startup handshake through the FULL dispatcher chain
// (registration → flexibility-map lookup → handler → response
// framing). v4 is what Java + franz-go negotiate by default; this
// test fails if any one piece is missing — a regression where v4
// is registered but flexibleRequestHeader[22] is unset, for
// example, would silently corrupt the response framing in a way
// the per-handler unit tests cannot see.
func TestInitProducerIdEndToEndV4(t *testing.T) {
	cfg := Config{BrokerID: 0, Host: "localhost", Port: 9092, ClusterID: "test"}
	b := New(cfg, NewMemoryStorage(), NewLocalLeaseManager(), NewAllowAllAuthEngine())
	d := protocol.NewDispatcher()
	b.RegisterHandlers(d)

	// Body that readFrame would hand the dispatcher AFTER stripping
	// the v4 request header (apiKey/version/corrId/clientId/tagged).
	w := codec.NewWriter()
	w.WriteCompactNullableString("", true) // null TransactionalID
	w.WriteInt32(60_000)                   // TransactionTimeoutMs
	w.WriteInt64(-1)                       // ProducerID hint (KIP-360)
	w.WriteInt16(-1)                       // ProducerEpoch hint
	w.WriteEmptyTaggedFields()
	body := w.Bytes()

	hdr := protocol.RequestHeader{APIKey: 22, APIVersion: 4, CorrelationID: 4242}
	resp, err := d.Dispatch(hdr, body, &connstate.ConnState{})
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	// v4 = flexible-response-header. Expected layout:
	//   corrID(4) | tagged-fields(1=0x00) | throttle(4) | errCode(2)
	//   pid(8)    | epoch(2)              | tagged-fields(1=0x00)
	const wantLen = 4 + 1 + 4 + 2 + 8 + 2 + 1
	if len(resp) != wantLen {
		t.Fatalf("v4 resp length = %d, want %d (hex=%x)", len(resp), wantLen, resp)
	}
	if got := int32(binary.BigEndian.Uint32(resp[0:4])); got != 4242 {
		t.Errorf("correlationID = %d, want 4242", got)
	}
	if resp[4] != 0x00 {
		t.Errorf("response-header tagged-fields byte = %#x, want 0x00", resp[4])
	}
	if errCode := int16(binary.BigEndian.Uint16(resp[9:11])); errCode != 0 {
		t.Errorf("errCode = %d, want 0", errCode)
	}
	pid := int64(binary.BigEndian.Uint64(resp[11:19]))
	if pid <= 0 {
		t.Errorf("ProducerID = %d, want a positive monotonic id", pid)
	}
	if epoch := int16(binary.BigEndian.Uint16(resp[19:21])); epoch != 0 {
		t.Errorf("epoch = %d, want 0 (stage A always returns fresh epoch)", epoch)
	}
	if resp[21] != 0x00 {
		t.Errorf("body trailing tagged-fields byte = %#x, want 0x00", resp[21])
	}
}

// TestInitProducerIdEndToEndV0 walks the legacy non-flexible path
// older clients (or franz-go negotiating down) take. Catches the
// regression where someone "modernizes" the codec to assume flexible
// — v0/v1 must stay non-flexible (no tagged fields anywhere in the
// header or body).
func TestInitProducerIdEndToEndV0(t *testing.T) {
	cfg := Config{BrokerID: 0, Host: "localhost", Port: 9092, ClusterID: "test"}
	b := New(cfg, NewMemoryStorage(), NewLocalLeaseManager(), NewAllowAllAuthEngine())
	d := protocol.NewDispatcher()
	b.RegisterHandlers(d)

	w := codec.NewWriter()
	w.WriteNullableString("", true) // null TransactionalID
	w.WriteInt32(60_000)
	body := w.Bytes()

	hdr := protocol.RequestHeader{APIKey: 22, APIVersion: 0, CorrelationID: 7}
	resp, err := d.Dispatch(hdr, body, &connstate.ConnState{})
	if err != nil {
		t.Fatalf("Dispatch v0: %v", err)
	}
	// v0: corrID(4) | throttle(4) | errCode(2) | pid(8) | epoch(2) — no tagged fields anywhere.
	const wantLen = 4 + 4 + 2 + 8 + 2
	if len(resp) != wantLen {
		t.Fatalf("v0 resp length = %d, want %d (hex=%x)", len(resp), wantLen, resp)
	}
	if errCode := int16(binary.BigEndian.Uint16(resp[8:10])); errCode != 0 {
		t.Errorf("v0 errCode = %d, want 0", errCode)
	}
}

// TestInitProducerIdRejectsVersionAboveV4 pins the registered max
// at v4. If a client (or a future wire-protocol bump) tries v5 or
// higher, the dispatcher must return UNSUPPORTED_VERSION (35) so
// the client retries at a known-good version, NOT silently fall
// through to a v4 decode of v5 bytes which would corrupt the PID
// the producer tags every batch with.
func TestInitProducerIdRejectsVersionAboveV4(t *testing.T) {
	cfg := Config{BrokerID: 0, Host: "localhost", Port: 9092, ClusterID: "test"}
	b := New(cfg, NewMemoryStorage(), NewLocalLeaseManager(), NewAllowAllAuthEngine())
	d := protocol.NewDispatcher()
	b.RegisterHandlers(d)

	hdr := protocol.RequestHeader{APIKey: 22, APIVersion: 5, CorrelationID: 99}
	resp, err := d.Dispatch(hdr, nil, &connstate.ConnState{})
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if errCode := int16(binary.BigEndian.Uint16(resp[4:6])); errCode != protocol.ErrUnsupportedVersion {
		t.Errorf("v5 errCode = %d, want %d (UNSUPPORTED_VERSION)", errCode, protocol.ErrUnsupportedVersion)
	}
}

// TestInitProducerIdAcceptsTransactionalRequest pins stage A's
// deliberately permissive behaviour for transactional-ID-bearing
// requests: we do NOT yet reject them (transactions are unimplemented,
// but rejection at this surface would block AdminClient calls that
// pre-allocate a transactional producer in setUp methods even when
// the test never sends transactional batches). The fence happens
// later, at AddPartitionsToTxn — when that handler lands it should
// be the ONE place transactions are gated, not InitProducerId.
//
// If this test starts failing, you've added rejection here; double-
// check that's intentional and that AddPartitionsToTxn is the
// right gate.
func TestInitProducerIdAcceptsTransactionalRequest(t *testing.T) {
	cfg := Config{BrokerID: 0, Host: "localhost", Port: 9092, ClusterID: "test"}
	b := New(cfg, NewMemoryStorage(), NewLocalLeaseManager(), NewAllowAllAuthEngine())
	d := protocol.NewDispatcher()
	b.RegisterHandlers(d)

	w := codec.NewWriter()
	w.WriteCompactNullableString("my-txn-id", false) // transactional producer
	w.WriteInt32(30_000)
	w.WriteInt64(-1)
	w.WriteInt16(-1)
	w.WriteEmptyTaggedFields()

	hdr := protocol.RequestHeader{APIKey: 22, APIVersion: 4, CorrelationID: 1}
	resp, err := d.Dispatch(hdr, w.Bytes(), &connstate.ConnState{})
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if errCode := int16(binary.BigEndian.Uint16(resp[9:11])); errCode != 0 {
		t.Errorf("transactional-ID request rejected at InitProducerId (errCode=%d) — stage A pins this as accepted; gate transactions at AddPartitionsToTxn instead", errCode)
	}
}

// TestRegisterHandlersCapsCreateTopicsAtV6 guards gh #73:
// CreateTopics (API key 19) must advertise max version 6, not v7. v7
// added a topic_id UUID field to CreatableTopicResult (KIP-516); our
// encoder doesn't write it, so a Java admin client negotiating v7 hits
// BufferUnderflowException reading the missing 16 bytes. Until the
// encoder writes topic_id, the dispatcher must keep the cap at v6.
func TestRegisterHandlersCapsCreateTopicsAtV6(t *testing.T) {
	cfg := Config{BrokerID: 0, Host: "localhost", Port: 9092, ClusterID: "test"}
	b := New(cfg, NewMemoryStorage(), NewLocalLeaseManager(), NewAllowAllAuthEngine())
	d := protocol.NewDispatcher()
	b.RegisterHandlers(d)

	versions := d.SupportedVersions()
	got, ok := versions[19]
	if !ok {
		t.Fatal("CreateTopics (key 19) not registered with the dispatcher")
	}
	want := [2]int16{0, 6}
	if got != want {
		t.Fatalf("CreateTopics version range = %v, want %v (gh #73 — v7 encoder is missing topic_id UUID)", got, want)
	}
}
