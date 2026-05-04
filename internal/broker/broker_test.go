package broker

import (
	"testing"

	"github.com/woestebanaan/skafka/internal/protocol"
)

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
