package main

import (
	"strings"
	"testing"

	"github.com/woestebanaan/skafka/internal/auth"
	"github.com/woestebanaan/skafka/internal/broker"
)

// TestValidateListenerSpecsRejectsMtlsWithoutTLS pins the gh #124
// constraint that mTLS requires TLS — no handshake means no client
// CN to extract.
func TestValidateListenerSpecsRejectsMtlsWithoutTLS(t *testing.T) {
	err := validateListenerSpecs([]listenerSpec{
		{Name: "bad", Port: 9092, Type: "internal", TLS: false,
			Authentication: listenerAuthSpec{Type: "mtls"}},
	})
	if err == nil || !strings.Contains(err.Error(), "mtls requires tls: true") {
		t.Errorf("validator: got %v, want error mentioning mtls+tls", err)
	}
}

func TestValidateListenerSpecsRejectsDuplicatePort(t *testing.T) {
	err := validateListenerSpecs([]listenerSpec{
		{Name: "a", Port: 9092, Type: "internal", Authentication: listenerAuthSpec{Type: "none"}},
		{Name: "b", Port: 9092, Type: "internal", Authentication: listenerAuthSpec{Type: "none"}},
	})
	if err == nil || !strings.Contains(err.Error(), "duplicate port") {
		t.Errorf("validator: got %v, want duplicate-port error", err)
	}
}

func TestValidateListenerSpecsRejectsDuplicateName(t *testing.T) {
	err := validateListenerSpecs([]listenerSpec{
		{Name: "plain", Port: 9092, Type: "internal", Authentication: listenerAuthSpec{Type: "none"}},
		{Name: "plain", Port: 9093, Type: "internal", Authentication: listenerAuthSpec{Type: "none"}},
	})
	if err == nil || !strings.Contains(err.Error(), "duplicate listener name") {
		t.Errorf("validator: got %v, want duplicate-name error", err)
	}
}

func TestValidateListenerSpecsRejectsUnknownAuthType(t *testing.T) {
	err := validateListenerSpecs([]listenerSpec{
		{Name: "x", Port: 9092, Type: "internal", Authentication: listenerAuthSpec{Type: "wat"}},
	})
	if err == nil || !strings.Contains(err.Error(), "unsupported authentication.type") {
		t.Errorf("validator: got %v, want unsupported-auth-type error", err)
	}
}

func TestValidateListenerSpecsRejectsUnknownType(t *testing.T) {
	err := validateListenerSpecs([]listenerSpec{
		{Name: "x", Port: 9092, Type: "route", Authentication: listenerAuthSpec{Type: "none"}},
	})
	if err == nil || !strings.Contains(err.Error(), "type must be") {
		t.Errorf("validator: got %v, want unsupported-type error", err)
	}
}

func TestValidateListenerSpecsAcceptsCanonicalCases(t *testing.T) {
	// All eleven legal combinations of (type, tls, authType) modulo the
	// mtls-requires-tls constraint. The validator should accept each.
	specs := []listenerSpec{
		{Name: "plain-int", Port: 9092, Type: "internal", TLS: false, Authentication: listenerAuthSpec{Type: "none"}},
		{Name: "plain-ext", Port: 9093, Type: "external", TLS: false, Authentication: listenerAuthSpec{Type: "none"}},
		{Name: "scram-int", Port: 9094, Type: "internal", TLS: false, Authentication: listenerAuthSpec{Type: "scram-sha-512"}},
		{Name: "scram-ext", Port: 9095, Type: "external", TLS: false, Authentication: listenerAuthSpec{Type: "scram-sha-512"}},
		{Name: "tls-int", Port: 9096, Type: "internal", TLS: true, Authentication: listenerAuthSpec{Type: "none"}},
		{Name: "tls-ext", Port: 9097, Type: "external", TLS: true, Authentication: listenerAuthSpec{Type: "none"}},
		{Name: "tls-scram-int", Port: 9098, Type: "internal", TLS: true, Authentication: listenerAuthSpec{Type: "scram-sha-512"}},
		{Name: "tls-scram-ext", Port: 9099, Type: "external", TLS: true, Authentication: listenerAuthSpec{Type: "scram-sha-512"}},
		{Name: "mtls-int", Port: 9100, Type: "internal", TLS: true, Authentication: listenerAuthSpec{Type: "mtls"}},
		{Name: "mtls-ext", Port: 9101, Type: "external", TLS: true, Authentication: listenerAuthSpec{Type: "mtls"}},
	}
	if err := validateListenerSpecs(specs); err != nil {
		t.Errorf("validator: rejected legal config: %v", err)
	}
}

// TestBuildListenerWireupRoutesEngines pins the gh #124 contract that
// listeners marked authentication.type=none route to AllowAllAuthEngine
// while non-none listeners route to RealAuthEngine.
func TestBuildListenerWireupRoutesEngines(t *testing.T) {
	specs := []listenerSpec{
		{Name: "anon", Port: 9092, Type: "internal", Authentication: listenerAuthSpec{Type: "none"}},
		{Name: "scram", Port: 9095, Type: "internal", Authentication: listenerAuthSpec{Type: "scram-sha-512"}},
	}
	// Sentinel engine — distinguishable from AllowAll via the unique
	// principal name returned by AuthenticateTLS.
	realSentinel := sentinelEngine{name: "real"}
	allowAll := broker.NewAllowAllAuthEngine()
	wire := buildListenerWireup(specs, "127.0.0.1", nil, realSentinel, allowAll)

	if len(wire.Configs) != 2 {
		t.Fatalf("Configs len=%d, want 2", len(wire.Configs))
	}
	if wire.Configs[0].Addr != "127.0.0.1:9092" || wire.Configs[1].Addr != "127.0.0.1:9095" {
		t.Errorf("Addr wiring wrong: %v", wire.Configs)
	}
	// "anon" goes to allowAll, "scram" goes to the sentinel real engine.
	if engAnon := wire.Engines.For("anon"); engAnon != auth.AuthEngine(allowAll) {
		t.Errorf("anon listener: got %T, want *AllowAllAuthEngine", engAnon)
	}
	if engScram := wire.Engines.For("scram"); engScram != auth.AuthEngine(realSentinel) {
		t.Errorf("scram listener: got %T, want sentinel", engScram)
	}
	// Empty fallback resolves to allowAll.
	if fb := wire.Engines.For(""); fb != auth.AuthEngine(allowAll) {
		t.Errorf("\"\" fallback: got %T, want allowAll", fb)
	}
}

// sentinelEngine is a tag-only AuthEngine used by the test above.
type sentinelEngine struct{ name string }

func (sentinelEngine) NewSASLExchange(string) (auth.SASLExchange, error) { return nil, nil }
func (sentinelEngine) AuthenticateTLS(string) (auth.Principal, error)     { return auth.Principal{}, nil }
func (sentinelEngine) Authorize(auth.Principal, auth.Resource, auth.Operation) bool {
	return true
}
func (sentinelEngine) CheckProduceQuota(auth.Principal, int) int32 { return 0 }
func (sentinelEngine) CheckFetchQuota(auth.Principal, int) int32   { return 0 }
func (sentinelEngine) RequiresPreAuth() bool                       { return true }
