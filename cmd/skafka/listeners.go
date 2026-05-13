package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"github.com/woestebanaan/skafka/internal/auth"
	"github.com/woestebanaan/skafka/internal/broker"
	"github.com/woestebanaan/skafka/internal/protocol"
)

// listenerSpec is the JSON shape parsed from SKAFKA_LISTENERS — the
// canonical per-listener config emitted by the chart's iteration over
// .Values.listeners. Mirrors Strimzi's three-axis model: `type`
// (internal/external) drives advertised-host computation, `tls`
// (false/true) drives encryption, `authentication.type` drives which
// AuthEngine handles the listener.
//
// gh #124: replaces the legacy SKAFKA_PORT / SKAFKA_TLS_LISTEN_ADDR /
// SKAFKA_AUTHED_LISTEN_ADDR env triplet. When SKAFKA_LISTENERS is
// unset the legacy path in main.go takes over (back-compat for
// pre-v0.1.122 chart values).
type listenerSpec struct {
	Name string `json:"name"`
	Port int    `json:"port"`
	// Type is "internal" or "external". External listeners get
	// per-broker FQDNs in MetadataResponse; internal listeners get
	// the headless-Service DNS. The MetadataHandler still keys on
	// the literal name string for now (see metadata.go); the type
	// field is reserved for the planned MetadataHandler refactor.
	Type           string           `json:"type"`
	TLS            bool             `json:"tls"`
	Authentication listenerAuthSpec `json:"authentication"`
}

type listenerAuthSpec struct {
	// Type is one of: "none", "scram-sha-512", "mtls", "plain".
	// "none" wires AllowAllAuthEngine (anonymous-OK); the rest wire
	// the broker-wide RealAuthEngine, which validates SCRAM / mTLS
	// principals against credentials.json + acls.json.
	Type string `json:"type"`
}

// parseListenersEnv decodes the SKAFKA_LISTENERS JSON env and runs
// the structural validators. Returns nil, nil when the env is unset
// (signal to the caller to use the legacy path).
func parseListenersEnv() ([]listenerSpec, error) {
	raw := os.Getenv("SKAFKA_LISTENERS")
	if raw == "" {
		return nil, nil
	}
	var specs []listenerSpec
	if err := json.Unmarshal([]byte(raw), &specs); err != nil {
		return nil, fmt.Errorf("parse SKAFKA_LISTENERS: %w", err)
	}
	if err := validateListenerSpecs(specs); err != nil {
		return nil, err
	}
	return specs, nil
}

// validateListenerSpecs enforces the three-axis-orthogonality
// constraints from gh #124. Each violation surfaces with a clear
// error message; main.go fatals on any of these to catch chart-
// template misconfigurations at startup rather than at request time.
func validateListenerSpecs(specs []listenerSpec) error {
	if len(specs) == 0 {
		return fmt.Errorf("SKAFKA_LISTENERS: at least one listener required")
	}
	names := make(map[string]struct{}, len(specs))
	ports := make(map[int]struct{}, len(specs))
	for i, s := range specs {
		if s.Name == "" {
			return fmt.Errorf("SKAFKA_LISTENERS[%d]: missing name", i)
		}
		if s.Port <= 0 || s.Port > 65535 {
			return fmt.Errorf("SKAFKA_LISTENERS[%d] %s: invalid port %d", i, s.Name, s.Port)
		}
		if s.Type != "internal" && s.Type != "external" {
			return fmt.Errorf("SKAFKA_LISTENERS[%d] %s: type must be \"internal\" or \"external\" (got %q)", i, s.Name, s.Type)
		}
		switch s.Authentication.Type {
		case "", "none", "scram-sha-512", "mtls", "plain":
			// OK
		default:
			return fmt.Errorf("SKAFKA_LISTENERS[%d] %s: unsupported authentication.type %q", i, s.Name, s.Authentication.Type)
		}
		// mTLS requires TLS — no handshake means no client CN to extract.
		if s.Authentication.Type == "mtls" && !s.TLS {
			return fmt.Errorf("SKAFKA_LISTENERS[%d] %s: authentication.type=mtls requires tls: true", i, s.Name)
		}
		if _, dup := names[s.Name]; dup {
			return fmt.Errorf("SKAFKA_LISTENERS: duplicate listener name %q", s.Name)
		}
		names[s.Name] = struct{}{}
		if _, dup := ports[s.Port]; dup {
			return fmt.Errorf("SKAFKA_LISTENERS: duplicate port %d (between listeners on the same broker)", s.Port)
		}
		ports[s.Port] = struct{}{}
	}
	return nil
}

// listenerWireup is the output of buildListenerWireup — the slice
// of ListenerConfig entries to hand to protocol.Server, plus the
// PerListenerAuthEngine map to hand to the dispatcher / broker, plus
// the names slice used by /healthz reporting.
type listenerWireup struct {
	Configs       []protocol.ListenerConfig
	Engines       auth.PerListenerAuthEngine
	ListenerNames []string // for observability.HealthHandler
	TLSActive     bool     // any listener has TLS=true (drives observability.TLSInfo)
}

// buildListenerWireup turns a parsed listener spec list into the
// wire-level config for protocol.Server + the per-listener auth
// engine map. RealAuthEngine is wired only for listener entries that
// declare a non-"none" auth type; the others get AllowAllAuthEngine.
//
// tlsCfg is the shared *tls.Config built from SKAFKA_TLS_CERT_FILE in
// main.go — every listener with tls=true reuses it. Per-listener cert
// files are a future extension.
func buildListenerWireup(
	specs []listenerSpec,
	host string,
	tlsCfg *tls.Config,
	realEng auth.AuthEngine, // nil when SKAFKA_AUTH_DISABLED=true OR no listener wants auth
	allowAll *broker.AllowAllAuthEngine,
) listenerWireup {
	out := listenerWireup{
		Engines: make(auth.PerListenerAuthEngine, len(specs)+1),
	}
	out.Engines[""] = allowAll // fallback for untagged conns
	for _, s := range specs {
		lc := protocol.ListenerConfig{
			Name: s.Name,
			Addr: host + ":" + strconv.Itoa(s.Port),
		}
		if s.TLS {
			lc.TLSConfig = tlsCfg
		}
		out.Configs = append(out.Configs, lc)
		if needsReal(s.Authentication.Type) && realEng != nil {
			out.Engines[s.Name] = realEng
		} else {
			out.Engines[s.Name] = allowAll
		}
		out.ListenerNames = append(out.ListenerNames, s.Name)
		if s.TLS {
			out.TLSActive = true
		}
	}
	return out
}

// needsReal reports whether an authentication type triggers the
// RealAuthEngine. "none" and "" (omitted) stay on AllowAll.
func needsReal(authType string) bool {
	switch authType {
	case "scram-sha-512", "mtls", "plain":
		return true
	default:
		return false
	}
}

// logListenerWireup emits one INFO line per listener so a chart
// misconfiguration is obvious in the log without cross-referencing
// SKAFKA_LISTENERS by hand.
func logListenerWireup(specs []listenerSpec) {
	for _, s := range specs {
		slog.Info("listener configured",
			"name", s.Name,
			"port", s.Port,
			"type", s.Type,
			"tls", s.TLS,
			"auth", firstNonEmpty(s.Authentication.Type, "none"))
	}
}

func firstNonEmpty(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}
