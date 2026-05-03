# Phase 7 Authentication Engine — Breakdown vs. Current Repo

Comparison of `skafka-plan-v3.md` (v3.3) §"Phase 7: Authentication Engine" (lines 1078–1081) against the state of `main` at commit `2be8730`.

Legend: ✅ done · 🟡 partial / minor deviation · ❌ not yet · ➕ extra (not in plan)

---

## What Phase 7 actually has to deliver

The plan's Phase 7 body is one sentence:

> SCRAM-SHA-512, mTLS, Kubernetes SA JWT. ACLs in `/data/__cluster/acls.json`, polled by brokers.

Combined with the broader auth references scattered through the plan (§Design Principles #8 "End-to-end TLS with per-broker advertised hostnames", §Phase 1 RBAC for KafkaUser/KafkaAcl Secrets), the authentication surface is:

| Auth mechanism | Required |
|---|---|
| **SASL/SCRAM-SHA-512** for username+password clients | ✅ |
| **SASL/PLAIN with Kubernetes ServiceAccount JWT** (validated via TokenReview) | ✅ |
| **mTLS** with CN-based principal extraction | ✅ (opportunistic) |
| **ACL engine** with literal+prefix patterns, allow/deny rules, hot-reload | ✅ |
| **Per-user quotas** (producer/consumer byte rates) | ✅ |
| **Hot-reload** of `credentials.json` + `acls.json` via fsnotify+poll | ✅ |
| **Anonymous fallthrough** for ungated connections | ✅ |

All seven exist on `main` — most landed during v2.6 development, with the fsnotify+poll fallback added in the Phase 3 follow-up. **Phase 7 is fundamentally complete.** The remaining work is coverage hardening and one configuration knob.

---

## What already exists

### Code

| Item | Where | Status |
|---|---|---|
| `Principal`, `Resource`, `Operation`, `Quotas` types | `internal/auth/auth.go` | ✅ |
| `AuthEngine` + `SASLExchange` interfaces | `internal/auth/auth.go:40-60` | ✅ |
| `RealAuthEngine` — production engine wiring SCRAM + SA + ACL + quotas | `internal/auth/engine.go` | ✅ |
| `ScramExchange` — SCRAM-SHA-512 server-side state machine (RFC 5802) | `internal/auth/scram.go` | ✅, with `internal/auth/scram_test.go` (181 lines) |
| `SAExchange` — SASL/PLAIN over Kubernetes ServiceAccount JWT (TokenReview) | `internal/auth/serviceaccount.go` | ✅ |
| `AuthenticateTLS(cn)` — mTLS CN → Principal lookup | `internal/auth/tls.go` | ✅ |
| `ACLEngine` — literal/prefix matching, allow/deny rules, decision cache | `internal/auth/acl.go` | ✅, with `internal/auth/acl_test.go` (124 lines) |
| `QuotaEnforcer` — per-user token-bucket producer/consumer throttling | `internal/auth/quota.go` | ✅, with `internal/auth/quota_test.go` (48 lines) |
| `CredentialLoader` — parses `credentials.json` (SCRAM keys, TLS CN→user, SA→user, quotas) | `internal/auth/loader.go` | ✅, with `internal/auth/loader_test.go` (74 lines) |
| `AllowAllAuthEngine` / `DenyAllAuthEngine` stubs for dev/tests | `internal/broker/stubs.go` | ✅ |

### Wire path

| Step | Where | Status |
|---|---|---|
| Connection sets `IsTLS=true` and extracts mTLS principal | `internal/protocol/server.go:131-150` | ✅ |
| Dispatcher gates pre-SASL API keys (only ApiVersions, SaslHandshake, SaslAuthenticate allowed) when `RequireSASL=true` | `internal/protocol/dispatch.go:36-55` | ✅ |
| `SaslHandshakeHandler` advertises supported mechanisms | `internal/protocol/handlers/sasl.go` | ✅ |
| `SaslAuthenticateHandler` drives the `SASLExchange.Step` loop | `internal/protocol/handlers/sasl.go` | ✅ |
| `ProduceHandler.principalFrom(conn)` derives the principal for authz checks | `internal/protocol/handlers/helpers.go` | ✅ |
| Per-request `Authorize(principal, resource, op)` calls in produce/fetch handlers | `internal/protocol/handlers/produce.go:71`, `fetch.go` | ✅ |
| Per-request quota deduction (`CheckProduceQuota`, `CheckFetchQuota`) | `internal/protocol/handlers/produce.go:65` | ✅ |

### Hot-reload

| Step | Where | Status |
|---|---|---|
| `ClusterFileWatcher` watches `acls.json` + `credentials.json` | `internal/storage/watcher.go` | ✅ |
| Watcher uses `internal/fsutil.FileWatcher` (fsnotify + 1s poll + 30s full-fire) | shared with `internal/assignment.FileStore` | ✅ Phase 3 follow-up |
| Watcher fires `RealAuthEngine.Reload()` on change | `cmd/skafka/main.go:212-232` | ✅ |
| `RealAuthEngine.Reload()` calls `creds.Reload()` + `acls.Reload()` atomically | `internal/auth/engine.go:62-65` | ✅ |

### Operator side (writes)

| Step | Where | Status |
|---|---|---|
| `KafkaUserReconciler` writes SCRAM credentials to `credentials.json` | `operator/controllers/kafkauser_controller.go` | ✅, with `credentials_test.go` |
| `KafkaUserGroupReconciler` manages user groups | `operator/controllers/kafkausergroup_controller.go` | ✅ |
| `KafkaAclReconciler` writes ACL rules to `acls.json` | `operator/controllers/kafkaacl_controller.go` | ✅, with `acls_test.go` |
| RBAC for the operator's PVC writes | Helm chart + `deploy/rbac/` | ✅ |

### TLS

| Item | Where | Status |
|---|---|---|
| `WatchingCertificate(certFile, keyFile)` returns a `*tls.Config` with hot-reload via fsnotify on parent dirs (catches Kubernetes Secret-mount symlink swaps) | `internal/protocol/tls.go` | ✅ |
| Cert rotation is debounced 200ms; surfaces `cert_reloads_total` metric | `internal/protocol/tls.go:62-98` | ✅ |
| TLS listener on configurable port (default 9093) | `internal/protocol/server.go:58-66`, helm chart | ✅ |

---

## What Phase 7 still needs (small)

### 1. mTLS client-cert enforcement is opportunistic

`WatchingCertificate` returns a `tls.Config` without setting `ClientAuth`. Today's behaviour:

- Client connects with cert → server extracts CN, runs through `AuthenticateTLS`, principal is `User:<mapped>`.
- Client connects WITHOUT cert → connection succeeds anyway, principal is `ANONYMOUS` until SASL completes.

For environments that want to enforce client certs (per the plan's "End-to-end TLS"), we'd add a config knob:

```go
type Config struct {
    // ...
    RequireClientCert bool // sets tls.Config.ClientAuth = RequireAndVerifyClientCert
    ClientCAs *x509.CertPool
}
```

Plus a Helm chart values knob. Small change; opt-in.

### 2. Compat tests don't exercise SASL or mTLS

`tests/kafka-compat/compat_test.go` runs the broker with `AllowAllAuthEngine` and no TLS. End-to-end tests where:

- franz-go connects with `SASL/SCRAM-SHA-512`, validates against a real `RealAuthEngine` loaded from a tempdir credentials.json,
- and/or connects with mTLS using a generated test cert chain,

would be the strongest proof the handshake matches what real clients expect. Not a correctness-regression-protection-tier need (existing unit tests cover the state machine), but **the compat tests are precisely where wire-level interop bugs surface** — see the franz-go consumer-group cold-start flake that turned out to be a synchronous-call latency bug (Phase 5 step 7's resolution). An auth-on compat test would catch analogous regressions if the SASL handshake ever drifts from the spec.

### 3. SCRAM-SHA-256 fallback (deferred — plan specifies 512)

The plan locks in SHA-512. Some Java clients default to SCRAM-SHA-256. Adding SHA-256 as a second mechanism is straightforward (parameterise `crypto/sha512` → either) but explicitly not in Phase 7's scope.

### 4. Documentation: which env var enables auth?

`SKAFKA_AUTH_DISABLED=true` flips to `AllowAllAuthEngine`. `SKAFKA_REQUIRE_SASL=true` sets `dispatch.RequireSASL`. Both are env-var-only today; not surfaced in the Helm chart values.yaml or documented. Small docs/values addition.

---

## Suggested implementation order

1. **mTLS client-cert enforcement opt-in** — `WatchingCertificate` gains a `RequireClientCert` parameter and an optional `ClientCAs *x509.CertPool`. Helm `auth.tls.requireClientCert` value flips through. ~1.5h.
2. **End-to-end SASL compat test** — `TestFranzGoSCRAMRoundTrip` in `tests/kafka-compat/`: spin up the broker with `RealAuthEngine` over a tempdir, register a user via `CredentialLoader`-style JSON, connect with `SASL/SCRAM-SHA-512` from franz-go, produce + consume successfully. ~2h.
3. **End-to-end mTLS compat test** — generate a self-signed CA + client cert + server cert at test setup; spin up the broker on a TLS port; franz-go connects with the client cert; assert the principal is correctly extracted from the CN. ~2h.
4. **Auth env-var docs** — surface `SKAFKA_AUTH_DISABLED` and `SKAFKA_REQUIRE_SASL` in `deploy/helm/skafka/values.yaml` (`auth.disabled: false`, `auth.requireSasl: false`) and document briefly in the chart README. ~0.5h.

Total: **5-6 hours of focused work** — same order of magnitude as Phase 6, but most is test infrastructure rather than production code. Each step is independent.

---

## Items deliberately NOT in Phase 7

- **OAuth2 / OIDC SASL mechanisms** — out of v1 scope; the plan only lists SCRAM-SHA-512, mTLS, and SA JWT.
- **GSS-API / Kerberos** — same; Kafka's native KAFKA_GSSAPI mechanism not in scope.
- **Cluster-level authorization** (e.g. allow-list of client IPs) — not in the plan; the existing `Resource{Type: "cluster"}` ACL surface covers what's needed.
- **Audit logging of allow/deny decisions** — useful but explicitly not Phase 7. Phase 10 (Observability) is where audit metrics would land.
- **Token rotation for SA JWT** — handled by the Kubernetes API server side; broker just validates each presented token via TokenReview.

---

## Open questions for Phase 7 implementation

- **mTLS client-cert enforcement default**: opt-in (`requireClientCert: false` by default) seems right — defaulting to required would break clients that expect SASL-only over TLS. Confirm with the user before flipping the default.
- **Where to host the test CA / cert generation helper** — `tests/testutil/tlscerts/` (new package, mirroring `tests/testutil/recordbatch/`)? Reusable across compat + integration tests.
- **TokenReview latency** — every SASL/PLAIN authentication round-trips to the Kubernetes API server. Acceptable for connection-establishment, but high enough that we should not call it on every Produce. Today the principal is cached on `connstate.ConnState` after SASL completes; verify that assumption holds.
- **Quota refill rate** — `internal/auth/quota.go` uses a token bucket. Default refill rate? Configurable?

---

## Summary

Phase 7 is **substantively complete** — every authentication mechanism the plan calls for already runs in production: SCRAM-SHA-512, mTLS opportunistic, SA JWT via TokenReview, ACLs with hot-reload, per-user quotas. The auth engine is plumbed through the produce/fetch hot paths, the operator writes credentials and ACL files to the shared PVC, and the cluster-config watcher (now sharing `internal/fsutil` with the assignment-file watcher from Phase 3 follow-up) catches changes within ~1s on NFS.

The remaining work is **observability of auth into integration tests** — the unit tests cover the state machines exhaustively (181 lines on SCRAM alone), but no compat test exercises the full handshake against a real client. Adding two compat tests (SCRAM and mTLS) is the highest-value Phase 7 work because that's where future regressions would actually surface.

A small mTLS-enforcement knob and a Helm values surface for the auth env vars are the only configuration deltas. No architectural change.

After Phase 7, what remains is Phase 8 (Helm chart polish — mostly already done), Phase 9 (external access via TLS passthrough router — infrastructure-shaped), and Phase 10 (observability metrics + Grafana dashboard).
