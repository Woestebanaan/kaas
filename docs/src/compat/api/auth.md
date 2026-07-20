# SASL authentication APIs

Per-API reference — see the [API support matrix](../api-matrix.md) for the generated version table.

Authentication in kaas is **per-listener**: each listener gets its own auth
engine, and the dispatcher's pre-auth gate rejects every API except
SaslHandshake (17), ApiVersions (18), and SaslAuthenticate (36) with
`CLUSTER_AUTHORIZATION_FAILED` (31) until the connection's SASL exchange
completes — see [Listeners, authentication, authorization](../../architecture/listeners-auth.md).
mTLS listeners satisfy the same gate at TLS-handshake time instead (the server
stamps the connection authenticated from the client certificate, with
[KIP-371](../kip/kip-371.md) principal mapping applied), so they never touch
these two APIs.

## SaslHandshake

Negotiates the SASL mechanism before authentication —
the first call every SASL client makes after ApiVersions.

**Versions**: v0–v1 (not flexible).

**Handling**: the handler advertises a fixed mechanism list, in preference
order: `SCRAM-SHA-512`, `PLAIN`. A supported mechanism is stamped on the
connection state so SaslAuthenticate instantiates the right exchange; an
unsupported one answers `UNSUPPORTED_SASL_MECHANISM` (33) with the list, and
nothing is stamped — the client must retry the handshake.

**Deviations from Apache 3.7**:

- The mechanism menu is `SCRAM-SHA-512` and `PLAIN` only. `SCRAM-SHA-256` is
  **not implemented** (the credentials pipeline materialises `scram-sha-512`
  entries only), and `GSSAPI` / `OAUTHBEARER` / delegation-token
  authentication are absent (see [Non-goals](../non-goals.md) and
  [KIP-554](../kip/kip-554.md) for the SCRAM-credential admin gap).
- The list is a broker-wide constant, not derived from the listener's
  configuration — a PLAIN-incapable listener still advertises PLAIN and fails
  at authenticate time instead.
- v0 is accepted on the wire, but the pre-KIP-152 flow it implies — bare SASL
  tokens sent without Kafka framing after the handshake — is not implemented.
  Clients must use [SaslAuthenticate](#saslauthenticate); every client
  from the KIP-152 era (Kafka 1.0+) does.

**Source**: `crates/kaas-broker/src/handlers/sasl.rs`,
`crates/kaas-codec/src/api/sasl_handshake.rs`.

**Verified by**: handler unit tests in
`crates/kaas-broker/src/handlers/sasl.rs` (known/unknown mechanism);
`bins/kaas/tests/auth_smoke.rs`; `scripts/kafka-acls.sh` and any script run
with an authenticated client properties file exercise it against a live
broker.

## SaslAuthenticate

Carries the SASL exchange itself (KIP-152 framing).

**Versions**: v0–v2 (flexible from v2).

**Handling**: on the first call the handler instantiates the **per-listener**
engine's exchange for the handshake-negotiated mechanism (defaulting to
SCRAM-SHA-512 if the client skipped the handshake), then steps the state
machine with each request's `auth_bytes`. SCRAM-SHA-512 is a full RFC 5802
server-side implementation (two round trips); PLAIN completes in one. On
completion the handler stamps the resolved principal and `sasl_done` on the
connection, which opens the dispatcher's pre-auth gate; the principal then
feeds ACL checks and quota buckets. A failed step answers
`SASL_AUTHENTICATION_FAILED` (58) and drops the exchange state, so the client
must restart from the handshake. Credentials come from
`/data/__cluster/credentials.json`, materialised by the operator from
`KafkaUser` CRs and hot-reloaded — see
[Kubernetes integration](../../architecture/kubernetes.md).

**Deviations from Apache 3.7**:

- **PLAIN is refused on non-TLS connections** with `NETWORK_EXCEPTION` (13),
  before the credential bytes are read. Apache allows `SASL_PLAINTEXT`; kaas
  deliberately does not ship a plaintext-password path.
- Session re-authentication (KIP-368) is not implemented;
  `session_lifetime_ms` is always 0, which clients read as "never expires" —
  the same wire behaviour as Apache with re-authentication disabled, so this
  only matters if you relied on `connections.max.reauth.ms`.
- A client that skips the handshake gets SCRAM-SHA-512 assumed, rather than
  Apache's handshake-required strictness.

**Source**: `crates/kaas-broker/src/handlers/sasl.rs`,
`crates/kaas-auth/src/scram.rs`, `crates/kaas-auth/src/plain.rs`,
`crates/kaas-auth/src/engine.rs`, `crates/kaas-protocol/src/dispatch.rs`
(pre-auth gate).

**Verified by**: `bins/kaas/tests/auth_smoke.rs`
(`scram_handshake_then_authenticate_unblocks_produce` drives the full SCRAM
exchange over a real socket and proves the gate opens); PLAIN/TLS unit tests
in `crates/kaas-broker/src/handlers/sasl.rs`; SCRAM vectors in
`crates/kaas-auth/src/scram.rs`.
