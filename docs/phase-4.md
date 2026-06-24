# Phase 4 — Auth

Detailed work plan for the fifth phase of the Rust rewrite. Companion to
[`rewrite.md`](./rewrite.md); the high-level summary lives there. Builds
on the codec scaffolding from [`phase-1.md`](./phase-1.md), the storage
engine from [`phase-2.md`](./phase-2.md), and the single-broker server
from [`phase-3.md`](./phase-3.md).

**Goal.** Port `archive/internal/auth/` to `crates/sk-auth` and wire it
into `sk-protocol` + `bins/skafka`. After Phase 4 a real
`kafka-console-{producer,consumer}` configured with
`security.protocol=SASL_SSL`, `sasl.mechanism=SCRAM-SHA-512`, and a valid
SCRAM credential round-trips against the Rust broker; an mTLS client
whose subject CN is in `credentials.json` round-trips; ACLs deny by
default and a `User:alice` allow on `topic/foo/Write` lets alice produce
to `foo` and nothing else; quota debt-carries across concurrent clients
(gh #125).

**Length.** ~2 weeks, single engineer. Workstream A (codec backfill for
keys 17 + 36) is small and unblocks the SASL handler in G. B
(primitives/types) blocks C/D/E/F. C/D/E/F land in parallel after B. G
threads them through `sk-protocol`. H closes with `bins/skafka` TLS
bring-up + end-to-end smoke.

**Out of scope for Phase 4.**

- Kubernetes ServiceAccount JWT auth (`SAExchange` in
  `archive/internal/auth/serviceaccount.go`) — needs a `kube::Client`,
  which lands in **Phase 7** with the operator. SASL PLAIN ships with a
  *static-credential* path against `credentials.json` (same byte shape
  but no TokenReview); the K8s-backed PLAIN path stays a TODO with a
  one-line stub returning `UNSUPPORTED_SASL_MECHANISM`.
- `DescribeUserScramCredentials` / `AlterClientQuotas` /
  `DescribeClientQuotas` admin APIs (keys 50, 48, 49) — Phase 5/7
  surface. The underlying `CredentialLoader::list_all_scram_users()` /
  `QuotaEnforcer::list_user_quotas()` accessors land in Phase 4 so the
  admin handlers can be a thin wrapper later, but the handlers
  themselves don't.
- Runtime `SetAuthEngines` swap on a live `Dispatcher` (the Go side has
  one). Phase 4 wires the selector at `Dispatcher::new`-time; runtime
  swap is a Phase-5/7 problem when the K8s watcher arrives.
- `ClusterFileWatcher` `notify`-driven hot reload. Phase 4 ships the
  `reload()` API on `CredentialLoader` / `AclEngine` + a
  `tokio::time::interval` poll loop in `bins/skafka/main.rs` (10 s
  default). inotify-driven reloads are a Phase 8 perf nicety, not a
  correctness gap.

**Prerequisite codec work.** Two new modules in `sk-codec/src/api/`:

| Key | API              | Versions | Flexible from | Notes                                                                                   |
|----:|------------------|---------:|--------------:|-----------------------------------------------------------------------------------------|
|  17 | SaslHandshake    | 0–1      | never         | `mechanism: String` request; `error_code` + `mechanisms: Vec<String>` response          |
|  36 | SaslAuthenticate | 0–2      | 2             | `auth_bytes: Bytes` request; `error_code` + `error_message` + `auth_bytes` + lifetime   |

Plus a small `pre_auth_keys` set lives in `sk-protocol` (not codec):
keys 17 + 18 + 36 — the Go `preSASLKeys` set verbatim.

**Scope boundary (what real clients exercise).** The Phase 4 broker
authenticates and authorizes every connection the kafka-console +
librdkafka + franz-rs SASL test suite throws at it: SCRAM-SHA-512
(`security.protocol=SASL_PLAINTEXT|SASL_SSL`,
`sasl.mechanism=SCRAM-SHA-512`), mTLS (`security.protocol=SSL` + client
cert against a CA the broker trusts), and PLAIN-with-static-credentials.
ACLs evaluate against the 8 Apache `Operation` values (Read, Write,
Create, Delete, Alter, Describe, DescribeConfigs, AlterConfigs) and 4
resource types (`topic`, `group`, `cluster`, `transactionalId`). Quotas
enforce `producerMaxByteRatePerBroker` / `consumerMaxByteRatePerBroker`
per-principal.

---

## Workstreams

Eight workstreams. A is small and unblocks G; B blocks C/D/E/F; C/D/E/F
land in parallel; G threads through `sk-protocol`; H closes with
`bins/skafka` + smoke.

- **A** — Codec backfill (SaslHandshake key 17, SaslAuthenticate key 36)
- **B** — `sk-auth` primitives + shared types (Principal, Resource, Operation, Quotas, AuthError)
- **C** — Credentials loader + SCRAM / PLAIN / mTLS engines
- **D** — Authorizer + ACL engine (AllowAll, SuperUser, Real)
- **E** — Quotas (token-bucket with debt-carry, gh #125)
- **F** — Principal mapping (gh #43, KIP-371)
- **G** — `sk-protocol` wire-up: pre-auth gate, SASL handlers, produce/fetch authorize+quota, mTLS extraction in server.rs
- **H** — `bins/skafka` TLS bring-up + CLI env + integration smoke

Dependencies: A blocks G; B blocks C/D/E/F; G blocked by C/D/E/F; H
blocked by G; A and B can land in parallel.

---

## A — Codec backfill (keys 17 + 36)

`crates/sk-codec/src/api/sasl_handshake.rs` and `sasl_authenticate.rs`,
same shape as the existing `init_producer_id.rs`. Owning `String` /
`Bytes` request types — control-path APIs, not hot-path. Add their
`SPEC` rows to `registry::ALL`.

`pre_auth_keys` lives in `sk-protocol/src/dispatch.rs`:

```rust
const PRE_AUTH_KEYS: &[i16] = &[17, 18, 36];
pub fn is_pre_auth(key: i16) -> bool { PRE_AUTH_KEYS.contains(&key) }
```

**Exit:** fixture round-trip for key 17 v0/v1 and key 36 v0/v1/v2;
`registry::ALL.len() == 8`.

---

## B — `sk-auth` primitives + types

`crates/sk-auth/src/lib.rs` grows the module tree:

```rust
pub mod acls;
pub mod authorizer;
pub mod credentials;
pub mod engine;
pub mod errors;
pub mod mtls;
pub mod plain;
pub mod principal_mapping;
pub mod quota;
pub mod scram;
pub mod selector;
```

Public types (`sk-protocol::connstate::Principal` is deleted and
re-exported from `sk-auth::Principal` so handlers have one canonical
type):

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Principal { pub name: String, pub kind: PrincipalKind }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PrincipalKind { User, ServiceAccount, Anonymous }

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resource { pub kind: ResourceKind, pub name: String, pub pattern: PatternType }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResourceKind { Topic, Group, Cluster, TransactionalId }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatternType { Literal, Prefix, Any, Match }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Operation { Read, Write, Create, Delete, Alter, Describe, DescribeConfigs, AlterConfigs }

#[derive(Debug, Clone)]
pub struct Quotas {
    pub producer_max_byte_rate_per_broker: Option<i64>,
    pub consumer_max_byte_rate_per_broker: Option<i64>,
    pub request_percentage: Option<i32>,
}
```

`sk-protocol` adds `sk-auth = { workspace = true }` as a regular dep
and `pub use sk_auth::Principal`.

`AuthError` enum (thiserror): `UnknownMechanism(String)`,
`MalformedSaslMessage`, `BadCredentials`, `BadCertificate`,
`Io(io::Error)`, `Json(serde_json::Error)`, `Regex(regex::Error)`,
`PrincipalMappingParse(String)`.

**Exit:** `cargo build -p sk-auth` succeeds; `sk-protocol` re-exports
`Principal` and downstream handlers compile against the merged type.

---

## C — Credentials loader + SCRAM / PLAIN / mTLS engines

`credentials.rs` mirrors `archive/internal/auth/loader.go` 1:1. Same
JSON shape (the Strimzi-compat file the operator writes):

```rust
#[derive(Debug, Deserialize)]
struct CredFile { version: u32, users: Vec<CredUser> }

#[derive(Debug, Deserialize)]
struct CredUser {
    username: String,
    #[serde(rename = "authType")] auth_type: String,
    scram: Option<ScramJson>,
    #[serde(rename = "tlsCN")] tls_cn: Option<String>,
    #[serde(rename = "serviceAccount")] sa: Option<SaJson>,
    quotas: Option<QuotasJson>,
}
```

`CredentialLoader` holds three `RwLock`-guarded maps: `by_username`,
`by_cn` (CN → username, for mTLS reverse lookup), `by_sa` (populated
but unused in Phase 4 — Phase 7 wires the K8s path). `reload(&self) ->
Result<(), AuthError>` reads-and-swaps; missing file is `Ok(())` so an
empty operator-side state doesn't fail broker boot. Decoded keys
(`stored_key`, `server_key`, `salt`) live as `Vec<u8>` from base64.

Trait `CredentialStore` mirrors the Go `CredentialStore` interface:
`lookup_scram`, `lookup_tls`, `lookup_sa`, `lookup_quotas`, plus
optional list accessors (`list_all_scram_users`, `list_all_quotas`) for
Phase 5/7 admin APIs.

`scram.rs` — SCRAM-SHA-512 server state machine over
`hmac::Hmac<Sha512>` + `sha2::Sha512`:

```rust
pub trait SaslExchange: Send {
    fn step(&mut self, client_msg: &[u8]) -> Result<(Vec<u8>, bool), AuthError>;
    fn principal(&self) -> Option<&Principal>;
}

pub struct ScramExchange<S: CredentialStore> {
    store: Arc<S>,
    state: ScramState,            // Start | AwaitFinal | Done
    username: String,
    client_first_bare: String,
    server_first: String,
    stored_key: Vec<u8>,
    server_key: Vec<u8>,
    full_nonce: String,
    principal: Option<Principal>,
}
```

Algorithm verbatim from `archive/internal/auth/scram.go`: GS2 strip,
parse `n=<user>,r=<clientNonce>`, generate 18-byte random suffix via
`rand::rngs::OsRng`, format `r=<full>,s=<base64salt>,i=<iterations>`,
on `client-final` compute
`auth_message = client_first_bare || ',' || server_first || ',' ||
client_final_without_proof`, verify `H(RecoveredKey) == StoredKey` via
`subtle::ConstantTimeEq`, return
`v=<base64(HMAC(server_key, auth_message))>`.

`plain.rs` — SASL PLAIN state machine. Phase 4 ships the
**static-credential** path: parse
`NUL || authzid || NUL || authcid || NUL || password`, look up the
credential record by `authcid`, compare password against an optional
`plainPassword` JSON field on `credentials.json`. The K8s-SA flavor is
a `PlainKind::Kubernetes` variant returning
`Err(UnknownMechanism("PLAIN-k8s not yet ported — Phase 7"))`.

`mtls.rs` — peer-cert principal extraction.
`mtls::authenticate(tls_session, store, mapper) -> Result<Principal>`.
Pulls `peer_certificates()` from
`tokio_rustls::server::TlsStream::get_ref().1`, parses the leaf cert
via the `x509-parser` crate (add to `[workspace.dependencies]`). Apply
the principal mapper to `peer.subject.to_string()` → if the result is
in `by_cn`, return `Principal { kind: User, name: ... }`. Mismatch →
`Err(BadCertificate)`.

**Exit:** `scram_test.rs` ports `archive/internal/auth/scram_test.go` —
a captured Apache 3.7 SCRAM exchange round-trips byte-equal (modulo the
RNG-driven nonce, which is re-injected via a deterministic-RNG seam).
`credentials_test.rs` round-trips the Strimzi-shape JSON. `mtls_test.rs`
round-trips an `rcgen`-minted cert chain.

---

## D — Authorizer + ACL engine

`acls.rs` mirrors `archive/internal/auth/acl.go` 1:1: `acls.json` JSON
shape, `AclRule` struct, deny-overrides-allow semantics,
`prefix`/`literal`/`any`/`match` pattern types, 5 s decision cache
keyed on `(principal_name, resource_type, resource_name, op)`. Use
`dashmap::DashMap` for the cache; the rule list itself sits behind
`arc_swap::ArcSwap<Vec<AclRule>>` so `reload()` swaps atomically and
the cache flushes in a separate step.

`authorizer.rs`:

```rust
pub trait Authorizer: Send + Sync + 'static {
    fn authorize(&self, p: &Principal, r: &Resource, op: Operation) -> bool;
}

pub struct AllowAllAuthorizer;
pub struct SuperUserAuthorizer { supers: HashSet<String>, inner: Arc<dyn Authorizer> }
// AclEngine: impl Authorizer for AclEngine
```

`AllowAllAuthorizer::authorize` always returns true;
`SuperUserAuthorizer::authorize` short-circuits if `p.name` is in the
set, else delegates. Matches the Go `SuperUserAuthorizer` shape
verbatim.

**Exit:** `acl_test.rs` ports `archive/internal/auth/acl_test.go` —
deny-wins, prefix vs literal vs any, `User:*` wildcard,
`Operation::All`. `super_user_test.rs` ports the early-allow contract.

---

## E — Quotas (token-bucket with debt-carry, gh #125)

`quota.rs` mirrors `archive/internal/auth/quota.go`. Single struct with
a `parking_lot::Mutex<HashMap<String, TokenBucket>>` (matches the Go
`sync.Mutex` shape and lock window — the critical section is dominated
by float arithmetic, not allocs, so `DashMap` would over-shard).
`TokenBucket` carries `producer_tokens: f64`, `consumer_tokens: f64`,
`last_refill: Instant`, `producer_rate: f64`, `consumer_rate: f64`.
Rates of `0.0` mean unlimited.

The check function is the gh #125 fix verbatim: deduct
unconditionally, let the bucket go negative, compute throttle from the
negative balance. **No clamp at 0.** The
`TestQuotaMultiClientContention`-shape test is reproduced in
`quota_test.rs` — three back-to-back drain → drain → drain calls must
yield monotonically increasing throttle, not three identical 1000 ms
responses.

`set_user_quota` + `describe_user_quota` + `list_user_quotas` ported in
the same shape — runtime overrides win over store-backed defaults; the
override map and the live bucket update together. This is the
Phase 5/7 surface for `AlterClientQuotas` / `DescribeClientQuotas`.

```rust
pub trait QuotaChecker: Send + Sync + 'static {
    fn check_produce_quota(&self, p: &Principal, bytes: usize) -> i32; // throttle_ms
    fn check_fetch_quota  (&self, p: &Principal, bytes: usize) -> i32;
}

pub struct NoQuotaChecker;
impl QuotaChecker for NoQuotaChecker { /* always 0 */ }

pub struct QuotaEnforcer<S: CredentialStore> { … }
impl<S: CredentialStore + 'static> QuotaChecker for QuotaEnforcer<S> { … }
```

`Instant::now()` calls go through a `Clock` trait (one impl:
`RealClock`; tests use a `MockClock` with a per-test
`Arc<Mutex<Instant>>`) so the multi-client contention test is
deterministic.

**Exit:** `quota_test.rs` ports the full Go suite (under-limit,
over-limit, unlimited, per-principal, multi-client contention). The
contention test fails fast if anyone re-introduces the
`*tokens < 0 → *tokens = 0` clamp.

---

## F — Principal mapping (gh #43, KIP-371)

`principal_mapping.rs` ports
`archive/internal/auth/principal_mapping.go` line-for-line. The `regex`
crate is already in `[workspace.dependencies]`. Two surfaces:

```rust
pub struct PrincipalMapper { rules: Vec<Rule> }

impl PrincipalMapper {
    pub fn parse(spec: &str) -> Result<Self, AuthError>;
    pub fn apply(&self, subject_dn: &str, cn: &str) -> String;
}
```

`Rule` enum: `Default` (return CN unchanged) | `Rule { regex,
replacement, case_flag }`. `CaseFlag = None | Lower | Upper`. The
`split_rules` heuristic ports verbatim — split only at `,` followed by
`RULE:` or `DEFAULT` so DN commas don't tear the regex body.

**Exit:** `principal_mapping_test.rs` ports the full Go suite. Add a
`proptest` round-trip: a random DN + a random
`RULE:^CN=([^,]+).*$/$1/L`-shape rule, assert the output is
`cn.to_lowercase()`.

---

## G — `sk-protocol` wire-up

Three changes:

### 1. Pre-auth gate in `dispatch.rs`

Add an `engines: Option<Arc<dyn AuthEngineSelector>>` field on
`Dispatcher`; `Dispatcher::with_auth(selector)` setter (registered
alongside `register`). The gate becomes — paraphrased, drops in where
the current "Phase 4 wires" stub lives:

```rust
if let Some(sel) = self.engines.as_ref() {
    let listener = conn.lock().listener_name.clone();
    let eng = sel.for_listener(&listener);
    if eng.requires_pre_auth() && !conn.lock().sasl_done && !is_pre_auth(api_key) {
        return error_body(spec, header.api_version, ERR_CLUSTER_AUTHORIZATION_FAILED);
    }
}
```

`ERR_CLUSTER_AUTHORIZATION_FAILED = 31` is already defined in
`dispatch.rs`.

### 2. SASL handlers in `sk-broker/src/handlers/sasl.rs`

One file, two `Handler` impls — matches Go's
`archive/internal/protocol/handlers/sasl.go`:

- `SaslHandshakeHandler { mechanisms: Vec<String> }` — advertises
  `["SCRAM-SHA-512", "PLAIN"]`; on first match, stamps
  `conn.sasl_mechanism = Some(req.mechanism)`. `ConnState` grows a
  `pub sasl_mechanism: Option<String>` field.
- `SaslAuthenticateHandler { engines: Arc<dyn AuthEngineSelector> }` —
  on first call, calls
  `engines.for_listener(name).new_sasl_exchange(mech)`; subsequent
  calls drive `exchange.step(auth_bytes)`; on `done`, stamp
  `conn.principal = Some(p); conn.sasl_done = true`. Reject PLAIN over
  non-TLS connections: if `conn.sasl_mechanism == "PLAIN" &&
  !conn.is_tls` → `NETWORK_EXCEPTION` (13).

`ConnState` grows two fields:

- `pub is_tls: bool` (set by the server when the accept loop wraps in
  `TlsAcceptor`).
- `pub sasl_state: Option<Box<dyn SaslExchange>>` (the per-connection
  exchange that survives across `SaslAuthenticate` calls; same shape as
  Go's `conn.SASLState`).

### 3. `produce.rs` / `fetch.rs` authorize + quota

Inject `Arc<dyn Authorizer>` + `Arc<dyn QuotaChecker>` into the
handlers (carried on `Broker`). Per partition:

```rust
let principal = conn.lock().principal.clone().unwrap_or_else(Principal::anonymous);
let resource = Resource::topic(&topic_name);
if !self.authorizer.authorize(&principal, &resource, Operation::Write) {
    return error_partition(p.index, ERR_TOPIC_AUTHORIZATION_FAILED /* 29 */);
}
let throttle_ms = self.quotas.check_produce_quota(&principal, total_bytes);
// propagate throttle_ms into the response
```

Same shape for Fetch (`Operation::Read`, `ERR_TOPIC_AUTHORIZATION_FAILED`,
`check_fetch_quota`). Anonymous principal is
`Principal { kind: PrincipalKind::Anonymous, name: "ANONYMOUS".into() }`;
the `AclEngine` denies it by default unless an ACL grants
`User:ANONYMOUS` explicitly (Strimzi-compat semantic, gh #126).

### 4. mTLS principal extraction in `server.rs`

After `TlsAcceptor::accept`, before the request loop, pull
`tls_stream.get_ref().1.peer_certificates()`; if present and the
listener's `AuthEngine` is `mtls`-capable, run `mtls::authenticate(...)`
and stamp `conn.principal` + `conn.sasl_done = true`. On rejection: log
+ close. Matches the Go path at
`archive/internal/protocol/server.go:251-292`.

**Exit:** unit tests in `sk-protocol` cover pre-auth gate (real engine
+ no SASL → 31; allow-all engine → through; key 18 always through);
the SASL handlers cover handshake → unknown mech 33; happy SCRAM round
trip; PLAIN-over-plain → 13.

---

## H — `bins/skafka` TLS bring-up + smoke

### CLI additions in `sk-broker/src/cli.rs`

```rust
pub struct Cli {
    // existing fields …
    pub auth_disabled: bool,                  // SKAFKA_AUTH_DISABLED (default false)
    pub authorization_type: String,           // SKAFKA_AUTHORIZATION_TYPE: "" | "simple"
    pub super_users: Vec<String>,             // SKAFKA_SUPER_USERS, comma-separated User:foo,User:bar
    pub ssl_principal_mapping_rules: String,  // SKAFKA_SSL_PRINCIPAL_MAPPING_RULES (default empty → CN)
}
```

`ListenerEntry` grows three fields (kept on the existing JSON shape):

```rust
pub struct ListenerEntry {
    // existing …
    pub tls: Option<TlsConfig>,                  // None → plain
    pub authentication_type: Option<String>,     // "none" | "scram-sha-512" | "mtls" | "plain"
}

pub struct TlsConfig {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub client_ca_path: Option<PathBuf>,         // Some → require client cert (mTLS)
}
```

### `bins/skafka/main.rs` boot order

1. Parse `Cli`. Build `CredentialLoader` from
   `<data_dir>/__cluster/credentials.json` (or in-memory empty store
   if `auth_disabled`).
2. Build `AclEngine` from `<data_dir>/__cluster/acls.json`.
3. Build `Authorizer`: if `authorization_type == "simple"` and
   `!auth_disabled` → `AclEngine`; else `AllowAllAuthorizer`. Wrap in
   `SuperUserAuthorizer` if `super_users` is non-empty.
4. Build `QuotaEnforcer` wrapping the loader; or `NoQuotaChecker` if
   `auth_disabled`.
5. Build `PrincipalMapper::parse(&cli.ssl_principal_mapping_rules)?`.
6. Per listener: pick the per-listener `AuthEngine` based on
   `authentication_type` — `none` → `AllowAllAuthEngine`;
   `scram-sha-512`/`plain`/`mtls` → `RealAuthEngine { creds, mapper }`.
   Build `AuthEngineSelector` as a
   `HashMap<String, Arc<dyn AuthEngine>>` keyed on `listener.name`.
7. Load TLS configs per listener with
   `tokio_rustls::rustls::ServerConfig::builder()` + `rustls_pemfile`
   (add to workspace deps). `client_ca_path: Some(...)` →
   `with_client_cert_verifier(...)`; else `with_no_client_auth()`.
8. Wire `Broker` to carry `authorizer: Arc<dyn Authorizer>` +
   `quotas: Arc<dyn QuotaChecker>`.
9. Register `Sasl{Handshake,Authenticate}` handlers; spawn a
   `tokio::time::interval(Duration::from_secs(10))` task that calls
   `loader.reload()` + `acls.reload()`.

### `bins/skafka/tests/auth_smoke.rs`

Four variants on top of the existing Phase 3 smoke harness:

- SCRAM-SHA-512 happy path: produce 100 records, fetch them back.
- mTLS happy path: same, with an `rcgen` cert chain.
- ACL deny: try to produce as a principal with no Write ACL on the
  topic → 29 wire error.
- Quota throttle: produce 5 MB to a principal with a 1 MB/s quota →
  response carries `throttle_time_ms > 0`.

**Exit:** all four smoke variants pass; SIGTERM still drains cleanly;
`cargo xtask ci` stays under 4 min on a warm cache.

---

## Phase 4 exit criteria (all must hold)

1. `cargo test --workspace --all-features` green, under 5 min warm.
2. `cargo clippy --workspace --all-targets -- -D warnings` and
   `cargo fmt --check` pass.
3. `sk_codec::api::registry::ALL.len() == 8` — keys 17 + 36 added with
   version ranges per the table.
4. `bins/skafka` binds at least one TLS listener; mTLS handshake
   against an `rcgen`-minted client cert succeeds and the principal
   lands on `ConnState::principal`.
5. SASL-SCRAM-SHA-512 against a `kafka-console-producer` configured
   with `security.protocol=SASL_PLAINTEXT, sasl.mechanism=SCRAM-SHA-512,
   sasl.jaas.config=...` produces and consumes records byte-equal.
6. With `SKAFKA_AUTHORIZATION_TYPE=simple` and no ACLs, every
   operation by a non-super-user returns 29
   (`TOPIC_AUTHORIZATION_FAILED`); adding an explicit
   `User:alice → topic/foo/Write` ACL lets alice produce to `foo` and
   only `foo`.
7. The gh #125 quota debt-carry test passes — three back-to-back
   `check_produce` calls on the same drained bucket yield strictly
   increasing throttle.
8. The pre-auth gate denies non-pre-SASL APIs on a `requires_pre_auth`
   listener with `ERR_CLUSTER_AUTHORIZATION_FAILED = 31`; keys 17, 18,
   36 always pass.
9. `sk_codec::tripwires::record_decode_count()` and
   `batch_reencode_count()` both read 0 after the auth smoke suite —
   auth didn't introduce a record-decode site.
10. Go tree under `archive/` unchanged; chart, CRDs, scripts, and
    `proto/heartbeat.proto` are bit-identical to their pre-Phase-4
    contents.

If any of these fail, do not merge — fix and re-run.

---

## Risks & mitigations

- **`rustls`'s subject-DN handling vs Apache's Java decoder.** `rustls`
  exposes the leaf cert as raw DER; Go's `crypto/x509` returns a parsed
  `pkix.Name`. The two render the DN to a string with subtly different
  separators (`,` vs `, `; component ordering). The principal mapper's
  regex is keyed on the rendered string, so a DN that mapped under the
  Go broker might miss under the Rust one. Mitigation: standardise on
  `x509_parser::prelude::X509Certificate::subject().to_string()` and
  document the exact rendering in `principal_mapping.rs`; port the Go
  `TestPrincipalMapper` table cases as fixtures of *DN-as-rendered-by-
  x509-parser* so a future renderer swap surfaces in tests.
- **Hot-reload via `tokio::time::interval` vs `notify` inotify.** The
  Go side uses inotify; the Phase 4 plan ships interval-poll. On a 10s
  tick a credential rotation hits a 10s window where the old creds
  still authenticate. For ops that's typically fine; for a
  tight-rotation case (e.g. cert-manager 1m TTL) it's a problem.
  Mitigation: ship interval-poll in Phase 4 (small surface, no `notify`
  portability surprises) but file a follow-up to swap in `notify`
  during Phase 8 alongside the observability bring-up.
- **SCRAM nonce determinism breaks fixture round-trip tests.** The
  server emits an 18-byte random nonce; running the test twice yields
  different bytes. Mitigation:
  `ScramExchange::new_with_rng(store, rng)` — production calls
  `OsRng`; tests inject a `StepRng` so the byte sequence is
  reproducible. Fixture round-trip pins the deterministic case.
- **Anonymous-principal default-deny breaks existing dev setups.**
  Today the Phase 3 broker authorises everything. Flipping
  `RealAuthEngine` on a chart that doesn't set ACLs would deny every
  Produce/Fetch from in-cluster bench tooling. Mitigation:
  `SKAFKA_AUTHORIZATION_TYPE=""` (default, not `simple`) keeps
  `AllowAllAuthorizer`; the chart explicitly opts in. Document in
  `CLAUDE.md` "Authentication is per-listener; authorization is
  cluster-wide" so a reader doesn't think the listener
  `authentication.type: none` covers them.
- **`subtle::ConstantTimeEq` vs the Go `crypto/subtle.ConstantTimeCompare`.**
  Both crates implement timing-safe equality; both are constant-time on
  the byte-length-equal path. Mitigation: skim the `subtle` crate docs
  in the PR description; not a code change.
- **PLAIN-static-credential isn't in the Go tree.** Adding it now risks
  divergence — a future port of the Go SA path would need to coexist.
  Mitigation: gate static PLAIN behind a `plainPassword` JSON field on
  `credentials.json` that the operator never writes; production
  credentials never carry it, so the static path is "test-only by
  virtue of nobody operating with it on". Reassess in Phase 7 when the
  K8s-SA `PlainKind::Kubernetes` lands and we can decide whether to
  delete the static path.
- **The `engines` selector field on `Dispatcher`.** The Go side has
  `SetAuthEngines` for runtime swaps. Phase 4 wires the selector at
  `Dispatcher::new`-time and never swaps. If a Phase 7/5 watcher needs
  to swap engines mid-flight, the field grows an `ArcSwap<...>`.
  Mitigation: explicit decision in the plan; don't pre-build the swap
  surface until something needs it.

---

## What this enables for Phase 5

After Phase 4 merges, Phase 5 (coordinator & assignment) lands by:

1. Replacing the `LocalLeaseManager` with the real `Coordinator`
   (assignment.json watcher) and threading `Authorizer` calls into the
   coordinator path (consumer-group APIs need `Operation::Read` on
   `Group` + `Operation::Describe`).
2. The codec backfill adds keys 8–16, 32, 42, 47, etc. — the
   coordinator/group surface.
3. The existing `AuthEngineSelector` + per-listener engine map plug
   straight into the new heartbeat-driven coordinator without further
   auth changes.
4. `DescribeClientQuotas` (48) / `AlterClientQuotas` (49) admin
   handlers become thin wrappers over `QuotaEnforcer::describe_user_quota`
   / `set_user_quota` (the accessors Phase 4 already shipped).

No further `sk-auth` changes — Phase 5 consumes the stable surface.
