# Phase 7 Breakdown: Authentication Engine

## Current State (end of Phase 6)

All tests pass. The broker runs with `AllowAllAuthEngine` (accepts everything) and
`DenyAllAuthEngine` (rejects everything — used in tests). The operator writes
`credentials.json` and `acls.json` atomically to `__cluster/`. The Phase 3
`ClusterFileWatcher` delivers inotify callbacks but its `onACLReload` and
`onCredReload` slots are wired to `nil` — the reload logic is what Phase 7 provides.

### Key files before starting Phase 7

| File | Role |
|---|---|
| `internal/auth/auth.go` | `AuthEngine` interface — **replaced** in Step 7.0 |
| `internal/broker/stubs.go` | `AllowAllAuthEngine` — updated in Step 7.0 |
| `internal/connstate/connstate.go` | `ConnState` — gains `SASLMechanism`, `SASLState` |
| `internal/protocol/handlers/sasl.go` | SASL handlers — rewritten for multi-step |
| `internal/protocol/dispatch.go` | Dispatcher — gains SASL enforcement gate |
| `internal/storage/watcher.go` | `ClusterFileWatcher` — callbacks wired in Step 7.6 |
| `operator/controllers/credentials.go` | `CredentialsFile` format — reused for reading |

### No new dependencies needed

All required libraries are already in `go.mod`:
- `golang.org/x/crypto` — SCRAM PBKDF2 (just added in Phase 6)
- `k8s.io/client-go` — TokenReview API
- `k8s.io/api/authentication/v1` — `TokenReview` type

---

## Interface changes at the start of Phase 7

### AuthEngine: single-step → exchange-based

The current `Authenticate(ctx, Credentials) (Principal, error)` cannot support SCRAM's
two-message exchange. Replace it with an exchange-based model. Also add `AuthenticateTLS`
for mTLS peer cert auth and `Reload` for hot-reload from disk.

File: `internal/auth/auth.go`

```go
// SASLExchange is the per-connection state machine for one SASL mechanism.
// It is created once per connection (after SaslHandshake) and step-called
// for each SaslAuthenticate message.
type SASLExchange interface {
    // Step processes the next client message and returns the server's response.
    // When done=true, authentication is complete and Principal() is valid.
    Step(clientMsg []byte) (serverMsg []byte, done bool, err error)
    Principal() Principal
}

// AuthEngine authenticates connections and authorizes operations.
type AuthEngine interface {
    // NewSASLExchange returns a fresh exchange for the given mechanism name
    // (e.g. "SCRAM-SHA-512", "PLAIN"). Returns an error for unsupported mechanisms.
    NewSASLExchange(mechanism string) (SASLExchange, error)
    // AuthenticateTLS authenticates a connection whose TLS peer cert has the given CN.
    AuthenticateTLS(cn string) (Principal, error)
    // Authorize checks whether principal may perform op on resource.
    Authorize(principal Principal, resource Resource, operation Operation) bool
}
```

### ConnState: add SASL exchange state

File: `internal/connstate/connstate.go`

```go
type ConnState struct {
    ClientID      string
    Principal     *auth.Principal
    SASLDone      bool
    SASLMechanism string          // set by SaslHandshakeHandler
    SASLState     auth.SASLExchange // nil until exchange started
}
```

### AllowAllAuthEngine: updated stub

```go
func (a *AllowAllAuthEngine) NewSASLExchange(_ string) (auth.SASLExchange, error) {
    return &allowAllExchange{}, nil
}
func (a *AllowAllAuthEngine) AuthenticateTLS(_ string) (auth.Principal, error) {
    return auth.Principal{Name: "ANONYMOUS", Kind: "User"}, nil
}
// Authorize stays true.
```

`allowAllExchange.Step` immediately returns `done=true` with an ANONYMOUS principal.

These three interface changes touch: `auth.go`, `stubs.go`, `sasl.go`, and the new
`internal/auth/engine.go`. Do them first.

---

## Step 7.1 — SCRAM-SHA-512 server exchange

File: `internal/auth/scram.go`

### Protocol overview (RFC 5802)

```
Client → Server: n,,n=username,r=clientNonce
Server → Client: r=clientNonce+serverNonce,s=base64Salt,i=8192
Client → Server: c=biws,r=fullNonce,p=base64ClientProof
Server → Client: v=base64ServerSignature
```

### ScramExchange struct

```go
type ScramExchange struct {
    store        CredentialStore  // supplies StoredKey+ServerKey for a username
    state        int              // 0=expect-client-first, 1=expect-client-final
    username     string
    clientNonce  string
    serverNonce  string
    serverFirst  string           // saved for AuthMessage construction
    clientFirstBare string        // saved for AuthMessage construction
    storedKey    []byte
    serverKey    []byte
    principal    auth.Principal
}
```

`CredentialStore` is an interface with one method:
```go
type CredentialStore interface {
    Lookup(username string) (StoredKey, ServerKey []byte, Salt []byte, Iterations int, ok bool)
}
```

### Step 0 → Step 1 (client-first → server-first)

```
Parse: "n,,n=alice,r=xyz123"
Extract: username="alice", clientNonce="xyz123"
Lookup credentials for "alice" (or return ErrUnknownUser)
Generate serverNonce (16 random bytes, base64)
Build: "r=xyz123+serverNonce,s=base64Salt,i=8192"
Save clientFirstBare="n=alice,r=xyz123" and serverFirst for AuthMessage
Return serverFirst, done=false
```

### Step 1 → done (client-final → server-final)

```
Parse: "c=biws,r=fullNonce,p=base64ClientProof"
Verify nonce matches
Build AuthMessage = clientFirstBare + "," + serverFirst + "," + clientFinalWithoutProof
Verify client proof:
    ClientSignature = HMAC(StoredKey, AuthMessage)
    RecoveredKey    = ClientProof XOR ClientSignature
    if H(RecoveredKey) != StoredKey → authentication failure
Compute ServerSignature = HMAC(ServerKey, AuthMessage)
Return "v=base64ServerSignature", done=true
Set principal = Principal{Name: username, Kind: "User"}
```

**Done when:** unit tests with fixed RFC 5802 test vectors verify the full two-step
exchange. Also verify that a wrong password returns an error (not a panic).

---

## Step 7.2 — Credentials loader and hot-reload

File: `internal/auth/loader.go`

The loader reads `credentials.json` (written by the operator) into an in-memory map and
implements `CredentialStore`. It also exposes a `Reload()` method wired to the
`ClusterFileWatcher.onCredReload` callback.

```go
type CredentialLoader struct {
    path string
    mu   sync.RWMutex
    data map[string]loadedCred // username → cred
}

type loadedCred struct {
    authType   string
    storedKey  []byte  // for scram
    serverKey  []byte
    salt       []byte
    iterations int
    tlsCN      string  // for tls
    saName     string  // for kubernetes-serviceaccount
    saNamespace string
    quotas     *auth.Quotas
}
```

`Reload()` atomically replaces the in-memory map from disk. The map is replaced
wholesale (not updated in-place) to avoid partial reads from concurrent lookups.

This file also defines `auth.Quotas`:
```go
type Quotas struct {
    ProducerByteRate  *int64
    ConsumerByteRate  *int64
    RequestPercentage *int32
}
```

---

## Step 7.3 — mTLS authentication

File: `internal/auth/tls.go`

When the broker accepts a TLS connection with a client certificate, it extracts the CN
and calls `AuthenticateTLS`:

```go
func (e *RealAuthEngine) AuthenticateTLS(cn string) (auth.Principal, error) {
    cred, ok := e.creds.LookupTLS(cn)
    if !ok {
        return auth.Principal{}, fmt.Errorf("tls: unknown CN %q", cn)
    }
    return auth.Principal{Name: cred.username, Kind: "User"}, nil
}
```

Wiring: in `server.go`'s `serveConn`, after `tls.Conn.Handshake()`, check
`tlsConn.ConnectionState().PeerCertificates`. If any cert is present, call
`AuthenticateTLS(cert.Subject.CommonName)` and set `conn.Principal`.

The TLS listener already exists at `:9093` (Phase 1 skeleton). The `Config.TLSConfig`
needs `ClientAuth: tls.RequestClientCert` (not `RequireAnyClientCert` — mTLS is
optional, SCRAM users connect to plain 9092).

**Done when:** unit test: create a TLS connection with a self-signed cert whose CN is
a registered user, verify `AuthenticateTLS` returns the correct principal.

---

## Step 7.4 — Kubernetes ServiceAccount JWT

File: `internal/auth/serviceaccount.go`

The PLAIN SASL mechanism carries the SA JWT in the password field:
```
\0\0{jwt-token}   (username and authzid are empty)
```

```go
type SAExchange struct {
    k8sClient kubernetes.Interface
    store     CredentialStore
    principal auth.Principal
}

func (e *SAExchange) Step(clientMsg []byte) ([]byte, bool, error) {
    // Parse PLAIN: NUL + authzid + NUL + authcid + NUL + password
    // (authzid and authcid may be empty; password = JWT token)
    parts := bytes.SplitN(clientMsg, []byte{0}, 3)
    if len(parts) != 3 {
        return nil, false, errors.New("sasl: malformed PLAIN message")
    }
    jwt := string(parts[2])

    tr := &authv1.TokenReview{
        Spec: authv1.TokenReviewSpec{Token: jwt},
    }
    result, err := e.k8sClient.AuthenticationV1().TokenReviews().Create(
        context.Background(), tr, metav1.CreateOptions{})
    if err != nil || !result.Status.Authenticated {
        return nil, false, errors.New("sasl: SA token authentication failed")
    }

    // Status.User.Username is "system:serviceaccount:{namespace}:{name}"
    name, ns := parseSAUsername(result.Status.User.Username)

    // Verify the SA is registered in credentials.json.
    if !e.store.LookupSA(ns, name) {
        return nil, false, fmt.Errorf("sasl: SA %s/%s not registered", ns, name)
    }

    e.principal = auth.Principal{
        Name: fmt.Sprintf("ServiceAccount:%s/%s", ns, name),
        Kind: "ServiceAccount",
    }
    return nil, true, nil
}
```

**Done when:** unit test with a fake k8s client that returns a successful `TokenReview`,
verifying the principal is set correctly.

---

## Step 7.5 — ACL enforcement engine

File: `internal/auth/acl.go`

```go
type ACLEngine struct {
    path  string
    mu    sync.RWMutex
    rules []aclRule

    cacheMu sync.Mutex
    cache   map[cacheKey]cachedDecision
}

type aclRule struct {
    principal  string   // "User:alice" or "Group:team"
    resource   auth.Resource
    operations map[auth.Operation]bool
    permission string   // "Allow" | "Deny"
}

type cacheKey struct {
    principal string
    resType   string
    resName   string
    op        auth.Operation
}

type cachedDecision struct {
    allowed   bool
    expiresAt time.Time
}
```

### Authorize logic

```
1. Check cache (5s TTL). Return cached decision if still valid.
2. For each rule matching principal AND resource AND operation:
     If permission == "Deny": deny immediately (deny takes precedence).
3. If any matching rule has permission == "Allow": allow.
4. Default: deny (deny-by-default).
5. Cache the decision for 5s.
6. Log denied decisions at WARN with principal, resource, operation.
```

### Pattern matching

| PatternType | Match condition |
|---|---|
| `literal` | `rule.Name == resource.Name` |
| `prefix` | `strings.HasPrefix(resource.Name, rule.Name)` |
| `any` | always matches |
| `match` | same as prefix for Phase 7 (full regex deferred) |

Resource type must also match (or be `"*"`).

### Principal matching

A rule matches the request principal if:
- `rule.principal == "User:*"` (wildcard — matches any user)
- `rule.principal == "User:" + principal.Name` (exact user match)
- `rule.principal == "Group:X"` — Phase 7 groups are pre-expanded to users by the
  operator, so this only needs to match `"Group:X"` literally (no group expansion needed
  in the broker — the operator already wrote individual `User:member` entries for each
  group member)

### Reload

`ACLEngine.Reload(path)` reads `acls.json`, parses, atomically replaces `e.rules`,
and flushes the cache.

**Done when:** unit tests covering:
- Deny takes precedence over Allow on the same resource
- Prefix matching (`payments-` matches `payments-events`)
- Wildcard user `User:*` matches any principal
- Default-deny for unmatched rules
- Cache TTL: two calls within 5s → second does not scan rules
- `Reload` picks up changed rules

---

## Step 7.6 — Quota enforcement

File: `internal/auth/quota.go`

```go
type QuotaEnforcer struct {
    mu     sync.Mutex
    buckets map[string]*tokenBucket // principal name → bucket
}

type tokenBucket struct {
    producerTokens float64
    consumerTokens float64
    lastRefill     time.Time
    producerRate   float64 // bytes/sec; 0 = unlimited
    consumerRate   float64
}

func (q *QuotaEnforcer) CheckProduce(principal auth.Principal, bytes int) int32
func (q *QuotaEnforcer) CheckFetch(principal auth.Principal, bytes int) int32
// Returns ThrottleTimeMs (0 = no throttle).
```

Token bucket logic:
```
refill: tokens += rate * elapsed_seconds
tokens = min(tokens, rate)         // cap at 1-second burst
if tokens >= bytes: deduct and return 0
throttleMs = ceil((bytes - tokens) / rate * 1000)
return throttleMs
```

`QuotaEnforcer` is populated from `CredentialLoader.Quotas` fields.

Wiring: `ProduceHandler.Handle` calls `quota.CheckProduce` and sets
`pr.ThrottleTimeMs`; `FetchHandler.Handle` calls `quota.CheckFetch`.

**Done when:** unit test: produce 2× the allowed quota in one call, verify
`ThrottleTimeMs > 0`.

---

## Step 7.7 — RealAuthEngine + wire

File: `internal/auth/engine.go`

```go
type RealAuthEngine struct {
    creds   *CredentialLoader
    acls    *ACLEngine
    quotas  *QuotaEnforcer
    k8s     kubernetes.Interface // nil if SA auth disabled
}

func NewRealAuthEngine(dataDir string, k8s kubernetes.Interface) (*RealAuthEngine, error)

func (e *RealAuthEngine) NewSASLExchange(mechanism string) (SASLExchange, error) {
    switch mechanism {
    case "SCRAM-SHA-512":
        return newScramExchange(e.creds), nil
    case "PLAIN":
        if e.k8s != nil {
            return newSAExchange(e.k8s, e.creds), nil
        }
        return nil, fmt.Errorf("PLAIN requires kubernetes client")
    }
    return nil, fmt.Errorf("unsupported mechanism: %q", mechanism)
}

func (e *RealAuthEngine) Reload() {
    _ = e.creds.Reload()
    _ = e.acls.Reload()
    e.quotas.UpdateFromCreds(e.creds)
    e.acls.flushCache()
}
```

### SaslHandshakeHandler: set mechanism on conn

```go
func (h *SaslHandshakeHandler) Handle(conn *connstate.ConnState, ...) {
    // existing validation logic ...
    if errCode == 0 {
        conn.SASLMechanism = req.Mechanism
    }
    // ...
}
```

### SaslAuthenticateHandler: multi-step exchange

```go
func (h *SaslAuthenticateHandler) Handle(conn *connstate.ConnState, ...) {
    // First call: create the exchange.
    if conn.SASLState == nil {
        exch, err := h.auth.NewSASLExchange(conn.SASLMechanism)
        if err != nil { /* return error response */ }
        conn.SASLState = exch
    }

    serverMsg, done, err := conn.SASLState.Step(req.AuthBytes)
    if err != nil { /* return error response */ }

    if done {
        p := conn.SASLState.Principal()
        conn.Principal = &p
        conn.SASLDone = true
    }

    resp := &api.SaslAuthenticateResponse{AuthBytes: serverMsg}
    // ...
}
```

### Dispatcher SASL gate

Add `RequireSASL bool` to `Dispatcher`. In `Dispatch`:
```go
// API keys allowed before SASL: ApiVersions(18), SaslHandshake(17), SaslAuthenticate(36).
var preSASLKeys = map[int16]bool{17: true, 18: true, 36: true}

if d.RequireSASL && !connState.SASLDone && !preSASLKeys[hdr.APIKey] {
    return errorResponse(hdr, int16(codec.ErrClusterAuthorizationFailed)), nil
}
```

### Wire ClusterFileWatcher callbacks

In `cmd/skafka/main.go`, when `SKAFKA_DATA_DIR` is set:

```go
authEngine, err := auth.NewRealAuthEngine(dataDir, k8sClient)

watcher := storage.NewClusterFileWatcher(
    filepath.Join(dataDir, "__cluster", "acls.json"),
    filepath.Join(dataDir, "__cluster", "credentials.json"),
    func(_ string) { authEngine.Reload() },
    func(_ string) { authEngine.Reload() },
)
go func() { _ = watcher.Run(ctx.Done()) }()
```

When `SKAFKA_DATA_DIR` is not set: use `AllowAllAuthEngine` (unchanged).

Also set `d.RequireSASL = os.Getenv("SKAFKA_REQUIRE_SASL") == "true"`.

**Done when:** the kafka-compat tests still pass (they use AllowAllAuthEngine) AND a new
integration test produces and consumes using SCRAM-SHA-512 credentials against the full
broker.

---

## Testing strategy

| Test | File | What it verifies |
|---|---|---|
| SCRAM two-step exchange | `internal/auth/scram_test.go` | RFC 5802 vectors, wrong password → error |
| SCRAM wrong nonce | same | `Step` returns error if fullNonce differs |
| mTLS CN lookup | `internal/auth/tls_test.go` | registered CN → principal; unknown CN → error |
| SA JWT happy path | `internal/auth/serviceaccount_test.go` | fake k8s TokenReview succeeds |
| SA JWT unregistered | same | registered in k8s but not in credentials → error |
| ACL deny-over-allow | `internal/auth/acl_test.go` | Deny rule beats Allow on same resource |
| ACL prefix match | same | `payments-` matches `payments-events` |
| ACL wildcard user | same | `User:*` matches any principal |
| ACL default deny | same | no matching rule → deny |
| ACL cache TTL | same | second call within 5s reuses cached result |
| ACL reload | same | changed rules take effect after Reload |
| Quota throttle | `internal/auth/quota_test.go` | over-quota produce → ThrottleTimeMs > 0 |
| CredentialLoader reload | `internal/auth/loader_test.go` | Reload picks up new credentials |
| Dispatcher SASL gate | `internal/protocol/dispatch_test.go` | non-SASL request before auth → error 31 |

---

## Step order summary

| Step | Files | Depends on |
|---|---|---|
| 7.0 Interface + stubs | `auth/auth.go`, `stubs.go`, `connstate.go` | nothing |
| 7.1 SCRAM exchange | `auth/scram.go` | 7.0 |
| 7.2 Credential loader | `auth/loader.go` | 7.0 |
| 7.3 mTLS | `auth/tls.go` | 7.0, 7.2 |
| 7.4 SA JWT | `auth/serviceaccount.go` | 7.0, 7.2 |
| 7.5 ACL engine | `auth/acl.go` | 7.0 |
| 7.6 Quota enforcer | `auth/quota.go` | 7.0, 7.2 |
| 7.7 RealAuthEngine + wire | `auth/engine.go`, `handlers/sasl.go`, `dispatch.go`, `main.go` | 7.1–7.6 |

Steps 7.1–7.6 are all independent and can be implemented in parallel once 7.0 is done.
