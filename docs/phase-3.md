# Phase 3 — Single-broker server

Detailed work plan for the fourth phase of the Rust rewrite. Companion to
[`rewrite.md`](./rewrite.md); the high-level summary lives there. Builds
on the codec scaffolding from [`phase-1.md`](./phase-1.md) and the
storage engine from [`phase-2.md`](./phase-2.md).

**Goal.** Land a single-broker `bins/skafka` binary that a real
`kafka-console-producer` and `kafka-console-consumer` round-trip cleanly
against. In-memory and on-disk storage modes both work; SIGTERM drains
cleanly without NFS silly-rename pain. No auth, no cluster, no consumer
groups, no transactions — those are Phases 4–7.

**Length.** ~2 weeks, single engineer. Workstream A (codec backfill)
unblocks the rest; once A is in, B/C/D/E/F can land in parallel; G/H
thread them together.

**Out of scope for Phase 3.** SASL handshake routing (Phase 4 —
key 17 + 36 land then), per-listener auth gate (Phase 4), consumer
groups (Phase 5 — keys 8–16, 42, 47), transactions beyond
non-transactional `InitProducerId` (Phase 6 — keys 22 transactional
path, 24, 25, 26, 27, 28), admin handlers (Phase 5/7 — keys 19, 20,
29–32, 37, 44, 48–51, 60), heartbeat-gRPC (Phase 5), `KafkaTopic`
CR watch (Phase 5/7 — topics seeded from `SKAFKA_TOPICS` env JSON
for the smoke test), `SegmentRef` / sendfile splice (Phase 8 — the
Bytes-returning Fetch path is the only Phase 3 contract).

**Prerequisite.** Phase 1 today ships exactly one of its 40 codec
modules (`api_versions.rs`). Phase 3 needs five more — Produce (key 0),
Fetch (key 1), ListOffsets (key 2), Metadata (key 3), InitProducerId
(key 22). Workstream A frontloads them; the remaining 34 keys land
alongside their Phase 4/5/6/7 consumers per the comment on gh #144.

**Scope boundary.** The Phase 3 broker handles every API key the
client-side hot path of `kafka-console-producer` + `kafka-console-consumer`
issues, plus the bootstrap calls those clients gate startup on. That
is keys **0, 1, 2, 3, 18, 22** — six entries. Anything outside that set
returns `UNSUPPORTED_VERSION` (35) until a later phase adds it.

---

## Workstreams

Eight workstreams. A blocks all downstream codec work; once A is in,
B/C/D/E/F land in any order; G threads them through `Broker`; H closes
with the `bins/skafka` binary + smoke test.

- **A** — Codec backfill (5 missing API modules in `sk-codec`)
- **B** — `sk-protocol::frame` connection wrapper
- **C** — `sk-protocol::connstate` per-connection state
- **D** — `sk-protocol::dispatch` API-key router
- **E** — `sk-protocol::server` multi-listener accept loop
- **F** — `sk-protocol::handlers/` one file per API
- **G** — `sk-broker` minimal glue (Broker + LocalLeaseManager + TopicRegistry)
- **H** — `bins/skafka/main.rs` + smoke test harness

Dependencies: A blocks F; B/C land any time after A starts; D blocks F;
E blocks H; G blocks F (handlers need a Broker context); H blocks the
exit gate.

---

## A — Codec backfill

Five new modules in `crates/sk-codec/src/api/`. Each follows the
shape established by `api_versions.rs`:

- `pub const SPEC: ApiSpec` row added to `registry::ALL`.
- `pub fn decode_request(buf: &mut Bytes, version: i16) -> Result<Request, CodecError>`.
- `pub fn encode_response(buf: &mut BytesMut, resp: &Response, version: i16) -> Result<(), CodecError>`.
- Matching `decode_response` + an `encode_request` pair, so the codec
  is bidirectional (the Phase 3 smoke test asserts byte equality on
  fixtures captured from Apache 3.7 against both directions).
- Per-key `request_hdr` / `response_hdr` const fns picking
  `HeaderVersion::V0/V1/V2` per the flexible-version table.

### Per-key version ranges (match `archive/internal/broker/broker.go:555-891`)

| Key | API                | Versions | Flexible from | Notes |
|----:|--------------------|---------:|--------------:|-------|
|  0  | Produce            | 3–9      | 9             | `records: Option<Bytes>` — byte-opaque; Phase 2 storage owns the bytes |
|  1  | Fetch              | 4–12     | 12            | `records: Option<Bytes>` per partition response; `session_id` always written as 0 per gh #4 |
|  2  | ListOffsets        | 1–7      | 6             | v1+ adds timestamp; isolation level in v2+; leader epoch in v4+ |
|  3  | Metadata           | 1–10     | 9             | v10+ carries `topic_id` UUID — Phase 3 writes the all-zero sentinel |
| 22  | InitProducerId     | 0–4      | 2             | Transactional path returns `TRANSACTIONAL_ID_NOT_FOUND` (74) until Phase 6 |

### Lifetimes

Per the phase-1 guidance: keep borrowing (`Request<'a>`) on the hot
path (Produce, Fetch) and fall back to owning `String` on the
admin-shaped wide-nested ones (Metadata, InitProducerId, ListOffsets
all own `String` — the cost is negligible since these are control-path
requests, not produce-volume).

### Fixtures

`crates/sk-codec/tests/fixtures/` finally gets populated. Capture one
request + response per (key, version) for these five keys against an
Apache Kafka 3.7 broker driven by `kafka-console-producer` +
`kafka-console-consumer`. Use the `xtask fixture-capture` shape
sketched in phase-1.md workstream E — Phase 3 lands the implementation
of that subcommand since this is the first time it's actually needed.

### Per-module tests

- Fixture byte-equal: decode → re-encode → assert equal.
- Proptest round trip on the typed structs.
- Tripwires assertion (`record_decode_count() == 0`,
  `batch_reencode_count() == 0`) at the end of every test.

**Exit:** `cargo test -p sk-codec` green; six entries in
`registry::ALL`; `response_from_registry` returns all six sorted by
key; fixtures byte-roundtrip for at least one version per key.

---

## B — `sk-protocol::frame` connection wrapper

Thin wrapper around `sk-codec::FrameReader` + `write_frame` that owns
the per-connection read budget and the response builder. Mirrors
`archive/internal/protocol/frame.go` + the `serveConn` request loop
in `archive/internal/protocol/server.go`.

`crates/sk-protocol/src/frame.rs`:

```rust
pub struct Connection<S: AsyncRead + AsyncWrite + Unpin> {
    inner: BufStream<S>,
    reader: FrameReader,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Connection<S> {
    pub fn new(stream: S, max_frame_bytes: usize) -> Self;
    pub async fn read_request(&mut self) -> Result<(RequestHeader, Bytes), FrameError>;
    pub async fn write_response(
        &mut self,
        correlation_id: i32,
        body: &[u8],
        header_version: HeaderVersion,
    ) -> io::Result<()>;
}
```

`read_request` issues one `read_frame`, peeks the first 2 bytes for
API key + 2 for version to select `HeaderVersion`, decodes the request
header, returns `(hdr, body_bytes)` where `body_bytes` is the slice
after the header. The `FrameError::Disconnected` path (clean EOF on a
new frame boundary) is mapped to the caller's "loop ends" signal —
matches the Go side's `errors.Is(err, io.EOF)` check in `serveConn`.

`write_response` always emits `[size:i32][correlation_id:i32][maybe
tagged-fields][body]`. Selecting whether to emit the tagged-fields
block is the dispatcher's job (it knows the API key + version); this
function just writes what it's given.

**Exit:** integration test: a `tokio::io::duplex` pair, one half
writes a captured ApiVersions request frame, the other half reads it
via `Connection::read_request`, asserts the parsed header matches the
fixture.

---

## C — `sk-protocol::connstate`

Per-connection mutable state — the Rust equivalent of
`archive/internal/connstate/connstate.go`. Phase 3 ships a minimal
shape; Phase 4 fills in `principal` + `sasl_done`; Phase 5 fills in
`splicer`.

`crates/sk-protocol/src/connstate.rs`:

```rust
#[derive(Debug)]
pub struct ConnState {
    pub listener_name: String,
    pub peer_addr: SocketAddr,
    pub principal: Option<Principal>,    // Phase 4 fills
    pub sasl_done: bool,                 // Phase 4 flips
}

#[derive(Debug, Clone)]
pub struct Principal {
    pub principal_type: String,
    pub name: String,
}
```

Stays tiny on purpose — no behaviour, just data. Lives behind
`Arc<Mutex<ConnState>>` since handlers can mutate `sasl_done` mid-
connection (Phase 4 wires that).

**Exit:** compiles, `Principal::default()` returns an "anonymous"
placeholder, `Debug` doesn't leak the principal name.

---

## D — `sk-protocol::dispatch`

Routes an incoming `(header, body)` pair to the right handler by API
key. Same contract as `archive/internal/protocol/dispatch.go`:
unsupported key → `UNSUPPORTED_VERSION` (35); version out of range →
`UNSUPPORTED_VERSION` except for key 18, which always returns a valid
response so clients can discover supported versions.

`crates/sk-protocol/src/dispatch.rs`:

```rust
#[async_trait]
pub trait Handler: Send + Sync + 'static {
    async fn handle(&self, conn: &Mutex<ConnState>, version: i16, body: Bytes)
        -> Result<BytesMut, HandlerError>;
}

pub struct Dispatcher {
    slots: [Option<HandlerSlot>; 96],   // Apache keys 0–~70; 96 leaves headroom
}

struct HandlerSlot {
    handler:     Arc<dyn Handler>,
    versions:    (i16, i16),
}

impl Dispatcher {
    pub fn new() -> Self;
    pub fn register(&mut self, api_key: i16, min: i16, max: i16, h: Arc<dyn Handler>);
    pub async fn dispatch(
        &self,
        conn: &Mutex<ConnState>,
        hdr: RequestHeader,
        body: Bytes,
    ) -> Result<(BytesMut, HeaderVersion), DispatchError>;
}
```

**Pre-auth gate** is stubbed in Phase 3: `dispatch` always proceeds
(no listener-level deny). Phase 4 wires `AuthEngineSelector` in.

**SplicingHandler** is intentionally absent — see "Out of scope". The
Bytes-returning Fetch path is the only Phase 3 contract; sendfile lands
in Phase 8 alongside `SegmentRef`.

**Why a fixed-size array vs HashMap.** API keys are dense small
integers (0–~70); array lookup is one cache line; HashMap is two
plus a hash. The Go side uses a map because Go's maps are cheap;
in Rust the array wins on dispatch latency without any flexibility
cost.

**Exit:** unit tests: registered key returns wrapped response; absent
key returns error response with code 35; out-of-range version returns
35 (except 18, which clamps to `max`).

---

## E — `sk-protocol::server` multi-listener accept loop

Models `archive/internal/protocol/server.go`'s multi-listener accept
loop. Decouples exposure (address) from encryption (TLS config) from
authentication (per-listener engine selector — Phase 4 wires that in
via the dispatcher).

`crates/sk-protocol/src/server.rs`:

```rust
pub struct ListenerConfig {
    pub name: String,
    pub addr: SocketAddr,
    pub pre_bound: Option<TcpListener>,    // tests use this to bind :0 themselves
    pub tls_config: Option<Arc<ServerConfig>>,   // rustls; Phase 4 wires real certs
}

pub struct ServerConfig {
    pub listeners: Vec<ListenerConfig>,
    pub max_frame_bytes: usize,            // default 100 MiB
}

pub struct Server {
    cfg: ServerConfig,
    dispatcher: Arc<Dispatcher>,
}

impl Server {
    pub fn new(cfg: ServerConfig, dispatcher: Arc<Dispatcher>) -> Self;
    pub async fn serve(self, cancel: CancellationToken) -> io::Result<()>;
}
```

`serve` opens every listener (returning early on the first bind
failure, closing any partial set), spawns one accept loop per listener,
and each accepted connection becomes its own `tokio::task` running:

```rust
loop {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => break,
        req = conn.read_request() => {
            let (hdr, body) = match req { Ok(r) => r, Err(FrameError::Disconnected) => break, Err(e) => { log; break } };
            let (resp_body, hv) = dispatcher.dispatch(&state, hdr.clone(), body).await?;
            conn.write_response(hdr.correlation_id, &resp_body, hv).await?;
        }
    }
}
```

Per-connection cancellation propagates from the root token. SIGTERM
cancels root → accept loops break → in-flight requests run to
completion (bounded by the dispatcher's per-handler timeout, not added
in Phase 3) → connection tasks exit → `serve` returns.

**TLS plumbing** lands in Phase 3 in skeleton: `tls_config: Option<...>`
is wired into the accept loop via `tokio-rustls::TlsAcceptor::accept`,
but Phase 3's `bins/skafka/main.rs` never sets it to `Some`. Phase 4
fills in cert + key loading and lights up TLS listeners.

**Exit:** integration test: spin up `Server` on `:0`, capture the
bound port, fire a `kafka-console-producer`-shaped ApiVersions request
via `tokio::net::TcpStream`, assert response. Cancel the token, assert
`serve` returns within 1s.

---

## F — Handlers

One free `async fn` per API, each in its own file under
`crates/sk-protocol/src/handlers/`. Wrap each with a thin
`struct HandlerName { ctx: Arc<HandlerCtx> }` that impls `Handler` so
the dispatcher can register it.

`crates/sk-protocol/src/handlers/mod.rs`:

```rust
pub struct HandlerCtx {
    pub broker: Arc<Broker>,
    pub engine: Arc<dyn StorageEngine>,
    pub topics: Arc<TopicRegistry>,
}
```

### `api_versions.rs`

Trivial:

```rust
let resp = sk_codec::api::api_versions::response_from_registry(/* throttle_time_ms */ 0);
encode_response(&mut out, &resp, version)?;
```

### `metadata.rs`

Reads `topics` registry. For each requested topic name (or all topics
when `topics == null`):
- Look up `TopicMeta { name, partition_count, topic_id }`. UNKNOWN_TOPIC
  (3) if missing and auto-create is off.
- Per partition: leader = self (only broker in the cluster);
  replicas = `[self]`; ISR = `[self]`; offline-replicas = `[]`.

`brokers` block: one entry — self. Port from `ConnState::listener_name`
resolved against `cfg.listeners` to pick the right advertised port.

`cluster_id`: read from `SKAFKA_CLUSTER_ID` env (default
`skafka-rust-dev`).

`controller_id`: -1 (no controller in single-broker dev mode).

`topic_id` on v10+: all-zero sentinel until Phase 7's operator mints
real UUIDs.

### `init_producer_id.rs`

```rust
if req.transactional_id.is_some() {
    return Response { error_code: 74 /* TRANSACTIONAL_ID_NOT_FOUND */, ... };
}
let pid = broker.next_producer_id();    // AtomicI64::fetch_add(1)
Response { error_code: 0, producer_id: pid, producer_epoch: 0, ... }
```

Phase 6 replaces this with the gh #22 rejoin contract + transactional
path.

### `produce.rs`

For each topic in `req.topics`, for each partition in `topic.partitions`:

```rust
match engine.append(topic.name, p.index, /* epoch */ 0, req.acks, p.records).await {
    Ok(base_offset) => PartitionResp { error_code: 0, base_offset, log_append_time_ms: -1,
                                       log_start_offset, ... },
    Err(StorageError::EpochMismatch)        => PartitionResp { error_code: 89, ... }, // NOT_LEADER
    Err(StorageError::OutOfOrderSequence)   => PartitionResp { error_code: 45, ... },
    Err(StorageError::DuplicateSequence)    => PartitionResp { error_code: 46, ... },
    Err(StorageError::InvalidProducerEpoch) => PartitionResp { error_code: 47, ... },
    Err(StorageError::UnknownTopicOrPartition) => PartitionResp { error_code: 3, ... },
    Err(StorageError::Stalled)              => PartitionResp { error_code: 5, ... }, // LEADER_NOT_AVAILABLE
    Err(_)                                  => PartitionResp { error_code: 1, ... }, // OFFSET_OUT_OF_RANGE / generic
}
```

`epoch = 0` because Phase 3 single-broker is uncontested leader; Phase 5
threads the real epoch from `Coordinator`.

### `fetch.rs`

Stateless full-fetch per gh #4. For each topic, for each partition:

```rust
let hwm = engine.high_watermark(t.name, p.partition_index)?;
let lso = engine.log_start_offset(t.name, p.partition_index)?;
let bytes = engine.read(t.name, p.partition_index, p.fetch_offset, p.partition_max_bytes as usize).await?;
PartitionResp {
    partition_index: p.partition_index,
    error_code: 0,
    high_watermark: hwm,
    last_stable_offset: hwm,         // read-uncommitted; Phase 6 differentiates for read-committed
    log_start_offset:   lso,
    aborted_transactions: vec![],    // empty; Phase 6 populates
    preferred_read_replica: -1,
    records: Some(bytes),
}
```

Response-level: `session_id = 0` always; `error_code = 0`;
`throttle_time_ms = 0`.

### `list_offsets.rs`

```rust
for t in &req.topics {
    for p in &t.partitions {
        let offset = match p.timestamp {
            -2 /* EARLIEST */ => engine.log_start_offset(t.name, p.partition_index)?,
            -1 /* LATEST   */ => engine.high_watermark(t.name, p.partition_index)?,
            ts                => engine.offset_for_timestamp(t.name, p.partition_index, ts)?.0,
        };
        ...
    }
}
```

Timestamp echo in v1+ responses: when the timestamp lookup matches,
echo the matched record's timestamp; for EARLIEST/LATEST sentinels,
echo `-1`. Leader epoch in v4+: always `0` in Phase 3.

**Exit per handler:** unit test feeds a captured request fixture
through the handler against a `MemoryStorage` pre-loaded with known
records; asserts the response matches a captured fixture or a
hand-constructed reference.

---

## G — `sk-broker` minimal glue

`Broker` is the runtime context every handler reads from. Phase 3's
shape stays tiny; Phase 5 adds `Coordinator`, Phase 6 adds
`TxnStateStore`, etc.

`crates/sk-broker/src/broker.rs`:

```rust
pub struct Broker {
    pub engine:        Arc<dyn StorageEngine>,
    pub topics:        Arc<TopicRegistry>,
    pub local_lease:   LocalLeaseManager,
    pub cluster_id:    String,
    pub broker_id:     i32,
    producer_id_counter: AtomicI64,
}

impl Broker {
    pub fn next_producer_id(&self) -> i64 {
        self.producer_id_counter.fetch_add(1, Ordering::Relaxed)
    }
}
```

`crates/sk-broker/src/topic_registry.rs`:

```rust
pub struct TopicRegistry {
    inner: RwLock<HashMap<String, TopicMeta>>,
}

#[derive(Debug, Clone)]
pub struct TopicMeta {
    pub name: String,
    pub partition_count: i32,
    pub topic_id: [u8; 16],     // all-zero in Phase 3
}

impl TopicRegistry {
    pub fn from_env_json(json: &str) -> Result<Self, ConfigError>;
    pub fn get(&self, name: &str) -> Option<TopicMeta>;
    pub fn all(&self) -> Vec<TopicMeta>;
    pub fn insert(&self, m: TopicMeta);
}
```

Seeded once at boot from `SKAFKA_TOPICS` env JSON:
`[{"name":"t1","partitions":3},{"name":"t2","partitions":1}]`. Phase 5
replaces this with a `KafkaTopic` CR watcher.

`crates/sk-broker/src/local_lease.rs`:

```rust
pub struct LocalLeaseManager;

impl LocalLeaseManager {
    pub fn leads(&self, _topic: &str, _partition: i32) -> bool { true }
    pub fn current_epoch(&self) -> u32 { 0 }
}
```

`crates/sk-broker/src/cli.rs` — env parsing collected here so
`main.rs` stays tiny:

```rust
pub struct Cli {
    pub listeners: Vec<ListenerConfig>,         // SKAFKA_LISTENERS JSON
    pub data_dir: Option<PathBuf>,              // SKAFKA_DATA_DIR
    pub flush_interval_messages: i64,           // SKAFKA_FLUSH_INTERVAL_MESSAGES (default 1)
    pub cluster_id: String,                     // SKAFKA_CLUSTER_ID
    pub broker_id: i32,                         // SKAFKA_BROKER_ID (or 0 in dev)
    pub topics_seed: String,                    // SKAFKA_TOPICS
    pub log_level: String,                      // RUST_LOG
}

impl Cli {
    pub fn from_env() -> Result<Self, ConfigError>;
}
```

**Exit:** unit test: `from_env_json` round-trips the seed shape;
`Broker::next_producer_id` is monotonic across concurrent threads;
empty registry returns `None` from `get`.

---

## H — `bins/skafka/main.rs` + smoke test

`bins/skafka/src/main.rs`:

```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = sk_broker::cli::Cli::from_env()?;
    init_tracing(&cli.log_level);

    let engine: Arc<dyn StorageEngine> = match (&cli.data_dir, std::env::var("MY_POD_NAME").ok()) {
        (Some(dir), _) => Arc::new(DiskStorageEngine::new(
            Arc::new(RealFs), dir.clone(),
            PartitionConfig::from_cli(&cli),
        )),
        _              => Arc::new(MemoryStorage::new()),
    };

    let topics = Arc::new(TopicRegistry::from_env_json(&cli.topics_seed)?);
    let broker = Arc::new(Broker::new(engine.clone(), topics, cli.cluster_id, cli.broker_id));

    let mut d = Dispatcher::new();
    let ctx = Arc::new(HandlerCtx { broker: broker.clone(), engine: engine.clone(), topics });
    d.register( 0, 3,  9, Arc::new(ProduceHandler::new(ctx.clone())));
    d.register( 1, 4, 12, Arc::new(FetchHandler::new(ctx.clone())));
    d.register( 2, 1,  7, Arc::new(ListOffsetsHandler::new(ctx.clone())));
    d.register( 3, 1, 10, Arc::new(MetadataHandler::new(ctx.clone())));
    d.register(18, 0,  4, Arc::new(ApiVersionsHandler::new()));
    d.register(22, 0,  4, Arc::new(InitProducerIdHandler::new(ctx.clone())));

    let cancel = CancellationToken::new();
    let server_cancel = cancel.clone();
    let server = Server::new(ServerConfig {
        listeners: cli.listeners,
        max_frame_bytes: 100 * 1024 * 1024,
    }, Arc::new(d));

    let server_task = tokio::spawn(async move { server.serve(server_cancel).await });

    wait_for_sigterm().await;
    tracing::info!("SIGTERM received; draining");
    cancel.cancel();
    server_task.await??;

    // Graceful storage drain per Phase 2's RelinquishAll plan.
    if let Some(disk) = engine.as_any().downcast_ref::<DiskStorageEngine>() {
        disk.relinquish_all().await?;
    }
    Ok(())
}
```

(`as_any()` + `downcast_ref` is the pragmatic seam — MemoryStorage's
relinquish is a no-op, so we only need to call it on the disk variant.
Add a `fn drain(&self) -> Pin<Box<dyn Future...>>` to `StorageEngine`
if the downcast bothers reviewers.)

`bins/skafka/tests/smoke.rs`:

```rust
#[tokio::test]
async fn produce_fetch_roundtrip_rdkafka() {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let mut broker = spawn_broker(&dir, port);

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", format!("127.0.0.1:{port}"))
        .create().unwrap();
    for i in 0..1000 {
        producer.send(FutureRecord::to("t1").key(&format!("k{i}")).payload(&format!("v{i}")), Duration::from_secs(5))
                .await.unwrap();
    }

    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", format!("127.0.0.1:{port}"))
        .set("group.id", "smoke")
        .set("enable.auto.commit", "false")
        .create().unwrap();
    consumer.subscribe(&["t1"]).unwrap();
    // poll N messages, assert keys + payloads byte-equal

    broker.kill().await;
}
```

**rdkafka in CI.** Add `rdkafka = { version = "0.36", features = ["cmake-build"] }`
as a dev-dep of `bins/skafka`. Builds librdkafka from source; ~2 min
cold, ~0 s warm under `Swatinem/rust-cache`. **Acceptable** —
beats the alternative of either taking a system-package dep on
`librdkafka-dev` (mismatched versions across runners) or rolling our
own minimal Kafka client just for tests.

**Exit:** the smoke test runs end-to-end in under 30 s, 1k messages
produced + fetched byte-equal, no panics, `relinquish_all` runs on
shutdown.

---

## Phase 3 exit criteria (all must hold)

1. `cargo test --workspace --all-features` green; total time under 5 min
   on a warm cache.
2. `cargo clippy --workspace --all-targets -- -D warnings` and
   `cargo fmt --check` pass.
3. Six entries in `sk_codec::api::registry::ALL`: keys 0, 1, 2, 3, 18,
   22. `response_from_registry` returns them sorted, with the version
   ranges in this doc's table.
4. `bins/skafka` accepts a connection from `kafka-console-producer`
   on a configured listener, persists records to either `MemoryStorage`
   (dev) or `DiskStorageEngine` (when `SKAFKA_DATA_DIR` is set).
5. `kafka-console-consumer --topic t1 --from-beginning` reads those
   records back byte-equal.
6. `bins/skafka/tests/smoke.rs` runs end-to-end via rdkafka, produces +
   fetches 1k records, no panics, byte-equal payload comparison passes.
7. SIGTERM → broker drains in-flight requests, calls
   `DiskStorageEngine::relinquish_all()` before exit, no `.nfsXXXX`
   files appear in `SKAFKA_DATA_DIR` after restart (smoke test asserts
   the directory tree contains only `.log` / `.index` / `.json` files
   after a restart).
8. `sk_codec::tripwires::record_decode_count()` and
   `batch_reencode_count()` both read 0 after the smoke run — proves
   byte-opacity end-to-end across the codec + storage + Produce/Fetch
   stack.
9. Fixtures for the 5 new codec keys captured under
   `crates/sk-codec/tests/fixtures/`; round-trip byte-equal asserted
   for at least one (key, version) per new key. README explains the
   refresh procedure.
10. Go tree under `archive/` unchanged; chart, CRDs, `scripts/`, and
    `proto/heartbeat.proto` are bit-identical to their pre-Phase-3
    contents.

If any of these fail, do not merge — fix and re-run.

---

## Risks & mitigations

- **rdkafka brings a C dep into CI.** `cmake-build` feature builds
  librdkafka from source; first build ~2 min, cached after. Worth it
  for the wire-level ground truth — franz-rs is younger and we'd
  rather verify against the same client the Apache project tests with.
  Mitigation: gate the smoke test behind a `--features smoke` flag if
  cold CI exceeds the 4-min Phase 0 budget; downgrade to franz-rs only
  if rdkafka's build genuinely blocks.
- **Flexible vs legacy response header per-API.** ApiVersions is the
  documented exception — its response header is always V0 even on
  flexible versions. The other five APIs follow `ApiSpec::is_flexible`
  on the response version. Get this wrong and every flexible-version
  client loops on tagged-field parse errors. Mitigation: the dispatcher
  returns the `HeaderVersion` to use alongside the body, derived from
  `ApiSpec::response_hdr(version)`; `Connection::write_response` honours
  it. Unit test: send a v3 ApiVersions request, assert the response
  header has no tagged-field block.
- **`Metadata` advertising the wrong port per listener.** Single-broker
  Phase 3 has one listener in the common case, so this is trivial; but
  if a tester runs the binary with multiple listeners (anon plain +
  TLS plain — possible even in dev), the response must point each
  client back at the listener it connected over, not the first one in
  the array. Mitigation: lookup keyed on `ConnState::listener_name`;
  unit test with a two-listener `Cli` asserts each client sees its own
  port echoed back.
- **`InitProducerId` non-transactional starvation.** `AtomicI64::fetch_add`
  is monotonic and never recycles; across broker restarts the counter
  resets to 0 unless persisted. Apache's behaviour is the same (PIDs
  reset on broker restart for non-transactional producers), so this is
  intentional — but document it in `init_producer_id.rs` so a reviewer
  doesn't add disk-backed persistence "to be safe".
- **`Bytes` lifetime through the handler chain.** `dispatch.dispatch`
  takes ownership of the body `Bytes`; handlers slice into it and
  pass slices into `engine.append`. `Bytes` is reference-counted so
  this is zero-copy as designed. Mitigation: a `proptest` round trip
  on a Produce-shaped request asserts the payload bytes the handler
  passes to `engine.append` are pointer-identical to a slice of the
  original request frame (== zero copy verified at the type level).
- **TLS plumbing wired but untested in Phase 3.** The accept loop calls
  `TlsAcceptor::accept` when `tls_config: Some(...)` but nothing
  exercises it. Mitigation: leave Phase 3's `bins/skafka/main.rs` with
  no TLS listener; add a `#[ignore]`-gated integration test that wires
  a self-signed `rcgen` cert through the path so Phase 4 doesn't
  discover a structural bug on day one.
- **`as_any` downcast on shutdown is fragile.** If a future engine impl
  isn't `MemoryStorage` or `DiskStorageEngine`, the downcast silently
  no-ops. Mitigation: add `fn drain(&self) -> Pin<Box<dyn Future...>>`
  to `StorageEngine` as a default-impl `async {}` method before merge
  — `MemoryStorage` keeps the default, `DiskStorageEngine` overrides
  to call `relinquish_all`. Eliminates the downcast entirely.

---

## What this enables for Phase 4

After Phase 3 merges, Phase 4 (auth) lands by:

1. Adding `crates/sk-auth` with `AuthEngine`, `AuthEngineSelector`,
   `Authorizer`, `QuotaChecker`, SCRAM/PLAIN/mTLS engines.
2. Wiring `AuthEngineSelector` into `Dispatcher` via a setter (same
   shape as `archive/internal/protocol/dispatch.go:SetAuthEngines`);
   the pre-auth gate stub in workstream D is replaced inline.
3. Adding `crates/sk-protocol/src/handlers/sasl.rs` (keys 17 + 36);
   registered in `main.rs` alongside the Phase 3 handlers.
4. `produce.rs` and `fetch.rs` calling
   `ctx.authorizer.authorize(principal, op, resource)` and
   `ctx.quotas.check_*` before hitting the engine.
5. TLS listeners actually configured in `main.rs` from cert/key files.

No further Phase 3 changes — Phase 4 is pure addition.
