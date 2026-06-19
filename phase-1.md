# Phase 1 — Wire codec

Detailed work plan for the second phase of the Rust rewrite. Companion to
[`rewrite.md`](./rewrite.md); the high-level summary lives there. Builds on
the workspace scaffolding landed in [`phase-0.md`](./phase-0.md).

**Goal.** Land a bit-perfect Rust replacement for
`archive/internal/protocol/codec/`. Every request/response the Go broker
encodes today decodes/re-encodes byte-identically against captured Apache
Kafka 3.7 fixtures, and the byte-opacity contract (broker is a byte mover,
not a byte interpreter) is enforced at the type level — there is no
`Record` struct anywhere in `sk-codec`, and the record-batch helpers in
`sk-codec` read the 61-byte v2 header only.

**Length.** ~2 weeks, single engineer. The work parallelises along the
"primitives + framing" / "per-API modules" / "fixtures + harness" axes,
so a second engineer can roughly halve calendar time after the
primitive layer is stable (workstream A).

**Out of scope for Phase 1.** Per-API handlers (Phase 3 — `sk-protocol`),
SASL handshake routing (Phase 4), record-batch *decoding* (never — byte
opacity is a hard invariant; see `archive/tests/byte-opacity/`). No
`Server::serve` bring-up: codec is a pure library; tests drive it
in-process with raw byte slices, not sockets.

**Scope boundary.** Every API key the Go broker currently registers
(`archive/internal/broker/broker.go` lines 555–891) must be representable.
That is API keys **0, 1, 2, 3, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 35, 36, 37, 42,
44, 47, 48, 49, 50, 51, 60** — 40 distinct keys across their full
supported version ranges. Anything outside that set (`EARLIEST_LOCAL_TIMESTAMP`,
KRaft-only keys, share-groups) stays absent per the parity boundary in
`CLAUDE.md`.

---

## Workstreams

Eight workstreams. A and B are sequential; once B lands, C/D/E/F can land
in parallel; G and H ride out at the end.

- **A** — Primitives (`primitives.rs`)
- **B** — Framing, headers, tagged fields (`frame.rs`, `headers.rs`, `tagged.rs`)
- **C** — CRC + record-batch header helpers + byte-opacity tripwire
- **D** — Per-API modules driven by a declarative registry
- **E** — Fixture capture infrastructure (`xtask fixture-capture`)
- **F** — Test harness (proptest, snapshot, cross-port golden files)
- **G** — CI lane for codec proptest sweep
- **H** — Docs + memory + exit criteria

Dependencies: A blocks everything else; B blocks D and F; C and E can land
any time after A; G is the last merge of the phase.

---

## A — Primitives

Free functions over `bytes::Bytes` / `&mut bytes::BytesMut`. No traits at
this layer — every primitive is a plain `pub fn`. Trait wrapping (`Decode`
/ `Encode`) lives one layer up in workstream D so generated code can
target it without re-implementing the byte twiddling.

`crates/sk-codec/src/primitives.rs` — read side mirrors
`archive/internal/protocol/codec/reader.go` 1:1:

```rust
pub fn read_i8 (buf: &mut Bytes) -> Result<i8,  CodecError>;
pub fn read_i16(buf: &mut Bytes) -> Result<i16, CodecError>;
pub fn read_i32(buf: &mut Bytes) -> Result<i32, CodecError>;
pub fn read_i64(buf: &mut Bytes) -> Result<i64, CodecError>;
pub fn read_u32(buf: &mut Bytes) -> Result<u32, CodecError>; // KIP-516 UUIDs
pub fn read_f64(buf: &mut Bytes) -> Result<f64, CodecError>; // KIP-546 quotas
pub fn read_bool(buf: &mut Bytes) -> Result<bool, CodecError>;

pub fn read_uvarint(buf: &mut Bytes) -> Result<u64, CodecError>;
pub fn read_varint (buf: &mut Bytes) -> Result<i64, CodecError>; // zigzag

pub fn read_string         (buf: &mut Bytes) -> Result<String,       CodecError>;
pub fn read_nullable_string(buf: &mut Bytes) -> Result<Option<String>, CodecError>;
pub fn read_compact_string         (buf: &mut Bytes) -> Result<String, CodecError>;
pub fn read_compact_nullable_string(buf: &mut Bytes) -> Result<Option<String>, CodecError>;

pub fn read_bytes         (buf: &mut Bytes) -> Result<Bytes,       CodecError>;
pub fn read_nullable_bytes(buf: &mut Bytes) -> Result<Option<Bytes>, CodecError>;
pub fn read_compact_bytes         (buf: &mut Bytes) -> Result<Bytes, CodecError>;
pub fn read_compact_nullable_bytes(buf: &mut Bytes) -> Result<Option<Bytes>, CodecError>;

pub fn read_raw(buf: &mut Bytes, n: usize) -> Result<Bytes, CodecError>;
pub fn read_uuid(buf: &mut Bytes) -> Result<[u8; 16], CodecError>;

pub fn read_array<T>(buf: &mut Bytes, f: impl FnMut(&mut Bytes) -> Result<T, CodecError>)
    -> Result<Vec<T>, CodecError>;
pub fn read_compact_array<T>(buf: &mut Bytes, f: impl FnMut(&mut Bytes) -> Result<T, CodecError>)
    -> Result<Vec<T>, CodecError>;
```

Write side mirrors `writer.go` 1:1; `BytesMut::put_*` underneath.
`write_compact_array` takes a closure that emits the elements after
`put_uvarint(len+1)`.

`CodecError` enum (thiserror) carries `UnexpectedEof`, `InvalidUtf8`,
`InvalidLength { got, max }`, `InvalidUvarint`, `InvalidUuid`. No
`#[from] io::Error` — framing-layer I/O lives in workstream B and uses a
distinct error type so the API surface of `primitives` is total over an
in-memory buffer.

Why `Bytes` and not `Cursor<&[u8]>`: `Bytes` is the type that flows from
`tokio::io::AsyncRead` via `Framed` (workstream B) and into the storage
engine's splice path. Passing `&mut Bytes` lets read functions
`buf.advance(n)` cheaply without copying, and lets `read_compact_bytes`
return a zero-copy `Bytes` sub-slice into the same arena — the
splice-fallback path in `archive/internal/protocol/codec/writer.go`'s
`WriteRaw` lands naturally.

`pub use bytes::{Bytes, BytesMut};` at the crate root so downstream
crates don't need a direct `bytes` dep just for the type alias.

**Exit:** every Go reader/writer method has a Rust counterpart with the
same name modulo case. `proptest` round-trip per type passes 10k cases.

---

## B — Framing, headers, tagged fields

`crates/sk-codec/src/frame.rs` — length-prefixed framing over async I/O:

```rust
pub async fn read_frame<R: AsyncRead + Unpin>(r: &mut R) -> Result<Bytes, FrameError>;
pub async fn write_frame<W: AsyncWrite + Unpin>(w: &mut W, body: &[u8]) -> io::Result<()>;
```

Frame is `[size:i32][body:size]`. `size > max_size` → `FrameTooLarge`
(default 100 MiB, configurable via constructor `FrameReader::new(max)`).
EOF before length → `Disconnected`, not `UnexpectedEof` — clients
disconnecting cleanly is normal and shouldn't log-spam.

`headers.rs` — request and response headers:

```rust
pub struct RequestHeader { pub api_key: i16, pub api_version: i16,
                           pub correlation_id: i32, pub client_id: Option<String> }

pub struct ResponseHeader { pub correlation_id: i32 }

pub fn decode_request_header (buf: &mut Bytes, version: HeaderVersion) -> Result<RequestHeader,  CodecError>;
pub fn encode_response_header(buf: &mut BytesMut, hdr: &ResponseHeader, version: HeaderVersion);

pub enum HeaderVersion { V0, V1, V2 } // V2 adds tagged fields
```

`HeaderVersion` is selected per (api_key, api_version) by the registry in
workstream D — Apache's `ApiKeys.requestHeaderVersion(apiKey, apiVersion)`
table. Bake it into a `const fn` so the dispatcher can call it without
allocating.

`tagged.rs` — KIP-482 flexible tagged-field block:

```rust
pub fn read_tagged_fields(buf: &mut Bytes) -> Result<(), CodecError>;
pub fn write_empty_tagged_fields(buf: &mut BytesMut);
pub fn write_tagged_fields(buf: &mut BytesMut, fields: &[TaggedField]);

pub struct TaggedField { pub tag: u64, pub value: Bytes }
```

The Go side discards unknown tags on read. Mirror that: read consumes the
bytes, returns `()`. Write supports emitting a list of `TaggedField` for
the few cases where Apache 3.7 sends a non-empty block (most paths use
`write_empty_tagged_fields` — a single `0` uvarint byte).

**Exit:** `read_frame` + `decode_request_header` against
`tests/fixtures/req-3-v9-metadata.bin` reproduces the parsed header. A
proptest round-trip on `TaggedField` lists holds.

---

## C — CRC + record-batch helpers + byte-opacity tripwire

`crc.rs` — thin wrapper around the `crc32c` crate (the same hardware-
accelerated impl Go uses via `hash/crc32` with the Castagnoli poly):

```rust
pub fn crc32c(bytes: &[u8]) -> u32;
pub fn verify_batch_crc(batch: &[u8]) -> Result<(), CrcError>;
```

`verify_batch_crc` is the ONLY function that touches a record-batch's
bytes from offset 21 onward, and even it only reads them as opaque input
to CRC32C — it does not parse the records. Byte-opacity is preserved.

`recordbatch_count.rs` — port `archive/internal/protocol/codec/recordbatch_count.go`
verbatim:

```rust
pub fn count_records_in_batches(b: &[u8]) -> i64;
pub fn count_records_in_batches_at<R: std::io::Read + std::io::Seek>(
    r: &mut R, pos: u64, length: usize) -> io::Result<i64>;
```

Both functions read only the 61-byte v2 batch header (`baseOffset`,
`batchLength`, `numRecords`); the records payload is left untouched. The
`ReadAt` variant exists so the storage engine can count records straight
off a segment file without undoing its `sendfile(2)` splice.

`tripwires.rs` — byte-opacity tripwire counters, mirroring
`archive/internal/observability/byteopacity.go`. The Go side puts these
in `observability/`; in Rust they live in `sk-codec` because the
invariant they protect is a property of *this* crate. Re-exported from
`sk-observability` (Phase 8) for the same metric name on the wire:

```rust
pub fn bump_codec_record_decode(site: &'static str);
pub fn bump_codec_batch_reencode(site: &'static str);

// Test-only handle; production code must never call inc().
#[cfg(test)]
pub fn record_decode_count() -> u64;
#[cfg(test)]
pub fn batch_reencode_count() -> u64;
```

In Phase 1 these are atomic counters in a static `AtomicU64`. Phase 8
swaps them for OTLP metric instruments behind the same function signature
— call sites stay identical.

There is **no `Record` struct in `sk-codec`**. Anything that needs a
decoded record lives in `crates/sk-test-harness::recordbatch` (mirrors
`archive/tests/testutil/recordbatch`). The crate's `lib.rs` carries a
doc-comment line stating the invariant in plain text so a reviewer who's
never read this plan can spot a violation.

**Exit:** `verify_batch_crc` against a librdkafka-captured batch fixture
passes; `count_records_in_batches` returns the expected total over
multi-batch fixtures; both tripwire counters read 0 after every
codec test.

---

## D — Per-API modules

Goal: one Rust module per API key, with `Request`, `Response`, and
`ALL_VERSIONS` exposed. Versioned encoding gated on a `version: i16`
parameter, identical to the Go signatures (`DecodeXxxRequest(r, version)`,
`EncodeXxxResponse(w, resp, version)`).

`crates/sk-codec/src/api/registry.rs` — single declarative table that
drives both the module list and the `ApiVersions` response:

```rust
pub struct ApiSpec {
    pub key:           i16,
    pub min_version:   i16,
    pub max_version:   i16,
    pub min_flexible:  Option<i16>,    // None if not yet flexible
    pub request_hdr_v: fn(i16) -> HeaderVersion,
    pub response_hdr_v: fn(i16) -> HeaderVersion,
}

pub const ALL: &[ApiSpec] = &[
    ApiSpec { key:  0, min_version: 3, max_version:  9, min_flexible: Some(9),
              request_hdr_v: produce::request_hdr_v, response_hdr_v: produce::response_hdr_v },
    ApiSpec { key:  1, min_version: 4, max_version: 12, min_flexible: Some(12),
              request_hdr_v: fetch::request_hdr_v,   response_hdr_v: fetch::response_hdr_v },
    // … one row per supported key, copied from the d.Register(...) calls
    //   in archive/internal/broker/broker.go:555-891
];
```

`ApiVersions` (key 18) is then trivial: `ALL.iter().map(|a| (a.key, a.min_version, a.max_version)).collect()`.

**Module layout.** One file per key, lowercased and snake-cased after the
Apache name (`produce.rs`, `fetch.rs`, `metadata.rs`, `find_coordinator.rs`,
…). The full list is the 40 keys enumerated in the scope boundary above.

**Per-module shape** (illustrated for `produce.rs`):

```rust
//! Produce — API key 0. v3–v9, flexible from v9 (KIP-482).
use crate::{primitives::*, tagged::*, Bytes, BytesMut, CodecError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request<'a> {
    pub transactional_id: Option<&'a str>,
    pub acks:             i16,
    pub timeout_ms:       i32,
    pub topics:           Vec<TopicProduce<'a>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicProduce<'a> {
    pub name:       &'a str,
    pub partitions: Vec<PartitionProduce<'a>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionProduce<'a> {
    pub index:   i32,
    pub records: Option<Bytes>,   // opaque record-batch bytes; NEVER parsed
}

pub fn decode_request(buf: &mut Bytes, version: i16) -> Result<Request<'_>, CodecError> { … }
pub fn encode_response(buf: &mut BytesMut, resp: &Response, version: i16) { … }

pub const VERSIONS: (i16, i16) = (3, 9);
pub(crate) fn request_hdr_v(_v: i16) -> HeaderVersion { … }
pub(crate) fn response_hdr_v(_v: i16) -> HeaderVersion { … }
```

The `records: Option<Bytes>` field is the byte-opacity contract on the
type level — a `Bytes` is a zero-copy view, not a parsed list of records.
A reviewer who proposes changing it to `Vec<Record>` is breaking the
invariant and the type signature flags it loudly.

**Lifetimes.** Request types are borrowing (`<'a>`) so a decoded request
holds zero-copy refs into the incoming frame buffer. Responses are owning
— they're built by the handler and serialised once. This matches how the
Go side uses `[]byte` for request strings (sliced into the connection
buffer) and `string` for response strings (built fresh by the handler).

**Generated vs hand-written.** Phase 1 hand-writes every module. Phase 1
does **not** introduce a Kafka-message-spec → Rust codegen pipeline. Kafka
publishes JSON schemas under `clients/src/main/resources/common/message/`,
but binding to those introduces a second toolchain (Kotlin/Java tool to
run the codegen, or a Rust port of it). The Go tree didn't take that
dependency and we don't either — at 40 keys × ~3 versions averaged, the
hand-written modules total ~9k LoC (`wc -l` of the Go `api/` directory),
which is bounded and one-shot. Re-evaluate before Phase 9 if Apache 4.x
parity gets serialised.

**Compaction / DRY.** Pull repeating sub-structures (`Topic { name, partitions: Vec<…> }`)
into shared helpers in `api/common.rs` only when the same shape appears
in ≥3 modules. Don't pre-factor.

**Exit:** every API key's `Request` + `Response` round-trips a captured
fixture byte-identically; the registry's `ALL_VERSIONS` table matches the
40-key set; `ApiVersions::Response` built from the registry matches the
Go broker's `ApiVersions` response byte-for-byte.

---

## E — Fixture capture

Real Kafka bytes are the only acceptable ground truth. Two sources:

1. **Apache Kafka 3.7 broker** — librdkafka and franz-go talking to a
   `bin/kafka-server-start.sh` instance. Captured on the wire via
   `tcpdump -w` and split into per-message files with a small Rust
   utility (`xtask fixture-capture`).
2. **Existing Go broker** — for keys where Apache only emits a couple of
   versions but skafka needs the others (e.g. metadata v0 still
   represented in the Go tree for legacy clients), capture against the
   Go binary running the same flag set as production.

`xtask/src/fixture_capture.rs`:

```rust
// cargo xtask fixture-capture --pcap dump.pcap --out crates/sk-codec/tests/fixtures/
//
// Walks the pcap, isolates each Kafka TCP stream by (src_ip, src_port,
// dst_ip, dst_port), peels off [size:i32][body:size] frames, parses the
// 4-byte (api_key, api_version) prefix, and writes one file per
// (api_key, api_version, direction) tuple as
//   req-{key}-v{ver}-{label}.bin
//   resp-{key}-v{ver}-{label}.bin
// `label` comes from a per-stream counter so multi-roundtrip fixtures
// don't collide.
```

`tests/fixtures/README.md` documents the capture procedure — which
clients to run, which broker config to use (`message.format.version=3.7`,
`inter.broker.protocol.version=3.7`), and how to refresh fixtures on a
parity-driven version bump. One-time capture cost is real but bounded.

**Cross-port fixtures.** `archive/tests/byte-opacity/` already has a
hand-rolled record-batch builder (`tests/testutil/recordbatch`). Mirror
the resulting fixture *bytes* into `crates/sk-codec/tests/fixtures/`
verbatim — don't port the builder. The Rust test loads the file and
asserts the round trip. This means a Go-side change that affects the
on-wire bytes is caught when the fixture refresh runs against the new Go
output before the Rust test runs.

`Cargo.toml`:

```toml
[dev-dependencies]
proptest    = { workspace = true }
insta       = { workspace = true }
tokio-test  = { workspace = true }
```

**Exit:** `tests/fixtures/` populated with ≥1 request and ≥1 response
fixture per (api_key, api_version) the broker registers. Total fixture
disk footprint < 2 MiB (the byte-opacity batches dominate; everything
else is tiny). README explains refresh.

---

## F — Test harness

Three test classes, all in `crates/sk-codec/tests/`:

1. **`fixtures.rs`** — golden-byte round trip. For each
   `tests/fixtures/*.bin`, decode → re-encode → assert byte-equal. Test
   matrix driven by a `walkdir` over the fixtures dir; one
   `#[test]` per file via `paste!` / a `build.rs` macro stub. Failures
   print a hex-diff of the first divergent byte (port
   `archive/tests/byte-opacity/tripwire_test.go`'s "first mismatch at
   byte N" hint).
2. **`proptest.rs`** — generic round trip via `proptest::Arbitrary`.
   `arbitrary::Arbitrary` impl per API type generates valid values;
   round-trip property: `decode(encode(x)) == x` and `encode(decode(b)) == b`.
   10k cases per type by default; CI override via `PROPTEST_CASES`.
3. **`tripwires.rs`** — at the end of every `fixtures.rs` and
   `proptest.rs` test, assert
   `tripwires::record_decode_count() == 0` and
   `tripwires::batch_reencode_count() == 0`. Mirrors
   `archive/tests/byte-opacity/tripwire_test.go`'s `assertTripwiresZero`.

**Snapshot testing for diagnostics, not correctness.** `insta` snapshots
get used for `Debug` printouts of decoded requests (so a fixture refresh
is visually reviewable in a PR diff), but NEVER as the round-trip
oracle. The oracle is the binary fixture; the snapshot is the human-
readable witness.

**No mocks at this layer.** The codec is pure functions over `Bytes`; the
test buffer is the test buffer.

**Exit:** `cargo test -p sk-codec` runs in < 30 s end-to-end (including
proptest sweep); every fixture passes; tripwires read 0.

---

## G — CI

The Phase 0 `ci.yml` already runs `cargo test --workspace --all-features`,
which exercises sk-codec. Two additions:

1. **Beef up the proptest budget on `main`.** Add a step that re-runs
   `cargo test -p sk-codec --release` with `PROPTEST_CASES=100000` on
   push to `main` only (not PRs — keeps PR CI under 4 min). Failures
   open an issue automatically via a `gh issue create` step on the
   `ubuntu-latest` runner; not blocking.
2. **`docker buildx` smoke for the codec test.** Already covered by the
   existing `docker-rust` job since `sk-codec` builds transitively.

No new workflow file. Edit the existing `ci.yml` in place.

**Exit:** PR CI stays under 4 min; nightly proptest run on `main` opens
a tracker issue on regression.

---

## H — Docs + memory + exit criteria

### Crate `README.md`

`crates/sk-codec/README.md` — replace the placeholder with:

- One paragraph: what the crate does, in terms a Go-skafka reviewer
  recognises ("port of `internal/protocol/codec/`; record-batch payload
  is byte-opaque").
- A 5-line "how to add an API key" recipe pointing at the registry
  entry, the per-key module, the fixture refresh.
- A pointer to the byte-opacity contract and the tripwire counters.

### `CLAUDE.md` update

In the "Code map" section, add a Rust-side entry under each Go-side
codec bullet so a reader scanning the doc sees both forms during the
mid-rewrite period.

In the "Common commands → Rust" block, add:

```bash
cargo test -p sk-codec                  # all codec tests including proptest
cargo test -p sk-codec --release        # full proptest sweep (slower)
cargo xtask fixture-capture --pcap …    # rebuild tests/fixtures/ from a tcpdump
```

### Memory note

After Phase 1 merges, save a feedback memory:

> "Phase 1 chose hand-written per-API modules over a Kafka-message-spec →
> Rust codegen pipeline. Reason: the Go tree didn't take that
> dependency, the LoC budget is bounded (9k one-shot), and a codegen
> pipeline adds a second toolchain (Kotlin or a Rust port of Kafka's
> generator). Revisit before Phase 9 if Apache 4.x parity gets queued."

So a later me doesn't try to "modernise" by introducing codegen on a
whim.

---

## Phase 1 exit criteria (all must hold)

1. `cargo test -p sk-codec` runs in < 30 s and is green.
2. Every API key the Go broker registers in
   `archive/internal/broker/broker.go:555-891` has a Rust module with
   `decode_request` + `encode_response` (or the inverse direction for
   keys the broker only sends, e.g. heartbeat in the controller path —
   spell those out individually).
3. For each (api_key, api_version) the Go broker supports, at least one
   captured fixture in `crates/sk-codec/tests/fixtures/` round-trips
   byte-identically.
4. `ApiVersions::Response` built from `api::registry::ALL` is
   byte-identical to the Go broker's `ApiVersions::Response` against
   the same fixture.
5. `tripwires::record_decode_count()` and `batch_reencode_count()` read
   `0` after the full `cargo test -p sk-codec` run.
6. Every `Produce`/`Fetch`-shaped request/response carries record-batch
   data as `Option<Bytes>` — no `Record` struct exists anywhere in
   `sk-codec` (grep gate: `! grep -rn 'struct Record\b' crates/sk-codec/`).
7. `cargo clippy -p sk-codec --all-targets -- -D warnings` passes; no
   `unwrap()` / `expect()` / `panic!()` / `as` casts outside `#[cfg(test)]`.
8. PR CI total stays under 4 min on the empty-cache case.
9. The Go tree under `archive/` is unchanged. The chart, CRDs, scripts,
   and `proto/heartbeat.proto` are bit-identical to their pre-Phase-1
   contents.

If any of these fail, do not merge — fix and re-run.

---

## Risks & mitigations

- **Fixture flakiness from non-deterministic Apache fields.** Some
  responses carry `throttle_time_ms` derived from quota state; capturing
  twice can produce non-identical bytes even for the same request.
  Mitigation: capture against a broker with quotas disabled
  (`producer_byte_rate` unset, no `ClientQuotas` config). For fields that
  vary on principle (`correlation_id` is the obvious one), fixtures
  cover them as part of the header, and the proptest sweep covers the
  full range — the fixture only proves "this exact byte sequence
  round-trips", not "every value of this field works".
- **Borrow-checker friction with `<'a>` request types.** Some APIs
  (CreateTopics, IncrementalAlterConfigs) nest deeply with multiple
  borrow lifetimes. Mitigation: fall back to owning `String` /
  `Bytes` for those modules; the cost is a `to_owned()` per string field
  on the hot path of admin APIs, which are rare-call by design. Don't
  fight the borrow checker on cold paths — owning types are simpler and
  the perf delta is zero on workload-relevant APIs (Produce/Fetch).
- **Tagged-field round-trip when the wire carries non-empty tags.** Most
  Apache code paths emit empty tag blocks, but a few (CreateTopics v7+
  with TopicID) start sending real tags. Mitigation: read side already
  preserves tag bytes verbatim into a `Vec<TaggedField>`; write side
  emits them back in tag order. A proptest property
  (`decode(encode(tags)) == tags`) covers permutations.
- **`crc32c` crate's hardware-acceleration fallback.** On runners
  without SSE4.2, the `crc32c` crate falls back to a software impl that
  is *bytewise* identical but slower. Mitigation: not a correctness
  issue; bench in Phase 8 if it dominates.
- **Byte-opacity drift in code review.** It's tempting to add a `Record`
  struct "just for tests". Mitigation: exit criterion #6 is a grep
  gate, and the Phase 1 `README.md` calls it out as a hard rule. If
  test code needs a decoded representation, it imports
  `sk-test-harness::recordbatch`, never adds one in `sk-codec`.

---

## What this enables for Phase 2

After Phase 1 merges, Phase 2 (storage) lands by:

1. Storing the `Option<Bytes>` produce payload straight to disk —
   no decode round trip, no re-encode on read. The byte-opacity
   guarantee of Phase 1 is what makes `sendfile(2)` (Rust:
   `tokio::fs::File` + `sendfile` syscall) a viable hot path.
2. Calling `recordbatch_count::count_records_in_batches_at` to
   maintain the offset index against a segment file without
   undoing the splice — the helper reads the 61-byte header and
   stops there.
3. Reusing `tripwires::bump_codec_batch_reencode` as the storage
   engine's own byte-opacity guard: any storage path that
   re-encodes a batch bumps the counter, and the byte-opacity
   integration test (ported from `archive/tests/byte-opacity/`)
   asserts zero at the end of every produce-fetch round trip.

No further codec changes — Phase 2 is pure storage work against a stable
codec API.
