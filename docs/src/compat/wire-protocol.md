# Wire protocol & framing

Length-prefixed frames, KIP-482 flexible versions with tagged fields, and byte-opaque RecordBatch handling.

Everything wire-level lives in `crates/kaas-codec`. The crate's standing
claim: every registered API key/version encodes and decodes byte-identically
against fixtures captured from Apache Kafka 3.7.

## Framing and headers

Kafka's transport is length-prefixed frames: a 4-byte big-endian size, then
the message. `frame.rs` implements the reader/writer (with a streaming
`FrameReader` for the server accept loop). Inside a frame:

- **Request header** — api key, api version, correlation ID, client ID, and
  (for flexible versions) tagged fields. The header's *own* version depends
  on `(api_key, api_version)`; `headers.rs` resolves it through the per-API
  `HeaderVersion` functions carried on each registry entry.
- **Response header** — correlation ID, plus tagged fields when flexible.

## KIP-482: flexible versions and tagged fields

From a per-API cutover version, Kafka switches to "flexible" encoding:
compact strings and arrays (varint lengths) and **tagged fields** — an
extensible `(tag, size, bytes)` section that lets brokers and clients attach
optional data without a version bump. `tagged.rs` implements the envelope.

Which version each API flips to flexible is data, not code: the "Flexible"
column in the [generated API matrix](api-matrix.md) comes straight from the
registry (`crates/kaas-codec/src/api/registry.rs`), whose `ApiSpec` table
also drives the ApiVersions response — the matrix cannot disagree with what
the broker advertises.

## The byte-opacity contract

There is **no `Record` struct in kaas** — deliberately. RecordBatch payloads
flow through the codec as `Option<bytes::Bytes>`: a zero-copy slice of the
request frame on the way in, the same bytes off disk on the way out. The
only code that reads past the fixed v2 batch header is CRC verification
(`crc.rs`, CRC32C over opaque input) and the batch-header walker in
`recordbatch_count.rs`.

Consequences worth internalizing:

- The log file *is* the wire format — Produce appends the client's bytes
  verbatim (base offset rewritten in place), Fetch returns them verbatim
  (see [Storage engine hot path](../architecture/storage-hot-path.md)).
- Per-record features (timestamps, headers, per-record validation) are
  honoured **byte-opaquely**: kaas preserves what producers wrote without
  interpreting it. Where Apache applies per-record semantics — e.g.
  tombstone expiry during compaction — kaas applies the batch-level
  equivalent.
- The contract is enforced, not aspirational: tripwire counters
  (`tripwires.rs`, surfaced as metrics — see
  [Observability](../architecture/observability.md)) must read zero after
  every test run, and any future record-decoding code path is required to
  bump them, making the first violation loud.

## Where to dig deeper

- [API support matrix](api-matrix.md) — every served key, generated from
  the registry.
- [Per-API reference](api-reference.md) — semantics and deviations per key.
- [KIP index](kip-index.md) — protocol-evolution KIPs and where kaas stands
  on each.
