# Phase 2 Breakdown: Kafka Protocol Layer

Goal: a hand-rolled broker-side Kafka wire protocol codec, TCP server, and
request handlers for all Priority 1 API keys. No franz-go or sarama imports
inside `internal/` — use them as reference source and as test clients only.

---

## Step 2.1 — Binary codec primitives

Files: `internal/protocol/codec/reader.go`, `internal/protocol/codec/writer.go`

Implement the low-level read/write primitives that every API codec builds on.
Get these right first — every bug here propagates to every API.

Reader:
- `ReadInt8`, `ReadInt16`, `ReadInt32`, `ReadInt64`
- `ReadUvarint`, `ReadVarint`              ← Kafka uses both; don't mix them up
- `ReadString`, `ReadNullableString`       ← prefixed by int16 length (-1 = null)
- `ReadCompactString`                      ← prefixed by uvarint length (newer APIs)
- `ReadBytes`, `ReadNullableBytes`
- `ReadArray(fn)` / `ReadCompactArray(fn)` ← int32 vs uvarint element count
- `ReadTaggedFields`                       ← flexible version APIs; skip unknown tags

Writer: symmetric set of the above.

Unit tests (tests/unit/codec_primitives_test.go):
- Round-trip encode → decode for every primitive
- Varint edge cases: 0, 1, -1, MaxInt32, MinInt32
- Null string and null bytes (length = -1)
- Empty array and compact array
- TaggedFields with zero tags (most common case)

**Done when:** all primitive round-trip tests pass; no dependency on any API shape

---

## Step 2.2 — Shared wire types

File: `internal/protocol/codec/types.go`

Define the structs used across multiple API codecs:

```
RecordBatch:
  baseOffset            int64
  batchLength           int32
  partitionLeaderEpoch  int32
  magic                 int8    ← must be 2
  crc                   uint32  ← CRC32C of all bytes after this field
  attributes            int16   ← bits: compression(0-2), timestampType(3),
                                         isTransactional(4), isControlBatch(5)
  lastOffsetDelta       int32
  baseTimestamp         int64
  maxTimestamp          int64
  producerId            int64
  producerEpoch         int16
  baseSequence          int32
  records               []Record

Record (within a batch):
  attributes            int8
  timestampDelta        varint
  offsetDelta           varint
  key                   []byte  ← nullable
  value                 []byte  ← nullable
  headers               []RecordHeader

RecordHeader:
  key                   string
  value                 []byte

ErrorCode: typed int16 — define constants for all codes used in v1:
  NONE=0, UNKNOWN_TOPIC_OR_PARTITION=3, LEADER_NOT_AVAILABLE=5,
  NOT_LEADER_OR_FOLLOWER=6, REQUEST_TIMED_OUT=7, TOPIC_AUTHORIZATION_FAILED=29,
  GROUP_AUTHORIZATION_FAILED=30, UNSUPPORTED_VERSION=35,
  INVALID_PRODUCER_EPOCH=47, (full list from spec)
```

Unit tests:
- RecordBatch encode → decode round-trip
- CRC32C field is correct after encode (verify against known-good bytes)
- Attributes bit flags set and read correctly

**Done when:** RecordBatch round-trip test passes with CRC32C validation

---

## Step 2.3 — CRC32C implementation

File: `internal/protocol/codec/crc32c.go`

- Use `hash/crc32` with the Castagnoli polynomial (NOT IEEE — common bug)
- `ComputeCRC(data []byte) uint32`
- `ValidateCRC(data []byte, expected uint32) error`
- The CRC covers all RecordBatch bytes after the crc field itself

Unit tests:
- Validate against known-good byte sequences from the Kafka protocol spec
- Verify Castagnoli is used, not IEEE (different polynomial, different result)

**Done when:** CRC matches known-good test vectors from the spec

---

## Step 2.4 — TCP server

File: `internal/protocol/server.go`

- Listen on configurable port (default 9092), optional TLS on 9093
- One goroutine per connection, context-aware shutdown
- Read loop: decode request frame, dispatch, write response frame

Request frame layout:
```
[total_length: int32][api_key: int16][api_version: int16]
[correlation_id: int32][client_id: nullable_string]
[tagged_fields if flexible version][body...]
```

Response frame layout:
```
[total_length: int32][correlation_id: int32]
[tagged_fields if flexible version][body...]
```

Connection-level state to track:
- Authenticated `Principal` (set after SASL exchange)
- `client_id` string
- Negotiated API versions (populated after ApiVersions exchange)

**Done when:** server accepts a raw TCP connection, reads a framed request,
and writes back a framed response (handler can be a stub at this point)

---

## Step 2.5 — Request dispatcher

File: `internal/protocol/dispatch.go`

- Map `api_key` → decode + handle + encode function
- All handlers must be goroutine-safe (called concurrently per connection)
- Unknown api_key: respond with `UNSUPPORTED_VERSION` error
- Unsupported version for a known key: respond with ApiVersions error

**Done when:** dispatcher routes a hardcoded test request to a stub handler
and returns a valid framed response

---

## Step 2.6 — ApiVersions handler (API key 18)

File: `internal/protocol/codec/api/api_versions.go`

Implement first because clients call this before anything else. If it's wrong,
no client will connect — they use the response to negotiate versions for all
subsequent calls.

Response must list every implemented API key with its supported min and max version:

| API key | Name | Min | Max |
|---|---|---|---|
| 0 | Produce | 3 | 9 |
| 1 | Fetch | 4 | 13 |
| 2 | ListOffsets | 1 | 7 |
| 3 | Metadata | 1 | 12 |
| 8 | OffsetCommit | 2 | 8 |
| 9 | OffsetFetch | 1 | 8 |
| 10 | FindCoordinator | 0 | 4 |
| 11 | JoinGroup | 2 | 9 |
| 12 | Heartbeat | 0 | 4 |
| 13 | LeaveGroup | 0 | 4 |
| 14 | SyncGroup | 0 | 5 |
| 15 | DescribeGroups | 0 | 5 |
| 16 | ListGroups | 0 | 4 |
| 17 | SaslHandshake | 0 | 1 |
| 18 | ApiVersions | 0 | 3 |
| 19 | CreateTopics | 0 | 7 |
| 20 | DeleteTopics | 0 | 6 |
| 29 | DescribeAcls | 0 | 3 |
| 30 | CreateAcls | 0 | 3 |
| 31 | DeleteAcls | 0 | 3 |
| 36 | SaslAuthenticate | 0 | 2 |

Unit tests:
- Response contains all listed API keys with correct min/max versions
- Version 3 uses compact arrays (flexible version); version 0-2 use legacy arrays

**Done when:** a real Kafka client (franz-go in test) calls ApiVersions and
gets back a valid response with no errors

---

## Step 2.7 — Per-API codecs: core produce/consume/metadata

Files in `internal/protocol/codec/api/`:

Implement in this order (unblocks the most downstream work first):

1. `metadata.go` (key 3) — clients call this immediately after ApiVersions
   - Request: list of topic names (empty = all topics)
   - Response: broker list + topic/partition metadata + leader info
   - Version range: v1–v12

2. `produce.go` (key 0) — write path
   - Request: acks, timeout, topic+partition → RecordBatch list
   - Response: per-partition base offset + error code + log append time
   - Version range: v3–v9

3. `fetch.go` (key 1) — read path
   - Request: max wait, min/max bytes, topic+partition → fetch offset
   - Response: per-partition records + high watermark + error code
   - Version range: v4–v13
   - Note: v4+ adds isolation_level field (read_uncommitted=0 for v1)

4. `list_offsets.go` (key 2)
   - Request: topic+partition → timestamp (-1=latest, -2=earliest)
   - Response: per-partition offset + timestamp + error code
   - Version range: v1–v7

Each file implements:
```go
func DecodeXxxRequest(r *Reader, version int16) (*XxxRequest, error)
func EncodeXxxResponse(w *Writer, resp *XxxResponse, version int16)
```

Unit tests for each: encode → decode round-trip at min and max supported version

**Done when:** all four encode/decode round-trips pass at every supported version

---

## Step 2.8 — Per-API codecs: SASL auth

Files: `internal/protocol/codec/api/sasl_handshake.go`,
       `internal/protocol/codec/api/sasl_authenticate.go`

Implement before consumer group APIs because auth precedes all group operations.

SaslHandshake (key 17):
- Request: mechanism name ("SCRAM-SHA-512", "PLAIN")
- Response: error code + list of enabled mechanisms

SaslAuthenticate (key 36):
- Request: auth_bytes (SASL payload, mechanism-specific)
- Response: error code + auth_bytes (server challenge/response)
- Session token in response (v2+)

**Done when:** round-trip tests pass; actual SASL logic wired in Phase 7

---

## Step 2.9 — Per-API codecs: consumer group

Files in `internal/protocol/codec/api/`:
`find_coordinator.go`, `join_group.go`, `heartbeat.go`,
`leave_group.go`, `sync_group.go`, `offset_commit.go`, `offset_fetch.go`,
`describe_groups.go`, `list_groups.go`

Implement as a batch — they share similar request/response shapes.

Key details:
- JoinGroup v2+: session_timeout_ms + rebalance_timeout_ms
- SyncGroup v0–v5: assignment bytes are opaque to the broker
- OffsetCommit v2+: timestamp field removed
- OffsetFetch v8+: uses compact arrays

Unit tests: round-trip for each at min and max supported version

**Done when:** all nine encode/decode round-trips pass

---

## Step 2.10 — Per-API codecs: admin

Files: `create_topics.go`, `delete_topics.go`, `describe_acls.go`,
       `create_acls.go`, `delete_acls.go`

CreateTopics (key 19): topic name + num_partitions + replication_factor + configs
DeleteTopics (key 20): list of topic names
DescribeAcls (key 29): filter by resource/principal/operation
CreateAcls (key 30): list of AclCreation entries
DeleteAcls (key 31): list of AclBinding filters

Unit tests: round-trip for each at min and max supported version

**Done when:** all five encode/decode round-trips pass

---

## Step 2.11 — Business logic handlers (stubs → real)

Files in `internal/protocol/handlers/`:
`produce.go`, `fetch.go`, `metadata.go`, `consumer_group.go`, `admin.go`

Wire the decoded requests to the core interfaces from Phase 1:
- Produce handler: calls `LeaseManager.IsLeader` + `StorageEngine.Append`
- Fetch handler: calls `StorageEngine.Read` + `StorageEngine.HighWatermark`
- Metadata handler: reads broker list from EndpointSlice cache + topic list from CRD cache
- Admin handlers: delegate to operator CRDs (return NOT_CONTROLLER for write ops in v1)
- Auth check on every handler: `AuthEngine.Authorize` before touching storage

At this stage StorageEngine and LeaseManager are still interfaces — pass in
stub implementations that return sensible defaults so the server is runnable.

**Done when:** server starts, accepts connections, and returns non-error
responses for Produce and Fetch against stub storage

---

## Step 2.12 — End-to-end compatibility tests

Directory: `tests/kafka-compat/`

Add franz-go and segmentio/kafka-go as test-only dependencies:
```
go get -d github.com/twmb/franz-go@latest        (test dep only)
go get -d github.com/segmentio/kafka-go@latest    (test dep only)
```

Test cases (each starts a real skafka server in-process or via TestMain):
1. franz-go client: ApiVersions negotiation succeeds
2. franz-go client: produce 1,000 records to a topic, consume all, verify order
3. kafka-go client: same produce + consume test
4. Both clients: produce to a non-existent topic → correct error code returned
5. Both clients: fetch from offset beyond high watermark → empty response, no error

kcat tests (deferred until kcat is installed):
- `kcat -b localhost:9092 -t test-topic -P` produce via CLI
- `kcat -b localhost:9092 -t test-topic -C` consume via CLI

**Done when:** franz-go and kafka-go produce+consume tests pass end-to-end
against a running skafka instance

---

## Dependencies needed before Phase 2 starts

| Item | Status |
|---|---|
| Go 1.26.1 | ✅ ready |
| golangci-lint | ✅ ready |
| franz-go (test dep) | ❌ add to go.mod in Step 2.12 |
| segmentio/kafka-go (test dep) | ❌ add to go.mod in Step 2.12 |
| kcat | ❌ defer until Step 2.12 |

No blockers — Phase 2 can start now with Step 2.1.
