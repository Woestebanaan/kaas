# Cluster & log-dir APIs

Per-API reference — see the [API support matrix](../api-matrix.md) for the generated version table.

## ApiVersions

The bootstrap call: tells the client which API keys and version ranges this
broker serves, so everything else on these pages is discoverable rather than
guessed.

**Versions**: v0–v4 (flexible from v3, [KIP-482](../kip/kip-482.md)).

**Handling**: the response is built directly from the codec's `ApiSpec`
registry — the same table that generates the
[API support matrix](../api-matrix.md) — so the advertised surface is the wire
truth by construction: 36 keys, sorted, deduplicated (a registry test asserts
both). The API is on the pre-auth allowlist, so it works before SASL
completes. Two protocol subtleties are implemented faithfully:

- **The v0-response-header quirk**: the ApiVersions *response* header is
  always encoded as header v0 — no tagged-field block — even on flexible
  request versions. This is Apache's documented exception, kept so a client
  that misjudged the broker's capabilities can still parse the error code.
- **Unknown-version fallback**: when a client requests a version outside the
  supported range, the dispatcher does not return `UNSUPPORTED_VERSION` (as it
  does for every other key) — ApiVersions is special-cased so version
  negotiation can always complete.

**Deviations from Apache 3.7**:

- The unknown-version fallback **clamps to the broker's max version and
  answers success**, where Apache answers error 35 in a v0-encoded body
  listing its supported range. Only clients newer than v4 can observe the
  difference.
- The v3+ request fields `client_software_name` / `client_software_version`
  are ignored — the handler doesn't decode the request body at all, so
  they never reach logs or metrics.
- No `SupportedFeatures` / `FinalizedFeatures` tagged fields (KIP-584) in the
  response. They're optional tagged fields, so clients treat their absence as
  "no feature versioning" — consistent with kaas having no KRaft feature
  levels (see [Non-goals](../non-goals.md)).

**Source**: `crates/kaas-broker/src/handlers/api_versions.rs`,
`crates/kaas-codec/src/api/api_versions.rs` (`response_from_registry`, the
always-v0 `response_hdr`), `crates/kaas-codec/src/api/registry.rs`,
`crates/kaas-protocol/src/dispatch.rs` (clamp + pre-auth allowlist).

**Verified by**: `scripts/kafka-broker-api-versions.sh` (asserts the required
API set is advertised and that every advertised range overlaps the Java
client's — any `[usable: -1]` line fails the run); handler and codec
round-trip tests in the source files above; dispatcher clamp tests in
`crates/kaas-protocol/src/dispatch.rs`.

## DescribeLogDirs

Reports log directories and per-partition sizes — `kafka-log-dirs.sh
--describe` and Kafbat-UI's storage pane.

**Versions**: v0–v1 (not flexible).

**Handling**: kaas reports exactly **one log directory per broker** — the
storage engine's data dir (the broker's mount of the shared volume, `/data` in
production). A null topics filter expands to every topic in the broker's
registry with all partitions; a named topic with an empty partition list
expands to all its partitions; unknown topics are silently dropped (matching
Apache, which omits partitions it doesn't host). Each partition row carries
`partition_size` from the storage engine, `offset_lag = 0`, and
`is_future_key = false`; the dir-level `error_code` is always 0.

**Deviations from Apache 3.7**:

- `partition_size` is only real for partitions **this broker currently
  leads** — the engine sums segment sizes of open partitions and reports 0
  for everything else. Apache reports sizes for every replica a broker hosts;
  kaas has no replicas, so per-partition sizes on a multi-broker cluster are
  scattered across the brokers that lead them (`kafka-log-dirs.sh` queries all
  brokers by default, so the union is complete — with zero-rows for the
  non-leaders).
- `offset_lag` is hardwired to 0 and `is_future_key` to false — coherent for
  a broker with no followers and no intra-broker reassignment, but a client
  should not read fetch-lag meaning into it.
- There is no JBOD story: one dir, always. AlterReplicaLogDirs and friends are
  not served (see [Non-goals](../non-goals.md)).

**Source**: `crates/kaas-broker/src/handlers/describe_log_dirs.rs`,
`crates/kaas-storage/src/disk.rs` (`partition_size`, `data_dir`),
`crates/kaas-codec/src/api/describe_log_dirs.rs`.

**Verified by**: `scripts/kafka-log-dirs.sh` (all-dirs describe plus
topic-filtered describe); `partition_size_sums_segment_sizes` in
`crates/kaas-storage/src/disk.rs`.
