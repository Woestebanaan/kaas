# KIP index

All KIPs the codebase references, split honestly: implemented, partial, or deliberate non-goal.

The split below is source-verified, not aspirational: "implemented" means the
behaviour ships and is exercised by tests or the
[shell-tool suite](verification.md); "partial" pages lead with what's
*missing*; non-goals get a rationale in [Non-goals](non-goals.md), not
silence.

> One correction relative to earlier planning documents: **KIP-516 is listed
> as partial, not implemented.** The operator mints and preserves topic IDs
> correctly, but the broker currently serves the all-zero sentinel on the
> wire — see the [KIP-516 page](kip/kip-516.md).

## Implemented (15)

| KIP | What it is | kaas page |
|---|---|---|
| KIP-13 | Per-broker client quotas (byte-rate throttling) | [KIP-13](kip/kip-13.md) |
| KIP-32 | Record timestamps (`CreateTime` / `LogAppendTime`) | [KIP-32](kip/kip-32.md) |
| KIP-58 | `min.compaction.lag.ms` compaction gate | [KIP-58](kip/kip-58.md) |
| KIP-98 | Exactly-once: idempotent producer + transactions | [KIP-98](kip/kip-98.md) |
| KIP-107 | `DeleteRecords` admin API (key 21) | [KIP-107](kip/kip-107.md) |
| KIP-195 | `CreatePartitions` admin API (key 37) | [KIP-195](kip/kip-195.md) |
| KIP-290 | Prefixed ACL resource patterns | [KIP-290](kip/kip-290.md) |
| KIP-339 | `IncrementalAlterConfigs` (key 44) | [KIP-339](kip/kip-339.md) |
| KIP-354 | Bounded tombstone retention (`delete.retention.ms`) | [KIP-354](kip/kip-354.md) |
| KIP-360 | Producer epoch bump on re-initialization | [KIP-360](kip/kip-360.md) |
| KIP-371 | mTLS principal mapping (`ssl.principal.mapping.rules`) | [KIP-371](kip/kip-371.md) |
| KIP-447 | EOS v2: producer-scalable transactional offsets | [KIP-447](kip/kip-447.md) |
| KIP-482 | Flexible versions + tagged fields | [KIP-482](kip/kip-482.md) |
| KIP-546 | Client-quota admin APIs (keys 48/49) | [KIP-546](kip/kip-546.md) |
| KIP-800 | Join/leave reason strings | [KIP-800](kip/kip-800.md) |

## Partial (6)

Each page leads with the "what's missing" block — these are the book's
credibility test.

| KIP | Landed | Missing | kaas page |
|---|---|---|---|
| KIP-101 | segment filenames carry the leader epoch | leader-epoch cache + lookup (`offset_for_leader_epoch` returns the `(-1,-1)` sentinel); wire key 23 unregistered | [KIP-101](kip/kip-101.md) |
| KIP-219 | `throttle_time_ms` computed (debt-carry) and returned | the broker never mutes the channel after responding — throttling relies on client cooperation | [KIP-219](kip/kip-219.md) |
| KIP-345 | `group.instance.id` plumbed through join/sync; static members survive the eviction sweep | `FENCED_INSTANCE_ID` fencing of duplicate static members | [KIP-345](kip/kip-345.md) |
| KIP-394 | `MEMBER_ID_REQUIRED` error code defined | the v4+ two-step join handshake — `join()` still takes the legacy assign-inline path | [KIP-394](kip/kip-394.md) |
| KIP-516 | operator mints `Status.TopicID` (v4 UUID, never rotated) | broker-side wire propagation — the production topic watch inserts the all-zero sentinel, so Metadata serves nil topic IDs | [KIP-516](kip/kip-516.md) |
| KIP-554 | operator-side SCRAM credential rotation path | wire keys 50/51 entirely — no codec modules, no dispatch | [KIP-554](kip/kip-554.md) |

## Deliberate non-goals (8)

Rationale for each in [Non-goals](non-goals.md).

| KIP | What it is | Why not |
|---|---|---|
| KIP-48 | Delegation tokens | no token-based auth surface; SCRAM/mTLS cover the deployment model |
| KIP-227 | Incremental fetch sessions | stateless by contract: `SessionID=0` on every response |
| KIP-405 | Tiered storage | deferred, not refused — the NFS substrate is already a near-tier |
| KIP-664 | Describe/ListTransactions tooling | follow-up; slot files are directly inspectable meanwhile |
| KIP-714 | Client metrics push | out of scope for the preview line |
| KIP-848 | Next-gen consumer rebalance protocol | post-3.7 |
| KIP-932 | Share groups (queues) | Kafka 4.0+ |
| KIP-1071 | Streams rebalance protocol | post-3.7 |
