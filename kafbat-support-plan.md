# kafbat-ui support plan

Plan for closing the API gap between skafka and what
[kafbat-ui](https://github.com/kafbat/kafka-ui) needs to render its pages
end-to-end. Today (after `v0.1.5-preview`) skafka supports keys
`0,1,2,3,8–20,29–32,36`. Most of kafbat works on that surface; this doc lists
the holes and how to close them.

## Step 0 — Verify first

Speculation is cheap. Before writing any handler, deploy kafbat against the
live broker and click through every page. Capture the actual API keys it
requests:

```bash
kubectl -n skafka logs skafka-0 -f | grep -i "unsupported"
```

The dispatcher logs unsupported keys; the resulting list replaces the
predictions below. Re-prioritise based on what kafbat actually calls.

**Estimated effort:** 30 min.

---

## Step 1 — Tier 1: small handlers, no storage changes

Each follows the exact pattern of `DescribeConfigs` (`v0.1.5-preview`):
codec types + Decode/Encode + handler + register in `broker.go` + smoke test
step. **2–4 hours each.**

| Order | API key | UI value | Touch points |
|-------|---------|----------|--------------|
| 1.1 | **DescribeLogDirs (35)** | Topic / broker "Size" columns | New `StorageEngine.LogDirSize(topic, partition) (int64, error)` walking segments. New codec file. Handler iterates `TopicSource.All()`, sums sizes. |
| 1.2 | **CreatePartitions (37)** | "Add partitions" button on the topic page | New `TopicWriter.IncreasePartitions(name, delta)`. Operator: update `KafkaTopic.spec.partitions`. Broker: register new partition with engine, acquire lease. |
| 1.3 | **OffsetDelete (47)** | Delete consumer-group offsets | New `Coordinator.DeleteOffsets(group, partitions)` persisted via existing `OffsetStore`. |

Each also gains a probe step in `scripts/smoke-test.sh` using the matching
Kafka CLI tool, so future regressions surface there:

- 1.1: `kafka-log-dirs.sh --describe`
- 1.2: `kafka-topics.sh --alter --partitions N`
- 1.3: `kafka-consumer-groups.sh --delete-offsets`

Release each as its own tag: `v0.1.6-preview`, `v0.1.7-preview`,
`v0.1.8-preview`.

---

## Step 2 — Tier 2: honest stubs

| API key | Plan | Notes |
|---------|------|-------|
| **AlterConfigs (33)** + **IncrementalAlterConfigs (44)** | Stub returning `INVALID_CONFIG` per resource. | ~1 hour. Real per-topic config storage is a Phase-11-sized feature (needs a config persistence layer on the PVC). Stubbing now is honest about read-only behaviour without implying support. |
| **DescribeCluster (60)** | Skip. | Modern kafbat falls back to Metadata when DescribeCluster is unavailable. Reconsider only if Step 0 shows it as a hard requirement. |

---

## Step 3 — Tier 3: storage-engine work

| API key | Why it's heavier | Sketch |
|---------|------------------|--------|
| **DeleteRecords (21)** | Mutates `logStart`, triggers segment deletion, requires index/timeindex fixups. | New `StorageEngine.TruncateBefore(topic, partition, offset)`. Advance `partitionState.logStart`, drop closed segments below the new offset, rewrite the active index if needed. **~1 day.** Only worth doing if kafbat users actually use the "Delete messages" button. |

---

## Out of scope

These belong to the v2 (Streams-compatible) milestone per
`project_skafka.md`; do **not** pull them in for kafbat support:

- **InitProducerId (22)**
- **DescribeProducers (61)**
- **DescribeTransactions (65)**

These are the transactional-producer surface and require a much larger
lift (idempotency state, transaction coordinator, segment-level
abort markers).

---

## Suggested execution order

1. **Step 0** — deploy kafbat, capture real punch list.
2. **Tier 1** in order 1.1 → 1.2 → 1.3, each as its own commit + release tag.
3. **AlterConfigs stub** once Step 0 reveals whether kafbat hides the
   edit affordance or surfaces the unsupported-API error confusingly.
4. **Re-check kafbat** end-to-end. Decide whether **DeleteRecords**
   (Tier 3) is worth implementing based on user demand.

**Total Tier 1 effort: ~1 day.** That should give a fully-usable kafbat for
the read-only + ACL-management workflow, which is what most teams actually
need a UI for.

---

## Implementation template

Every new API key follows the same five-step pattern. Capturing it here so
it's mechanical for whoever picks up the next item:

1. **Codec types** — add `XxxRequest` / `XxxResponse` structs in
   `internal/protocol/codec/api/<name>.go`.
2. **Encode / Decode** — implement `DecodeXxxRequest` and `EncodeXxxResponse`
   for the version range. If max version is < 4 (or whatever the spec says
   for that key), non-flexible encoding is enough.
3. **Round-trip test** — add a test in
   `internal/protocol/codec/api/remaining_codecs_test.go` exercising
   request decode and response encode for every supported version.
4. **Handler** — implement in `internal/protocol/handlers/` (extend
   `admin.go` for admin keys, otherwise its own file). Wire in any new
   storage / coordinator / topic dependencies via interfaces.
5. **Register** — add `d.Register(<key>, <minVer>, <maxVer>, ...)` in
   `Broker.RegisterHandlers` (`internal/broker/broker.go`) and add a smoke
   test step.

If a new ApiVersion negotiation is flexible (v4+ for many keys), update
`flexibleMin` in `internal/protocol/frame.go` accordingly.
