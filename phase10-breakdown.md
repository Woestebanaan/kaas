# Phase 10 Breakdown — Observability

> Plan reference: `skafka-plan-v3.md` lines 1111–1211.
>
> Phase 10 is the closing phase of the v1 architecture: every metric,
> healthz field, and Grafana panel called for in the plan should
> emit, with byte-opacity tripwires that alert if anyone violates the
> "broker is a byte mover" invariant.

## What Phase 10 actually means

There are three distinct deliverables:

1. **Metrics emission.** The plan lists ~50 Prometheus metric names.
   Roughly half exist as instruments on the `Metrics` struct in
   `internal/observability/metrics.go`; only ~10 are actually
   `Add()`-ed or `Record()`-ed at call sites. The rest are
   either declared-but-unused (no producer in the codebase) or
   not declared at all. Phase 10 closes that gap.
2. **Healthz schema.** The plan specifies a JSON schema for `/healthz`
   that includes `is_controller`, `controller_id`, `heartbeat_rtt_ms`,
   `assignment_version`, `partitions_led`, etc. The current handler
   only returns broker_id + listeners + tls. Most of those fields are
   reachable from the v3 runtime (BrokerCoordinator, ControllerWatch)
   but aren't surfaced through the handler.
3. **Grafana dashboard updates.** A `deploy/grafana/skafka-dashboard.json`
   exists with panels for the v2.6-era metrics (produce/fetch
   throughput, latency, TLS, auth). The plan calls for v3.3 additions:
   produce batch size distribution, compression breakdown, codec
   tripwire counters, controller failover panels.

## Plan vs. current state

| Plan metric | In `Metrics` struct? | Emitted at call sites? | Notes |
|---|---|---|---|
| **Throughput** | | | |
| `skafka_produce_records_total{topic}` | ✅ ProduceRecords | ✅ produce.go:130 | |
| `skafka_produce_bytes_total{topic}` | ✅ ProduceBytes | ✅ produce.go:127 | |
| `skafka_fetch_records_total{topic, consumer_group}` | ✅ FetchRecords | ✅ fetch.go:103 (no group label) | Missing `consumer_group` label |
| `skafka_fetch_bytes_total{topic, consumer_group}` | ✅ FetchBytes | ✅ fetch.go:101 | Missing `consumer_group` label |
| `skafka_produce_batches_total{topic, compression}` | ❌ | ❌ | NEW — needs codec to peek the compression bits without decoding |
| `skafka_produce_batch_size_bytes{topic}` | ❌ | ❌ | NEW histogram |
| **Storage** | | | |
| `skafka_partition_high_watermark{topic, partition}` | ❌ | ❌ | Needs ObservableGauge over engine.HighWatermark() |
| `skafka_storage_write_latency_seconds{topic}` | ✅ WriteLatency | ✅ produce.go:120 | |
| `skafka_storage_read_latency_seconds{topic}` | ✅ ReadLatency | ✅ fetch.go:96 | |
| `skafka_storage_fsync_latency_seconds` | ✅ FsyncLatency | ❌ | Declared but never emitted — DiskStorageEngine.Append doesn't time fsync |
| `skafka_segment_count{topic, partition}` | ❌ | ❌ | NEW ObservableGauge over engine state |
| `skafka_recovery_duration_seconds{topic, partition}` | ❌ | ❌ | NEW histogram around recovery.go's scan loop |
| **Coordination** | | | |
| `skafka_is_controller{broker}` | ❌ | ❌ | NEW ObservableGauge — query ControllerWatch.CurrentHolder() |
| `skafka_controller_failovers_total` | ❌ | ❌ | NEW counter, increment in election callback |
| `skafka_controller_failover_duration_seconds` | ❌ | ❌ | NEW histogram, time from lease loss to AssignmentLoop start |
| `skafka_assignment_version` | ❌ | ❌ | NEW ObservableGauge — Coordinator.Snapshot().AssignmentVersion |
| `skafka_assignment_changes_total` | ❌ | ❌ | NEW counter in OnAssignmentChange |
| `skafka_assignment_file_writes_total{result}` | ❌ | ❌ | NEW counter in FileStore.Write |
| `skafka_assignment_file_write_latency_seconds` | ❌ | ❌ | NEW histogram in FileStore.Write |
| `skafka_assignment_file_size_bytes` | ❌ | ❌ | NEW ObservableGauge from latest file stat |
| `skafka_assignment_pushes_total` | ❌ | ❌ | NEW counter in HeartbeatServer broadcast |
| `skafka_assignment_polls_total{change_detected}` | ❌ | ❌ | NEW counter in AssignmentPoll loop |
| `skafka_stale_assignments_rejected_total` | ❌ | ❌ | NEW counter in Coordinator epoch-fence check |
| `skafka_assignment_cr_mirror_writes_total{result}` | ❌ | ❌ | NEW counter in K8sMirror |
| `skafka_heartbeat_rtt_seconds{broker}` | ❌ | ❌ | NEW histogram — broker-side RTT measurement |
| `skafka_heartbeat_misses_total{broker}` | ❌ | ❌ | NEW counter when SelfFence fires |
| `skafka_self_fence_events_total{broker}` | ❌ | ❌ | NEW counter — same site as heartbeat_misses |
| `skafka_broker_count_alive` | ❌ | ❌ | NEW ObservableGauge — len(brokerSource.AliveBrokers) |
| `skafka_broker_count_assigned` | ❌ | ❌ | NEW ObservableGauge — len(distinct brokers in assignment.json) |
| `skafka_takeover_duration_seconds{topic, partition}` | ❌ | ❌ | NEW histogram around TakeoverDriver.OnAssignmentChange |
| `skafka_takeover_safety_delay_seconds{topic, partition}` | ❌ | ❌ | NEW gauge — recorded when takeover starts |
| **Byte-opacity tripwires** | | | |
| `skafka_codec_record_decode_total` | ❌ | ❌ | NEW counter — MUST stay at zero. Increment in any code path that decodes individual records (none should exist) |
| `skafka_codec_batch_reencode_total` | ❌ | ❌ | NEW counter — same. Increment in any code path that re-encodes a RecordBatch (none should exist) |
| **CRC validation** | | | |
| `skafka_produce_crc_failures_total{topic}` | ❌ | ❌ | NEW counter — already detect CRC failures in produce.go (`ErrCorruptMessage`); just add the counter |
| **Leadership** | | | |
| `skafka_partition_leader{topic, partition}` | ❌ | ❌ | NEW ObservableGauge — leases.LeaderFor |
| `skafka_partition_epoch{topic, partition}` | ❌ | ❌ | NEW ObservableGauge — Coordinator.CurrentEpoch |
| **NFS / storage** | | | |
| `skafka_storage_estale_total` | ❌ | ❌ | NEW counter — wrap fs syscalls with ESTALE detection |
| `skafka_storage_open_retries_total` | ❌ | ❌ | NEW counter in segment open path |
| `skafka_storage_fsync_errors_total` | ❌ | ❌ | NEW counter — emit on engine.Append fsync error |
| **Consumer groups** | | | |
| `skafka_consumer_group_lag{topic, partition, consumer_group}` | ❌ | ❌ | ObservableGauge — high_watermark - committed_offset (read from offsetStore) |
| `skafka_consumer_group_members{consumer_group}` | ❌ | ❌ | NEW ObservableGauge — coordinator.Manager state |
| `skafka_consumer_group_rebalances_total{consumer_group}` | ✅ GroupRebalances | ❌ | Declared but never emitted — Coordinator never increments |
| **Auth** | | | |
| `skafka_auth_success_total{mechanism}` | ✅ AuthSuccess | ✅ sasl.go:121 (no `mechanism` label) | Missing `mechanism` label |
| `skafka_auth_failure_total{mechanism, reason}` | ✅ AuthFailure | ✅ sasl.go:104 (no labels) | Missing `mechanism` + `reason` labels |
| `skafka_acl_deny_total{principal, resource_type}` | ✅ ACLDeny | ✅ acl.go:133 | Need to confirm labels match plan |
| `skafka_quota_throttle_total{principal}` | ✅ QuotaThrottle | ❌ | No quota engine yet — declared placeholder |
| **External** | | | |
| `skafka_external_connections_total{mode, broker_id}` | ✅ Connections | ✅ server.go:116 (no mode/broker labels) | Missing `mode`, `broker_id` labels |
| `skafka_tls_handshakes_total{broker, result}` | ✅ TLSHandshakes | ✅ server.go:135-140 (only `result`) | Missing `broker` label (low value — broker is implicit per pod) |
| `skafka_cert_reload_total{broker}` | ✅ CertReloads | ✅ tls.go:131 | Missing `broker` label |
| `skafka_not_leader_returned_total{topic, partition}` | ❌ | ❌ | NEW counter — increment alongside the ErrNotLeaderOrFollower assignment in produce.go:105 / fetch.go:76 / list_offsets.go:36 |

| Plan healthz field | Currently in /healthz? | Source |
|---|---|---|
| `status: "ok"` | ✅ | hardcoded |
| `broker_id` | ✅ | env |
| `is_controller` | ❌ | broker.ControllerWatch.CurrentHolder() == self |
| `controller_id` | ❌ | broker.ControllerWatch.CurrentHolder() |
| `controller_epoch` | ❌ | broker.ControllerWatch.CurrentEpoch() |
| `heartbeat_rtt_ms` | ❌ | last RTT recorded by HeartbeatClient.Send |
| `heartbeat_age_ms` | ❌ | ms since last heartbeat received (broker.SelfFence) |
| `assignment_version` | ❌ | broker.Coordinator.Snapshot().AssignmentVersion |
| `assignment_age_ms` | ❌ | ms since AssignmentStore last applied a change |
| `partitions_led` | ❌ | count of partitions where Coordinator.Owns is true |
| `partitions_assigned` | ❌ | count of partitions where assignment.json says self |
| `partitions_recovering` | ❌ | count of partitions in TakeoverDriver's in-progress map |

## Gaps to close

### Gap #1: Surface the v3 runtime state through `/healthz` (P0) — DONE

> **Status:** shipped. `internal/observability/health.go` now defines a
> `RuntimeState` interface and the handler returns the plan's full
> schema (`is_controller`, `controller_id`, `controller_epoch`,
> `heartbeat_age_ms`, `assignment_version`, `assignment_age_ms`,
> `partitions_led`, `partitions_assigned`, `partitions_recovering`).
> `cmd/skafka/cluster_runtime.go` adds a `healthRuntimeState` adapter
> that reads from `*broker.Coordinator` + `*broker.ControllerWatch` +
> `*broker.HeartbeatClient`. `cmd/skafka/main.go` plumbs it through
> a new `healthServerConfig`. Three unit tests in
> `internal/observability/health_test.go` cover the no-runtime path,
> the full plan schema, and the "no measurement yet" case where -1
> from the source becomes JSON-omitted.
>
> Two fields are explicit follow-ups for Gap #3:
> `HeartbeatRTTMs` (needs heartbeat protocol echo) and
> `PartitionsRecovering` (needs TakeoverDriver instrumentation).
> Both return -1 / 0 in the meantime — correct in steady state.

### Gap #1 (original): Surface the v3 runtime state through `/healthz` (P0)

The plan's `/healthz` schema is the operator's go-to debug endpoint
for "is this broker actually doing what the controller said". Today
it's a stub. Wiring it requires plumbing the BrokerCoordinator +
ControllerWatch + HeartbeatClient through to `internal/observability/health.go`,
which today knows nothing about the v3 runtime.

**Scope**:
- Define a `RuntimeStateSource` interface in `internal/observability/`
  with the methods needed for the plan's fields (controller info,
  heartbeat ages, assignment version, partition counts).
- Make `*broker.Coordinator` + `*broker.ControllerWatch` +
  `*broker.HeartbeatClient` together implement that interface (or
  pass an adapter from `cmd/skafka/main.go`).
- Replace `HealthHandler` with a constructor that takes the source
  and returns a richer JSON.
- Keep the v2.6-fallback "no v3 runtime" path working — local-dev
  mode without k8s should still get a sensible (mostly-zero) response.

**Test**: `internal/observability/health_test.go` — table-driven
test that passes a stub source and asserts the JSON shape.

### Gap #2: Emit declared-but-unused metrics (P0) — DONE

> **Status:** shipped.
>
> - `FsyncLatency` → `internal/storage/engine.go` `flushLocked` now
>   records the log+index Sync + manifest write duration.
> - `LeaseAcquired` / `LeaseLost` → repurposed for the v3
>   cluster-controller lease (description updated; v2.6 partition-Lease
>   semantics are gone). Emitted from
>   `cmd/skafka/cluster_runtime.go`'s onAcquired / onLost callbacks.
>   Sum across brokers ≈ total controller failovers.
> - `GroupRebalances` → `internal/coordinator/group.go`
>   `completeRebalance` increments with `consumer_group` label.
> - `QuotaThrottle` → kept as a forward-compat placeholder; struct
>   comment now spells out that there is no v1 emitter and that
>   dashboards should treat it as flat-zero.
>
> Also fixed a flake in the Phase 9 `TestExternalListenerPerBrokerHostnames`
> (the dial-trace assertion raced franz-go's connection-pool warm-up).
> Replaced the trace check with a direct Metadata request +
> per-broker `host` assertion — deterministic and stronger.

### Gap #2 (original): Emit declared-but-unused metrics (P0)

Five existing metrics are declared in the `Metrics` struct but never
emitted. Either delete them or wire them — declared-unused is worse
than missing because dashboards built against them will silently
flat-line:

- `FsyncLatency` → wrap `engine.Append` fsync call with timing.
- `LeaseAcquired` / `LeaseLost` → emit in
  `internal/lease/k8s_manager.go` callbacks.
- `GroupRebalances` → emit in `internal/coordinator/manager.go`
  rebalance completion path.
- `QuotaThrottle` → no quota engine exists; either delete the
  instrument or leave an explicit `// PLAN: post-v1` note.

Most are 1–3 line changes at known call sites. `LeaseAcquired/Lost`
become tricky in v3 because the partition-Lease path is gone —
treating "controller-Lease acquired" as the new emission point is
the right call (one event per failover, not per partition).

**Test**: spot-check via `internal/observability/metrics_test.go`
that each instrument records ≥1 sample under a synthetic harness.

### Gap #3b: Broker-side counters — DONE

> **Status:** shipped. Five instruments wired:
>
> - `HeartbeatRTT` — histogram. Required a proto extension:
>   `ControllerCommand.broker_status_timestamp_ms` echoes the
>   most recent `BrokerStatus.timestamp_ms` the controller saw.
>   Broker computes RTT = now - echo on every PING. Skipped when
>   echo is zero (first PING after stream open, before the
>   controller has seen any BrokerStatus).
> - `HeartbeatMisses` — incremented at `produce.checkOwnership`
>   when `heartbeatFresh()` returns false. Hot-path, but only
>   under outage; in steady state, zero emissions.
> - `SelfFenceEvents` — same site as HeartbeatMisses. Each
>   rejected produce on stale heartbeat = one self-fence event.
> - `AssignmentPolls{change_detected}` — emitted in Coordinator's
>   Watch loop. `applyIfNew` now returns a bool to drive the label.
> - `StaleAssignmentsRejected` — incremented in the epoch-fence
>   path inside applyIfNew when `a.ControllerEpoch < leaseEpoch`.

### Gap #3c: ObservableGauges via callback registry — DONE

> **Status:** shipped. Eight gauges installed on the meter at
> `Bootstrap` time, with a single shared callback that pulls all
> values from a `GaugeSource` interface implemented by cmd/skafka:
>
> Cluster-level (no labels):
> - `skafka.is.controller`        — 0/1; sum across fleet ≈ 1
> - `skafka.assignment.version`   — most recent applied
> - `skafka.broker.count.alive`   — live brokers from the registry
> - `skafka.broker.count.assigned`— distinct brokers in assignment.json
> - `skafka.assignment.file.size` — bytes; early-warning for the 1MB CR-status truncation cap
>
> Per-partition (labels: topic, partition):
> - `skafka.partition.leader`         — broker ordinal
> - `skafka.partition.epoch`          — leadership epoch
> - `skafka.partition.high.watermark` — only meaningful on the leader broker
>
> Design choice: gauges are installed in observability.Bootstrap, not
> by the runtime owner. Keeps the lifecycle simple (gauges always
> exist; callback returns zero-valued samples until SetGaugeSource
> is called by cmd/skafka after the cluster runtime is up). Two
> unit tests cover the no-source-installed and populated-source paths.
>
> Follow-ups deferred to keep this commit focused:
> - `skafka.segment.count{topic, partition}` — needs a SegmentCount
>   method on DiskStorageEngine; left for a small follow-up.
> - HighWatermark is reported as 0 on non-leader brokers (engine
>   doesn't track HWM for partitions it doesn't lead). Acceptable —
>   a Prometheus `max by (topic, partition)` query gives the
>   leader-only view.

### Gap #3a: Controller-side counters — DONE

> **Status:** shipped. Added 7 instruments and wired each at its
> existing call site:
>
> - `ControllerFailovers` (renamed from LeaseAcquired) — at
>   onAcquired in cluster_runtime. LeaseLost dropped (no plan
>   equivalent; redundant with the failover counter).
> - `ControllerFailoverDuration` — histogram around the first
>   recompute+write inside AssignmentLoop.Start. Approximates
>   data-plane downtime (won-lease → first-write).
> - `AssignmentChanges` — per recomputeAndWrite call.
> - `AssignmentFileWrites{result}` + `AssignmentFileWriteLatency` —
>   wrapped FileStore.Write so timing covers the full
>   marshal+open+write+sync+rename sequence; result label tags
>   ok|error.
> - `AssignmentPushes` — per heart.PushAssignmentChanged broadcast.
> - `CRMirrorWrites{result}` — at K8sMirror.Mirror exit; labels
>   distinguish ok / error / not_found (operator hasn't created
>   the CR yet — non-fatal but worth a counter).
>
> Updated the Grafana "Lease events" panel to "Controller failovers
> (1m)" with both fleet and per-broker series.

### Gap #3 (original): Add the v3.3 coordination metrics (P0)

The biggest hole. The plan lists ~15 metrics specific to the v3
runtime — assignment_version, heartbeat_rtt, self_fence, takeover,
controller_failover — none of which exist. They're the operator's
primary "is the cluster healthy" signal; without them, the runtime
behaves correctly but is invisible to dashboards.

**Scope**: in three sub-batches by call site:

a. **Controller / assignment loop** (in `internal/controller/`):
   `controller_failovers_total`, `controller_failover_duration_seconds`,
   `assignment_changes_total`, `assignment_file_writes_total`,
   `assignment_file_write_latency_seconds`, `assignment_pushes_total`,
   `assignment_cr_mirror_writes_total`. Most are 1-line increments at
   existing logging sites.

b. **Broker coordinator / heartbeat** (in `internal/broker/`):
   `heartbeat_rtt_seconds`, `heartbeat_misses_total`,
   `self_fence_events_total`, `assignment_polls_total`,
   `stale_assignments_rejected_total`. RTT measurement requires a
   send-time stamp echoed back in `HeartbeatPing`; the protobuf has
   a `timestamp_ms` field already — just round-trip it.

c. **ObservableGauges** (need a callback registry):
   `is_controller`, `assignment_version`, `assignment_file_size_bytes`,
   `partition_leader`, `partition_epoch`, `broker_count_alive`,
   `broker_count_assigned`, `partition_high_watermark`,
   `segment_count`. ObservableGauges have a different shape from
   counters — they need a `MeterProvider.RegisterCallback` that
   fires on each scrape. One callback registry per package
   (controller, broker, storage) is cleaner than threading the
   meter through every type.

**Test**: integration coverage already exists for failover scenarios
(tests/controller-failover/); add metric assertions at the test seams.

### Gap #4: Add labels to existing metrics (P1) — DONE

> **Status:** shipped. Audit found:
>
> - `auth_success_total` already had `mechanism` (Phase 7).
> - `auth_failure_total` already had `mechanism` + `reason` (Phase 7),
>   but the SASL handler was missing two failure paths
>   (unsupported_mechanism, plaintext_connection) — both now emit.
> - mTLS auth path in `protocol/server.go` was emitting neither
>   AuthSuccess nor AuthFailure — added with `mechanism=mtls` and
>   `reason=cert_rejected` on the failure side.
> - `connections_total` was labelled `listener=internal|external`;
>   renamed to `mode=plaintext|tls` to match the plan name. Listener
>   semantics map 1:1 onto wire-protocol mode (internal=plaintext,
>   external=TLS).
> - `tls_handshakes_total{result}` and `acl_deny_total{principal,
>   resource_type}` were already correctly labelled.
> - `cert_reload_total` and `tls_handshakes_total` skip the `broker`
>   label per the plan (OTel resource attribute already attaches
>   broker_id).
> - `fetch_records_total{consumer_group}` deferred — adds N×consumer_groups
>   cardinality and is computed differently for lag anyway.

### Gap #4 (original): Add labels to existing metrics (P1)

Several emitted metrics drop labels the plan calls for. Adding
labels widens cardinality, so each one is a judgment call:

| Metric | Plan labels | Today | Recommendation |
|---|---|---|---|
| `auth_success_total` | `mechanism` | none | **Add** — bounded set: PLAIN, SCRAM-SHA-512, mTLS |
| `auth_failure_total` | `mechanism, reason` | none | **Add** — same `mechanism`; `reason` ∈ {bad_creds, expired_token, no_principal} |
| `connections_total` (= external_connections_total) | `mode, broker_id` | none | **Skip `broker_id`** (OTel resource attribute already carries it). **Add `mode` ∈ {plaintext, tls}** |
| `tls_handshakes_total` | `broker, result` | only `result` | **Skip `broker`** (resource attr) — `result` is enough |
| `cert_reload_total` | `broker` | none | **Skip** — resource attr |
| `fetch_records_total` / `fetch_bytes_total` | `consumer_group` | only `topic` | **Defer to v1.5** — adds N×consumer_groups cardinality; maps directly to lag, which is computed differently anyway |

**Scope**: small — each label addition is ≤10 lines at the emit
site. Document the "broker label drops to resource attribute" choice
so future contributors don't re-add it.

### Gap #5: Add the byte-opacity tripwires (P1) — DONE

> **Status:** shipped.
>
> - `CodecRecordDecode` + `CodecBatchReencode` Int64Counters live on
>   the `Metrics` struct.
> - `observability.BumpCodecRecordDecode(ctx, site)` /
>   `BumpCodecBatchReencode(ctx, site)` are the canonical entry
>   points — every increment also logs a `slog.Warn` so production
>   logs surface the violation before alerts fire. As of v1, no
>   skafka code path calls them.
> - `tests/byte-opacity/tripwire_test.go` replaces the placeholder
>   with two real tests:
>   - `TestStorageRoundTripIsByteIdentical` — multi-codec
>     (snappy/gzip/lz4/zstd/none) Append→Read round-trip; asserts
>     byte-identical AND tripwires at zero.
>   - `TestBumpCodecRecordDecodeIncrements` — meta-test that the
>     tripwire counters DO fire when Bump* is called, so the alert
>     wiring is real.
>
> Also exposed `observability.NewMetrics` (renamed from `newMetrics`)
> so the test can install a ManualReader-backed registry without
> needing the full Bootstrap path.

### Gap #5 (original): Add the byte-opacity tripwires (P1)

Plan lines 1153–1157 are explicit: these counters MUST stay at zero
in steady state; if they ever increment, code is violating the
"broker is a byte mover" invariant. Today they don't exist.

**Scope**:
- Add `CodecRecordDecode`, `CodecBatchReencode` as Int64Counters.
- The "increment site" is paradoxical — there should be NO call
  site, ever. Add the counters and wire them in any function that
  inspects record content (currently none). The point is the
  *absence* of increments — alerting fires if they ever go above zero.
- In `tests/byte-opacity/`, register a custom reader that asserts
  these counters stay at zero through the full produce/fetch
  round-trip (plan line 1352–1354).

**Why this matters**: this is the closest thing to a compile-time
guarantee that the byte-opacity invariant holds. A future refactor
that decodes a record (e.g. for "smart" partitioning or some
debugging helper) would silently break the byte-opacity contract;
the tripwire makes that loud.

### Gap #6: Update the Grafana dashboard for v3.3 panels (P1) — DONE

> **Status:** shipped. `deploy/grafana/skafka-dashboard.json` grew
> from 8 to 22 panels, organised in rows:
>
>   - y=22: Controller failovers + failover duration p99
>   - y=30: Current controller (per broker), broker_alive vs assigned, assignment.json size (with 1MB cap thresholds)
>   - y=36: Assignment version per broker, assignment.json write latency
>   - y=44: Heartbeat RTT p50/p99, self-fence + miss events
>   - y=52: Stale assignments rejected, assignment polls (change/no-change), CR mirror writes by result
>   - y=58: Byte-opacity tripwires (full-width, red threshold at value=1)
>   - y=66: Partition HWM by topic, partition leader churn
>
> Each panel has a description that explains what "good" looks like
> and what an anomaly indicates — the dashboard doubles as runbook
> documentation. The byte-opacity tripwire panel uses absolute
> thresholds so any non-zero point lights up red.

### Gap #6 (original): Update the Grafana dashboard for v3.3 panels (P1)

`deploy/grafana/skafka-dashboard.json` has 9 panels for v2.6 metrics.
The plan adds:

- Produce batch size distribution (helps tune client batch.size).
- Produce compression breakdown (verify clients are compressing).
- Codec tripwire counters (skafka_codec_record_decode_total etc. —
  should be flat lines at zero).
- Controller failover panel — `is_controller` per broker over time.
- Assignment version + polling rate.
- Heartbeat RTT histogram.
- Per-partition leader assignment heatmap.

**Scope**: dashboard JSON is mostly cookie-cutter — copy an existing
panel, change the `expr`, adjust the title. The v3.3 panels can't
ship until Gap #3 emits the metrics, so this is post-Gap-#3.

### Gap #7: PrometheusRule (alerting) for the failure modes (P2) — DONE

> **Status:** shipped. `deploy/helm/skafka/templates/prometheusrule.yaml`
> gated on `observability.alerts.enabled`. 9 alerts across 4 groups:
>
>   - **byteopacity**:
>     - `SkafkaByteOpacityViolated` (critical) — codec_record_decode or
>       codec_batch_reencode increased.
>   - **coordination**:
>     - `SkafkaSelfFencing` (warning) — broker rejecting writes on stale
>       heartbeat for 2m.
>     - `SkafkaStaleControllerWriting` (critical) — ex-controller still
>       writing assignment.json for 5m.
>     - `SkafkaNoCurrentController` (critical) — sum(skafka_is_controller) == 0.
>     - `SkafkaBrokerCountMismatch` (warning) — alive > assigned for 5m.
>   - **assignment_loop**:
>     - `SkafkaAssignmentFileWriteFailing` (critical) — write errors for 2m.
>     - `SkafkaAssignmentFileSizeApproachingCap` (warning) — size > 512KB for 10m.
>     - `SkafkaCRMirrorErrorSustained` (warning) — mirror errors for 15m.
>   - **heartbeat**:
>     - `SkafkaHeartbeatRTTHigh` (warning) — p99 RTT above threshold for 5m.
>
> Thresholds are exposed in `values.yaml` under
> `observability.alerts.thresholds`. `additionalLabels` merges into
> every rule's labels block for Alertmanager routing (e.g.
> `team: data-platform`). Each alert's `description` is written as
> runbook copy — operators paged on the alert have enough context to
> start an investigation without reading the source.

### Gap #7 (original): PrometheusRule (alerting) for the failure modes (P2)

The plan doesn't explicitly mandate a `PrometheusRule`, but operating
the cluster without one means failover signals only show up in
dashboards. Recommended alerts:

- `skafka_self_fence_events_total > 0` for 1m → "broker fenced itself"
- `skafka_codec_record_decode_total > 0` ever → "byte-opacity violated"
- `skafka_controller_failovers_total > 0` increase → informational
- `skafka_storage_estale_total > 0` for 5m → "NFS volume issues"
- `skafka_assignment_polls_total{change_detected="true"} == 0` for 1h → "controller may be stuck" (low fire rate is normal — but ZERO across an hour is pathological)
- `skafka_consumer_group_lag > 1e6` per partition → standard lag alert

**Scope**: a `templates/prometheusrule.yaml` template gated on
`observability.alerts.enabled`. Off by default; operators opt in if
they have Prometheus Operator installed.

## Recommended ordering

1. **Gap #1** — `/healthz` schema. Visible day-1 win; operators can
   `kubectl exec -- curl localhost:8080/healthz` and see what's
   happening. Also unblocks Gap #3's runtime-state plumbing —
   many of the same wiring decisions overlap.
2. **Gap #2** — emit declared-unused metrics. Trivial; closes the
   "dashboard says zero, but is it really zero?" ambiguity.
3. **Gap #3a/3b** — controller + broker counters. The plumbing is
   the bulk of the work; counters are the easier half.
4. **Gap #5** — byte-opacity tripwires. Small; depends on Gap #3
   landing the counter pattern. Pairs well with the existing
   `tests/byte-opacity/` suite.
5. **Gap #4** — labels. Small per-site; safest to do after the
   bigger metric additions land.
6. **Gap #3c** — ObservableGauges. Pattern-heavy; pick one (e.g.
   `is_controller`) as a reference implementation, then template
   the others.
7. **Gap #6** — Grafana dashboard. Post-emission.
8. **Gap #7** — PrometheusRule. Optional polish; ship as part of
   the v0.1.0 release alongside the dashboard.

## Out of scope for Phase 10

- **Real quota engine** — `quota_throttle_total` exists as a
  placeholder; actual per-principal rate limiting is post-v1.
- **OTLP push for metrics** — bootstrap.go has the OTLP TRACE
  exporter; metrics ride Prometheus pull only. Adding an OTLP push
  metric exporter is straightforward but isn't in the plan and
  doubles the egress cost without clear value.
- **/metrics auth** — the chart's metrics port is unauthenticated,
  trusting the cluster network boundary. mTLS-protected metrics
  would be a future enhancement.
- **Per-listener metrics differentiation beyond `mode`** — labels
  like `listener=internal/external` would balloon cardinality.
  `mode=plaintext/tls` is the right granularity.
- **High-cardinality metrics** — anything labelled by `principal` or
  `client_id` is a cardinality timebomb; the plan calls out specific
  per-principal metrics (auth, ACL deny, quota) that we accept
  knowingly. Don't add more without a budget conversation.

## Acceptance criteria for "Phase 10 done"

- [ ] `/healthz` returns the full plan schema; verified by a unit
      test in `internal/observability/health_test.go`.
- [ ] All metrics in `Metrics` struct have at least one emit site
      (no declared-unused instruments).
- [ ] All controller / heartbeat / assignment metrics from the
      plan emit at least one sample under
      `tests/controller-failover/`.
- [ ] Byte-opacity tripwires are wired and asserted at zero in
      `tests/byte-opacity/`.
- [ ] Grafana dashboard renders the v3.3 panels (visual smoke
      test, not CI).
- [ ] PrometheusRule template lints (`helm template ...
      | promtool check rules`).

## What this leaves for the v1 release

After Phase 10, the v1 architecture is feature-complete per the
plan:

- Broker is a byte mover (Phase 1–3 + tripwires).
- Cluster controller via shared Lease (Phase 4–6).
- SCRAM + mTLS auth (Phase 7).
- Helm chart + operator + cert-manager + Gateway API external
  listener (Phase 8–9).
- Full metrics + healthz + dashboards + alerts (Phase 10).

Outstanding items for v1.0 release polish (separate from v1
architecture): tagged release (currently `v0.1.0-preview`), CI
publishing flow check, integration test on AWS EFS / Azure Files
(currently only csi-driver-nfs is tested), kafka-dump-log.sh
on-disk format check (plan line 1394).
