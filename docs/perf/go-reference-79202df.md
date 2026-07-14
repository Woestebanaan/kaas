# Go reference bench capture — `v0.1.190-preview` (`79202df`)

Phase 9 workstream A.3 (gh #152): the Go-flavor half of the
`bench-compare` Strimzi-ratio gate. This capture never happened in
Phase 8 (both sides tripped the old `activeDeadlineSeconds=1200`
Job ceiling), and every pre-2026-07-12 number is unusable anyway —
the NAS uplink ran at 100BaseT until the cable fix that day.

## Environment

- **Date:** 2026-07-14 (08:58–10:04 UTC), single-node k3s on `nixos`,
  NFS-RWX PVC, NAS uplink ~830 Mbit (post-cable-fix), node on WiFi.
- **skafka:** Go flavor `ghcr.io/woestebanaan/skafka-preview:0.1.190-preview`,
  3 brokers, `SKAFKA_FLUSH_INTERVAL_MESSAGES=10000`, acks=all, TLS
  listener (:9093).
- **Strimzi yardstick:** `kafka-cluster` (3 dual-role nodes) in the
  `strimzi` namespace — deployed configuration untouched between this
  capture and the Rust capture, so the ratio column is comparable.
- **Workload:** `kafka-producer-perf-test` Job, 5 pods × 1 M × 1 KB
  records, unthrottled, `acks=all`; 120 s cooldown between phases and
  between runs; NFS-liveness snapshots before/during each phase
  (see `bench-compare` skill).

## Protocol

5 runs; drop the fastest and slowest by skafka throughput; average the
middle 3 (`memory/feedback_bench_methodology.md`).

### Per-run raw results

| Run | skafka MB/s | strimzi MB/s | sk/st |
|-----|-------------|--------------|-------|
| 1   | 64.39       | 12.48        | 5.16x |
| 2 † | 64.61       | 12.65        | 5.11x |
| 3 † | 62.38       | 12.91        | 4.83x |
| 4   | 64.08       | 12.99        | 4.93x |
| 5   | 64.06       | 12.74        | 5.03x |

† dropped (fastest / slowest by skafka throughput).

### Aggregate (runs 1, 4, 5)

| Metric                   | skafka (Go) | strimzi  | go/st ratio |
|--------------------------|-------------|----------|-------------|
| Throughput (MB/s) [sum]  | 64.18       | 12.74    | 5.040x      |
| Records/sec [sum]        | 67292.68    | 13348.92 | 5.043x      |
| avg latency (ms)         | 2349.61     | 12062.68 | 0.197x      |
| p50 (ms)                 | 1837.27     | 11074.27 | 0.163x      |
| p95 (ms)                 | 6808.13     | 21343.73 | 0.320x      |
| p99 (ms)                 | 10369.13    | 30867.67 | 0.333x      |
| p99.9 (ms)               | 12620.27    | 33110.53 | 0.377x      |
| max (ms, worst pod)      | 15789.67    | 36020.33 | 0.433x      |

Run-to-run spread on skafka throughput: 62.38–64.61 MB/s (±1.8 % around
the mean) — well inside the methodology's noise band; no run was
excluded for anomaly, only by protocol.

## Gate

The Rust capture (`rust-phase-9-<sha>.md`) must land its per-axis
ratios within ±5 % of this table's ratio column.

> **Note on the absolute Strimzi numbers.** This Strimzi deployment
> answers ~12.7 MB/s with multi-second p50s on the shared NFS
> substrate — the yardstick's *absolute* health doesn't matter for the
> gate (both flavors measure against the identical deployment within
> the same hour); it exists to cancel out cluster-state noise.
