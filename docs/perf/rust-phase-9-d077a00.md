# Rust bench capture — `v0.2.0-preview` (`d077a00`) — GATE: FAIL

Phase 9 workstream A.3 (gh #152): the Rust half of the `bench-compare`
Strimzi-ratio gate, against
[`go-reference-79202df.md`](./go-reference-79202df.md). Same
environment, same protocol, same (verified-stable) Strimzi yardstick,
captured ~1 h after the Go reference on 2026-07-14.

## The before-picture: the gate caught a real bug

The first capture attempt ran against `v0.1.190-preview` and measured
**23.2 MB/s (1.86× Strimzi)** vs the Go reference's 64.2 MB/s (5.04×)
— a 63 % ratio shortfall. Root cause:
`SKAFKA_FLUSH_INTERVAL_MESSAGES` was parsed by the CLI and then
dropped on the floor in `build_engine`, so every Rust deployment ran
honest flush-per-batch regardless of the configured `10000`. Fixed in
`bc7e606` (ships in `v0.2.0-preview`); the capture below is the
post-fix build.

## Capture (5 × bench-compare, drop fastest+slowest, avg 3)

Per-run skafka throughput: 53.14 / 48.48 / 44.83 / 46.76 / 46.38 MB/s
(runs 1†, 2, 3†, 4, 5 — † dropped by protocol). Spread ±8 % — wider
than the Go reference's ±1.8 %; the declining-then-recovering shape is
consistent with the known page-cache eviction cycle
(`memory/feedback_bench_methodology.md`).

| Metric                   | Rust     | strimzi  | rust/st | go/st (ref) | Δ ratio |
|--------------------------|----------|----------|---------|-------------|---------|
| Throughput (MB/s) [sum]  | 47.21    | 12.69    | 3.720x  | 5.040x      | −26.2 % |
| Records/sec [sum]        | 49500.96 | 13310.42 | 3.720x  | 5.043x      | −26.2 % |
| avg latency (ms)         | 3238.70  | 12130.79 | 0.267x  | 0.197x      | +35.4 % |
| p50 (ms)                 | 3116.53  | 11107.53 | 0.283x  | 0.163x      | +73.8 % |
| p95 (ms)                 | 6044.60  | 21341.60 | 0.283x  | 0.320x      | −11.5 % |
| p99 (ms)                 | 7824.87  | 30022.93 | 0.263x  | 0.333x      | −20.9 % |
| p99.9 (ms)               | 9011.80  | 31886.13 | 0.283x  | 0.377x      | −24.8 % |
| max (ms, worst pod)      | 12003.67 | 33280.33 | 0.367x  | 0.433x      | −15.3 % |

(For latency rows, a LOWER ratio is better; Δ ratio > 0 means Rust is
relatively worse than Go on that axis, < 0 relatively better.)

## Verdict

**FAIL against the ±5 % gate**: throughput ratio −26.2 %, p50 ratio
+73.8 %. Per the phase-8 §F.4 handling this sits in the
profile-and-fix band (not the >30 % stop-the-phase band, which the
pre-fix 63 % gap would have hit).

The shape of the miss is informative: Rust's **tail-latency ratios
beat Go's** (p95/p99/p99.9/max all relatively better) while median
latency and throughput lag ~26 %. Fatter median + tighter tails
suggests the flush/group-commit cycle is syncing more often than the
configured interval implies (smaller effective commit batches), or a
per-append cost (lock hold, wakeup latency) that Go's path amortises
— not an architectural regression. First profiling lead: the
committer wakeup/trigger interaction in `sk-storage`'s
`partition.rs`/`committer.rs` under `flush_interval_messages » 1`,
with cargo-flamegraph via the ARC runners.

## Consequences for the phase

- The 72 h bake proceeds on `v0.2.0-preview` — it validates
  correctness/stability, and its own bench tolerance is
  self-referenced to THIS capture (±15 % single-run band).
- **The default flip (workstream E / `v0.2.1-preview`) is blocked**
  until a build closes the throughput-ratio gap to within ±5 % and
  re-passes this gate. The choreography table shifts right by design.
- CPU/RSS were not gated per plan (report-only): not captured this
  round — the bench Job doesn't snapshot broker cgroup stats; noted
  for the profiling pass.
