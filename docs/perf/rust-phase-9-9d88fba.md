# Rust bench capture — `v0.2.2-preview` (`9d88fba`) — GATE: FAIL (−23.7 %)

Second Rust half of the phase-9 A.3 ratio gate (gh #152, gh #188),
against [`go-reference-79202df.md`](./go-reference-79202df.md). Same
environment and protocol as the
[`v0.2.0-preview` capture](./rust-phase-9-d077a00.md); run on
2026-07-14 15:07–16:05 UTC after the `v0.2.1`/`v0.2.2` hotfixes.

## What changed since the v0.2.0 capture

- `bc7e606` (in v0.2.0) had already fixed the flush-interval wiring
  (−63 % → −26.2 %).
- `4fdea95` (v0.2.1) aligned the acks=all wait semantics with Go:
  only the append that crosses the flush-interval threshold waits for
  its fsync; appends landing while a flush is in flight no longer
  park. **Effect: −26.2 % → −23.7 %** — real but not the dominant
  term the latency signature suggested.

## Capture (5 × bench-compare, drop fastest+slowest, avg 3)

Per-run skafka throughput: 48.69 / 49.60 / 50.79† / 49.77 / 47.59†
MB/s († dropped). Spread ±3 % — tighter than the v0.2.0 capture's ±8 %.

| Metric                   | Rust v0.2.2 | strimzi  | rust/st | go/st (ref) | Δ ratio |
|--------------------------|-------------|----------|---------|-------------|---------|
| Throughput (MB/s) [sum]  | 49.35       | 12.84    | 3.847x  | 5.040x      | −23.7 % |
| Records/sec [sum]        | 51748.66    | 13466.57 | 3.843x  | 5.043x      | −23.8 % |
| avg latency (ms)         | 3092.72     | 11883.63 | 0.260x  | 0.197x      | +32.0 % |
| p50 (ms)                 | 2984.60     | 10742.07 | 0.280x  | 0.163x      | +71.8 % |
| p95 (ms)                 | 6107.87     | 21163.47 | 0.290x  | 0.320x      | −9.4 %  |
| p99 (ms)                 | 8123.93     | 30729.33 | 0.263x  | 0.333x      | −20.9 % |
| p99.9 (ms)               | 9670.73     | 32360.60 | 0.300x  | 0.377x      | −20.4 % |
| max (ms, worst pod)      | 12986.33    | 33684.67 | 0.383x  | 0.433x      | −11.5 % |

## New evidence: the broker is wait-bound, not CPU-bound

`kubectl top` sampled every 15 s across two active bench phases:
all three brokers sat at **11–18 millicores against a 2-CPU limit**
(< 1 % utilisation) while serving ~49 MB/s aggregate. Neither flavor
approaches the ~100 MB/s NAS line rate either, so both are I/O-wait
shaped — Go simply waits less per unit of work.

Next profiling lead: **per-append blocking-syscall count**. With CPU
ruled out and fsync waits now Go-identical, the remaining suspect is
how many blocking NFS round trips each batch costs on the hot path
(write() granularity per batch — log write, index entry, manifest
touches — under the NFS mount's semantics; see #166 for the
sync-export angle). Plan: strace -c / flamegraph on a broker pod
during a bench phase, or temporary syscall counters in `segment.rs`.

## Consequences

- Gate remains **FAIL**; the default flip stays blocked on gh #188.
- The 72 h bake proceeds on `v0.2.2-preview` (correctness matrix is
  green; #187 verified fixed on this build). Its bench tolerance
  band self-references THIS capture: 49.35 MB/s ± 15 % single-run.
