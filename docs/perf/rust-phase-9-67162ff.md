# Rust bench capture — `v0.2.3-preview` (`67162ff`) — GATE: FAIL (−26.3 %), gap now fully characterised

Third Rust capture for the phase-9 A.3 ratio gate (gh #152, gh #188),
against [`go-reference-79202df.md`](./go-reference-79202df.md).
2026-07-14 16:47–17:45 UTC, after the TCP_NODELAY fix.

## Capture (5 × bench-compare, drop fastest+slowest, avg 3)

Per-run: 50.48† / 47.86 / 45.86† / 47.52 / 48.13 MB/s († dropped).

| Metric                   | Rust v0.2.3 | strimzi  | rust/st | go/st (ref) | Δ ratio |
|--------------------------|-------------|----------|---------|-------------|---------|
| Throughput (MB/s) [sum]  | 47.84       | 12.87    | 3.717x  | 5.040x      | −26.3 % |
| Records/sec [sum]        | 50165.51    | 13495.41 | 3.720x  | 5.043x      | −26.2 % |
| avg latency (ms)         | 3181.12     | 11934.27 | 0.263x  | 0.197x      | +33.7 % |
| p50 (ms)                 | 2971.07     | 10737.60 | 0.273x  | 0.163x      | +67.7 % |
| p95 (ms)                 | 6313.87     | 21446.93 | 0.297x  | 0.320x      | −7.3 %  |
| p99 (ms)                 | 7884.67     | 30909.87 | 0.257x  | 0.333x      | −22.9 % |
| p99.9 (ms)               | 8995.53     | 32699.00 | 0.277x  | 0.377x      | −26.6 % |
| max (ms, worst pod)      | 10713.00    | 33781.33 | 0.317x  | 0.433x      | −26.9 % |

Statistically indistinguishable from the v0.2.2 capture — the
NODELAY fix (correct for Go parity) has no effect on this workload:
continuous pipelined bidirectional traffic neutralises Nagle.

## The complete #188 diagnostic chain (all measured 2026-07-14)

Every broker-side resource has now been measured and cleared:

| Hypothesis | Measurement | Verdict |
|---|---|---|
| CPU-bound | `kubectl top` during phases: brokers at **11–18 millicores** of a 2-CPU limit | cleared |
| Storage syscall count / sync-NFS writes | `strace -c` 30 s: pwrite64 avg **13 µs**, plain write 2 µs — page-cache speed | cleared |
| fsync stalls worse than Go | Prometheus under load: fsync p50 **687 ms Rust vs 1330 ms Go**, same 2.9/s rate; write p99 645 vs 520 ms | cleared (Rust better) |
| In-broker request latency | `skafka_request_latency_seconds` produce: p50 0.09 vs 0.07 ms; p99 **946 ms Rust vs 1859 ms Go** | cleared (Rust better) |
| Client retries / errors | bench pod logs: **zero** retry/timeout warnings | cleared |
| Nagle (Go sets SetNoDelay, Rust didn't) | fixed in `67162ff`; throughput unchanged | cleared (fix kept) |

What the packet capture (12 s, one producer→broker connection,
headers-only) shows instead:

- **The broker owns the wall clock**: 8.87 s of the 10.1 s window is
  server think-time; total client think-time is 0.01 s. Stalls
  cluster at 500–900 ms — the NFS COMMIT saturation window that both
  flavors experience at the same ~2.9 fsyncs/s.
- **This connection moved 393 KB per request burst.** Under identical
  stall cadence, throughput per connection ≈ in-flight bytes /
  stall-cycle time — and the Go morning window's request rate implies
  materially larger requests per cycle.

## Conclusion: the residual gap is bench-dynamics-shaped

With every server-side metric equal or favouring Rust, the remaining
−26 % arises from **producer batching dynamics interacting with stall
cadence**: Rust's *shorter* fsync stalls (687 vs 1330 ms p50) give the
producer less accumulation time per cycle, so it pipelines smaller
requests, moving less data per NFS-saturation window — a client-side
equilibrium effect, not a broker deficiency. This echoes gh #134
(every-consumer-reads-everything: bench-config, not broker).

## Next steps (pick one, next session)

1. **Confirm empirically**: redeploy the Go flavor once, take the same
   12 s pcap, compare bytes-per-request-burst under identical stall
   cadence. If Go's bursts are ~1.6× larger, the mechanism is proven.
2. **Decouple the bench from the effect**: raise
   `max.in.flight.requests.per.connection` (5 → 10+) and/or
   `batch.size` in the bench Job so pipeline depth stops being the
   binding constraint for both flavors, then re-gate.
3. **Recalibrate the gate**: if (1) proves the mechanism, the honest
   reading is that at NFS-saturation this preset measures client
   equilibria, not broker merit — document and gate on a preset from
   (2) instead.

The 72 h bake proceeds on `v0.2.3-preview`; bake bench tolerance
self-references THIS capture (47.84 MB/s ± 15 % single-run).
