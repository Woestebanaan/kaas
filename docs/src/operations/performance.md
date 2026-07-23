# Performance vs Strimzi

Where kaas stands against Strimzi-managed Apache Kafka on the same
substrate, and why: group-commit fsync versus page-cache acks.

Benchmark reports are recorded under `docs/perf-results/` in the
repository; this chapter summarizes the current head-to-head series and
the methodology behind it. Treat every number as bound to its
configuration — both systems ran on the *same* single-node k3s host and
the same NFS export, which is a valid relative comparison and an unusual
absolute environment.

## The current head-to-head series

The `bench-compare-v2` harness runs both systems through the same client
matrix: plain and idempotent produce (5 producer pods, 1 KB records,
`acks=all`), group and no-group consume, a consumer-group scale-up, and
a Kafka Streams wordcount. The 2026-07-21 → 2026-07-23 series (kaas
`v0.2.18-preview`, 3 brokers, **default honest fsync** — the equivalent
of `log.flush.interval.messages=1`; Strimzi/Kafka 4.2.0, 3 brokers)
gives, as ranges across runs:

| Scenario | kaas / Strimzi throughput | Read |
|---|---|---|
| produce, `acks=all` | **1.66× – 2.70×** (typically ~1.7–1.8×) | kaas leads, with p99 latency 2–3× lower |
| produce, idempotent | **1.71× – 4.48×** (typically ~1.9×) | kaas leads |
| consume (group) | **0.95× – 1.02×** | reproducibly at parity |
| consume (no group) | 0.19× – 2.01× | noise-dominated; kaas typically trails on raw fan-out reads |

From the most recent report
(`docs/perf-results/bench-compare-v2-20260723-063513Z.md`): produce
20.2 MB/s vs 12.2 MB/s summed across producers, with p50 7.7 s vs
11.9 s and p99 9.8 s vs 24.2 s under saturation; group consume
127.6 vs 126.0 MB/s.

Two scenarios are deliberately not summarized into a verdict: the
**no-group consume** spread is too wide to call anything but noisy on
this rig, and the **rebalance scale-up** comparison is currently
polluted by harness artifacts (runs where one side's pod logs are
missed produce nonsense ratios) — a green number you can't trust is
worse than no number, so it stays unquoted until the harness reports
both sides cleanly.

Earlier results that showed kaas *behind* on produce predate two fixes
that invalidated them: a broker bug where the flush-interval setting
was parsed but dropped, and a NAS cabling fault that capped the storage
link at ~10 MB/s until 2026-07-12. An earlier headline of 3.7× came
from a single run with a relaxed flush interval; the series above
supersedes it.

## Why the shapes differ

The architectural difference drives both columns. Apache acknowledges
`acks=all` once the write reaches the ISR's page caches — fsync happens
later, asynchronously. kaas has [no
replication](../compat/non-goals.md), so its `acks=all` at default
settings means a real NFS COMMIT round-trip before the ack; the
[group-commit design](../architecture/storage-hot-path.md) exists to
share one COMMIT across every concurrently-parked producer, which is
how an honest-fsync broker ends up ahead of a page-cache-ack broker on
a substrate where COMMIT latency dominates. The flush-interval dial
([storage](./storage.md)) trades durability back toward Apache's
posture where page-cache-equivalent semantics are acceptable.

## Methodology

Perf conclusions on this project follow rules learned the hard way:

- **Ranges over single runs** — single runs on a shared home-lab node
  are noise-dominated; one recorded pattern is a 3-fast-2-slow cycle
  driven by page-cache eviction. The table above quotes the spread
  across the series, not a best run.
- **Substrate liveness checks** — each bench snapshots NFS RPC counters
  and node network rates, so a degraded NAS link (see above) shows up
  in the report instead of silently poisoning the numbers.
- **Cooldowns between runs** (120 s in the compare harness) so one
  system's tail I/O doesn't bleed into the other's warm-up.
- **Distrust surprising verdicts** — a PASS can come from a stale
  topic and a FAIL from a harness bug; identical failures on both
  systems indict the harness, not the brokers.

## Dead-ends already tried

Recorded from earlier tuning rounds so they aren't re-litigated without
new evidence: PGO builds, `FADV_SEQUENTIAL` on segment reads, and
flush-interval `0` (pure throughput mode) all failed to move
steady-state numbers meaningfully on this substrate — the NFS COMMIT
round-trip, not CPU or readahead, is the dominant cost.
