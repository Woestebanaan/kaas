# Performance vs Strimzi

Where kaas stands against Strimzi-managed Apache Kafka and why: group-commit fsync versus page-cache acks.

Benchmark reports are recorded under `docs/perf-results/` in the
repository; this chapter summarizes the latest recorded head-to-head and
the methodology behind it. Treat every number as bound to its
configuration — both systems ran on the *same* single-node k3s host and the
same NFS export, which is a valid relative comparison and an unusual
absolute environment.

## Latest recorded head-to-head

From `docs/perf-results/bench-compare-20260715-045401Z.md` (kaas
`0.2.3-preview`, 3 brokers, `flushIntervalMessages: 10000`; Strimzi/Kafka
4.2.0, 3 brokers; 5 producer pods × 1 M records × 1 KB, `acks=all`, shared
NFS backend):

| Metric | kaas | Strimzi | ratio |
|---|---|---|---|
| Throughput (sum) | 44.2 MB/s | 12.0 MB/s | 3.7× |
| p50 latency | 2.9 s | 11.8 s | 0.24× |
| p99 latency | 8.7 s | 30.2 s | 0.29× |

Caveats stated plainly: this is a **single run** (the methodology below
calls for five); `flushIntervalMessages: 10000` relaxes kaas's
default-honest fsync cadence to approximate Apache's page-cache-ack
posture, which is the apples-to-apples setting; and Strimzi on NFS is not
Strimzi's optimal substrate. Earlier results that showed kaas *behind*
predate two fixes that invalidated them: a broker bug where the
flush-interval env was parsed but dropped, and a NAS cabling fault that
capped the storage link at ~10 MB/s until 2026-07-12.

## Why the shapes differ

The architectural difference that drives the latency shape: Apache
acknowledges `acks=all` once the write is replicated to the ISR's page
caches — fsync happens later, asynchronously. kaas has
[no replication](../compat/non-goals.md), so its `acks=all` at default
settings means a real NFS COMMIT round-trip; the
[group-commit design](../architecture/storage-hot-path.md) exists to share
that round-trip across concurrent producers, and the flush-interval dial
([storage](./storage.md)) trades it away where page-cache-equivalent
durability is acceptable.

## Methodology

Perf conclusions on this project follow rules learned the hard way:

- **Five-run averages with outlier exclusion** — single runs on a shared
  home-lab node are noise-dominated; one recorded pattern is a
  3-fast-2-slow cycle driven by page-cache eviction.
- **Substrate liveness checks** — each bench snapshots NFS RPC counters
  and node network rates, so a degraded NAS link (see above) shows up in
  the report instead of silently poisoning the numbers.
- **Cooldowns between runs** (120 s in the compare harness) so one
  system's tail I/O doesn't bleed into the other's warm-up.

## Dead-ends already tried

Recorded from earlier tuning rounds so they aren't re-litigated without
new evidence: PGO builds, `FADV_SEQUENTIAL` on segment reads, and
`FLUSH_INTERVAL_MESSAGES=0` (pure throughput mode) all failed to move
steady-state numbers meaningfully on this substrate — the NFS COMMIT
round-trip, not CPU or readahead, is the dominant cost.
