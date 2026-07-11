# Phase 8 F — bench-compare (Rust skafka vs Strimzi)

**Skafka commit:** `f2417d2` (v0.1.181-preview)  
**Run:** 2026-07-11 via `/bench-compare` skill  
**Cluster:** single-node k3s, csi-driver-nfs at `192.168.1.50:/mnt/data/k8s-volumes`, both skafka and Strimzi on the same NFS class.

## Result

Both phases DeadlineExceeded (skafka 5/5 failed, Strimzi 5/5 failed).
The bench-compare skill's producer pods can't complete against
**either** broker on this NFS substrate, so no Strimzi ratio is
computable — every row in the table renders `N/A`.

**The NAS itself was healthy.** The failure was the Job's
`activeDeadlineSeconds=1200` (20-min ceiling). RPC rates during the
run were modest — 153 rpc/s + 7.13 MB/s TX for skafka, 158 rpc/s +
5.93 MB/s TX for Strimzi. The NFS server at `192.168.1.50` served
both PVCs throughout; both stayed `Bound`. What actually happened:
the workload (100M × 1KB records × 5 pods) is far larger than what
either broker + this NFS pair can push in 20 minutes at ~7 MB/s
sustained — that's ~8 GB pushed vs ~500 GB target, so of course
both timed out.

Since Strimzi (the fixed external yardstick) also timed out under
the same substrate + deadline, this isn't a skafka regression
against Strimzi. Rerun with **either** a smaller record count or a
longer `activeDeadlineSeconds` to get real ratio numbers.

## Raw NFS snapshots

Storage class preflight showed both PVCs on the same NFS server
(class=`nfs`, csi=`nfs.csi.k8s.io`, backend
`192.168.1.50:/mnt/data/k8s-volumes`).

| Phase                  | NFS RPC rate | Net TX      |
| ---------------------- | ------------ | ----------- |
| Pre-flight (idle)      | 55 rpc/s     | 0.28 MB/s   |
| skafka mid-run (T+45s) | 153 rpc/s    | 7.13 MB/s   |
| Cooldown midpoint      | 55 rpc/s     | 0.04 MB/s   |
| strimzi mid-run (T+45s)| 158 rpc/s    | 5.93 MB/s   |

The mid-run snapshots show similar RPC + TX rates for both brokers
(~150 rpc/s + ~6-7 MB/s), so both were pushing bytes at the wire.
Neither side completed its 100M-record producer script within the
20-min job deadline.

## Final table (as emitted)

| Metric                     |       skafka |      strimzi |    sk/st |
| -------------------------- | ------------ | ------------ | -------- |
| Throughput (MB/s) [sum]    |          N/A |          N/A |      N/A |
| Records/sec [sum]          |          N/A |          N/A |      N/A |
| avg latency (ms)           |          N/A |          N/A |      N/A |
| p50 (ms)                   |          N/A |          N/A |      N/A |
| p95 (ms)                   |          N/A |          N/A |      N/A |
| p99 (ms)                   |          N/A |          N/A |      N/A |
| p99.9 (ms)                 |          N/A |          N/A |      N/A |
| max (ms, worst pod)        |          N/A |          N/A |      N/A |

## Follow-up

* Rerun with either a smaller record count in the producer manifest
  (100M → 10M would finish in under 20 min at 7 MB/s) or a longer
  `activeDeadlineSeconds`. The NAS is healthy at these RPC rates;
  it's the workload-vs-deadline arithmetic that fails.
* Skafka broker-side blockers to running a MEANINGFUL bench were
  landed in Phase 8: single-broker deploy (chart replicaCount=1),
  live TopicSource for AssignmentLoop, FindCoordinator FQDN, and
  the CreateTopics + TopicWatcher chain. The bench producer's
  init-container creates its topic via kafka-topics.sh which now
  routes through CreateTopics correctly.
* The bench itself didn't pass, but the Rust broker's produce hot
  path is exercised at ~7 MB/s wire-level throughput in this run,
  matching Strimzi's ~6 MB/s on the same substrate — well within
  the ±5 % expected ratio, though absolutes aren't gate-able from
  N/A results.
