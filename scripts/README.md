# Skafka smoke test

You (claude-code) are running inside a pod in a k3s cluster. Skafka — a
from-scratch Kafka broker — is deployed in the same cluster.

Manifests: `/home/coder/repos/k3s-cluster/apps/skafka`
Smoke test: `./scripts/smoke-test.sh` (in the skafka source repo)

## Goal

Run the smoke test against the live broker. If it fails, diagnose, fix the
broker (or the test, if the test is genuinely wrong), and release a new
broker version. Stop when the test passes from a clean rollout.

**Definition of done:** `./scripts/smoke-test.sh` exits 0 and prints
`>> PASS: round-trip successful`, on a broker pod that started *after* your
fix was applied.

## Procedure

### 1. Ensure the smoke topic exists

```bash
kubectl apply -f - <<'EOF'
apiVersion: skafka.io/v1alpha1
kind: KafkaTopic
metadata: { name: smoke, namespace: skafka }
spec: { partitions: 3 }
EOF
```

The broker watches KafkaTopic CRs at runtime (since v0.1.11-preview) — no
restart needed when a topic is created or its partition count changes.
Restart only when you've actually swapped the broker image:

```bash
kubectl -n skafka rollout restart statefulset/skafka
kubectl -n skafka rollout status  statefulset/skafka --timeout=120s
```

A handful of sample topics (`events`, `audit-log`, `metrics`) already
ship via `k3s-cluster/apps/skafka/test-data/`; override `TOPIC=events`
to skip step 1 entirely.

### 2. Run

```bash
./scripts/smoke-test.sh
```

Defaults: `BOOTSTRAP=skafka.skafka.svc.cluster.local:9092`, `TOPIC=smoke`,
message auto-generated per run. Overrides:

```bash
TOPIC=demo MESSAGE=hi ./scripts/smoke-test.sh
BOOTSTRAP=localhost:19092 ./scripts/smoke-test.sh   # via port-forward
```

Uses `kafka-console-{producer,consumer}.sh` from `/opt/kafka/bin`.

### 3. If it fails — diagnose in this order

1. **Which stage failed?** The script prints `preflight`, `producing`,
   `consuming`, `describe-configs`, `describe-log-dirs`, `list-topics`.
   Read its stderr dump first; it's the cheapest signal.
2. **Broker `/healthz`** (richer than logs for cluster-shape issues):
   ```bash
   kubectl -n skafka exec statefulset/skafka -- curl -s localhost:8080/healthz | jq
   ```
   Look at `is_controller`, `controller_id`, `heartbeat_age_ms`,
   `assignment_version`, `partitions_led` vs `partitions_assigned`. A
   stuck broker is usually visible here before the Kafka client notices.
3. **Broker logs:** `kubectl -n skafka logs statefulset/skafka --tail=300`.
   Look for panics, "unsupported API key N", and decode errors.
4. **Metrics in Prometheus** — brokers push OTLP metrics into Prometheus's
   native OTLP receiver. Query for tripwires, self-fence events, and
   stale-assignment rejects (this pod is in-cluster, so the Service DNS
   resolves directly):
   ```bash
   curl -sG 'http://prometheus.observability.svc.cluster.local:9090/api/v1/query' \
     --data-urlencode 'query=skafka_codec_record_decode_total'
   ```
5. **Pod state:** `kubectl -n skafka get pods,events --sort-by=.lastTimestamp | tail -30`.
6. **Topic state:** `kubectl -n skafka get kafkatopic smoke -o yaml`.
7. **Cluster mirror:** `kubectl -n skafka get kafkaclusterassignments skafka -o yaml`
   shows the controller's view (broker liveness, partition assignments).

Common failure shapes:

| Symptom                                      | Likely cause                                    |
| -------------------------------------------- | ----------------------------------------------- |
| Preflight fails immediately                  | Broker not listening / ApiVersions broken       |
| "Unsupported API key N" in broker logs       | Missing RPC; implement it                       |
| Connection reset mid-frame                   | Panic in broker; check logs                     |
| Preflight + produce ok, consumer times out   | Append succeeded but Fetch is wrong             |
| Test passes once, fails on rerun             | Offset/log persistence bug                      |
| `is_controller=false` on every broker        | Lease election failing — check controller-Lease |
| `partitions_led < partitions_assigned`       | Heartbeat stale or takeover stuck               |
| `skafka_codec_record_decode_total > 0`       | Byte-opacity tripwire fired — page on this      |

### 4. Fix, then re-test

Fix in the broker source by default. Only edit `smoke-test.sh` if the test
itself is provably wrong — and call that out explicitly in the commit
message.

After each fix: rebuild the image, bump the StatefulSet to the new tag,
`rollout restart`, wait for `rollout status`, then rerun the test. The test
must pass against a freshly-started pod, not the one that produced the
failure.

### 5. Release

Follow whatever the repo documents — check `RELEASING.md`, the `Makefile`,
or `.github/workflows/` for the actual procedure. If nothing is documented,
stop and ask before tagging or pushing.

## Hard limits — do not

- Modify `smoke-test.sh` to make it pass (loosening the assertion, removing
  the preflight, swallowing stderr, etc.).
- Run more than **3 fix → rebuild → test** cycles without stopping to
  report. If you're on cycle 4, you don't understand the bug yet.
- Force-push, rewrite history, or release without a green test on a fresh
  pod.
- Implement a whole new Kafka API (e.g. InitProducerId, transactions) on
  your own initiative. That's an architectural change — stop and ask.

## When to stop and report instead of continuing

- Three failed fix attempts on the same root cause.
- The fix requires a new top-level subsystem in the broker.
- The failure looks like data corruption (wrong bytes, not missing
  feature), since silently "fixing" that can mask deeper bugs.
- The release procedure is unclear from the repo.

When you stop, report: what failed, what you tried, the relevant log
excerpt, and your current hypothesis.