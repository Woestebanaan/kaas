# Verification story

How parity claims are backed: the `scripts/kafka-*.sh` shell-tool suite, the integration tests, the parity project board, and the bench methodology.

A compatibility claim is only as good as the thing that would catch it being
wrong. kaas layers four:

## 1. The Apache shell-tool suite (`scripts/kafka-*.sh`)

41 per-tool scripts at the repo root run the *actual Apache Kafka
distribution's* shell tools (`kafka-topics`, `kafka-console-producer`,
`kafka-consumer-groups`, `kafka-acls`, `kafka-{producer,consumer}-perf-test`,
`kafka-verifiable-{producer,consumer}`, …) against a live kaas cluster —
the same binaries an operator would point at Apache, unchanged. Each script
sources `scripts/_common.sh` for the shared `BOOTSTRAP` / `KAFKA_BIN` /
`skip` helpers; defaults target the in-cluster Service DNS.

Two honesty conventions:

- **Skips are explicit, not silent.** Scripts covering features that are
  [non-goals](non-goals.md) or post-3.7 (KRaft tools, share groups, …)
  print a one-line reason and `exit 77` — discoverable, and never
  pretending to test something that can't work.
- **The baseline is recorded and diffable.** `scripts/.parity-baseline.txt`
  pins the expected per-script result. Current baseline (captured
  2026-07-19 against `v0.2.4-preview`, production 3-broker shape on
  NFS-RWX): **21 PASS / 20 SKIP / 0 FAIL**. Every SKIP maps to a
  documented non-goal or a post-3.7 feature. Reruns assert against the
  baseline, so a PASS→FAIL downgrade is a regression, not an anecdote.

## 2. In-process integration tests

Run on every CI push (`cargo test --workspace --all-features`):

- `bins/kaas/tests/smoke.rs` — wire-level round trips against an
  in-process broker.
- `bins/kaas/tests/auth_smoke.rs` — SASL/SCRAM handshake + ACL enforcement.
- `bins/kaas/tests/byte_opacity.rs` — asserts the
  [tripwire counters](wire-protocol.md) read zero after real traffic.
- `bins/kaas/tests/cluster_bringup.rs` + `cluster_smoke.rs` — multi-broker
  assignment, takeover, coordinator routing.
- `bins/kaas/tests/eos_v2.rs` — the full KIP-447
  consume-process-produce-commit round trip.
- `crates/kaas-controller/tests/controller_failover.rs` +
  `stale_controller_race.rs` — election handoff and the stale-epoch fence.

Plus per-crate unit tests — including the codec's fixture tests, which pin
encode/decode byte-identity against captures from Apache Kafka 3.7.

## 3. The parity project board

The [kaas-migration-parity](https://github.com/users/Woestebanaan/projects/2)
GitHub project tracks the feature matrix item by item — the working surface
where "what does 3.7 do here?" questions get resolved before they become
code. When a feature is ambiguous, the default is *match Apache Kafka 3.7*,
never invent kaas-specific semantics.

## 4. Docs that can't rot

Two CI gates keep this book honest (`cargo xtask check-docs-drift`):

- The [API support matrix](api-matrix.md) is generated from the codec
  registry; CI fails if the committed page drifts from the wire surface.
- Every `crates/…` / `bins/…` source path cited anywhere in the book is
  checked against the tree; a refactor that moves a file fails CI until the
  citation is fixed.

## Performance verification

Benchmarks are treated with the same suspicion as compatibility claims:
multi-run (5×) averaging with outlier exclusion, NFS RPC + network-rate
snapshots as a NAS-liveness probe, and recorded reports under
`docs/perf-results/`. Current standing and methodology live in
[Performance vs Strimzi](../operations/performance.md).
