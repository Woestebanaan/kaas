# kaas — Architecture

**This document has moved into the kaas book** (Part I — Architecture).

kaas is a from-scratch Apache Kafka 3.7 wire-compatible broker that runs on
Kubernetes: no KRaft (a Kubernetes Lease elects the controller), no
replication/ISR (single-writer-per-partition on shared RWX storage), no
`__transaction_state` internal topic (slot-sharded JSON files on the shared
volume). Apache Kafka clients connect unchanged.

Read the architecture chapters in the book:

- Published: <https://woestebanaan.github.io/kaas/> (Part I).
- Build locally: `cargo xtask docs` (or `cargo xtask docs --serve` for live
  preview).
- Source chapters: [`docs/src/architecture/`](./src/architecture/) —
  starting at [`overview.md`](./src/architecture/overview.md).

Per-feature reference detail stays in [`CLAUDE.md`](../CLAUDE.md); the
release procedure in [`RELEASING.md`](./RELEASING.md).
