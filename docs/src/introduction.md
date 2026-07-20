# Introduction

kaas is a from-scratch Apache Kafka 3.7 wire-compatible broker that runs on Kubernetes — no KRaft, no replication, no ZooKeeper; Kubernetes primitives and a shared RWX volume do the heavy lifting instead.

Apache Kafka clients — Java, librdkafka, franz-go, the stock shell tools —
connect to kaas unchanged. What changes is everything behind the socket:

- **Controller election is a Kubernetes Lease**, not a Raft quorum. The
  API server is the metadata store; there is no gossip protocol and no
  replicated state machine in the broker.
- **Storage is single-writer-per-partition on a shared RWX volume**
  (NFSv4-class), not N-way broker replication. Split-brain safety comes
  from epoch-prefixed segment filenames; durability comes from the
  substrate.
- **Topics, users, ACLs, and quotas are Kubernetes CRs**, reconciled by a
  bundled operator that stays off the hot path entirely.

Two binaries ship from this repo: the broker (`bins/kaas`) and the
operator (`bins/kaas-operator`), plus a Helm chart that deploys both. The
release line is `v0.2.x-preview`.

## The parity target

kaas targets **Apache Kafka 3.7** for wire-protocol and Kafka Streams
parity — and this book is deliberately structured to *prove* that claim
rather than assert it. [Part II](compat/wire-protocol.md) carries a
[generated API matrix](compat/api-matrix.md) that CI pins to the actual
wire surface, a [KIP index](compat/kip-index.md) that says implemented /
partial / non-goal honestly, and a
[verification story](compat/verification.md) covering the Apache
shell-tool suite that runs against every release.

## Reading this book

- **[Getting Started](getting-started.md)** — deploy with Helm, or run a
  dev broker locally.
- **Part I — Architecture** — how it works, starting at the
  [system overview](architecture/overview.md).
- **Part II — Kafka Compatibility** — the prove-it section.
- **Part III — Code Tour** — the workspace, crate by crate.
- **Part IV — Operations** — the chart, storage substrates, releasing,
  performance.
