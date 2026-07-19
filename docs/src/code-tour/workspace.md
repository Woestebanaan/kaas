# Workspace layout & crate dependency graph

Twelve library crates, two binaries, and an xtask runner — who depends on whom, and why the layering looks the way it does.

## Crate dependency graph

An arrow reads "depends on". Verified against each crate's `Cargo.toml`
`[dependencies]` (runtime deps only, dev-dependencies excluded).

```mermaid
flowchart LR
    subgraph binaries
        kaas_bin["bins/kaas<br/>broker entrypoint"]
        op_bin["bins/kaas-operator<br/>operator entrypoint"]
    end

    broker["kaas-broker<br/>broker glue, Coordinator,<br/>takeover, handlers/*"]
    protocol["kaas-protocol<br/>dispatch, listener bring-up"]
    codec["kaas-codec<br/>wire frames, per-API codecs"]
    auth["kaas-auth<br/>SCRAM, mTLS, ACLs, quotas"]
    storage["kaas-storage<br/>engine, segments, idempotence"]
    coordinator["kaas-coordinator<br/>consumer groups + txns"]
    controller["kaas-controller<br/>election, balancer,<br/>assignment writer"]
    k8s["kaas-k8s<br/>endpoints, identity,<br/>topic watcher"]
    opapi["kaas-operator-api<br/>CRD types (kube-derive)"]
    opctl["kaas-operator-controllers<br/>reconcilers"]

    kaas_bin --> broker
    kaas_bin --> controller
    kaas_bin --> k8s
    kaas_bin --> protocol
    kaas_bin --> codec
    kaas_bin --> auth
    kaas_bin --> storage
    kaas_bin --> coordinator

    op_bin --> opctl
    op_bin --> opapi
    op_bin --> controller

    broker --> protocol
    broker --> codec
    broker --> auth
    broker --> storage
    broker --> coordinator
    broker --> opapi

    protocol --> codec
    protocol --> auth
    protocol --> storage

    controller --> broker
    controller --> coordinator

    k8s --> broker
    k8s --> coordinator
    k8s --> opapi

    opctl --> opapi
    opctl --> storage
```

Two crates are left off the diagram to keep it readable:

- **`kaas-observability`** is depended on by every crate above except
  `kaas-codec` and `kaas-operator-api` (and by both bins); its own single
  dependency is `kaas-codec`, for the byte-opacity tripwire counters.
- **`kaas-test-harness`** depends on nothing in the workspace — it carries the
  byte-opacity test fixtures and the `recordbatch` helper, the only place a
  decoded-record representation is allowed to live.

> This graph is hand-maintained (checked against `Cargo.toml` on 2026-07-19).
> Auto-generating it from `cargo metadata` is a possible future
> `gen-api-matrix`-style xtask.
