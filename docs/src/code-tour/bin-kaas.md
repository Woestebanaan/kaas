# kaas (broker binary)

The broker entrypoint: dispatcher registration, env wiring, the cluster runtime, and the graceful SIGTERM drain.

`bins/kaas` is deliberately thin on logic and thick on *wiring* — it's
where every seam the library crates expose gets a production
implementation plugged in.

**`main.rs`**: parses `KAAS_LISTENERS` (default: one plain listener on
`0.0.0.0:9092`), selects storage (`KAAS_DATA_DIR` set → disk engine;
unset → in-memory dev mode), builds the per-listener auth engines and the
cluster-wide authorizer/quota checker, registers every handler with the
dispatcher, spawns the kube topic watch (in cluster mode), starts
`/healthz`/`/readyz`, and owns the **graceful SIGTERM drain** — relinquish
every open partition (persisting manifests + producer snapshots, closing
FDs), then flush remaining manifests as defence-in-depth
([file-handle ownership](../architecture/file-handles.md)).

**`cluster.rs`**: the cluster runtime — Lease election glue, heartbeat
client/server wiring, broker-set watcher (2 s alive-set poll), topic-change
notifier, the assignment loop when this broker is controller, the
fence/marker watchers, the txn timeout reaper, and the **assignment-source
hot-swap** (the boot-time always-true stub replaced by the real
`Coordinator`-backed source once the runtime is up — the gh #92 dance
described in [Consumer-group
coordination](../architecture/consumer-groups.md)).

**Mode selection**: `MY_POD_NAME` set → cluster mode; unset → dev mode
(no cluster runtime, local-lease "I lead everything", in-memory storage).

**Integration tests** live in `bins/kaas/tests/` — `smoke.rs`,
`auth_smoke.rs`, `byte_opacity.rs`, `cluster_bringup.rs`,
`cluster_smoke.rs`, `eos_v2.rs` — the suites the
[verification story](../compat/verification.md) leans on.

**Start reading at** `main.rs` top to bottom — it reads as a map of the
whole system.
