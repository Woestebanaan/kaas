# kaas-k8s

Broker-side Kubernetes helpers: peer-endpoint watching, pod identity, the topic watcher, and the partitions-ready readiness gate.

Every module follows the same two-layer split: a **pure-state core** that
tests exercise directly, and a **kube-bound pump** (behind the default
`kube-watchers` feature) that feeds it events.

**Module map**: `identity.rs` (`BrokerIdentity` — parses the ordinal out of
the StatefulSet pod name; no kube dependency), `endpoints.rs`
(`BrokerRegistry` over the headless Service's EndpointSlices — the broker's
live view of its peers, feeding FindCoordinator and Metadata),
`topic_watcher.rs` (a pure-state `KafkaTopic` cache + divergence detector),
`readiness.rs` (the `kaas.rs/PartitionsReady` pod readiness gate),
`kube_watchers.rs` (the pumps: lease watch, endpoint watch, topic watch,
readiness patching).

**An honesty note mirrored from Part II**: the production topic pump is
`kube_watchers::run_topic_watch`, whose callbacks carry only
`(name, partitions)` — the richer `TopicWatcher` cache (with
`deletionTimestamp`-immediate delete events and `Status.TopicID`
stashing) is **not wired in**. Two consequences documented elsewhere:
topic-delete FD-closing rides the assignment recompute instead of the
immediate event, and Metadata serves all-zero topic IDs
([KIP-516](../compat/kip/kip-516.md)).

**The pump is self-healing, and has to be** (gh #202). Two properties, both
easy to omit and neither optional:

- It **restarts its own stream** with exponential backoff instead of
  returning when the stream ends. Kube ends streams routinely; the earlier
  version returned `Ok(())` and trusted the caller to restart it, which the
  caller never did — so a single relist silently ended topic tracking for
  the life of the process.
- It **reconciles on relist**. `Event::Init` opens a set, `InitApply`
  fills it, `InitDone` retracts every previously-reported topic absent from
  it. A topic deleted while the watch was disconnected produces no `Delete`
  event, so the diff is the only thing that can notice it.

The state backing that diff deliberately outlives any single stream — that's
the whole point, and it's why it lives in `TopicWatchState` rather than
inside the stream loop. A relist cut short by a restart drops its partial
set rather than retracting topics it never finished enumerating.

**Invariant callers must hold**: nothing here may block request handling —
every consumer of this crate's state reads a cached view, and a dead API
server only freezes that view *temporarily*, until the watch reconnects and
reconciles ([runtime
independence](../architecture/runtime-independence.md)).

**Start reading at** `endpoints.rs` (the cleanest example of the
two-layer split), then `kube_watchers.rs`.
