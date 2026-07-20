# Broker/operator runtime independence

Why the operator is a startup/admission component, not a hot-path dependency — the Produce/Fetch path makes zero Kubernetes API calls.

This is the most important architectural fact in kaas, and the easiest one to
misread from the directory layout: the repo ships a broker *and* an operator,
so it looks like a classic operator-managed system where the operator sits in
the middle of everything. It doesn't. The operator is a **startup and
admission** component:

- Brokers read `KafkaTopic` CRs at startup and watch them for new topics and
  partition expansion — but the read is **non-fatal**. A missing or unreachable
  API server only blocks *new* topic creation; existing topics keep serving.
- The Produce/Fetch hot path makes **zero Kubernetes API calls**. Ownership
  lookups are in-memory, against the broker's view of
  [`assignment.json`](./controller.md).
- Authentication and authorization state (`credentials.json`, `acls.json`) is
  read from the shared volume with hot-reload — the operator *writes* those
  files when `KafkaUser` CRs change, but a broker never asks Kubernetes a
  question to authenticate a client.
- Brokers serve traffic while the operator is crash-looping, upgrading, or
  deleted. What degrades without the operator: new `KafkaTopic`/`KafkaUser`
  CRs stop being materialized, and external-listener plumbing (Certificates,
  TLSRoutes) stops reconciling. What does not degrade: every already-created
  topic, credential, and ACL.

## The one Kubernetes dependency on the control path

Brokers do keep two long-lived Kubernetes watches: the `kaas-controller`
Lease (controller election) and the `KafkaTopic` CR watch (topic catalog).
Both feed the *control* plane — assignment recomputation — not the data
plane. If the API server goes away, the current controller keeps its Lease
view, `assignment.json` stays where it is, and partition leadership simply
stops *changing* until the API server returns. Clients notice nothing.

"Until the API server returns" is load-bearing, and it costs the topic watch
real machinery to honour (gh #202). `run_topic_watch` rebuilds its stream on
end with exponential backoff rather than returning, because kube ends watch
streams for entirely routine reasons — relist, apiserver rollout, network
blip — and a watch that exits on the first one stops tracking topics
*permanently*, with no error to log. It also treats each relist as a full
reconcile: the topic set carried by `Event::InitApply` is diffed against what
the watch last reported, and anything missing is retracted. Without that
diff, a topic deleted while the watch was disconnected would never produce a
`Delete` event and would linger in the registry forever — the broker serving
Metadata for a topic that no longer exists, and the controller assigning its
partitions to brokers that then fail to open them.

## Admin writes go through CRs — deliberately

The broker isn't strictly read-only against Kubernetes: two admin handlers
**write** `KafkaTopic` CRs, so that the operator remains the single
materializer of topic state:

- `CreatePartitions` (key 37) patches `spec.partitions`.
- `IncrementalAlterConfigs` (key 44) patches `spec.config` per key.

Both route through `crates/kaas-broker/src/topic_cr_writer.rs`; the operator
then creates partition directories / rewrites `.config.json` exactly as if a
human had edited the CR. This keeps one writer for on-disk topic layout while
still serving the Kafka admin surface. (It's also why broker RBAC carries
`update,patch` on `kafkatopics` — see [Kubernetes
integration](./kubernetes.md).)

## The line not to cross

If a change adds a broker→operator *runtime* dependency — a CR watch that
blocks request handling, a reconcile the hot path waits on — that's an
architectural change, not an implementation detail. The invariant to
preserve: **a broker that has already started serves Produce/Fetch with the
Kubernetes API server unreachable.**
