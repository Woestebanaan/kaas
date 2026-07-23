# Controller, leases & assignment.json

Controller election via a Kubernetes Lease, and `assignment.json` on the
shared volume as the single source of truth for partition leadership.

In Apache Kafka, the controller is elected by the metadata quorum —
KRaft today, ZooKeeper before it — and partition leadership reaches the
brokers through a replicated metadata log. kaas replaces that entire
machine with two much smaller parts (the first of the [three
substitutions](./overview.md)): election is a Kubernetes **Lease**, and
propagation is a **JSON file on the shared volume**.

The "controller" is just a broker holding the `kaas-controller` Lease —
there is no separate process and no Raft quorum. The Lease's
`leaseTransitions` counter is the cluster's epoch source: it increments
exactly when the holder changes, and a releasing controller re-sends it
so the epoch fence never rewinds.

```mermaid
sequenceDiagram
    participant L as Kubernetes Lease<br/>kaas-controller
    participant C as kaas-0<br/>(controller)
    participant A as assignment.json<br/>/data/__cluster/
    participant B as kaas-1<br/>(peer broker)

    B->>C: heartbeat gRPC :9094<br/>bidi stream, 1 s PING cadence
    C->>L: acquire (server-side apply)<br/>holderIdentity = kaas-0,<br/>leaseTransitions +1 on takeover
    L-->>C: epoch = leaseTransitions
    Note over C: recompute triggers:<br/>first Lease win · KafkaTopic change ·<br/>broker join/leave (2 s alive-set poll)
    C->>C: balancer: partition +<br/>consumer-group assignments
    C->>A: write tmp + fsync + rename<br/>{controller_epoch, assignment_version,<br/>partitions, consumerGroups}
    C-->>B: heartbeat push: ASSIGNMENT_CHANGED
    B->>A: re-read (1 s mtime poll,<br/>push is the fast path)
    B->>B: reject if controller_epoch<br/>< Lease epoch<br/>(stale-controller fence)
    B->>B: partition takeover diff:<br/>take over → open FDs + recover<br/>relinquish → close FDs
    B->>B: consumer-group takeover diff<br/>+ orphan sweep
```

The controller also mirrors each written assignment into the
`KafkaClusterAssignments` CR — a fire-and-forget debug surface for
`kubectl`; brokers never read it. There is no per-partition Lease: the
singleton controller Lease is the only Kubernetes coordination
primitive, and everything downstream of it travels through
`assignment.json` on the shared volume.

## What the controller does

The Lease holder takes on four extra responsibilities:

- **Observes peer brokers** via the heartbeat gRPC stream every broker
  dials into it. A broker that stops heartbeating ages out of the alive
  set — there is no controlled-shutdown RPC; the controller learns of a
  departure by timeout and rebalances reactively.
- **Computes assignments** — partition leadership and consumer-group
  placement — over the alive set.
- **Writes `assignment.json`**, epoch-prefixed, tmp + fsync + rename.
  Every broker rejects an assignment whose epoch is stale, so a deposed
  controller coming back from a GC pause can't roll the cluster
  backwards.
- **Mirrors to Kubernetes**, for
  `kubectl get kafkaclusterassignments` diagnostics only.

## When it recomputes

| Trigger | How the controller notices |
|---|---|
| First win of the controller Lease | initial recompute |
| `KafkaTopic` CR added / modified / deleted | the topic watch's change notification |
| Broker joins or leaves the alive set | the broker-set watcher's 2 s alive-set poll |

The alive set the balancer feeds on is the set of heartbeat-connected
brokers that report themselves healthy — a broker's own 1 s liveness
tick, trusted unconditionally. Kubernetes endpoint readiness is only
the bootstrap fallback for a freshly elected controller that no broker
has dialed into yet, so a controller elected mid-rollout doesn't
compute an empty assignment. How a broker earns — and loses — its
place in the alive set is the subject of [Honest readiness & rollout
pacing](./readiness-rollout.md).

## How peers follow

Non-controller brokers watch `assignment.json` via file notification
plus a 1 s poll; the heartbeat stream's `ASSIGNMENT_CHANGED` push is
the fast path, the poll the backstop. On every accepted assignment the
broker diffs the new leadership map against what it currently serves,
opening or relinquishing partitions in the storage engine to match (see
[File-handle ownership](./file-handles.md)), and does the same for
consumer groups (see [Consumer-group
coordination](./consumer-groups.md)).

Everything that needs a leadership answer — the Metadata response, the
Produce/Fetch ownership check, `/healthz`'s `partitions_led` — sources
from the broker's view of `assignment.json`. There is no second
authority to disagree with.

## Local-dev mode

When the broker starts outside a pod (the `MY_POD_NAME` env unset), the
cluster runtime isn't started at all: storage flips to in-memory and a
local shim answers "yes, I lead" for every partition. This is a
dev-loop convenience, not a single-node production mode — nothing is
persisted.

## Implementation notes (for contributors)

- Controller-side logic lives in `crates/kaas-controller`:
  `heartbeat_server.rs` (serves `proto/heartbeat.proto`),
  `balancer.rs` (assignment computation), `assignment_writer.rs`
  (epoch-prefixed write), `k8s_mirror.rs` (the CR mirror).
- Recompute wiring — the topic-watch callback (gh #74) and the 2 s
  broker-set watcher (gh #77) — is in `bins/kaas/src/cluster.rs`.
- The broker-side assignment watcher and stale-epoch rejection live in
  `crates/kaas-broker/src/coordinator.rs`; making `assignment.json`
  the single leadership authority was the gh #75 cleanup. The
  deposed-controller race is pinned down by
  `crates/kaas-controller/tests/stale_controller_race.rs`.
- Dev-mode selection is in `bins/kaas/src/main.rs`; the always-leader
  shim is `crates/kaas-broker/src/local_lease.rs`.
