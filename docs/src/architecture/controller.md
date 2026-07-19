# Controller, leases & assignment.json

Controller election via a Kubernetes Lease, and `assignment.json` on the shared volume as the single source of truth for partition leadership.

The "controller" is just a broker holding the `kaas-controller` Lease — there
is no separate process and no Raft quorum. The Lease's `leaseTransitions`
counter is the cluster's epoch source: it increments exactly when the holder
changes, and a releasing controller re-sends it so the epoch fence never
rewinds.

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
    B->>B: apply_if_new — reject if<br/>controller_epoch < Lease epoch<br/>(stale-controller fence)
    B->>B: TakeoverDriver diff:<br/>take_over → open FDs + recover<br/>relinquish → close FDs
    B->>B: GroupTakeoverDriver diff<br/>+ orphan sweep
```

The controller also mirrors each written assignment into the
`KafkaClusterAssignments` CR — a fire-and-forget debug surface for `kubectl`;
brokers never read it. There is no per-partition Lease: the singleton
controller Lease is the only Kubernetes coordination primitive, and everything
downstream of it travels through `assignment.json` on the shared volume.
