# Kubernetes integration

The four CRDs, their reconcilers, reconcile-time cleanup (no finalizers), and the broker's RBAC surface.

## Operator reconcile loops

One reconciler per CRD. None of them use cleanup finalizers — deleting a CR
never blocks on the operator being alive; owned Kubernetes resources carry
`OwnerReferences` so garbage collection is K8s-native, and on-disk leftovers
are reclaimed by a leader-elected sweep at operator startup.

```mermaid
flowchart LR
    api["Kubernetes API<br/>watch streams"]

    subgraph operator["kaas-operator — single replica, leader-elected"]
        rt["KafkaTopic reconciler<br/>requeue 300 s"]
        ru["KafkaUser reconciler<br/>await_change"]
        rc["KafkaCluster reconciler<br/>requeue 300 s"]
        sweep["startup sweep — once, on leadership:<br/>drop topic dirs + credential entries<br/>with no matching CR"]
    end

    api --> rt
    api --> ru
    api --> rc

    rt --> dirs["partition dirs<br/>/data/&lt;topic&gt;/&lt;0..N&gt;/ + .config.json"]
    rt --> tstat["Status.TopicID — v4 UUID minted on<br/>first reconcile, never rotated (KIP-516)"]
    ru --> creds["__cluster/credentials.json (upsert user)<br/>__cluster/acls.json (rebuilt from all users)"]
    ru --> secret["&lt;user&gt;-kafka-credentials Secret<br/>OwnerReference → K8s GC"]
    rc --> plumbing["cert-manager Certificates ·<br/>per-broker Services · TLSRoutes<br/>OwnerReferences → K8s GC"]
    rc --> kca["KafkaClusterAssignments CR<br/>create-only; the controller broker<br/>mirrors assignments into it"]
    sweep --> dirs
    sweep --> creds
```

Reconciler guard rails worth knowing:

- **KafkaTopic** refuses partition decrease (`Ready=False`, no filesystem
  mutation) — partitions only grow, matching Kafka semantics.
- **KafkaUser** with a missing referenced Secret parks on `await_change`
  instead of hot-looping.
- **KafkaClusterAssignments** has no reconciler at all: the operator only
  creates it (with an OwnerReference); its status is written fire-and-forget by
  the controller broker, and brokers never read it back.
- A CR with `deletionTimestamp` set is left untouched by the reconcilers;
  cleanup happens via K8s GC (owned resources) and the startup sweep (on-disk
  state).

On the broker side, the CRD surface is read-mostly — but not read-only:
`CreatePartitions` and `IncrementalAlterConfigs` patch `KafkaTopic` CRs
(`spec.partitions` / `spec.config`), which is why broker RBAC carries
`update,patch` on `kafkatopics` in addition to the read verbs.
