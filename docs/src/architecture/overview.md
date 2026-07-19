# System overview

The moving parts at a glance: broker pods, the operator, the shared RWX PVC, the Kubernetes API — and where Apache Kafka clients plug in.

```mermaid
flowchart TB
    k8s["Kubernetes API<br/>Leases · CRDs · Services · RBAC"]
    clients["Apache Kafka clients<br/>Java · librdkafka · franz-go<br/>Produce / Fetch / Metadata / SASL …"]

    subgraph deployment["kaas deployment"]
        operator["kaas-operator<br/>(Deployment, 1 replica)"]
        subgraph brokers["kaas brokers (StatefulSet, N replicas)"]
            b0["kaas-0<br/>controller — holds the<br/>kaas-controller Lease"]
            b1["kaas-1"]
            b2["kaas-2"]
        end
        pvc[("Shared RWX PVC — NFSv4<br/>/data/__cluster/<br/>assignment.json · credentials.json · acls.json<br/>txn_state/ · fence_log/ · marker_queue/<br/>__consumer_offsets/<br/>/data/&lt;topic&gt;/&lt;partition&gt;/<br/>segments · manifest.json · producer-state.snapshot")]
    end

    operator -- "reconcile CRs" --> k8s
    brokers -- "watch Lease + CRs" --> k8s
    operator -- "writes credentials.json, acls.json,<br/>partition dirs" --> pvc
    brokers -- "append / read segments,<br/>assignment.json" --> pvc
    b1 -- "heartbeat gRPC :9094" --> b0
    b2 -- "heartbeat gRPC :9094" --> b0
    clients -- "Kafka wire protocol,<br/>per-listener ports" --> brokers
```

Kubernetes is the only control plane — there is no peer gossip protocol and no
replicated state machine. Three deliberate divergences from Apache Kafka: no
KRaft (controller election is a Kubernetes Lease), no replication/ISR
(single-writer-per-partition on shared storage), and no `__transaction_state`
internal topic (slot-sharded JSON files on the shared volume).
