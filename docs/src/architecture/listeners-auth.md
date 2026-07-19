# Listeners, authentication, authorization

Strimzi-shaped listeners, per-listener authentication engines, and cluster-wide ACL and quota enforcement.

## The pre-auth gate on an authed listener

Each listener gets its own auth engine, selected by listener name. Anonymous
listeners use an allow-all engine (no SASL, no principal); on authenticated
listeners the dispatcher blocks every API except the pre-auth allowlist —
SaslHandshake (17), ApiVersions (18), SaslAuthenticate (36) — until the SASL
exchange completes:

```mermaid
sequenceDiagram
    participant C as Client
    participant D as Dispatcher<br/>(per-listener gate)
    participant S as SCRAM-SHA-512 engine

    C->>D: ApiVersions (18)
    D-->>C: ok — pre-auth allowlist: 17 / 18 / 36
    C->>D: Metadata (3), before SASL
    D-->>C: CLUSTER_AUTHORIZATION_FAILED (31)<br/>in-band error, connection stays open
    C->>D: SaslHandshake (17), mechanism SCRAM-SHA-512
    D-->>C: supported: SCRAM-SHA-512, PLAIN
    C->>D: SaslAuthenticate (36)<br/>client-first: n,,n=user,r=client-nonce
    D->>S: step exchange (state kept on ConnState)
    S-->>C: server-first: r=combined-nonce,<br/>s=salt, i=iterations
    C->>D: SaslAuthenticate (36)<br/>client-final: c=biws, r, p=proof
    S->>S: recompute signature, constant-time<br/>compare against StoredKey
    S-->>C: server-final: v=server-signature, done
    Note over D: ConnState: principal = User:name,<br/>sasl_done = true
    C->>D: Metadata (3)
    D-->>C: dispatched — authorization now via<br/>cluster-wide ACLs + quotas
```

An mTLS listener satisfies the same gate at the TLS handshake instead: the
server extracts the principal from the client certificate (through the
KIP-371 principal-mapping rules) and marks the connection authenticated before
any Kafka API arrives.

**Authentication is per-listener; authorization is cluster-wide.** Once a
principal is on the connection, Produce/Fetch and the admin surfaces consult
the single cluster-wide authorizer and quota checker — which is what lets an
anonymous `plain` listener and an authed SCRAM listener share one ACL/quota
policy.
