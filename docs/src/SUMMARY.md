# Summary

[Introduction](introduction.md)

- [Getting Started](getting-started.md)

# Part I — Architecture

- [System overview](architecture/overview.md)
- [Broker/operator runtime independence](architecture/runtime-independence.md)
- [Controller, leases & assignment.json](architecture/controller.md)
- [Storage engine hot path](architecture/storage-hot-path.md)
- [File-handle ownership & takeover](architecture/file-handles.md)
- [Consumer-group coordination](architecture/consumer-groups.md)
- [Transactions & idempotence](architecture/transactions.md)
- [Listeners, authentication, authorization](architecture/listeners-auth.md)
- [Kubernetes integration](architecture/kubernetes.md)
- [Observability](architecture/observability.md)

# Part II — Kafka Compatibility

- [Wire protocol & framing](compat/wire-protocol.md)
- [API support matrix](compat/api-matrix.md)
- [Per-API reference](compat/api-reference.md)
- [KIP index](compat/kip-index.md)
- [Per-KIP details](compat/kip-details.md)
- [Non-goals](compat/non-goals.md)
- [Verification story](compat/verification.md)

# Part III — Code Tour

- [Workspace layout & crate dependency graph](code-tour/workspace.md)
  - [kaas-codec](code-tour/kaas-codec.md)
  - [kaas-protocol](code-tour/kaas-protocol.md)
  - [kaas-storage](code-tour/kaas-storage.md)
  - [kaas-coordinator](code-tour/kaas-coordinator.md)
  - [kaas-broker](code-tour/kaas-broker.md)
  - [kaas-controller](code-tour/kaas-controller.md)
  - [kaas-auth](code-tour/kaas-auth.md)
  - [kaas-k8s](code-tour/kaas-k8s.md)
  - [kaas-observability](code-tour/kaas-observability.md)
  - [kaas-operator-api](code-tour/kaas-operator-api.md)
  - [kaas-operator-controllers](code-tour/kaas-operator-controllers.md)
  - [kaas-test-harness](code-tour/kaas-test-harness.md)
  - [kaas (broker binary)](code-tour/bin-kaas.md)
  - [kaas-operator (operator binary)](code-tour/bin-kaas-operator.md)

# Part IV — Operations

- [Helm chart & listener configuration](operations/helm.md)
- [Storage substrate requirements](operations/storage.md)
- [Releasing](operations/releasing.md)
- [Performance vs Strimzi](operations/performance.md)
