# kaas-operator (operator binary)

The operator entrypoint driving the reconcilers in `kaas-operator-controllers`.

A small binary by design: it boots the three reconcilers (KafkaTopic,
KafkaUser, KafkaCluster) against a namespace-scoped `kube::Client`, runs
the leader-elected startup sweep, serves `/healthz` + `/readyz` over axum,
and shuts down cleanly on SIGTERM.

Configuration is all environment (chart-templated by
`deploy/helm/kaas/templates/operator-deployment.yaml`): `KAAS_DATA_DIR`
(shared PVC mount, default `/data`), `KAAS_NAMESPACE`, `KAAS_LOG_LEVEL` /
`KAAS_LOG_FORMAT`, the metrics/health bind addresses, and the standard
`OTEL_EXPORTER_OTLP_*` variables consumed by
[kaas-observability](kaas-observability.md)'s bootstrap.

Remember the architectural stance whenever tempted to grow this binary:
the operator is a startup/admission component. Brokers serve traffic while
it's down ([runtime independence](../architecture/runtime-independence.md));
anything that would put it on a request path belongs elsewhere.
