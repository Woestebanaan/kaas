#!/usr/bin/env bash
# Deferred — KIP-714 client telemetry is Early Access in Apache Kafka 3.7
# and not yet implemented in skafka. The broker would need to ship the
# new ClientMetrics admin APIs (PushTelemetryRequest / GetTelemetrySubscriptionsRequest).
# Revisit when client-side metrics push becomes a parity priority.

. "$(dirname "$0")/_common.sh"
skip "manages KIP-714 client metrics subscriptions; broker support not yet implemented"
