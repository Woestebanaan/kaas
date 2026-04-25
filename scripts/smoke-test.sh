#!/usr/bin/env bash
# Produce "Hello world" to a skafka topic and consume it back to verify the
# round-trip works end-to-end. Uses kafka-console-{producer,consumer}.sh from
# /opt/kafka/bin. Intended to be run from inside the cluster (the in-cluster
# Service DNS is the default bootstrap).
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-skafka.skafka.svc.cluster.local:9092}"
TOPIC="${TOPIC:-smoke}"
MESSAGE="${MESSAGE:-Hello world}"
TIMEOUT_MS="${TIMEOUT_MS:-15000}"

echo ">> bootstrap: ${BOOTSTRAP}"
echo ">> topic:     ${TOPIC}"

echo ">> producing one message"
# Disable idempotence: skafka does not implement InitProducerId (API key 22),
# which the modern producer would otherwise call on startup.
echo "${MESSAGE}" | kafka-console-producer.sh \
  --bootstrap-server "${BOOTSTRAP}" \
  --topic "${TOPIC}" \
  --producer-property enable.idempotence=false \
  --producer-property acks=1

echo ">> consuming from beginning (timeout ${TIMEOUT_MS}ms)"
out=$(kafka-console-consumer.sh \
  --bootstrap-server "${BOOTSTRAP}" \
  --topic "${TOPIC}" \
  --from-beginning \
  --timeout-ms "${TIMEOUT_MS}" 2>/dev/null || true)

echo "${out}"

if grep -Fxq "${MESSAGE}" <<<"${out}"; then
  echo ">> PASS: round-trip successful"
  exit 0
fi
echo ">> FAIL: expected ${MESSAGE@Q} not found in consumer output" >&2
exit 1
