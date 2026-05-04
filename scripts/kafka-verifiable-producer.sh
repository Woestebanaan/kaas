#!/usr/bin/env bash
# Test kafka-verifiable-producer.sh against skafka.
#
# Scenarios:
#   1. Produce 1000 records with acks=all, verify success line count

. "$(dirname "$0")/_common.sh"

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 1

echo ">> Scenario 1: 1000 records, acks=all"
"$KAFKA_BIN/kafka-verifiable-producer.sh" \
  --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" \
  --max-messages 1000 --acks -1 | tee "$TMP/out.json"

echo ">> assert ack=success line count >= 1000"
ok=$(grep -c '"name":"producer_send_success"' "$TMP/out.json" || true)
[ "$ok" -ge 1000 ] || { echo "FAIL: only $ok success lines" >&2; exit 1; }

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --delete --topic "$TOPIC" || true

echo ">> PASS"
