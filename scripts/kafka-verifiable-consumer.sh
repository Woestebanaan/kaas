#!/usr/bin/env bash
# Test kafka-verifiable-consumer.sh against skafka.
#
# Scenarios:
#   1. Seed 1000 records; consumer reports records_consumed >= 1000

. "$(dirname "$0")/_common.sh"

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 1

echo ">> seeding 1000 records"
"$KAFKA_BIN/kafka-verifiable-producer.sh" \
  --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" \
  --max-messages 1000 --acks -1 >/dev/null

echo ">> Scenario 1: consume 1000 records"
"$KAFKA_BIN/kafka-verifiable-consumer.sh" \
  --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" \
  --group-id "verif-cg-$$" --max-messages 1000 --reset-policy earliest \
  | tee "$TMP/out.json"

count=$(grep -oE '"records_consumed":[0-9]+' "$TMP/out.json" \
  | awk -F: '{s+=$2} END{print s+0}')
echo "records_consumed total: $count"
[ "$count" -ge 1000 ] || { echo "FAIL: consumed $count < 1000" >&2; exit 1; }

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --delete --topic "$TOPIC" || true

echo ">> PASS"
