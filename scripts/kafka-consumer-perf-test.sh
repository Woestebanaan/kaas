#!/usr/bin/env bash
# Test kafka-consumer-perf-test.sh against skafka.
#
# Scenarios:
#   1. Seed 10k records, then run consumer-perf-test for that count

. "$(dirname "$0")/_common.sh"

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --create --topic "$TOPIC" --partitions 3 --replication-factor 1

echo ">> seeding 10k records"
"$KAFKA_BIN/kafka-producer-perf-test.sh" --topic "$TOPIC" \
  --num-records 10000 --record-size 1024 --throughput -1 \
  --producer-props bootstrap.servers="$BOOTSTRAP" acks=1 >/dev/null

echo ">> Scenario 1: consume 10k records"
"$KAFKA_BIN/kafka-consumer-perf-test.sh" \
  --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" \
  --messages 10000 --group "perf-cg-$$" --timeout 60000

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --delete --topic "$TOPIC" || true

echo ">> PASS"
