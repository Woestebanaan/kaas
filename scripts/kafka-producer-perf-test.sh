#!/usr/bin/env bash
# Test kafka-producer-perf-test.sh against skafka.
#
# Scenarios:
#   1. 10k records, 1KB payload, acks=1
#   2. Same with --producer-props compression.type=snappy

. "$(dirname "$0")/_common.sh"

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --create --topic "$TOPIC" --partitions 3 --replication-factor 1

echo ">> Scenario 1: 10k records / 1KB / acks=1"
"$KAFKA_BIN/kafka-producer-perf-test.sh" --topic "$TOPIC" \
  --num-records 10000 --record-size 1024 --throughput -1 \
  --producer-props bootstrap.servers="$BOOTSTRAP" acks=1

echo ">> Scenario 2: same, with snappy compression"
"$KAFKA_BIN/kafka-producer-perf-test.sh" --topic "$TOPIC" \
  --num-records 10000 --record-size 1024 --throughput -1 \
  --producer-props bootstrap.servers="$BOOTSTRAP" acks=1 compression.type=snappy

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --delete --topic "$TOPIC" || true

echo ">> PASS"
