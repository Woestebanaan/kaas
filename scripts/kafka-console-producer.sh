#!/usr/bin/env bash
# Test kafka-console-producer.sh against skafka.
#
# Scenarios:
#   1. Produce a few lines to a fresh topic
#   2. Produce with explicit --property parse.key=true and a key separator

. "$(dirname "$0")/_common.sh"

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 1

echo ">> Scenario 1: produce 3 plain lines"
printf 'one\ntwo\nthree\n' | \
  "$KAFKA_BIN/kafka-console-producer.sh" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC"

echo ">> Scenario 2: produce keyed records (k:v)"
printf 'k1:v1\nk2:v2\n' | \
  "$KAFKA_BIN/kafka-console-producer.sh" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" \
    --property parse.key=true --property key.separator=:

echo ">> verifying with kafka-get-offsets.sh: high-watermark should be 5"
out=$("$KAFKA_BIN/kafka-get-offsets.sh" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" --time -1)
echo "$out"
echo "$out" | grep -qE "${TOPIC}:0:5\$" || { echo "FAIL: end-offset != 5" >&2; exit 1; }

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --delete --topic "$TOPIC" || true

echo ">> PASS"
