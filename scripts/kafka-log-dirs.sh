#!/usr/bin/env bash
# Test kafka-log-dirs.sh against skafka.
#
# Scenarios:
#   1. --describe with no --topic-list returns all log dirs
#   2. --describe filtered to a specific topic returns only that topic

. "$(dirname "$0")/_common.sh"

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 1

echo ">> Scenario 1: --describe (all dirs)"
"$KAFKA_BIN/kafka-log-dirs.sh" --bootstrap-server "$BOOTSTRAP" --describe | tee "$TMP/all.json"
grep -q '"logDir"' "$TMP/all.json" || { echo "FAIL: no logDir in response" >&2; exit 1; }

echo ">> Scenario 2: --describe --topic-list $TOPIC"
out=$("$KAFKA_BIN/kafka-log-dirs.sh" --bootstrap-server "$BOOTSTRAP" \
  --describe --topic-list "$TOPIC")
echo "$out"
echo "$out" | grep -q "\"$TOPIC\"" || { echo "FAIL: topic missing" >&2; exit 1; }

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --delete --topic "$TOPIC" || true

echo ">> PASS"
