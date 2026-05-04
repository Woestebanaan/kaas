#!/usr/bin/env bash
# Test kafka-topics.sh against skafka.
#
# Scenarios:
#   1. --create a topic with N partitions
#   2. --list and assert the topic appears
#   3. --describe and assert partition count
#   4. --delete and assert it's gone

. "$(dirname "$0")/_common.sh"

PARTITIONS=3

echo ">> Scenario 1: create topic '$TOPIC' with $PARTITIONS partitions"
"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --create --topic "$TOPIC" --partitions "$PARTITIONS" --replication-factor 1

echo ">> Scenario 2: --list contains '$TOPIC'"
"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list | grep -qx "$TOPIC"

echo ">> Scenario 3: --describe reports $PARTITIONS partitions"
desc=$("$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe --topic "$TOPIC")
echo "$desc"
echo "$desc" | grep -qE "PartitionCount: ?$PARTITIONS"

echo ">> Scenario 4: --delete and confirm"
"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --delete --topic "$TOPIC"
sleep 2
if "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list | grep -qx "$TOPIC"; then
  echo "FAIL: topic still listed after delete" >&2; exit 1
fi

echo ">> PASS"
