#!/usr/bin/env bash
# Test kafka-consumer-groups.sh against skafka.
#
# Scenarios:
#   1. --list (initially the test group is absent)
#   2. Produce + consume with --group <id> to create the group
#   3. --describe shows the group, members, and offsets
#   4. --reset-offsets --to-earliest --execute

. "$(dirname "$0")/_common.sh"

GROUP="skafka-test-cg-$$"

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --create --topic "$TOPIC" --partitions 2 --replication-factor 1

echo ">> seeding 5 messages"
printf 'a\nb\nc\nd\ne\n' | \
  "$KAFKA_BIN/kafka-console-producer.sh" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC"

echo ">> Scenario 1: --list (group should NOT exist yet)"
"$KAFKA_BIN/kafka-consumer-groups.sh" --bootstrap-server "$BOOTSTRAP" --list

echo ">> Scenario 2: consume into group '$GROUP'"
"$KAFKA_BIN/kafka-console-consumer.sh" --bootstrap-server "$BOOTSTRAP" \
  --topic "$TOPIC" --group "$GROUP" --from-beginning \
  --max-messages 5 --timeout-ms 10000 >/dev/null

echo ">> Scenario 3: --describe group"
"$KAFKA_BIN/kafka-consumer-groups.sh" --bootstrap-server "$BOOTSTRAP" \
  --describe --group "$GROUP"

echo ">> Scenario 4: --reset-offsets --to-earliest --execute"
"$KAFKA_BIN/kafka-consumer-groups.sh" --bootstrap-server "$BOOTSTRAP" \
  --reset-offsets --to-earliest --topic "$TOPIC" --group "$GROUP" --execute

"$KAFKA_BIN/kafka-consumer-groups.sh" --bootstrap-server "$BOOTSTRAP" \
  --delete --group "$GROUP" || true
"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --delete --topic "$TOPIC" || true

echo ">> PASS"
