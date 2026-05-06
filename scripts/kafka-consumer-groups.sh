#!/usr/bin/env bash
# Test kafka-consumer-groups.sh against skafka.
#
# Scenarios:
#   1. --list (initially the test group is absent)
#   2. Produce + consume with --group <id> to create the group
#   3. --describe shows the group, members, and offsets
#   4. --reset-offsets --to-earliest --execute
#   5. --delete (gh #89): the group must actually disappear from
#      --list afterwards.

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

echo ">> Scenario 5: --delete must actually remove the group"
# Pre-#89 this returned UnsupportedVersionException and we
# tolerated it via "|| true". Now the broker advertises key 42
# and the call must succeed AND the group must vanish from
# subsequent --list output. Capture stderr so a regression
# (e.g. NON_EMPTY_GROUP if the consumer state leaks) is loud.
"$KAFKA_BIN/kafka-consumer-groups.sh" --bootstrap-server "$BOOTSTRAP" \
  --delete --group "$GROUP" 2>"$TMP/del.err" \
  || { echo "FAIL: --delete failed"; cat "$TMP/del.err" >&2; exit 1; }

if "$KAFKA_BIN/kafka-consumer-groups.sh" --bootstrap-server "$BOOTSTRAP" --list \
    | grep -qx "$GROUP"; then
  echo "FAIL: deleted group still appears in --list" >&2
  exit 1
fi

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --delete --topic "$TOPIC" || true

echo ">> PASS"
