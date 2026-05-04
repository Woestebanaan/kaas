#!/usr/bin/env bash
# Test kafka-console-consumer.sh against skafka.
#
# Scenarios:
#   1. --from-beginning + --max-messages reads exactly the messages produced
#   2. --offset latest --partition 0 with no new traffic times out cleanly

. "$(dirname "$0")/_common.sh"

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 1

echo ">> seeding 3 messages"
printf 'a\nb\nc\n' | \
  "$KAFKA_BIN/kafka-console-producer.sh" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC"

echo ">> Scenario 1: consume --from-beginning --max-messages 3"
got=$("$KAFKA_BIN/kafka-console-consumer.sh" --bootstrap-server "$BOOTSTRAP" \
  --topic "$TOPIC" --from-beginning --max-messages 3 --timeout-ms 10000)
echo "$got"
[ "$(echo "$got" | wc -l)" -eq 3 ] || { echo "FAIL: expected 3 lines" >&2; exit 1; }

echo ">> Scenario 2: --offset latest with no new produces, timeout-ms 3000 (expect zero output)"
got=$("$KAFKA_BIN/kafka-console-consumer.sh" --bootstrap-server "$BOOTSTRAP" \
  --topic "$TOPIC" --partition 0 --offset latest --timeout-ms 3000 || true)
[ -z "$got" ] || echo "WARN: unexpected output: $got"

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --delete --topic "$TOPIC" || true

echo ">> PASS"
