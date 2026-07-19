#!/usr/bin/env bash
# Test kafka-console-consumer.sh against kaas.
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

echo ">> Scenario 3: --isolation-level read_committed (gh #31 wire surface)"
# v4+ FetchRequest carries IsolationLevel. Read-committed against
# a topic with no in-flight txns must return the same records as
# read-uncommitted (LSO = HighWatermark when no txns are active).
got=$("$KAFKA_BIN/kafka-console-consumer.sh" --bootstrap-server "$BOOTSTRAP" \
  --topic "$TOPIC" --from-beginning --max-messages 3 --timeout-ms 10000 \
  --consumer-property isolation.level=read_committed)
echo "$got"
[ "$(echo "$got" | wc -l)" -eq 3 ] || {
  echo "FAIL: read_committed returned $(echo "$got" | wc -l) lines, want 3 (no in-flight txns)" >&2
  exit 1
}

echo ">> Scenario 4: --partition / --offset earliest + formatter props"
# Verifies the metadata path returns offsets correctly via a
# different selector shape. The Kafka 4.x default formatter (no
# --formatter override needed; the old kafka.tools.DefaultMessageFormatter
# class was renamed) still honours print.partition / print.offset.
got=$("$KAFKA_BIN/kafka-console-consumer.sh" --bootstrap-server "$BOOTSTRAP" \
  --topic "$TOPIC" --partition 0 --offset earliest --max-messages 3 --timeout-ms 10000 \
  --property print.partition=true \
  --property print.offset=true)
echo "$got"
[ "$(echo "$got" | wc -l)" -eq 3 ] || { echo "FAIL: partition+offset selector" >&2; exit 1; }

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --delete --topic "$TOPIC" || true

echo ">> PASS"
