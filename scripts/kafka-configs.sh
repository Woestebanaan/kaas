#!/usr/bin/env bash
# Test kafka-configs.sh against skafka.
#
# Scenarios:
#   1. --describe broker config (read path)
#   2. --describe topic config (read path)
#   3. --alter topic config — currently a GAP (issue #9), expected to fail.
#      Marked as expected-fail until IncrementalAlterConfigs is implemented.

. "$(dirname "$0")/_common.sh"

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 1

echo ">> Scenario 1: --describe broker config"
"$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
  --entity-type brokers --entity-default --describe

echo ">> Scenario 2: --describe topic config for '$TOPIC'"
"$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
  --entity-type topics --entity-name "$TOPIC" --describe

echo ">> Scenario 3 (XFAIL, gap #9): --alter topic config retention.ms"
if "$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
     --entity-type topics --entity-name "$TOPIC" \
     --alter --add-config retention.ms=60000 2>&1; then
  echo "UNEXPECTED PASS — IncrementalAlterConfigs may now be implemented; close gap #9."
else
  echo "(expected) alter rejected — broker work needed (#9)"
fi

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --delete --topic "$TOPIC" || true

echo ">> PASS (read paths)"
