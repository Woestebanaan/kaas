#!/usr/bin/env bash
# Test kafka-cluster.sh against skafka.
#
# Scenarios:
#   1. cluster-id returns a non-empty cluster ID

. "$(dirname "$0")/_common.sh"

echo ">> Scenario 1: cluster-id"
# Java tool prints "Cluster ID: <id>" with colon separator, not equals.
# Extract everything after the first colon, trim leading spaces.
id=$("$KAFKA_BIN/kafka-cluster.sh" cluster-id --bootstrap-server "$BOOTSTRAP" \
  | sed -n 's/^Cluster ID:[[:space:]]*//p' | head -1)
echo "cluster-id=$id"
[ -n "$id" ] || { echo "FAIL: empty cluster-id" >&2; exit 1; }

echo ">> PASS"
