#!/usr/bin/env bash
# Test kafka-cluster.sh against skafka.
#
# Scenarios:
#   1. cluster-id returns a non-empty cluster ID

. "$(dirname "$0")/_common.sh"

echo ">> Scenario 1: cluster-id"
id=$("$KAFKA_BIN/kafka-cluster.sh" cluster-id --bootstrap-server "$BOOTSTRAP" \
  | awk -F= '/^Cluster ID:/ {print $2}' | tr -d ' ')
echo "cluster-id=$id"
[ -n "$id" ] || { echo "FAIL: empty cluster-id" >&2; exit 1; }

echo ">> PASS"
