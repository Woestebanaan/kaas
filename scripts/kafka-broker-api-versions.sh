#!/usr/bin/env bash
# Test kafka-broker-api-versions.sh against skafka.
#
# Scenarios:
#   1. ApiVersions response lists at minimum the core APIs the broker advertises

. "$(dirname "$0")/_common.sh"

echo ">> Scenario 1: query advertised API versions"
out=$("$KAFKA_BIN/kafka-broker-api-versions.sh" --bootstrap-server "$BOOTSTRAP")
echo "$out"

# Sanity-check a handful of must-have APIs by name.
required=(Produce Fetch Metadata ApiVersions FindCoordinator JoinGroup SyncGroup Heartbeat OffsetCommit OffsetFetch)
for api in "${required[@]}"; do
  echo "$out" | grep -qE "^[[:space:]]*$api\b" || { echo "FAIL: $api not advertised" >&2; exit 1; }
done

echo ">> PASS"
