#!/usr/bin/env bash
# Test kafka-producer-perf-test.sh against skafka.
#
# Scenarios:
#   1. 10k records, 1KB payload, acks=1
#   2. Same with --producer-props compression.type=snappy

. "$(dirname "$0")/_common.sh"

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --create --topic "$TOPIC" --partitions 3 --replication-factor 1

echo ">> Scenario 1: 10k records / 1KB / acks=1"
"$KAFKA_BIN/kafka-producer-perf-test.sh" --topic "$TOPIC" \
  --num-records 10000 --record-size 1024 --throughput -1 \
  --producer-props bootstrap.servers="$BOOTSTRAP" acks=1

echo ">> Scenario 2: same, with snappy compression"
"$KAFKA_BIN/kafka-producer-perf-test.sh" --topic "$TOPIC" \
  --num-records 10000 --record-size 1024 --throughput -1 \
  --producer-props bootstrap.servers="$BOOTSTRAP" acks=1 compression.type=snappy

echo ">> Scenario 3 (gh #14): oversized record (>1MB) must be rejected"
# Apache broker default max.message.bytes=1048588. Skafka mirrors that
# via SKAFKA_MAX_MESSAGE_BYTES (chart-default 1048588). Push one
# 2 MiB record with max.request.size lifted client-side so the
# rejection comes from the broker, not the client's own pre-flight.
# Expected: the perf tool prints a RecordTooLargeException /
# RECORD_LIST_TOO_LARGE error and exits non-zero.
if "$KAFKA_BIN/kafka-producer-perf-test.sh" --topic "$TOPIC" \
     --num-records 1 --record-size $((2 * 1024 * 1024)) --throughput -1 \
     --producer-props bootstrap.servers="$BOOTSTRAP" acks=1 \
        max.request.size=$((4 * 1024 * 1024)) \
        buffer.memory=$((8 * 1024 * 1024)) \
     >"$TMP/oversize.out" 2>&1; then
  echo "FAIL: 2 MiB record was accepted; broker is not enforcing max.message.bytes (#14)" >&2
  tail -5 "$TMP/oversize.out" >&2
  exit 1
fi
if grep -qE "RECORD_LIST_TOO_LARGE|RECORD_TOO_LARGE|too large" "$TMP/oversize.out"; then
  echo "(expected) oversized record rejected by broker"
else
  echo "FAIL: 2 MiB record was rejected, but the error doesn't look like the broker's max.message.bytes gate" >&2
  tail -5 "$TMP/oversize.out" >&2
  exit 1
fi

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --delete --topic "$TOPIC" || true

echo ">> PASS"
