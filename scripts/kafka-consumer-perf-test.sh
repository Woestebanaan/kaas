#!/usr/bin/env bash
# Test kafka-consumer-perf-test.sh against kaas.
#
# Scenarios:
#   1. Seed 10k records, then run consumer-perf-test for that count
#   2. Assert every partition received at least one new record (catches
#      the kperf-0 split-brain regression, gh #75)

# Override TOPIC default to a pre-provisioned KafkaTopic CR so we don't
# trigger CreateTopics v7 (broken encoder in kaas — BufferUnderflowException
# on the client, gh #73). The CR lives in
# k3s-cluster/apps/kaas/kafka-topics/kperf.yaml.
TOPIC="${TOPIC:-kperf}"

. "$(dirname "$0")/_common.sh"

# Verify the pre-provisioned topic exists (Metadata RPC, which works). We
# cannot use kafka-topics.sh --create here because the modern Java admin
# client calls CreateTopics unconditionally and kaas encodes its v7
# response incorrectly (see comment above). The topic is managed by the
# KafkaTopic CR, so just assert visibility.
"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list | grep -qx "$TOPIC" \
  || { echo "FAIL: topic $TOPIC missing — apply k3s-cluster/apps/kaas/kafka-topics/kperf.yaml" >&2; exit 1; }

echo ">> capturing pre-produce per-partition offsets"
before=$("$KAFKA_BIN/kafka-get-offsets.sh" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" --time -1)

echo ">> seeding 10k records"
"$KAFKA_BIN/kafka-producer-perf-test.sh" --topic "$TOPIC" \
  --num-records 10000 --record-size 1024 --throughput -1 \
  --producer-props bootstrap.servers="$BOOTSTRAP" acks=1 >/dev/null

echo ">> verifying every partition received messages (Scenario 2)"
after=$("$KAFKA_BIN/kafka-get-offsets.sh" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" --time -1)
empty_partitions=()
while IFS=: read -r _ partition offset_after; do
  [ -z "$partition" ] && continue
  offset_before=$(awk -F: -v p="$partition" '$2==p {print $3}' <<<"$before")
  : "${offset_before:=0}"
  if [ "$offset_after" -le "$offset_before" ]; then
    empty_partitions+=("$partition (was $offset_before, now $offset_after)")
  fi
done <<<"$after"

if [ ${#empty_partitions[@]} -gt 0 ]; then
  echo "FAIL: partitions received no new records — likely the kperf-0 split-brain (gh #75):" >&2
  printf '  %s\n' "${empty_partitions[@]}" >&2
  exit 1
fi

echo ">> Scenario 1: consume 10k records"
"$KAFKA_BIN/kafka-consumer-perf-test.sh" \
  --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" \
  --messages 10000 --group "perf-cg-$$" --timeout 60000

# Topic is CR-managed; do not delete here.

echo ">> PASS"
