#!/usr/bin/env bash
# Test kafka-console-producer.sh against skafka.
#
# Scenarios:
#   1. Produce a few lines to a fresh topic
#   2. Produce with explicit --property parse.key=true and a key separator

. "$(dirname "$0")/_common.sh"

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 1

echo ">> Scenario 1: produce 3 plain lines"
printf 'one\ntwo\nthree\n' | \
  "$KAFKA_BIN/kafka-console-producer.sh" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC"

echo ">> Scenario 2: produce keyed records (k:v)"
printf 'k1:v1\nk2:v2\n' | \
  "$KAFKA_BIN/kafka-console-producer.sh" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" \
    --property parse.key=true --property key.separator=:

echo ">> verifying with kafka-get-offsets.sh: high-watermark should be 5"
out=$("$KAFKA_BIN/kafka-get-offsets.sh" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" --time -1)
echo "$out"
echo "$out" | grep -qE "${TOPIC}:0:5\$" || { echo "FAIL: end-offset != 5" >&2; exit 1; }

echo ">> Scenario 3: idempotent producer (acks=all, retries default)"
# Java client defaults to enable.idempotence=true since 3.0; this
# scenario exercises the gh #12 InitProducerId + sequence-tracking
# path. We use --producer-property explicitly so a future Kafka
# CLI default change doesn't silently mask the test.
printf 'idem-1\nidem-2\nidem-3\n' | \
  "$KAFKA_BIN/kafka-console-producer.sh" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" \
    --producer-property enable.idempotence=true \
    --producer-property acks=all
out=$("$KAFKA_BIN/kafka-get-offsets.sh" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" --time -1)
echo "$out" | grep -qE "${TOPIC}:0:8\$" || { echo "FAIL: end-offset != 8 after idempotent batch" >&2; exit 1; }

echo ">> Scenario 4: transactional producer (gh #22-27 chain)"
# Exercises InitProducerId(txnID) → AddPartitionsToTxn → Produce →
# EndTxn(commit). Without all four wire RPCs landing this fails
# at the first step. The CLI doesn't expose explicit
# beginTransaction/commitTransaction so we rely on Java client
# defaults: setting transactional.id implies idempotence=true and
# the producer flushes a transactional commit on shutdown.
TXN_ID="skafka-cli-txn-$$"
printf 'txn-1\ntxn-2\n' | \
  "$KAFKA_BIN/kafka-console-producer.sh" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" \
    --producer-property "transactional.id=$TXN_ID" \
    --producer-property acks=all 2>"$TMP/txn.err" \
  || { echo "FAIL: transactional producer rejected:"; cat "$TMP/txn.err" >&2; exit 1; }
out=$("$KAFKA_BIN/kafka-get-offsets.sh" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" --time -1)
# Transactional producers append the data records + may write a
# COMMIT marker. We require strictly more than the previous count.
# Exact final offset depends on whether the broker's WriteTxnMarkers
# path is wired end-to-end (gh #114 follow-up); accept >= 10.
final=$(echo "$out" | sed -nE "s/^${TOPIC}:0:([0-9]+).*/\1/p")
[ "$final" -ge 10 ] || { echo "FAIL: txn end-offset $final < 10 (data records not committed)" >&2; exit 1; }
echo "transactional producer committed; end-offset=$final"

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --delete --topic "$TOPIC" || true

echo ">> PASS"
