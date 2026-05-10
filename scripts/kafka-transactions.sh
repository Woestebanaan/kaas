#!/usr/bin/env bash
# Test kafka-transactions.sh against skafka.
#
# As of v0.1.98 the producer-side transactional wire surface is
# complete: InitProducerId (gh #22) → AddPartitionsToTxn (gh #23)
# → AddOffsetsToTxn (gh #24) → TxnOffsetCommit (gh #27) → EndTxn
# (gh #25/#26). The admin-protocol surface exercised by
# kafka-transactions.sh is more layered: --list calls
# ListTransactions (API key 66) which iterates every txn coordinator
# — skafka doesn't implement that key yet, so --list returns
# UNSUPPORTED_VERSION. --describe-transactions (API key 65) is in
# the same boat.
#
# This script still exercises what does work — a transactional
# producer round-trip via kafka-verifiable-producer, which uses the
# producer wire RPCs only (no admin):
#
#   1. Drive a transactional verifiable-producer
#   2. Confirm the records are present at the topic
#   3. Try --list and accept UNSUPPORTED_VERSION (admin path is
#      tracked separately)

. "$(dirname "$0")/_common.sh"

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --create --topic "$TOPIC" --partitions 2 --replication-factor 1

TXN_ID="skafka-cli-txn-$$"

echo ">> Scenario 1: kafka-verifiable-producer with --transactional-id"
# verifiable-producer commits the records inside one txn and
# prints '{"name":"shutdown_complete",...}' on success.
out=$("$KAFKA_BIN/kafka-verifiable-producer.sh" \
  --broker-list "$BOOTSTRAP" \
  --topic "$TOPIC" \
  --max-messages 20 \
  --transactional-id "$TXN_ID" \
  --transaction-duration-ms 5000 \
  --producer-property "acks=all" 2>&1) || true
echo "$out" | tail -5
echo "$out" | grep -q '"name":"shutdown_complete"' \
  || { echo "FAIL: verifiable-producer did not reach shutdown_complete (txn commit broken?)" >&2; exit 1; }

echo ">> Scenario 2: confirm records landed (end-offset > 0)"
out=$("$KAFKA_BIN/kafka-get-offsets.sh" --bootstrap-server "$BOOTSTRAP" \
  --topic "$TOPIC" --time -1)
echo "$out"
total=$(echo "$out" | awk -F: '{ s += $3 } END { print s }')
[ "${total:-0}" -gt 0 ] || { echo "FAIL: transactional records didn't land — end-offsets sum=$total" >&2; exit 1; }
echo "transactional records committed: end-offset sum=$total"

echo ">> Scenario 3: --list (admin path, gh follow-up if missing)"
# ListTransactions is API key 66. If the broker advertises it, the
# call lists active txns (often empty after our verifiable run
# already committed). If not, --list will surface a clean error.
if "$KAFKA_BIN/kafka-transactions.sh" --bootstrap-server "$BOOTSTRAP" \
     --list 2>"$TMP/list.err"; then
  echo "ListTransactions implemented — that's a parity win"
else
  err=$(cat "$TMP/list.err")
  case "$err" in
    *UNSUPPORTED_VERSION*|*UnsupportedVersion*|*unsupported*)
      echo "ListTransactions not yet wired (API 66) — known gap"
      ;;
    *)
      echo "FAIL: --list failed with unexpected error:" >&2
      echo "$err" >&2
      exit 1
      ;;
  esac
fi

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --delete --topic "$TOPIC" || true

echo ">> PASS (producer-side txn path works end-to-end via verifiable-producer)"
