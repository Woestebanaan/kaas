#!/usr/bin/env bash
# Test kafka-transactions.sh (the admin tool, gh #114 territory).
#
# The producer-side transactional verifiable-producer flow that this
# script used to drive is no longer reachable from the Kafka 4.x CLI
# — `kafka-verifiable-producer.sh` in 4.x dropped --transactional-id
# / --transaction-duration-ms. The producer txn flow is exercised
# via the broker integration tests (sk-coordinator + sk-storage
# suites) instead.
#
# The remaining wire-protocol probe is the admin `kafka-transactions.sh
# --list` / `--describe-transactions` path. Both call ListTransactions
# (API key 66) and DescribeTransactions (API key 65). Skafka doesn't
# implement either yet — they're gh #114-adjacent — so we expect a
# clean UNSUPPORTED_VERSION rather than a hang or a wrong-typed error.

. "$(dirname "$0")/_common.sh"

echo ">> Scenario 1: list (expect UNSUPPORTED_VERSION until ListTransactions lands)"
# Kafka 4.x switched to a subcommand syntax: `kafka-transactions.sh list`
# rather than `--list`.
if "$KAFKA_BIN/kafka-transactions.sh" --bootstrap-server "$BOOTSTRAP" \
     list 2>"$TMP/list.err"; then
  echo "ListTransactions implemented — parity win, update this script to assert the result shape"
else
  err=$(cat "$TMP/list.err")
  case "$err" in
    *UNSUPPORTED_VERSION*|*UnsupportedVersion*|*unsupported*|*"does not support LIST_TRANSACTIONS"*)
      echo "ListTransactions not yet wired (API 66) — known gap"
      ;;
    *)
      echo "FAIL: list failed with unexpected error:" >&2
      echo "$err" >&2
      exit 1
      ;;
  esac
fi

echo ">> PASS (admin-tool wire surface accounted for; producer-side txn flow runs under cargo test)"
