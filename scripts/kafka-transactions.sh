#!/usr/bin/env bash
# Test kafka-transactions.sh against skafka.
#
# BLOCKED: Kafka transactions are not yet implemented (#22-31). This script is
# scaffolded so it can be wired up immediately when the broker work lands.
# Until then it is expected to FAIL — running it surfaces exactly which RPC
# is missing.
#
# Scenarios (once transactions ship):
#   1. --list (initial state, may be empty)
#   2. Drive a transactional producer to populate state
#   3. --describe-producers shows the producer
#   4. --describe-transactions shows the transaction
#   5. --abort hangs / forces an abort

. "$(dirname "$0")/_common.sh"

if [ "${ALLOW_XFAIL:-}" != "1" ]; then
  skip "transactions are a known gap (#22-31). Set ALLOW_XFAIL=1 to run anyway and confirm the broker rejects InitProducerId."
fi

echo ">> Scenario 1: --list (expected to fail until transactions ship)"
"$KAFKA_BIN/kafka-transactions.sh" --bootstrap-server "$BOOTSTRAP" --list

echo ">> If the above succeeded, transactions are implemented — close gaps #22-31."
