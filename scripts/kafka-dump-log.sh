#!/usr/bin/env bash
# Test kafka-dump-log.sh against a kaas segment file.
#
# kafka-dump-log inspects local segment files; it does not talk to a broker.
# Run it inside a broker pod (or on a node with broker /data mounted) and pass
# a segment path via SEGMENT.
#
# Scenarios:
#   1. --print-data-log on a segment dumps records with offsets and CRCs

. "$(dirname "$0")/_common.sh"

SEGMENT="${SEGMENT:-}"

if [ -z "$SEGMENT" ] || [ ! -f "$SEGMENT" ]; then
  skip "set SEGMENT=/data/<topic>/<partition>/<...>.log to a real segment file. Inside the broker pod: kubectl -n kaas exec sts/kaas -- ls /data/"
fi

echo ">> Scenario 1: --print-data-log $SEGMENT"
"$KAFKA_BIN/kafka-dump-log.sh" --print-data-log --files "$SEGMENT" | head -50

echo ">> PASS"
