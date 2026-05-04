#!/usr/bin/env bash
# Non-applicable for skafka.
#
# Replication / ISR is a stated non-goal — skafka is single-writer per
# partition. There is nothing to "verify replicas" against.

. "$(dirname "$0")/_common.sh"
skip "verifies follower-replica consistency; skafka has no replicas (non-goal)"
