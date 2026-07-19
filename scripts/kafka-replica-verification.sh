#!/usr/bin/env bash
# Non-applicable for kaas.
#
# Replication / ISR is a stated non-goal — kaas is single-writer per
# partition. There is nothing to "verify replicas" against.

. "$(dirname "$0")/_common.sh"
skip "verifies follower-replica consistency; kaas has no replicas (non-goal)"
