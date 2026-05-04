#!/usr/bin/env bash
# Non-applicable for skafka.
#
# kafka-storage.sh formats KRaft metadata directories. skafka uses K8s Leases
# instead of KRaft for cluster metadata; KRaft is a stated non-goal in
# CLAUDE.md.

. "$(dirname "$0")/_common.sh"
skip "formats KRaft metadata storage; skafka does not use KRaft (non-goal)"
