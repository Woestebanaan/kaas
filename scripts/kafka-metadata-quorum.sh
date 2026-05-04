#!/usr/bin/env bash
# Non-applicable for skafka.
#
# kafka-metadata-quorum.sh inspects the KRaft controller quorum. skafka has
# no metadata quorum — cluster controllership runs on K8s Leases. KRaft is
# a stated non-goal in CLAUDE.md.

. "$(dirname "$0")/_common.sh"
skip "inspects KRaft quorum state; skafka uses K8s Leases instead (non-goal)"
