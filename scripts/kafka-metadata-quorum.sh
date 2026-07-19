#!/usr/bin/env bash
# Non-applicable for kaas.
#
# kafka-metadata-quorum.sh inspects the KRaft controller quorum. kaas has
# no metadata quorum — cluster controllership runs on K8s Leases. KRaft is
# a stated non-goal in CLAUDE.md.

. "$(dirname "$0")/_common.sh"
skip "inspects KRaft quorum state; kaas uses K8s Leases instead (non-goal)"
