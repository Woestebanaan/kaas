#!/usr/bin/env bash
# Non-applicable for kaas.
#
# kafka-storage.sh formats KRaft metadata directories. kaas uses K8s Leases
# instead of KRaft for cluster metadata; KRaft is a stated non-goal in
# CLAUDE.md.

. "$(dirname "$0")/_common.sh"
skip "formats KRaft metadata storage; kaas does not use KRaft (non-goal)"
