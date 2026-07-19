#!/usr/bin/env bash
# Non-applicable for kaas.
#
# kafka-leader-election.sh triggers preferred / unclean leader election for
# replicated partitions. kaas uses K8s Leases for partition leadership,
# driven by the controller broker via assignment.json. There is no
# operator-triggered election in the Kafka sense.

. "$(dirname "$0")/_common.sh"
skip "manual leader election for replicated partitions; kaas uses K8s Leases (non-goal)"
