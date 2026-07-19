#!/usr/bin/env bash
# Non-applicable for kaas.
#
# Manual partition reassignment via this tool relies on the Kafka admin
# protocol's AlterPartitionReassignments. kaas's controller assigns
# partitions automatically via the rendezvous-hash balancer; there is no
# operator-driven reassignment surface today. Could be revisited if needed.

. "$(dirname "$0")/_common.sh"
skip "manual partition reassignment; kaas's controller balancer drives this automatically"
