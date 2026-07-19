#!/usr/bin/env bash
# Non-applicable for kaas.
#
# kafka-metadata-shell.sh opens an interactive shell over a KRaft metadata
# log snapshot. kaas has no metadata log — controller state is the
# assignment.json file on the shared PVC plus K8s Leases.

. "$(dirname "$0")/_common.sh"
skip "shells into a KRaft metadata log; kaas has no such log (non-goal)"
