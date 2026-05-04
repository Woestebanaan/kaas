#!/usr/bin/env bash
# Out of 3.7 scope — the unified `kafka-groups.sh` tool is part of the
# share-groups / next-gen rebalance work (KIP-848 + KIP-932), Kafka 4.0+.
# For classic consumer groups in 3.7, use kafka-consumer-groups.sh.

. "$(dirname "$0")/_common.sh"
skip "unified groups tool (Kafka 4.0+); use kafka-consumer-groups.sh for 3.7 parity"
