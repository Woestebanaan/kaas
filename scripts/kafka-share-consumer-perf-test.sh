#!/usr/bin/env bash
# Out of 3.7 scope — share groups (KIP-932) ship in Kafka 4.0+.
# kaas's parity target is 3.7; share-group APIs are not implemented.

. "$(dirname "$0")/_common.sh"
skip "KIP-932 share groups (Kafka 4.0+); out of 3.7 parity scope"
