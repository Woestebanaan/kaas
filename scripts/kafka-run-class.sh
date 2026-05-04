#!/usr/bin/env bash
# Non-applicable for skafka.
#
# kafka-run-class.sh is a generic JVM launcher used internally by the
# other kafka-*.sh scripts. It does not talk to a broker and is not a
# parity surface.

. "$(dirname "$0")/_common.sh"
skip "generic JVM class runner; not a broker-facing tool"
