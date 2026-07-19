#!/usr/bin/env bash
# Non-applicable for kaas.
# Same reason as kafka-server-start.sh: kaas is not a Kafka JVM.
# Use `kubectl scale sts/kaas --replicas=0` or pod deletion.

. "$(dirname "$0")/_common.sh"
skip "stops the Apache Kafka broker JVM; not applicable to the kaas binary"
