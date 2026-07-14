#!/usr/bin/env bash
# Non-applicable for skafka.
# Same reason as kafka-server-start.sh: skafka is not a Kafka JVM.
# Use `kubectl scale sts/skafka --replicas=0` or pod deletion.

. "$(dirname "$0")/_common.sh"
skip "stops the Apache Kafka broker JVM; not applicable to the skafka binary"
