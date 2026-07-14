#!/usr/bin/env bash
# Non-applicable for skafka.
#
# kafka-server-start.sh launches an Apache Kafka broker JVM. skafka is a
# distinct binary started by the StatefulSet (bins/skafka). Use
# `kubectl rollout restart sts/skafka` instead.

. "$(dirname "$0")/_common.sh"
skip "starts the Apache Kafka broker JVM; skafka is a separate process managed by StatefulSet"
