#!/usr/bin/env bash
# Non-applicable for kaas.
#
# kafka-server-start.sh launches an Apache Kafka broker JVM. kaas is a
# distinct binary started by the StatefulSet (bins/kaas). Use
# `kubectl rollout restart sts/kaas` instead.

. "$(dirname "$0")/_common.sh"
skip "starts the Apache Kafka broker JVM; kaas is a separate process managed by StatefulSet"
