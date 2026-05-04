#!/usr/bin/env bash
# Non-applicable for skafka.
#
# kafka-jmx.sh queries Kafka's JMX MBeans. skafka exposes telemetry over
# OTLP (push) into Prometheus's native OTLP receiver; there is no JMX
# endpoint. Use Prometheus / Grafana for skafka observability.

. "$(dirname "$0")/_common.sh"
skip "JMX query tool; skafka emits OTLP metrics — query Prometheus directly"
