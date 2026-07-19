#!/usr/bin/env bash
# Non-applicable for kaas.
#
# kafka-jmx.sh queries Kafka's JMX MBeans. kaas exposes telemetry over
# OTLP (push) into Prometheus's native OTLP receiver; there is no JMX
# endpoint. Use Prometheus / Grafana for kaas observability.

. "$(dirname "$0")/_common.sh"
skip "JMX query tool; kaas emits OTLP metrics — query Prometheus directly"
