#!/usr/bin/env bash
# Shared helpers for the kafka-*.sh test scripts. Source this from each script.
#
#   . "$(dirname "$0")/_common.sh"
#
# Provides:
#   $BOOTSTRAP     bootstrap server (override with env var, default in-cluster Service DNS)
#   $KAFKA_BIN     path to Apache Kafka shell tools (defaults to /opt/kafka/bin)
#   $TOPIC         per-run unique test topic name
#   $TMP           per-run scratch dir, auto-cleaned on exit
#   skip "<reason>"   print reason and exit 77 (autoconf "skipped" exit code)
#   need <bin>     skip if a required tool is not on PATH

set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kaas.kaas.svc.cluster.local:9092}"
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"
TOPIC="${TOPIC:-kaas-test-$$-$(date +%s)}"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

skip() {
  echo ">> SKIP: $*" >&2
  exit 77
}

need() {
  command -v "$1" >/dev/null 2>&1 || skip "missing required tool: $1"
}

if [ ! -x "$KAFKA_BIN/kafka-topics.sh" ]; then
  skip "Kafka CLI not found at $KAFKA_BIN; set KAFKA_BIN=/path/to/kafka/bin"
fi

echo ">> bootstrap: $BOOTSTRAP"
echo ">> kafka bin: $KAFKA_BIN"
