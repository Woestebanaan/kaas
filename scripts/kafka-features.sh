#!/usr/bin/env bash
# Non-applicable for kaas (today).
#
# kafka-features.sh manages KRaft feature flags / metadata.version upgrades.
# kaas has no equivalent feature-flag plane (KRaft non-goal). If an
# external client probes ApiVersions for tagged feature info, that's
# covered by kafka-broker-api-versions.sh instead.

. "$(dirname "$0")/_common.sh"
skip "manages KRaft feature levels; not applicable (non-goal)"
