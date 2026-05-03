#!/usr/bin/env bash
# Round-trip smoke test for skafka: produce N unique messages, consume them back.
# Uses kafka-console-{producer,consumer}.sh from /opt/kafka/bin. Intended to be
# run from inside the cluster (the in-cluster Service DNS is the default).
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-skafka.skafka.svc.cluster.local:9092}"
TOPIC="${TOPIC:-smoke}"
TIMEOUT_MS="${TIMEOUT_MS:-15000}"
COUNT="${COUNT:-1000}"
# Per-run token so we never pass on stale messages from an earlier run.
TOKEN="${TOKEN:-$(date -u +%Y%m%dT%H%M%SZ)-$$-${RANDOM}}"
MESSAGE="${MESSAGE:-Hello world ${TOKEN}}"

TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT

log()  { printf '>> %s\n' "$*"; }
fail() { printf '!! %s\n' "$*" >&2; exit 1; }

log "bootstrap: ${BOOTSTRAP}"
log "topic:     ${TOPIC}"
log "token:     ${TOKEN}"
log "count:     ${COUNT}"

# --- 0. preflight ----------------------------------------------------------
# Exercises ApiVersions + Metadata. Surfaces wire-protocol problems before we
# blame produce/consume for them.
log "preflight: kafka-broker-api-versions"
if ! kafka-broker-api-versions.sh \
        --bootstrap-server "${BOOTSTRAP}" \
        >"${TMP}/api-versions.out" 2>"${TMP}/api-versions.err"; then
    cat "${TMP}/api-versions.err" >&2
    fail "preflight failed: broker did not respond to ApiVersions"
fi

# --- 1. produce ------------------------------------------------------------
# enable.idempotence=false avoids InitProducerId (API key 22), which skafka
# does not implement yet.
# Every line carries the run TOKEN and a zero-padded sequence number so the
# verify step can grep for exact-match presence of all COUNT messages.
awk -v msg="${MESSAGE}" -v n="${COUNT}" 'BEGIN { for (i = 1; i <= n; i++) printf "%s seq=%06d\n", msg, i }' \
    >"${TMP}/messages.in"
log "producing: ${COUNT} messages (prefix ${MESSAGE@Q})"
if ! kafka-console-producer.sh \
        --bootstrap-server "${BOOTSTRAP}" \
        --topic "${TOPIC}" \
        --producer-property enable.idempotence=false \
        --producer-property acks=1 \
        <"${TMP}/messages.in" \
        >"${TMP}/produce.out" 2>"${TMP}/produce.err"; then
    cat "${TMP}/produce.err" >&2
    fail "producer failed"
fi

# --- 2. consume ------------------------------------------------------------
# kafka-console-consumer exits non-zero on --timeout-ms even when it received
# messages, so we ignore its exit code and check output instead.
log "consuming from beginning (timeout ${TIMEOUT_MS}ms)"
kafka-console-consumer.sh \
    --bootstrap-server "${BOOTSTRAP}" \
    --topic "${TOPIC}" \
    --from-beginning \
    --timeout-ms "${TIMEOUT_MS}" \
    >"${TMP}/consume.out" 2>"${TMP}/consume.err" || true

# --- 3. describe-configs ---------------------------------------------------
# Exercises DescribeConfigs (API key 32). kafbat-ui and kafka-configs.sh both
# depend on it; if the broker doesn't support it, kafka-configs.sh prints
# "The node does not support DESCRIBE_CONFIGS" and exits non-zero.
log "describe-configs: kafka-configs.sh --describe --topic ${TOPIC}"
if ! kafka-configs.sh \
        --bootstrap-server "${BOOTSTRAP}" \
        --entity-type topics --entity-name "${TOPIC}" --describe \
        >"${TMP}/configs.out" 2>"${TMP}/configs.err"; then
    cat "${TMP}/configs.err" >&2
    fail "describe-configs failed"
fi

# --- 3b. describe-log-dirs --------------------------------------------------
# Exercises DescribeLogDirs (API key 35). kafbat-ui shows partition disk
# usage from this; without it, sizes render as "0 Bytes / N/A segment(s)".
log "describe-log-dirs: kafka-log-dirs.sh --describe"
if ! kafka-log-dirs.sh \
        --bootstrap-server "${BOOTSTRAP}" \
        --topic-list "${TOPIC}" \
        --describe \
        >"${TMP}/logdirs.out" 2>"${TMP}/logdirs.err"; then
    cat "${TMP}/logdirs.err" >&2
    fail "describe-log-dirs failed"
fi

# --- 3c. list-topics --------------------------------------------------------
# Exercises Metadata-with-empty-topic-list (API key 3). kafka-topics.sh --list
# is what every admin UI and CLI uses to populate the topic picker; if the
# broker can't enumerate topics the entire cluster reads as empty.
log "list-topics: kafka-topics.sh --list"
if ! kafka-topics.sh \
        --bootstrap-server "${BOOTSTRAP}" \
        --list \
        >"${TMP}/topics.out" 2>"${TMP}/topics.err"; then
    cat "${TMP}/topics.err" >&2
    fail "list-topics failed"
fi
if ! grep -Fxq -- "${TOPIC}" "${TMP}/topics.out"; then
    {
        echo "expected topic ${TOPIC@Q} in --list output"
        echo "--- topics stdout ---"
        cat "${TMP}/topics.out" || true
    } >&2
    fail "topic ${TOPIC@Q} not present in --list output"
fi

# --- 4. verify -------------------------------------------------------------
# Every produced line must appear in the consumer output. Using -Fxc with the
# produced lines as patterns counts how many consume.out lines exact-match one
# of our messages — equals COUNT iff all were delivered (TOKEN keeps us from
# colliding with leftovers from earlier runs).
matched=$(grep -Fxc -f "${TMP}/messages.in" "${TMP}/consume.out" 2>/dev/null || true)
matched=${matched:-0}
if [ "${matched}" -lt "${COUNT}" ]; then
    {
        echo "expected ${COUNT} messages, only matched ${matched} in consumer output"
        echo "--- first 5 missing ---"
        grep -Fxv -f "${TMP}/consume.out" "${TMP}/messages.in" | head -n 5 || true
        echo "--- consumer stdout (last 50 lines) ---"
        tail -n 50 "${TMP}/consume.out" || true
        echo "--- consumer stderr (last 50 lines) ---"
        tail -n 50 "${TMP}/consume.err" || true
    } >&2
    fail "not all produced messages found in consumer output (${matched}/${COUNT})"
fi

log "PASS: round-trip successful"