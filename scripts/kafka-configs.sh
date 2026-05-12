#!/usr/bin/env bash
# Test kafka-configs.sh against skafka.
#
# Scenarios:
#   1. --describe broker config (read path)
#   2. --describe topic config (read path)
#   3. --alter topic config — currently a GAP (issue #9), expected to fail.
#      Marked as expected-fail until IncrementalAlterConfigs is implemented.
#   4. --describe --all (broad DescribeConfigs surface).
#   5. --describe specific broker by id.
#   6-9. Client quota CRUD via AlterClientQuotas / DescribeClientQuotas
#      (api keys 49 / 48). Currently a GAP (issue #122), expected to fail
#      until the quota engine lands.

. "$(dirname "$0")/_common.sh"

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 1

echo ">> Scenario 1: --describe broker config"
"$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
  --entity-type brokers --entity-default --describe

echo ">> Scenario 2: --describe topic config for '$TOPIC'"
"$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
  --entity-type topics --entity-name "$TOPIC" --describe

echo ">> Scenario 3 (XFAIL, gap #9): --alter topic config retention.ms"
if "$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
     --entity-type topics --entity-name "$TOPIC" \
     --alter --add-config retention.ms=60000 2>&1; then
  echo "UNEXPECTED PASS — IncrementalAlterConfigs may now be implemented; close gap #9."
else
  echo "(expected) alter rejected — broker work needed (#9)"
fi

echo ">> Scenario 4: --describe with --all (every config key, not just overridden)"
# Exercises the broader DescribeConfigs surface — every key
# returned must have its source (DEFAULT_CONFIG, TOPIC_CONFIG, etc).
# Pre-#93 only overridden keys were returned; the broker config
# should now expose its full key set.
out=$("$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
  --entity-type topics --entity-name "$TOPIC" --describe --all 2>&1)
echo "$out" | head -20
echo "$out" | grep -q 'cleanup.policy' || { echo "FAIL: cleanup.policy not in --describe --all output" >&2; exit 1; }

echo ">> Scenario 5: --describe specific broker config"
# Per-broker (id=0) describe vs default. Required for tools like
# kafbat-ui that query individual brokers.
"$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
  --entity-type brokers --entity-name 0 --describe 2>&1 | head -5

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --delete --topic "$TOPIC" || true

# ---------------------------------------------------------------------------
# Client quota scenarios (issue #122 — accept-all-4-keys, enforce-3).
# Every quota path goes via AlterClientQuotas (api_key=49) and
# DescribeClientQuotas (api_key=48). The tool also drives the broker's
# Produce/Fetch throttling — see scenario 9 for the live-throttle probe.
# ---------------------------------------------------------------------------

QUOTA_USER="skafka-quota-test-$$"

echo ">> Scenario 6 (XFAIL, gap #122): --alter user quota (producer_byte_rate)"
# 1 MiB/s producer quota on a synthetic user. Once #122 lands, the broker
# must (a) accept the alter, (b) persist it (file or in-memory), (c) return
# it on describe.
if "$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
     --entity-type users --entity-name "$QUOTA_USER" \
     --alter --add-config 'producer_byte_rate=1048576' 2>&1; then
  echo "UNEXPECTED PASS — quota engine may be live; verify scenario 7 + 9 pass too, then close #122."
else
  echo "(expected) alter rejected — quota engine not implemented yet (#122)"
fi

echo ">> Scenario 7 (XFAIL, gap #122): --describe user quota round-trip"
# Even if scenario 6 returned an error, the tool will issue
# DescribeClientQuotas to render the result. Once #122 is in, this should
# echo back what we just set.
out=$("$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
  --entity-type users --entity-name "$QUOTA_USER" --describe 2>&1 || true)
echo "$out" | head -5
if echo "$out" | grep -q 'producer_byte_rate=1048576'; then
  echo "UNEXPECTED PASS — describe returned the alter we just made; #122 ready to close."
else
  echo "(expected) describe didn't round-trip producer_byte_rate (#122)"
fi

echo ">> Scenario 8 (XFAIL, gap #122): --alter all 4 Apache quota keys"
# Wire-protocol compatibility check: skafka must accept all 4 Apache keys
# (including controller_mutation_rate, which it stores but doesn't enforce —
# accept-but-no-op for round-trip compat with kafka-configs.sh that round-
# trips a full quota config). Tools that re-issue every field on a
# single-field edit shouldn't break.
if "$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
     --entity-type users --entity-name "$QUOTA_USER" \
     --alter --add-config 'producer_byte_rate=1048576,consumer_byte_rate=2097152,request_percentage=50,controller_mutation_rate=10' 2>&1; then
  echo "UNEXPECTED PASS — 4-key alter accepted; verify --describe round-trip on all four."
else
  echo "(expected) 4-key alter rejected (#122)"
fi

echo ">> Scenario 9 (XFAIL, gap #122): --describe with --entity-default (user-level default)"
# Default-entity precedence (rule 6 in issue #122's 8-level hierarchy):
# 'users/<default>' applies to every authenticated principal that doesn't
# have a more specific override.
out=$("$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
  --entity-type users --entity-default --describe 2>&1 || true)
echo "$out" | head -5
echo "(observed default-user quota config above; should round-trip an alter)"

echo ">> Scenario 10 (XFAIL, gap #122): live throttle probe"
# End-to-end check that the throttle is server-enforced (KIP-219 ordering:
# server sends response with throttle_time_ms then mutes). We pin the user
# at 1 MiB/s producer quota and try to push 10 MiB in 1s. Expected outcome
# (post-#122): the producer-perf tool reports an effective throughput
# bounded near the quota and a non-zero average request latency reflecting
# the throttle. Pre-#122 the test passes the broker uncapped, so this is
# observable as 'the throttle never fires'.
PROBE_TOPIC="quota-probe-$$"
"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --create --topic "$PROBE_TOPIC" --partitions 1 --replication-factor 1 \
  >/dev/null 2>&1 || true
probe_out=$("$KAFKA_BIN/kafka-producer-perf-test.sh" \
  --topic "$PROBE_TOPIC" \
  --num-records 10000 --record-size 1024 --throughput -1 \
  --producer-props bootstrap.servers="$BOOTSTRAP" client.id="$QUOTA_USER" 2>&1 || true)
echo "$probe_out" | tail -3
# Heuristic: if the perf tool reports MB/sec well above 1.0 the throttle
# isn't firing. Once #122 ships, expect the rate to plateau near 1 MB/s.
if echo "$probe_out" | grep -Eq '(\([0-9]+\.[0-9]+) MB/sec\)' && \
   echo "$probe_out" | awk '/records\/sec/ { for(i=1;i<=NF;i++) if($i=="MB/sec)") { gsub(/\(/,"",$(i-1)); if($(i-1)+0 < 1.5) exit 0; else exit 1; } } END { exit 1 }'; then
  echo "UNEXPECTED PASS — observed throttled throughput, #122 may be live."
else
  echo "(expected) producer ran uncapped at >1 MB/s — quota not enforced (#122)"
fi

"$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
  --entity-type users --entity-name "$QUOTA_USER" \
  --alter --delete-config 'producer_byte_rate,consumer_byte_rate,request_percentage,controller_mutation_rate' \
  >/dev/null 2>&1 || true
"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --delete --topic "$PROBE_TOPIC" >/dev/null 2>&1 || true

echo ">> PASS (read paths + quota XFAILs as expected pending #122)"
