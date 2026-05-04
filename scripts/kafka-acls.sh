#!/usr/bin/env bash
# Test kafka-acls.sh against skafka.
#
# Requires auth + an admin principal. Override:
#   ADMIN_PROPS=/path/to/admin-client.properties (e.g. SASL/SCRAM admin user)
#
# Scenarios:
#   1. --list (initial state)
#   2. --add an ACL: User:alice can READ topic test-acl
#   3. --list shows the new ACL
#   4. --remove and verify it's gone

. "$(dirname "$0")/_common.sh"

ADMIN_PROPS="${ADMIN_PROPS:-}"
EXTRA=()
[ -n "$ADMIN_PROPS" ] && EXTRA+=(--command-config "$ADMIN_PROPS")

if [ ${#EXTRA[@]} -eq 0 ]; then
  skip "ADMIN_PROPS not set — kafka-acls.sh requires an authenticated admin client. Set ADMIN_PROPS=/path/to/admin.properties."
fi

ACL_TOPIC="acl-test-$$"
PRINCIPAL="User:alice"

echo ">> Scenario 1: --list (initial)"
"$KAFKA_BIN/kafka-acls.sh" --bootstrap-server "$BOOTSTRAP" "${EXTRA[@]}" --list

echo ">> Scenario 2: --add allow $PRINCIPAL READ on topic $ACL_TOPIC"
"$KAFKA_BIN/kafka-acls.sh" --bootstrap-server "$BOOTSTRAP" "${EXTRA[@]}" \
  --add --allow-principal "$PRINCIPAL" --operation READ --topic "$ACL_TOPIC"

echo ">> Scenario 3: --list shows it"
"$KAFKA_BIN/kafka-acls.sh" --bootstrap-server "$BOOTSTRAP" "${EXTRA[@]}" \
  --list --topic "$ACL_TOPIC" | grep -q "$PRINCIPAL"

echo ">> Scenario 4: --remove and verify"
"$KAFKA_BIN/kafka-acls.sh" --bootstrap-server "$BOOTSTRAP" "${EXTRA[@]}" \
  --remove --allow-principal "$PRINCIPAL" --operation READ --topic "$ACL_TOPIC" --force
out=$("$KAFKA_BIN/kafka-acls.sh" --bootstrap-server "$BOOTSTRAP" "${EXTRA[@]}" --list --topic "$ACL_TOPIC")
echo "$out" | grep -q "$PRINCIPAL" && { echo "FAIL: ACL still present" >&2; exit 1; }

echo ">> PASS"
