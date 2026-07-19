#!/usr/bin/env bash
# Non-applicable for kaas.
#
# Delegation tokens (KIP-48) require a SASL/SCRAM-DELEGATION-TOKEN
# mechanism and a token issuance/expiration plane. kaas's auth is
# SCRAM-SHA-256/512, mTLS, and a K8s ServiceAccount JWT exchange — no
# delegation-token issuance. Could be revisited later.

. "$(dirname "$0")/_common.sh"
skip "manages delegation tokens; kaas's auth has no delegation-token plane"
