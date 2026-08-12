#!/usr/bin/env bash
# infra/compose/try-forbidden-write.sh
#
# M14 - the ACL denial proof, as a runnable harness.
#
# docs/PLAN.md's M14 line is: "payment-api may write payments.requested but not ledger.entries -
# prove it by trying". This script does exactly that, using payment-api's OWN SCRAM credential
# (not admin's, which is a superuser and would succeed at both):
#
#   1. ALLOWED   payment-api -> payments.payment-requested.v1        expected: success
#   2. FORBIDDEN payment-api -> ledger.ledger-entry-recorded.v1      expected: TopicAuthorizationException
#
# Both halves matter. A denial on its own proves nothing (a broken credential denies everything);
# the allowed write is the control that shows authentication succeeded and the denial is the
# AUTHORIZER's decision, not a login failure.
#
# The record written by step 1 is a raw string on an Avro topic, so it is a poison pill for
# psp-connector/analytics/realtime-gateway's deserializers - all three route it to their error
# handler and move on (that is what M8's error handling is for), but do not be surprised by a
# deserialization error in their logs afterwards. Pass --forbidden-only to skip step 1 entirely.
#
# Usage:
#   ./try-forbidden-write.sh                  # both halves
#   ./try-forbidden-write.sh --forbidden-only # denial only, writes nothing anywhere
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# shellcheck disable=SC1091
[ -f .env ] && set -a && source .env && set +a

: "${PAYMENT_API_KAFKA_PASSWORD:?PAYMENT_API_KAFKA_PASSWORD not set - is infra/compose/.env present?}"

ALLOWED_TOPIC="payments.payment-requested.v1"
FORBIDDEN_TOPIC="ledger.ledger-entry-recorded.v1"
BOOTSTRAP="kafka1:9092,kafka2:9092,kafka3:9092"   # the SCRAM client listener, not 9094

# payment-api's credential, passed into the container as an env var rather than written to a file
# on the host - the container renders it to /tmp/payment-api.properties itself.
JAAS="org.apache.kafka.common.security.scram.ScramLoginModule required username=\"payment-api\" password=\"${PAYMENT_API_KAFKA_PASSWORD}\";"

# DO NOT DECIDE ON THE EXIT CODE. kafka-console-producer exits 0 even when the broker rejected
# every single record: the send is asynchronous and the failure is delivered to
# ErrorLoggingCallback, which logs and returns - it never propagates to the process exit status.
# Verified during M14: the forbidden write below prints
#   ERROR ... TopicAuthorizationException: Not authorized to access topics: [...]
# and then exits 0. A harness that trusts `if produce ...; then echo PASS` reports that the
# forbidden write SUCCEEDED, which is a false negative on the one thing this script exists to
# check. Both halves below therefore inspect the OUTPUT.
#
# produce_as_payment_api <topic> -> prints the CLI's combined output
produce_as_payment_api() {
  local topic="$1"
  docker compose exec -T -e JAAS="$JAAS" -e TOPIC="$topic" -e BOOTSTRAP="$BOOTSTRAP" kafka1 bash -c '
    printf "security.protocol=SASL_PLAINTEXT\nsasl.mechanism=SCRAM-SHA-512\nsasl.jaas.config=%s\n" \
      "$JAAS" > /tmp/payment-api.properties
    chmod 600 /tmp/payment-api.properties
    echo "m14-acl-probe" | kafka-console-producer \
      --bootstrap-server "$BOOTSTRAP" \
      --producer.config /tmp/payment-api.properties \
      --producer-property max.block.ms=15000 \
      --topic "$TOPIC"
  ' 2>&1
}

status=0

if [[ "${1:-}" != "--forbidden-only" ]]; then
  echo "=== 1/2  ALLOWED: User:payment-api -> ${ALLOWED_TOPIC} ==============================="
  out="$(produce_as_payment_api "$ALLOWED_TOPIC")"
  if grep -qE "Exception|ERROR" <<< "$out"; then
    echo "  FAIL  the ALLOWED write was refused - this is a broken setup, not a denial proof:" >&2
    echo "$out" >&2
    status=1
  else
    echo "  PASS  write accepted, no error (the ACL matrix grants payment-api Write on this topic)"
  fi
  echo
fi

echo "=== 2/2  FORBIDDEN: User:payment-api -> ${FORBIDDEN_TOPIC} ============================"
out="$(produce_as_payment_api "$FORBIDDEN_TOPIC")"

if grep -q "TopicAuthorizationException" <<< "$out"; then
  echo "  PASS  refused by the authorizer:"
  grep -E "TopicAuthorizationException|Not authorized" <<< "$out" | sed 's/^/        /' | head -5
  echo
  echo "        Note what the error says: 'Not authorized to access topics'. Deny-by-default"
  echo "        means payment-api cannot even fetch METADATA for this topic - to that principal"
  echo "        the topic does not exist, it is not merely unwritable."
elif grep -qE "Exception|ERROR" <<< "$out"; then
  echo "  FAIL  the write failed, but not with an authorization error. That usually means the" >&2
  echo "        credential itself is wrong (SaslAuthenticationException) or the listener/" >&2
  echo "        advertised.listeners config is wrong (a plain timeout). Full output:" >&2
  echo "$out" >&2
  status=1
else
  echo "  FAIL  the forbidden write SUCCEEDED - no error at all. Authorization is not being" >&2
  echo "        enforced. Check KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND=false and" >&2
  echo "        KAFKA_AUTHORIZER_CLASS_NAME on the brokers, and that User:payment-api is not in" >&2
  echo "        KAFKA_SUPER_USERS." >&2
  status=1
fi

echo
if [[ "$status" -eq 0 ]]; then
  echo "ACL denial proof: PASS"
else
  echo "ACL denial proof: FAIL - see above" >&2
fi
exit "$status"
