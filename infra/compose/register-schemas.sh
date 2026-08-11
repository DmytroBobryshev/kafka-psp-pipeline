#!/usr/bin/env bash
# infra/compose/register-schemas.sh
#
# M9 Phase 1: sets Schema Registry's subject-level compatibility mode BEFORE any producer ever
# registers a schema against it, so the very first registration is already governed by the
# chosen policy rather than whatever the registry's out-of-the-box default happens to be -
# "set the subject compatibility mode explicitly rather than relying on the default."
#
# payments.payment-requested.v1-value -> BACKWARD. This is not a fresh decision made here: ADR-
# 0001's "Versioning rule" already commits the whole system to BACKWARD compatibility ("Schema
# Registry compatibility is BACKWARD ... enforced in M9") - this script is that enforcement for
# the one subject M9 Phase 1 actually created. BACKWARD means a new schema version must be able
# to read data written with the PREVIOUS version, which is exactly what a rolling deploy needs:
# psp-connector's consumer can upgrade to a new schema before every payment-api instance has
# (or vice versa, for additive changes), because whichever version is reading was built to
# tolerate the other's already-written data. It is also the compatibility mode ADR-0001's
# topic-versioning rule assumes: additive/compatible changes evolve the subject in place (this
# is what BACKWARD allows); anything BACKWARD would reject (a field remove/rename/retype) is by
# definition the "breaking change" ADR-0001 says gets a new v2 TOPIC instead, not a schema fight.
#
# Idempotent - PUT /config/<subject> is an upsert, same "script-driven provisioning" convention
# as create-topics.sh / register-connector.sh. Can be run before OR after the subject has any
# registered schema versions - Schema Registry stores subject-level config independently of
# whether a version exists yet.
#
# Usage:
#   ./register-schemas.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# shellcheck disable=SC1091
[ -f .env ] && set -a && source .env && set +a

SR_URL="http://localhost:${SCHEMA_REGISTRY_PORT:-8081}"

if ! command -v jq > /dev/null 2>&1; then
  echo "jq is required - install it (e.g. 'brew install jq')." >&2
  exit 1
fi

echo "Waiting for Schema Registry at ${SR_URL} ..."
ready=0
for _ in $(seq 1 30); do
  if curl -sf "${SR_URL}/subjects" > /dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 2
done
if [[ "$ready" -ne 1 ]]; then
  echo "Schema Registry not reachable at ${SR_URL} after 60s." >&2
  exit 1
fi

# subject | compatibility
#
# M9 Phase 2 additions: payments.payment-status-changed.v1-value (psp-connector -> ledger +
# webhook-notifier's planner - the multi-consumer topic cut in place, drain-to-latest strategy,
# see services/psp-connector/README.md's M9 Phase 2 section), ledger.ledger-entry-recorded.v1-value
# (ledger, no live consumer yet - accept-and-document-skip strategy, see
# services/ledger/README.md's M9 Phase 2 section), and the four subjects for
# webhooks.webhook-delivery-requested.v2's base topic plus its three retry tiers (the ADR-0001
# v2-topic route, demonstrated here specifically because this chain is entirely internal to one
# service and so costs zero cross-team dual-write complexity - see
# services/webhook-notifier/README.md's M9 Phase 2 section). webhooks.webhook-delivery-requested.
# v2.dlq intentionally has NO subject here: it stays on the byte-tolerant JsonSerializer, never
# registers a schema.
SUBJECTS=(
  "payments.payment-requested.v1-value|BACKWARD"
  "payments.payment-status-changed.v1-value|BACKWARD"
  "ledger.ledger-entry-recorded.v1-value|BACKWARD"
  "webhooks.webhook-delivery-requested.v2-value|BACKWARD"
  "webhooks.webhook-delivery-requested.v2.retry.5s-value|BACKWARD"
  "webhooks.webhook-delivery-requested.v2.retry.1m-value|BACKWARD"
  "webhooks.webhook-delivery-requested.v2.retry.15m-value|BACKWARD"
)

echo "Setting compatibility for ${#SUBJECTS[@]} subject(s) ..."
echo

fail=0
response_file="$(mktemp)"
trap 'rm -f "$response_file"' EXIT

for row in "${SUBJECTS[@]}"; do
  IFS='|' read -r subject compat <<< "$row"

  http_code=$(curl -s -o "$response_file" -w '%{http_code}' \
    -X PUT "${SR_URL}/config/${subject}" \
    -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
    -d "{\"compatibility\":\"${compat}\"}")

  if [[ "$http_code" == "200" ]]; then
    printf '  OK    %-45s compatibility=%s\n' "$subject" "$compat"
  else
    printf '  FAIL  %-45s HTTP %s\n' "$subject" "$http_code"
    jq . "$response_file" >&2 2>/dev/null || cat "$response_file" >&2
    fail=1
  fi
done

echo
if [[ "$fail" -eq 0 ]]; then
  echo "Done. Verify with:"
  echo "  curl -s ${SR_URL}/config/payments.payment-requested.v1-value | jq ."
else
  echo "One or more subjects failed - see FAIL lines above." >&2
  exit 1
fi
