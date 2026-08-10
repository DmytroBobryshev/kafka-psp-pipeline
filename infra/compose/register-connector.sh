#!/usr/bin/env bash
# infra/compose/register-connector.sh
#
# Idempotent registration of the M6 payment-outbox Debezium connector - same "idempotent,
# script-driven provisioning" convention as create-topics.sh, one layer up the stack (topics vs.
# a Connect connector). Renders infra/compose/connect/payment-outbox-connector.json (the
# ${PAYMENT_API_DB_USER}-style placeholders in that file are for a human reading it; this script
# resolves the real values from .env via jq, not shell substitution, so there's no risk of a
# password containing a JSON-special character breaking the payload) and PUTs the result to
# POST /connectors/<name>/config - which Kafka Connect's REST API defines as an upsert: it
# creates the connector if the name doesn't exist yet, or updates its config in place if it does.
# Re-running this script is therefore always safe.
#
# Usage:
#   ./register-connector.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# shellcheck disable=SC1091
[ -f .env ] && set -a && source .env && set +a

CONNECT_URL="http://localhost:${KAFKA_CONNECT_PORT:-8083}"
CONNECTOR_NAME="payment-outbox-connector"
TEMPLATE="connect/payment-outbox-connector.json"

if ! command -v jq > /dev/null 2>&1; then
  echo "jq is required (JSON templating + status parsing) - install it (e.g. 'brew install jq')." >&2
  exit 1
fi

echo "Waiting for Kafka Connect REST API at ${CONNECT_URL} ..."
ready=0
for _ in $(seq 1 30); do
  if curl -sf "${CONNECT_URL}/connectors" > /dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 2
done
if [[ "$ready" -ne 1 ]]; then
  echo "Kafka Connect REST API not reachable at ${CONNECT_URL} after 60s." >&2
  echo "Check: docker compose ps kafka-connect / docker compose logs kafka-connect" >&2
  exit 1
fi

# Resolve the three ${...}-style placeholders from .env via jq (proper JSON string escaping,
# no shell/sed quoting hazards) and extract just the inner "config" object - that's the exact
# body shape PUT /connectors/<name>/config expects.
CONFIG_JSON=$(jq \
  --arg user "${PAYMENT_API_DB_USER}" \
  --arg pass "${PAYMENT_API_DB_PASSWORD}" \
  --arg db "${PAYMENT_API_DB}" \
  '.config["database.user"] = $user
   | .config["database.password"] = $pass
   | .config["database.dbname"] = $db
   | .config' \
  "$TEMPLATE")

echo "Registering ${CONNECTOR_NAME} ..."
response_file="$(mktemp)"
trap 'rm -f "$response_file"' EXIT

http_code=$(curl -s -o "$response_file" -w '%{http_code}' \
  -X PUT "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/config" \
  -H 'Content-Type: application/json' \
  -d "${CONFIG_JSON}")

if [[ "$http_code" != "200" && "$http_code" != "201" ]]; then
  echo "  FAIL  ${CONNECTOR_NAME}: HTTP ${http_code}" >&2
  jq . "$response_file" >&2 2>/dev/null || cat "$response_file" >&2
  exit 1
fi
echo "  OK    ${CONNECTOR_NAME} registered/updated (HTTP ${http_code} - 201 created, 200 updated)"

echo
echo "Waiting for connector + task 0 to report RUNNING ..."
restarted=0
for _ in $(seq 1 30); do
  status_json=$(curl -s "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status")
  connector_state=$(echo "$status_json" | jq -r '.connector.state // "UNKNOWN"')
  task0_state=$(echo "$status_json" | jq -r '.tasks[0].state // "NO_TASKS"')

  if [[ "$connector_state" == "RUNNING" && "$task0_state" == "RUNNING" ]]; then
    echo "  OK    connector=RUNNING task0=RUNNING"
    echo
    echo "$status_json" | jq .
    exit 0
  fi

  # PUTting a new config does NOT automatically restart an already-FAILED task (a real gotcha -
  # see services/payment-api/README.md's M6 troubleshooting section) - a task that failed under a
  # PREVIOUS config stays FAILED until explicitly restarted, even once the config that caused the
  # failure has been fixed. Try exactly once per invocation before giving up, so re-running this
  # script after fixing the connector JSON is enough on its own - no separate manual restart step.
  if [[ ("$connector_state" == "FAILED" || "$task0_state" == "FAILED") && "$restarted" -eq 0 ]]; then
    echo "  ...   task FAILED under a possibly-stale config; restarting connector+task once and retrying"
    curl -s -X POST "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/restart?includeTasks=true&onlyFailed=false" > /dev/null
    restarted=1
    sleep 3
    continue
  fi

  if [[ "$connector_state" == "FAILED" || "$task0_state" == "FAILED" ]]; then
    echo "  FAIL  connector or task reported FAILED (after one restart attempt):" >&2
    echo "$status_json" | jq . >&2
    exit 1
  fi

  sleep 2
done

echo "Connector/task did not reach RUNNING within 60s - last status:" >&2
echo "$status_json" | jq . >&2
exit 1
