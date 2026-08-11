#!/usr/bin/env bash
# infra/compose/create-topics.sh
#
# Idempotent topic creation for the M2 infrastructure baseline. Every topic here comes straight
# from docs/diagrams/topic-map.md (names per ADR-0001, keys/partitions per ADR-0003). Re-running
# this script is safe: `kafka-topics --create --if-not-exists` skips topics that already exist
# and never touches partition count or configs on an existing topic (partition counts only go
# up per the topic-map's "Change rules" - this script intentionally does not auto-alter a live
# topic's partition count, since that's a deliberate migration, not a re-run).
#
# Deliberately NOT created here (see docs/diagrams/topic-map.md):
#   - `analytics-streams.v1-*` topics: created by Kafka Streams itself in M10. Hand-creating
#     them would use the wrong internal naming/config and Streams would refuse to adopt them.
#   - `connect.configs` / `connect.offsets` / `connect.status`: created by Kafka Connect workers
#     when Connect is deployed in M6/M13. Not needed until then.
#
# Usage:
#   ./create-topics.sh              # create topics via `docker compose exec` (default)
#   BOOTSTRAP=localhost:29092 ./create-topics.sh --host   # create via a host-installed kafka-topics
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# shellcheck disable=SC1091
[ -f .env ] && set -a && source .env && set +a

MODE="compose"
if [[ "${1:-}" == "--host" ]]; then
  MODE="host"
fi

INTERNAL_BOOTSTRAP="kafka1:9092,kafka2:9092,kafka3:9092"
HOST_BOOTSTRAP="localhost:${KAFKA1_EXTERNAL_PORT:-29092},localhost:${KAFKA2_EXTERNAL_PORT:-29093},localhost:${KAFKA3_EXTERNAL_PORT:-29094}"

run_kafka_topics() {
  if [[ "$MODE" == "host" ]]; then
    kafka-topics --bootstrap-server "$HOST_BOOTSTRAP" "$@"
  else
    docker compose exec -T kafka1 kafka-topics --bootstrap-server "$INTERNAL_BOOTSTRAP" "$@"
  fi
}

# name | partitions | replication.factor | cleanup.policy | retention.ms (-1 = infinite)
TOPICS=(
  "payments.payment-requested.v1|12|3|delete|604800000"
  "payments.payment-status-changed.v1|12|3|delete|604800000"
  "refunds.refund-requested.v1|6|3|delete|604800000"
  "refunds.funds-reserved.v1|6|3|delete|604800000"
  "refunds.refund-completed.v1|6|3|delete|604800000"
  "refunds.refund-failed.v1|6|3|delete|604800000"
  "refunds.reservation-released.v1|6|3|delete|604800000"
  "ledger.ledger-entry-recorded.v1|6|3|delete|2592000000"
  "merchants.merchant-config-changed.v1|3|3|compact|-1"
  "webhooks.webhook-delivery-requested.v1|6|3|delete|259200000"
  "webhooks.webhook-delivery-requested.v1.retry.5s|6|3|delete|259200000"
  "webhooks.webhook-delivery-requested.v1.retry.1m|6|3|delete|259200000"
  "webhooks.webhook-delivery-requested.v1.retry.15m|6|3|delete|259200000"
  "webhooks.webhook-delivery-requested.v1.dlq|3|3|delete|2592000000"
  # M9 Phase 2: the webhook delivery chain cut to a NEW v2 topic set (Avro base + 3 retry tiers +
  # dlq) rather than migrating v1 in place - see services/webhook-notifier/README.md's M9 Phase 2
  # section. v1 (above) is retired: left in the cluster holding M8's poisoned-record evidence,
  # no longer produced or consumed by webhook-notifier. Same partition/RF/retention specs as v1,
  # per docs/diagrams/topic-map.md's "Change rules" (new topic -> new row, same conventions).
  "webhooks.webhook-delivery-requested.v2|6|3|delete|259200000"
  "webhooks.webhook-delivery-requested.v2.retry.5s|6|3|delete|259200000"
  "webhooks.webhook-delivery-requested.v2.retry.1m|6|3|delete|259200000"
  "webhooks.webhook-delivery-requested.v2.retry.15m|6|3|delete|259200000"
  "webhooks.webhook-delivery-requested.v2.dlq|3|3|delete|2592000000"
  "payments.payment-requested.v1.psp-connector.dlq|3|3|delete|2592000000"
  "payments.payment-status-changed.v1.ledger.dlq|3|3|delete|2592000000"
  "psp.provider-status-query.v1|6|3|delete|3600000"
  "psp.provider-status-reply.v1|6|3|delete|3600000"
)

echo "Creating ${#TOPICS[@]} topics via mode='${MODE}' ..."
echo

fail=0
for row in "${TOPICS[@]}"; do
  IFS='|' read -r name partitions rf cleanup retention <<< "$row"

  if run_kafka_topics --create --if-not-exists \
      --topic "$name" \
      --partitions "$partitions" \
      --replication-factor "$rf" \
      --config "cleanup.policy=${cleanup}" \
      --config "retention.ms=${retention}"; then
    printf '  OK    %-55s partitions=%-3s rf=%-2s cleanup=%-8s retention.ms=%s\n' \
      "$name" "$partitions" "$rf" "$cleanup" "$retention"
  else
    printf '  FAIL  %s\n' "$name"
    fail=1
  fi
done

echo
if [[ "$fail" -eq 0 ]]; then
  echo "All topics present. Verify with:"
  echo "  docker compose exec kafka1 kafka-topics --bootstrap-server ${INTERNAL_BOOTSTRAP} --list"
else
  echo "One or more topics failed to create - see FAIL lines above." >&2
  exit 1
fi
