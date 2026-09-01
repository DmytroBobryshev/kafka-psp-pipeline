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
# M14: the cluster requires authentication. Both modes below run as the `admin` SCRAM superuser -
# see the CONTAINER_ADMIN_CONFIG/HOST_ADMIN_CONFIG block for how the credentials are supplied.
# `--host` mode additionally needs the .env values in the environment, which the `source .env`
# below already does.
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

# ---------------------------------------------------------------------------------------------
# M14: the cluster now requires SASL/SCRAM-SHA-512 authentication, so every CLI call needs a
# --command-config. This script runs as the `admin` SCRAM superuser (infra/compose/.env's
# KAFKA_ADMIN_USER / KAFKA_ADMIN_PASSWORD).
#
#   compose mode: docker-compose.yml renders the properties file into every broker container at
#                 /etc/kafka/secrets/admin.properties (a Compose `configs:` entry with inline
#                 content, interpolated from .env - so the credentials never exist as a file in
#                 this repository).
#   host mode:    the same content is written to a mode-600 temp file, removed on exit.
#
# Without it the CLI hangs and then reports "Timed out waiting for a node assignment" or
# "Connection to node -1 (kafka1/172.x.x.x:9092) failed authentication due to: Unexpected
# handshake request with client mechanism" - both of which read like a network problem.
# See infra/compose/README.md's "M14 - Security" section.
# ---------------------------------------------------------------------------------------------
CONTAINER_ADMIN_CONFIG="/etc/kafka/secrets/admin.properties"
HOST_ADMIN_CONFIG=""

if [[ "$MODE" == "host" ]]; then
  HOST_ADMIN_CONFIG="$(mktemp)"
  chmod 600 "$HOST_ADMIN_CONFIG"
  trap 'rm -f "$HOST_ADMIN_CONFIG"' EXIT
  cat > "$HOST_ADMIN_CONFIG" <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="${KAFKA_ADMIN_USER:-admin}" password="${KAFKA_ADMIN_PASSWORD:?KAFKA_ADMIN_PASSWORD must be set - source infra/compose/.env}";
EOF
fi

run_kafka_topics() {
  if [[ "$MODE" == "host" ]]; then
    kafka-topics --bootstrap-server "$HOST_BOOTSTRAP" --command-config "$HOST_ADMIN_CONFIG" "$@"
  else
    docker compose exec -T kafka1 kafka-topics --bootstrap-server "$INTERNAL_BOOTSTRAP" \
      --command-config "$CONTAINER_ADMIN_CONFIG" "$@"
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
  # M23: refund trail (PENDING/IPN_RECEIVED/VERIFIED) - config copied from refunds.refund-completed.v1.
  "refunds.refund-status-changed.v1|6|3|delete|604800000"
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
  # M13: claim-check demo. One event per aggregate (see docs/diagrams/topic-map.md), keyed by
  # disputeId - 3 partitions/30 d retention puts it in the same low-volume, audit-shaped tier as
  # the config topic above and ledger.ledger-entry-recorded.v1.
  "disputes.dispute-opened.v1|3|3|delete|2592000000"
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

# ---------------------------------------------------------------------------------------------
# M10: compaction tuning for merchants.merchant-config-changed.v1
#
# `kafka-topics --create --if-not-exists` NEVER alters an existing topic's configs, and this
# topic already existed from the M2 baseline - so these go through `kafka-configs --alter`, which
# IS idempotent and does apply to a live topic.
#
# What each setting does, and why a compacted topic needs all four rather than just
# cleanup.policy=compact:
#
#   cleanup.policy=compact
#     Retain AT LEAST the last value for every key, forever, instead of deleting whole segments
#     by age. This is what makes the topic a durable key/value table and what makes a
#     GlobalKTable's bootstrap O(merchants) rather than O(config changes).
#
#   min.cleanable.dirty.ratio=0.1   (broker default 0.5)
#     The log cleaner only picks a partition when dirty_bytes / total_bytes exceeds this. At the
#     0.5 default, HALF the log must be duplicate/obsolete records before anything is cleaned -
#     on a low-volume config topic that can be never. 0.1 makes cleaning start after 10% churn,
#     which is what turns "send a tombstone and watch the key disappear" into an experiment that
#     finishes. It is deliberately aggressive for a learning cluster: cleaning is CPU + IO, and
#     production values trade promptness for that cost.
#
#   delete.retention.ms=60000       (broker default 86400000 = 24 h)
#     How long a TOMBSTONE is kept after the cleaning pass that could have removed it. This is
#     THE setting that answers "why is my tombstone still there?". A tombstone is not removed
#     immediately, and must not be: a consumer that is behind (or a GlobalKTable bootstrapping
#     for the first time) has to actually SEE the null-valued record to learn the key is gone. If
#     tombstones vanished the instant compaction ran, a slow consumer would read the log, never
#     see the delete, and keep the row forever. 24 h is the production-safe answer to "how far
#     behind may a consumer be"; 60 s is chosen here so the drill is watchable, and it is exactly
#     the wrong value for a real cluster.
#
#   segment.ms=60000 / segment.bytes=1048576
#     The log cleaner works on CLOSED segments and NEVER touches the ACTIVE (currently-written)
#     segment - the broker is appending to it, so its offsets are still moving and it cannot be
#     rewritten in place. With the 7-day default segment.ms on a topic that gets a handful of
#     records, every record lives in the active segment and NOTHING is ever compacted, no matter
#     what the dirty ratio says. This is the single most common "compaction doesn't work" cause.
#     Rolling a new segment every 60 s (or 1 MiB, whichever comes first) guarantees records become
#     eligible. Cost: many small segments, i.e. more open file handles and more index files.
#
#   max.compaction.lag.ms=60000
#     Upper bound on how long a dirty record may go uncompacted, regardless of dirty ratio. Belt
#     and braces for the drill: it forces the cleaner to run on a nearly-clean, nearly-idle
#     partition that min.cleanable.dirty.ratio alone would leave alone forever.
#
# name | config=value,config=value...
EXTRA_CONFIGS=(
  "merchants.merchant-config-changed.v1|cleanup.policy=compact,min.cleanable.dirty.ratio=0.1,delete.retention.ms=60000,segment.ms=60000,segment.bytes=1048576,max.compaction.lag.ms=60000"
)

run_kafka_configs() {
  if [[ "$MODE" == "host" ]]; then
    kafka-configs --bootstrap-server "$HOST_BOOTSTRAP" --command-config "$HOST_ADMIN_CONFIG" "$@"
  else
    docker compose exec -T kafka1 kafka-configs --bootstrap-server "$INTERNAL_BOOTSTRAP" \
      --command-config "$CONTAINER_ADMIN_CONFIG" "$@"
  fi
}

echo
echo "Applying per-topic config overrides (${#EXTRA_CONFIGS[@]}) ..."
for row in "${EXTRA_CONFIGS[@]}"; do
  IFS='|' read -r name configs <<< "$row"
  if run_kafka_configs --alter --entity-type topics --entity-name "$name" --add-config "$configs" > /dev/null; then
    printf '  OK    %-55s %s\n' "$name" "$configs"
  else
    printf '  FAIL  %s\n' "$name"
    fail=1
  fi
done

echo
if [[ "$fail" -eq 0 ]]; then
  echo "All topics present. Verify with:"
  echo "  docker compose exec kafka1 kafka-topics --bootstrap-server ${INTERNAL_BOOTSTRAP} \\"
  echo "    --command-config ${CONTAINER_ADMIN_CONFIG} --list"
else
  echo "One or more topics failed to create - see FAIL lines above." >&2
  exit 1
fi
