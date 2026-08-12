#!/usr/bin/env bash
# infra/compose/kafka-init/init-security.sh
#
# M14 - the security bootstrap for the compose cluster. Runs ONCE per `docker compose up` in the
# `kafka-init` container (see docker-compose.yml), then exits 0. Everything that needs a Kafka
# credential depends on it with `condition: service_completed_successfully`.
#
# It does exactly two things:
#   1. creates one SASL/SCRAM-SHA-512 credential per client principal;
#   2. applies the full ACL matrix from docs/diagrams/topic-map.md.
#
# THIS FILE IS THE EXECUTABLE SOURCE OF TRUTH FOR THE ACL MATRIX. The table in
# infra/compose/README.md's "M14 - Security" section is its prose form - change one, change both.
#
# Why it talks to port 9094 (the BROKER / inter-broker listener, SASL/PLAIN) and not 9092:
# SCRAM credentials live in the cluster metadata log, so the thing that CREATES them cannot
# authenticate with one. The PLAIN `broker` superuser comes from the brokers' own config file and
# exists from process start. This is the same chicken-and-egg the listener map exists to solve.
#
# Idempotent, by construction:
#   - `kafka-configs --alter --add-config 'SCRAM-SHA-512=[...]'` overwrites an existing credential;
#   - `kafka-acls --add` on a binding that already exists is a no-op, not an error.
# So `docker compose up kafka-init` is always safe, and is the way to apply a new ACL after
# editing this file.
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kafka1:9094,kafka2:9094,kafka3:9094}"
CLIENT_CONFIG="${CLIENT_CONFIG:-/etc/kafka/secrets/broker-admin.properties}"

# ---------------------------------------------------------------------------------------------
# 0. Wait for the cluster. The compose healthcheck already gates this container on all three
#    brokers being healthy, so this is belt-and-braces for the `docker compose up kafka-init`
#    re-run path (which does not re-evaluate healthchecks).
# ---------------------------------------------------------------------------------------------
echo "==> Waiting for Kafka at ${BOOTSTRAP} ..."
for attempt in $(seq 1 30); do
  if kafka-broker-api-versions --bootstrap-server "$BOOTSTRAP" \
      --command-config "$CLIENT_CONFIG" > /dev/null 2>&1; then
    echo "    cluster reachable (attempt ${attempt})"
    break
  fi
  if [[ "$attempt" -eq 30 ]]; then
    echo "    FAIL: cluster not reachable after 60s." >&2
    echo "    If the brokers are up but this times out, the usual cause is a mismatch between" >&2
    echo "    KAFKA_LISTENER_NAME_BROKER_PLAIN_SASL_JAAS_CONFIG and the broker-admin.properties" >&2
    echo "    rendered from KAFKA_BROKER_USER/KAFKA_BROKER_PASSWORD." >&2
    exit 1
  fi
  sleep 2
done

# ---------------------------------------------------------------------------------------------
# 1. SCRAM-SHA-512 credentials, one per principal.
#
#    The `broker` principal is NOT here: it is SASL/PLAIN, defined in the brokers' own config,
#    for the bootstrap reason explained at the top of this file.
# ---------------------------------------------------------------------------------------------
# principal | password (from the environment, which docker-compose fills from .env)
USERS=(
  "admin|${KAFKA_ADMIN_PASSWORD}"
  "payment-api|${PAYMENT_API_KAFKA_PASSWORD}"
  "psp-connector|${PSP_CONNECTOR_KAFKA_PASSWORD}"
  "ledger|${LEDGER_KAFKA_PASSWORD}"
  "webhook-notifier|${WEBHOOK_NOTIFIER_KAFKA_PASSWORD}"
  "analytics|${ANALYTICS_KAFKA_PASSWORD}"
  "realtime-gateway|${REALTIME_GATEWAY_KAFKA_PASSWORD}"
  "connect|${CONNECT_KAFKA_PASSWORD}"
  "schema-registry|${SCHEMA_REGISTRY_KAFKA_PASSWORD}"
  "akhq|${AKHQ_KAFKA_PASSWORD}"
  "kafka-exporter|${KAFKA_EXPORTER_KAFKA_PASSWORD}"
)

echo
echo "==> Creating ${#USERS[@]} SCRAM-SHA-512 credentials ..."
for row in "${USERS[@]}"; do
  IFS='|' read -r user password <<< "$row"
  if [[ -z "$password" ]]; then
    echo "    FAIL: no password in the environment for '${user}' - check infra/compose/.env" >&2
    exit 1
  fi
  kafka-configs --bootstrap-server "$BOOTSTRAP" --command-config "$CLIENT_CONFIG" \
    --alter --add-config "SCRAM-SHA-512=[password=${password}]" \
    --entity-type users --entity-name "$user" > /dev/null
  printf '    OK  User:%s\n' "$user"
done

# ---------------------------------------------------------------------------------------------
# 2. ACLs.
#
#    Helpers. Each takes a principal, then a resource, then one or more operations. `--add` is
#    additive and idempotent; nothing here removes, so a stale ACL from an earlier edit survives
#    until you delete it explicitly (kafka-acls --remove) or reset the cluster.
#
#    Kafka IMPLIES Describe from Read/Write/Delete/Alter, so the explicit `Describe` below is
#    redundant to the broker - it is there so that `kafka-acls --list` reads as documentation
#    rather than as something you have to know the implication table to interpret.
# ---------------------------------------------------------------------------------------------
acl() { kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$CLIENT_CONFIG" --add "$@" > /dev/null; }

# topic_acl <principal> <literal|prefixed> <topic> <op> [op...]
topic_acl() {
  local principal="$1" pattern="$2" topic="$3"; shift 3
  local ops=(); for op in "$@"; do ops+=(--operation "$op"); done
  acl --allow-principal "User:${principal}" --topic "$topic" --resource-pattern-type "$pattern" "${ops[@]}"
  printf '    OK  User:%-17s topic %-9s %-55s %s\n' "$principal" "$pattern" "$topic" "$*"
}

# group_acl <principal> <literal|prefixed> <group> <op> [op...]
group_acl() {
  local principal="$1" pattern="$2" group="$3"; shift 3
  local ops=(); for op in "$@"; do ops+=(--operation "$op"); done
  acl --allow-principal "User:${principal}" --group "$group" --resource-pattern-type "$pattern" "${ops[@]}"
  printf '    OK  User:%-17s group %-9s %-55s %s\n' "$principal" "$pattern" "$group" "$*"
}

# cluster_acl <principal> <op> [op...]
cluster_acl() {
  local principal="$1"; shift
  local ops=(); for op in "$@"; do ops+=(--operation "$op"); done
  acl --allow-principal "User:${principal}" --cluster "${ops[@]}"
  printf '    OK  User:%-17s cluster %-63s %s\n' "$principal" "" "$*"
}

# txn_acl <principal> <literal|prefixed> <transactional.id> <op> [op...]
txn_acl() {
  local principal="$1" pattern="$2" txid="$3"; shift 3
  local ops=(); for op in "$@"; do ops+=(--operation "$op"); done
  acl --allow-principal "User:${principal}" --transactional-id "$txid" --resource-pattern-type "$pattern" "${ops[@]}"
  printf '    OK  User:%-17s txnid %-9s %-55s %s\n' "$principal" "$pattern" "$txid" "$*"
}

echo
echo "==> Applying ACLs (deny-by-default is ON: anything not below is refused) ..."

# --- payment-api (:8085) ---------------------------------------------------------------------
# Produces merchant config directly (M10) and the request-reply query (M12); consumes the reply.
#
# payments.payment-requested.v1 / refunds.refund-requested.v1 are ALSO granted even though since
# M6 those topics are fed by the outbox -> Debezium -> Connect path under User:connect. The grant
# is the "allowed" half of docs/PLAN.md's M14 proof ("payment-api may write payments.requested but
# not ledger.entries - prove it by trying"): a denial demo is only honest if the permitted write
# genuinely succeeds with the same credential. See README's "ACL denial proof".
topic_acl payment-api literal  "merchants.merchant-config-changed.v1"  Write Describe
topic_acl payment-api literal  "psp.provider-status-query.v1"          Write Describe
topic_acl payment-api literal  "payments.payment-requested.v1"         Write Describe
topic_acl payment-api literal  "refunds.refund-requested.v1"           Write Describe
topic_acl payment-api literal  "psp.provider-status-reply.v1"          Read Describe
# group.id is minted per instance (payment-api.replies.<host>.<suffix>, M12) - a literal ACL
# would break on every restart, which is precisely what prefixed resource patterns are for.
group_acl payment-api prefixed "payment-api.replies."                  Read Describe

# --- psp-connector (:8086) -------------------------------------------------------------------
topic_acl psp-connector literal  "payments.payment-requested.v1"                Read Describe
topic_acl psp-connector literal  "refunds.funds-reserved.v1"                    Read Describe
topic_acl psp-connector literal  "psp.provider-status-query.v1"                 Read Describe
topic_acl psp-connector literal  "payments.payment-status-changed.v1"           Write Describe
topic_acl psp-connector literal  "refunds.refund-completed.v1"                  Write Describe
topic_acl psp-connector literal  "refunds.refund-failed.v1"                     Write Describe
topic_acl psp-connector literal  "psp.provider-status-reply.v1"                 Write Describe
# Its DeadLetterPublishingRecoverer target (ADR-0006). Write only: ADR-0006 is explicit that a
# DLQ is not a queue, and this service has no replay consumer.
topic_acl psp-connector literal  "payments.payment-requested.v1.psp-connector.dlq" Write Describe
# Prefixed, not literal `psp-connector.v1`: the duplicates-vs-loss drill overrides group.id from
# the command line (--spring.kafka.consumer.group-id=psp-connector.autocommit...), and those
# throwaway groups are still this principal's.
group_acl psp-connector prefixed "psp-connector"                                Read Describe

# --- ledger (:8087) --------------------------------------------------------------------------
topic_acl ledger literal  "payments.payment-status-changed.v1"        Read Describe
topic_acl ledger literal  "refunds.refund-requested.v1"               Read Describe
topic_acl ledger literal  "refunds.refund-completed.v1"               Read Describe
# refunds.refund-failed.v1 is BOTH consumed and produced here - the documented saga exception
# (see ledger's ReleaseRefundUseCase javadoc), so it needs Read AND Write.
topic_acl ledger literal  "refunds.refund-failed.v1"                  Read Write Describe
topic_acl ledger literal  "ledger.ledger-entry-recorded.v1"           Write Describe
topic_acl ledger literal  "refunds.funds-reserved.v1"                 Write Describe
topic_acl ledger literal  "refunds.reservation-released.v1"           Write Describe
topic_acl ledger literal  "payments.payment-status-changed.v1.ledger.dlq" Write Describe
group_acl ledger prefixed "ledger.v1"                                 Read Describe
# THE ONE PRINCIPAL WITH TRANSACTIONS. ledger's producer is transactional
# (transactional.id = ledger-tx-<instance-id>-<n>), and a transactional producer authorizes
# against a TransactionalId resource that has nothing to do with topics: without this it fails at
# initTransactions() with TransactionalIdAuthorizationException, long before any topic ACL is
# consulted. Prefixed because the suffix is derived per producer instance.
# `Write` on the TransactionalId plus `Read` on the consumer group is also what
# sendOffsetsToTransaction() needs to commit offsets inside the transaction.
txn_acl   ledger prefixed "ledger-tx-"                                Write Describe

# --- webhook-notifier (:8088) ----------------------------------------------------------------
topic_acl webhook-notifier literal  "payments.payment-status-changed.v1" Read Describe
topic_acl webhook-notifier literal  "refunds.refund-completed.v1"        Read Describe
topic_acl webhook-notifier literal  "refunds.refund-failed.v1"           Read Describe
# One prefixed grant covers the whole M9-Phase-2 delivery chain: the v2 base topic, its three
# retry tiers and its DLQ. This service produces AND consumes every hop (planner -> executor ->
# retry -> dlq -> replay), which is exactly why the chain lives under one prefix.
topic_acl webhook-notifier prefixed "webhooks.webhook-delivery-requested.v2" Read Write Describe
# planner.v1 / executor.v1 / dlq-replay.v1 - three consumer identities in one service.
group_acl webhook-notifier prefixed "webhook-notifier."                 Read Describe

# --- analytics (:8089) - Kafka Streams --------------------------------------------------------
topic_acl analytics literal  "payments.payment-status-changed.v1"   Read Describe
topic_acl analytics literal  "payments.payment-requested.v1"        Read Describe
topic_acl analytics literal  "merchants.merchant-config-changed.v1" Read Describe
# THE STREAMS GOTCHA. Streams CREATES its own internal topics (changelogs, repartitions) through
# an embedded AdminClient at startup. With deny-by-default and no Create, KafkaStreams goes to
# ERROR during StreamThread startup with a TopicAuthorizationException wrapped in a
# StreamsException - which reads like a topology bug, not a permissions bug.
#
# `All` on a PREFIXED resource, not a cluster-wide Create: Kafka authorizes CreateTopics against
# the Topic resource being created (falling back to Cluster only if there is no topic-level
# grant), so a prefix ACL is sufficient AND is genuinely least-privilege - the prefix is this
# application's own namespace. `All` rather than an enumerated list because the internal topic
# NAMES are derived by Streams from the topology and change as it grows (M11's saga changelog,
# M13's repartition topic); enumerating them is a maintenance trap. Streams also deletes records
# from repartition topics (deleteRecords) and reads/writes changelogs, so Read/Write/Delete/
# DescribeConfigs would all have to be listed anyway.
topic_acl analytics prefixed "analytics-streams.v1"                 All
group_acl analytics prefixed "analytics-streams.v1"                 Read Describe
# M13's plain @KafkaListener(batch = true), a separate consumer identity from the Streams app.
group_acl analytics prefixed "analytics.status-audit-batch.v1"      Read Describe
# Streams' AdminClient calls describeCluster() during startup and rebalances.
cluster_acl analytics Describe

# --- realtime-gateway (:8090) ----------------------------------------------------------------
# Read-only by construction: it fans events out over SSE and produces nothing at all.
topic_acl realtime-gateway literal  "payments.payment-requested.v1"      Read Describe
topic_acl realtime-gateway literal  "payments.payment-status-changed.v1" Read Describe
topic_acl realtime-gateway prefixed "refunds."                           Read Describe
# Unique group.id per instance (realtime-gateway.<host>.<suffix>, M12) - prefixed for the same
# reason as payment-api's reply group.
group_acl realtime-gateway prefixed "realtime-gateway."                  Read Describe

# --- Kafka Connect (worker + both connectors, one principal) ----------------------------------
# The worker's own bookkeeping topics. It CREATES these itself on first start via its AdminClient
# (auto.create.topics.enable=false does not apply to an explicit CreateTopics call), so Create is
# required, and DescribeConfigs because it verifies connect.configs is actually compacted.
topic_acl connect prefixed "connect."                            Read Write Create Describe DescribeConfigs
# Debezium's outbox source connector routes to these two (route.topic.replacement in
# payment-outbox-connector.json). THIS is the principal that really writes
# payments.payment-requested.v1 in the running system.
topic_acl connect literal  "payments.payment-requested.v1"       Write Describe
topic_acl connect literal  "refunds.refund-requested.v1"         Write Describe
# mongo-audit-sink consumes ledger.ledger-entry-recorded.v1 and writes its own
# ...v1.mongo-audit-sink.dlq - one prefixed grant covers both.
topic_acl connect prefixed "ledger.ledger-entry-recorded.v1"     Read Write Create Describe
# The worker's own group, then the sink connector's consumer group (Connect names it
# connect-<connector name>).
group_acl connect prefixed "kafka-connect-psp"                   Read Describe
group_acl connect prefixed "connect-mongo-audit-sink"            Read Describe
cluster_acl connect Describe

# --- Schema Registry --------------------------------------------------------------------------
# The registry IS a Kafka application: every schema is a record in the compacted `_schemas`
# topic, which it creates on first start if missing.
topic_acl schema-registry literal  "_schemas"        Read Write Create Describe DescribeConfigs
# Leader election among registry instances runs through the group coordinator, on the group named
# by schema.registry.group.id (default: schema-registry).
group_acl schema-registry prefixed "schema-registry" Read Describe
cluster_acl schema-registry Describe DescribeConfigs

# --- AKHQ (UI) --------------------------------------------------------------------------------
# Cluster-wide but READ-ONLY BY CONSTRUCTION: no Write, no Create, no Delete anywhere. AKHQ can
# browse every topic and every group and cannot produce a single record - which is a better
# demonstration of what per-principal ACLs buy you than any amount of prose.
topic_acl akhq literal  "*" Read Describe DescribeConfigs
group_acl akhq literal  "*" Read Describe
cluster_acl akhq Describe DescribeConfigs

# --- kafka-exporter (Prometheus) --------------------------------------------------------------
# Metadata only - Describe on topics and groups is enough for broker/partition/ISR/leader counts
# and consumer lag. It reads offsets, never message bodies, and the ACLs say exactly that.
topic_acl kafka-exporter literal "*" Describe
group_acl kafka-exporter literal "*" Describe
cluster_acl kafka-exporter Describe

# `broker` and `admin` are in super.users (docker-compose.yml KAFKA_SUPER_USERS) and deliberately
# have NO stored ACLs - a superuser bypasses the authorizer entirely, so an ACL for one would be
# dead config that reads as though it were load-bearing.

echo
echo "==> Done. Inspect with:"
echo "    docker compose exec kafka1 kafka-configs --bootstrap-server kafka1:9092 \\"
echo "      --command-config /etc/kafka/secrets/admin.properties --describe --entity-type users"
echo "    docker compose exec kafka1 kafka-acls --bootstrap-server kafka1:9092 \\"
echo "      --command-config /etc/kafka/secrets/admin.properties --list"
