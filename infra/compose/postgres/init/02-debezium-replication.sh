#!/bin/bash
# infra/compose/postgres/init/02-debezium-replication.sh
#
# Runs once, automatically, on first container start (mounted into
# /docker-entrypoint-initdb.d/, alongside 01-init-databases.sh which it depends on running
# first - init scripts in this directory execute in lexical filename order, hence the "02-"
# prefix).
#
# M6 (transactional outbox / Debezium): the Postgres connector reads payment_api's outbox_event
# table via LOGICAL REPLICATION (the WAL, not SELECT polling), which requires the connecting role
# to hold the REPLICATION attribute. Rather than introduce a new superuser-ish credential shared
# across services, this grants REPLICATION to the SAME per-service role payment-api already owns
# its schema with (PAYMENT_API_DB_USER, created by 01-init-databases.sh) - the Debezium connector
# authenticates as that same user, so the connector can only ever replicate payment_api's own
# database, preserving the ADR-0005 per-service isolation property (a REPLICATION-privileged role
# can stream WAL for ANY database on the cluster it can connect to, but this role still cannot
# CONNECT to ledger or psp_connector - see 01-init-databases.sh's per-database REVOKE/GRANT).
#
# CREATE PUBLICATION for the outbox table happens separately: the connector is configured with
# publication.autocreate.mode=filtered (infra/compose/connect/payment-outbox-connector.json),
# and PostgreSQL lets a role create a publication for tables it OWNS without needing extra
# grants here - payment_api owns outbox_event (Flyway ran V2 as that role), so no additional
# privilege is required for that part.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    ALTER ROLE "${PAYMENT_API_DB_USER}" WITH REPLICATION;
EOSQL
echo "[init-debezium-replication] granted REPLICATION to role '${PAYMENT_API_DB_USER}'"
