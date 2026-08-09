#!/bin/bash
# infra/compose/postgres/init/01-init-databases.sh
#
# Runs once, automatically, on first container start (mounted into
# /docker-entrypoint-initdb.d/). Creates one database + one login role per service, each role
# granted ONLY its own database - the "physical vs logical separation" shortcut from ADR-0005
# (docs/adr/0005-database-per-service.md): one Postgres container in compose, but every service
# still gets its own database and its own credentials, so a cross-database query fails loudly
# exactly like it would with separate containers.
#
# Services per docs/PLAN.md persistence table: payment-api, ledger, psp-connector.
set -euo pipefail

create_service_db() {
  local db="$1" user="$2" password="$3"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE USER "${user}" WITH PASSWORD '${password}';
    CREATE DATABASE "${db}" OWNER "${user}";
    REVOKE ALL PRIVILEGES ON DATABASE "${db}" FROM PUBLIC;
    GRANT ALL PRIVILEGES ON DATABASE "${db}" TO "${user}";
EOSQL
  echo "[init-databases] created database '${db}' owned by role '${user}'"
}

create_service_db "${PAYMENT_API_DB}" "${PAYMENT_API_DB_USER}" "${PAYMENT_API_DB_PASSWORD}"
create_service_db "${LEDGER_DB}" "${LEDGER_DB_USER}" "${LEDGER_DB_PASSWORD}"
create_service_db "${PSP_CONNECTOR_DB}" "${PSP_CONNECTOR_DB_USER}" "${PSP_CONNECTOR_DB_PASSWORD}"
