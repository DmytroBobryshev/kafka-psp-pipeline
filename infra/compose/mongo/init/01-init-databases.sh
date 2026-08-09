#!/bin/bash
# infra/compose/mongo/init/01-init-databases.sh
#
# Runs once, automatically, on first container start (mounted into
# /docker-entrypoint-initdb.d/). The official mongo image executes this BEFORE --auth is
# switched on for the real listener, so mongosh needs no credentials here even though the
# server ends up auth-enabled afterwards (MONGO_INITDB_ROOT_USERNAME/PASSWORD are consumed by
# the entrypoint itself, not by this script).
#
# One MongoDB container, one logical database + one scoped user per service - same "physical
# vs logical separation" shortcut as Postgres (ADR-0005, docs/adr/0005-database-per-service.md).
# Services per docs/PLAN.md persistence table: webhook-notifier, analytics, audit-trail.
# audit-trail is owned by no service (it's populated by a Kafka Connect sink in M13) but still
# gets its own database and user so the sink connector can be scoped to it alone later.
set -euo pipefail

mongosh <<EOF
use ${WEBHOOK_NOTIFIER_DB}
db.createUser({ user: "${WEBHOOK_NOTIFIER_DB_USER}", pwd: "${WEBHOOK_NOTIFIER_DB_PASSWORD}", roles: [{ role: "readWrite", db: "${WEBHOOK_NOTIFIER_DB}" }] });
db.createCollection("_init");

use ${ANALYTICS_DB}
db.createUser({ user: "${ANALYTICS_DB_USER}", pwd: "${ANALYTICS_DB_PASSWORD}", roles: [{ role: "readWrite", db: "${ANALYTICS_DB}" }] });
db.createCollection("_init");

use ${AUDIT_TRAIL_DB}
db.createUser({ user: "${AUDIT_TRAIL_DB_USER}", pwd: "${AUDIT_TRAIL_DB_PASSWORD}", roles: [{ role: "readWrite", db: "${AUDIT_TRAIL_DB}" }] });
db.createCollection("_init");
EOF

echo "[init-databases] created databases: ${WEBHOOK_NOTIFIER_DB}, ${ANALYTICS_DB}, ${AUDIT_TRAIL_DB}"
