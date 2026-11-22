#!/usr/bin/env bash
# Idempotently register (or update) the Debezium PostgreSQL connector.
# Uses PUT /connectors/{name}/config, which creates or updates in place.
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://127.0.0.1:8083}"
CONNECTOR_NAME="${CONNECTOR_NAME:-products-connector}"
MAX_WAIT_SECONDS="${MAX_WAIT_SECONDS:-120}"

echo "Waiting for Kafka Connect at ${CONNECT_URL} ..."
elapsed=0
until curl -fs "${CONNECT_URL}/connectors" > /dev/null; do
  if (( elapsed >= MAX_WAIT_SECONDS )); then
    echo "ERROR: Kafka Connect not reachable after ${MAX_WAIT_SECONDS}s" >&2
    exit 1
  fi
  sleep 3
  elapsed=$((elapsed + 3))
done

# Least-privilege replication role from postgres/init.sql (local-dev defaults)
DB_USER="${DB_USER:-debezium}"
DB_PASSWORD="${DB_PASSWORD:-debezium}"

DB_HOST="${DB_HOST:-postgres}"
DB_NAME="${DB_NAME:-inventory}"
TOPIC_PREFIX="${TOPIC_PREFIX:-dbserver1}"
SLOT_NAME="${SLOT_NAME:-debezium_slot}"
# Debezium defaults unless overridden
POLL_INTERVAL_MS="${POLL_INTERVAL_MS:-500}"
RETRIABLE_WAIT_MS="${RETRIABLE_WAIT_MS:-10000}"
STATUS_INTERVAL_MS="${STATUS_INTERVAL_MS:-10000}"
# Optional TLS to the database (path inside the Connect container)
TLS_CONFIG=""
if [[ -n "${DB_SSLROOTCERT:-}" ]]; then
  TLS_CONFIG="\"database.sslmode\": \"verify-full\", \"database.sslrootcert\": \"${DB_SSLROOTCERT}\","
fi

# Unquoted heredoc: only the ${...} variables set above are expanded below.
read -r -d '' CONFIG <<JSON || true
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "tasks.max": "1",
  "database.hostname": "${DB_HOST}",
  "database.port": "5432",
  "database.user": "${DB_USER}",
  "database.password": "${DB_PASSWORD}",
  "database.dbname": "${DB_NAME}",
  ${TLS_CONFIG}
  "topic.prefix": "${TOPIC_PREFIX}",
  "plugin.name": "pgoutput",
  "publication.name": "dbz_publication",
  "publication.autocreate.mode": "disabled",
  "slot.name": "${SLOT_NAME}",
  "table.include.list": "public.products",
  "snapshot.mode": "initial",
  "decimal.handling.mode": "string",
  "time.precision.mode": "connect",
  "tombstones.on.delete": "false",
  "skipped.operations": "none",
  "heartbeat.interval.ms": "10000",
  "poll.interval.ms": "${POLL_INTERVAL_MS}",
  "retriable.restart.connector.wait.ms": "${RETRIABLE_WAIT_MS}",
  "status.update.interval.ms": "${STATUS_INTERVAL_MS}",
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "false",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false"
}
JSON

echo "Registering connector '${CONNECTOR_NAME}' ..."
# Body and status in one capture (no temp file: nothing to lose when it fails).
#   409 = Connect is rebalancing, 000 = no response
#   400 + "connect" = Debezium validates against the live database, which may still be
#                     coming back (e.g. right after a failover); any other 400 is a real error
for attempt in 1 2 3 4 5 6 7 8 9 10 11 12; do
  resp=$(curl -s -m 60 -w $'
%{http_code}'     -X PUT -H "Content-Type: application/json"     --data "${CONFIG}"     "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/config") || true
  http_code="${resp##*$'
'}"
  body="${resp%$'
'*}"
  case "${http_code}" in
    200|201) break ;;
    409|000|"") ;;
    400) grep -qiE 'connection|connect to' <<< "${body}" || break ;;
    *) break ;;
  esac
  echo "HTTP ${http_code}, retrying (${attempt}): ${body:0:300}" >&2
  sleep 5
done

if [[ "${http_code}" != "200" && "${http_code}" != "201" ]]; then
  echo "ERROR: registration failed (HTTP ${http_code}): ${body}" >&2
  exit 1
fi
echo "OK (HTTP ${http_code})"

sleep 3
echo "Connector status:"
curl -s "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" || true   # informational
echo
