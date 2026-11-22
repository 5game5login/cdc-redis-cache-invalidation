#!/usr/bin/env bash
# Node control script. Lives in /opt/cdc on both nodes; run as root through SSM.
# Local test mode: set CDC_HOME, SIDE, CDC_STORE (a directory) in the environment.
set -Eeuo pipefail
# SSM shows only output, so a failing step must say which one it was
trap 'echo "[cdcctl] ERROR rc=$? line ${LINENO}: ${BASH_COMMAND}" >&2' ERR

CDC_HOME="${CDC_HOME:-/opt/cdc}"
cd "${CDC_HOME}"
# node.conf is written by user-data: SIDE, AWS_REGION, CDC_STORE, secret ids, endpoints
# shellcheck disable=SC1091
if [[ -f node.conf ]]; then set -a; . ./node.conf; set +a; fi
: "${SIDE:?SIDE must be a or b}"
COMPOSE_FILES=(-f "compose.${SIDE}.yml")
[[ -f compose.override.yml ]] && COMPOSE_FILES+=(-f compose.override.yml)
CONNECT_URL="http://127.0.0.1:${CONNECT_PORT:-8083}"
TOOLS_OPTS=()

log() { echo "[cdcctl ${SIDE}] $*" >&2; }
dc() { docker compose "${COMPOSE_FILES[@]}" --env-file .env --env-file state.env --env-file tuning.env "$@"; }
tools() { dc run --rm -T --no-deps "${TOOLS_OPTS[@]}" tools "$@"; }
secret() { aws secretsmanager get-secret-value --region "${AWS_REGION}" --secret-id "$1" --query SecretString --output text; }
state() { grep -E "^$1=" state.env | cut -d= -f2-; }
tuning() { grep -E "^$1=" tuning.env | cut -d= -f2-; }

put() { # put <local> <name>
  if [[ "${CDC_STORE}" == s3://* ]]; then
    aws s3 cp --quiet "$1" "${CDC_STORE}/$2"
  else
    mkdir -p "$(dirname "${CDC_STORE}/$2")"
    cp "$1" "${CDC_STORE}/$2.tmp" && mv "${CDC_STORE}/$2.tmp" "${CDC_STORE}/$2"
  fi
}
get() { # get <name> <local>; fails if absent. Publishes atomically.
  if [[ "${CDC_STORE}" == s3://* ]]; then
    aws s3 cp --quiet "${CDC_STORE}/$1" "$2.part" 2> /dev/null || return 1
  else
    cp "${CDC_STORE}/$1" "$2.part" 2> /dev/null || return 1
  fi
  chmod 644 "$2.part"
  mv "$2.part" "$2"
}
wait_get() { # wait_get <name> <local> <timeout>
  local deadline=$((SECONDS + $3))
  until get "$1" "$2"; do
    (( SECONDS >= deadline )) && { log "timed out waiting for $1"; return 1; }
    sleep 1
  done
}
wait_file() { # wait_file <path> <timeout>
  local deadline=$((SECONDS + $2))
  until [[ -f "$1" ]]; do
    (( SECONDS >= deadline )) && { log "timed out waiting for $1"; return 1; }
    sleep 1
  done
}

write_state() { # write_state <epoch>: everything versioned by epoch lives here
  local e="$1" suffix=""
  (( e > 0 )) && suffix="e${e}"
  cat > state.env.tmp <<S
CDC_EPOCH=${e}
TOPIC_PREFIX=dbserver$((e + 1))
SLOT_NAME=debezium_slot${suffix:+_${suffix}}
CONNECTOR_NAME=products-connector${suffix:+-${suffix}}
S
  mv state.env.tmp state.env
}

fetch_secrets() {
  umask 077
  mkdir -p secrets/kafka secrets/pg run
  local redis db tls
  redis="$(secret "${REDIS_SECRET_ID}")"
  {
    echo "KAFKA_PRIVATELINK_HOST=${KAFKA_PRIVATELINK_HOST}"
    echo "REDIS_HOST=${REDIS_HOST}"
    echo "REDIS_PASSWORD=${redis}"
    echo "REDIS_TLS=true"
    if [[ "${SIDE}" == a ]]; then
      local ssl="sslmode=verify-full&sslrootcert=/etc/cdc/rds-ca.pem"
      db="$(secret "${DB_SECRET_ID}")"
      pw() { jq -er ".$1" <<< "${db}"; }
      echo "DEBEZIUM_PASSWORD=$(pw debezium)"
      echo "MONITOR_PASSWORD=$(pw cdc_monitor)"
      echo "APP_PASSWORD=$(pw cdc_app)"
      echo "MONITOR_DSN=postgresql://cdc_monitor:$(pw cdc_monitor)@${DB_HOST}:5432/${DB_NAME}?${ssl}"
      echo "APP_DSN=postgresql://cdc_app:$(pw cdc_app)@${DB_HOST}:5432/${DB_NAME}?${ssl}"
      echo "SLOT_ADMIN_DSN=postgresql://debezium:$(pw debezium)@${DB_HOST}:5432/${DB_NAME}?${ssl}"
    fi
  } > .env.tmp
  mv .env.tmp .env
  tls="$(secret "${KAFKA_TLS_SECRET_ID}")"
  jq -er .ca <<< "${tls}" > secrets/kafka/ca.pem
  if [[ "${SIDE}" == a ]]; then
    jq -er '.key + .cert' <<< "${tls}" > secrets/kafka/server.pem
    curl -fsS --retry 5 -o secrets/pg/rds-ca.pem https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem
    chmod 444 secrets/pg/rds-ca.pem
  else
    jq -er '.key + .cert' <<< "${tls}" > secrets/kafka/client.pem
  fi
  # apache/kafka runs as uid 1000, Debezium as 1001, tools as 10001
  chmod 755 secrets secrets/pg
  chown -R 1000:1000 secrets/kafka
  chmod 500 secrets/kafka
  chmod 400 secrets/kafka/*
  chown 10001:10001 run
}

admin() { # run a tools command with the admin DSN, fetched only for this call
  local dsn="${ADMIN_DSN:-}" pw
  if [[ -z "${dsn}" ]]; then
    pw="$(secret "${DB_ADMIN_SECRET_ID}")"
    dsn="postgresql://cdc_admin:${pw}@${DB_HOST}:5432/${DB_NAME}?sslmode=verify-full&sslrootcert=/etc/cdc/rds-ca.pem"
  fi
  ADMIN_DSN="${dsn}" tools "$@"
}

init_db() { admin python init_db.py; }

kt() { dc exec -T kafka "/opt/kafka/bin/$1" --bootstrap-server localhost:9092 "${@:2}" < /dev/null; }

acls() { # B's client certificate may only read the CDC topics
  local p="User:CN=mm2-b"
  kt kafka-acls.sh --add --allow-principal "${p}" --operation Read --operation Describe \
    --operation DescribeConfigs --topic dbserver --resource-pattern-type prefixed > /dev/null
  log "read-only ACLs set for ${p}"
}

topic() { echo "$(state TOPIC_PREFIX).public.products"; }

wait_topic() { # librdkafka rechecks a missing topic only every 5 min, so wait before starting it
  local t list out deadline=$((SECONDS + $1))
  t="$(topic)"
  while true; do
    # capture, then match: `cmd | grep -q` can fail with SIGPIPE under pipefail
    list="$(kt kafka-topics.sh --list 2>&1)" || log "topic list failed: ${list}"
    grep -qx "${t}" <<< "${list}" && break
    (( SECONDS >= deadline )) && { log "topic ${t} did not appear; last list: ${list}"; return 1; }
    sleep 10 # each list starts a JVM
  done
  # explicit: side B has topic auto-creation off
  for _ in 1 2 3; do
    out="$(kt kafka-topics.sh --create --if-not-exists --topic "${t}.dlq"            --partitions 1 --replication-factor 1 2>&1)" && return 0
    log "DLQ topic creation failed, retrying: ${out}"
    sleep 3
  done
  return 1
}

register() {
  local ca=""
  [[ -s secrets/pg/rds-ca.pem ]] && ca=/etc/cdc/rds-ca.pem
  # shellcheck disable=SC2016  # ${env:...} is resolved by Kafka Connect, not bash
  POLL_INTERVAL_MS="$(tuning POLL_INTERVAL_MS)" RETRIABLE_WAIT_MS="$(tuning RETRIABLE_WAIT_MS)" \
  STATUS_INTERVAL_MS="$(tuning STATUS_INTERVAL_MS)" \
  DB_HOST="${DB_HOST}" DB_USER=debezium DB_PASSWORD='${env:DB_PASSWORD}' DB_NAME="${DB_NAME}" \
  TOPIC_PREFIX="$(state TOPIC_PREFIX)" SLOT_NAME="$(state SLOT_NAME)" \
  CONNECTOR_NAME="$(state CONNECTOR_NAME)" CONNECT_URL="${CONNECT_URL}" DB_SSLROOTCERT="${ca}" \
    bash ./register-connector.sh
}

connector_state() {
  local body
  body="$(curl -fs -m 10 "${CONNECT_URL}/connectors/$(state CONNECTOR_NAME)/status")" || { echo UNREACHABLE; return 0; }
  jq -r '[.connector.state, (.tasks[0].state // "NONE")] | join("/")' <<< "${body}"
}

wait_connector() { # wait_connector <timeout>
  local deadline=$((SECONDS + $1))
  until [[ "$(connector_state)" == RUNNING/RUNNING ]]; do
    (( SECONDS >= deadline )) && { log "connector is $(connector_state)"; return 1; }
    sleep 2
  done
}

build() { docker build -q -t cdc-tools:local . > /dev/null; }

up() {
  [[ -f state.env ]] || write_state 0
  mkdir -p run
  build
  if [[ "${SIDE}" == a ]]; then
    dc up -d --wait kafka
    acls
    dc up -d --wait connect
    init_db
    register
    wait_connector 180
  else
    sed "s|@KAFKA_PRIVATELINK_HOST@|${KAFKA_PRIVATELINK_HOST}|g" mm2.properties.tmpl > mm2.properties
    dc up -d --wait kafka
    dc up -d mm2
  fi
  wait_topic 900
  start_workers
  log "up"
}

start_workers() {
  if [[ "${SIDE}" == a ]]; then dc up -d worker watchdog; else dc up -d worker; fi
}

# ---- failover runbook ------------------------------------------------------
bump_epoch() { # bump_epoch <new epoch>: README "Epoch runbook". Safe to rerun after a failure.
  local new="$1" old
  old="$(state CDC_EPOCH)"
  if (( new == old + 1 )); then
    # Saved first, so a rerun still knows what to clean up once state.env moves on
    [[ -f "state.env.e${old}" ]] || cp state.env "state.env.e${old}"
    write_state "${new}"
  elif (( new == old && new > 0 )) && [[ -f "state.env.e$((new - 1))" ]]; then
    log "resuming bump to epoch ${new}"
  else
    log "refusing: epoch ${old} -> ${new}"; return 1
  fi
  dc stop worker
  if [[ "${SIDE}" == a ]]; then
    dc stop watchdog
    local prev="state.env.e$((new - 1))" old_slot old_connector code
    old_slot="$(grep -E '^SLOT_NAME=' "${prev}" | cut -d= -f2-)"
    old_connector="$(grep -E '^CONNECTOR_NAME=' "${prev}" | cut -d= -f2-)"
    code="$(curl -s -m 100 -o /dev/null -w '%{http_code}' -X DELETE "${CONNECT_URL}/connectors/${old_connector}")" || true
    [[ "${code}" == 204 || "${code}" == 404 ]] || { log "deleting ${old_connector}: HTTP ${code}"; return 1; }
    tools python bench_node.py drop-slot --slot "${old_slot}"
    register
    wait_connector 180
  fi
  wait_topic 900
  start_workers
  log "epoch -> ${new}"
}

stale_check() { # a worker one epoch behind must refuse to start
  local e out rc=0
  e=$(( $(state CDC_EPOCH) - 1 ))
  (( e >= 0 )) || { log "no older epoch to test"; return 1; }
  out="$(TOOLS_OPTS=(-e "CDC_EPOCH=${e}" -e PG_DSN=); tools timeout 30 python -m worker.consumer 2>&1)" || rc=$?
  if (( rc != 0 && rc != 124 )) && grep -q EpochRegression <<< "${out}"; then
    echo "STALE_REFUSED epoch=${e} rc=${rc}"
  else
    echo "STALE_NOT_REFUSED epoch=${e} rc=${rc}"; tail -5 <<< "${out}"; return 1
  fi
}

status() {
  local s='[]' c=n/a
  if [[ "${SIDE}" == a ]]; then s="$(tools python bench_node.py slots)"; c="$(connector_state)"; fi
  jq -nc --arg side "${SIDE}" --arg epoch "$(tools python bench_node.py epoch)" --arg conn "${c}" \
     --arg topic "$(topic)" --arg slot "$(state SLOT_NAME)" --argjson slots "${s}" \
     --arg worker "$(dc ps --format '{{.State}}' worker)" \
     '{side:$side,cache_epoch:$epoch,connector:$conn,topic:$topic,slot:$slot,slots:$slots,worker:$worker}'
}

# ---- latency tuning ---------------------------------------------------------
tune() { # tune KEY=VALUE ...: change settings on this node and apply them
  local kv k
  for kv in "$@"; do
    k="${kv%%=*}"
    grep -qE "^${k}=" tuning.env || { log "unknown setting ${k}"; return 1; }
    [[ "${kv#*=}" =~ ^[0-9.]+$ ]] || { log "not a number: ${kv}"; return 1; }
    sed -i "s|^${k}=.*|${kv}|" tuning.env
  done
  if [[ "${SIDE}" == a ]]; then register > /dev/null; wait_connector 180; fi
  start_workers
  grep -v '^#' tuning.env
}

soak_stages() { # soak_stages <run>: side A, after a soak run
  get "$1/watch-b.json" "run/$1/watch-b.json" || log "no watch-b.json"
  tools python stage_probe.py "$1"
}

# ---- benchmark -------------------------------------------------------------
clock() { chronyc tracking 2> /dev/null | awk -F' : ' '/System time/{print $2}' || true; }
prep_run() { mkdir -p "run/$1"; chmod 777 "run/$1" 2> /dev/null || true; }
finish_observe() { # add clock info, publish
  local f="run/$1/observe-${SIDE}.json"
  jq -c --arg c "$(clock)" '.clock=$c' "${f}" > "${f}.pub"
  put "${f}.pub" "$1/observe-${SIDE}.json"
}

bench_prepare() {
  prep_run "$1"
  tools python bench_node.py prepare --run "$1"
  put "run/$1/plan.json" "$1/plan.json"
}

bench_run() { # side A: observe locally and write concurrently
  local run="$1" rc=0 pid
  wait_get "${run}/ready-b" "run/${run}/ready-b" 180
  tools python bench_node.py observe --run "${run}" --side a --timeout 240 & pid=$!
  wait_file "run/${run}/ready-a" 60
  tools python bench_node.py write --run "${run}"
  put "run/${run}/writes.json" "${run}/writes.json"
  put "run/${run}/truth.json" "${run}/truth.json"
  wait "${pid}" || rc=$?
  finish_observe "${run}"
  return "${rc}"
}

bench_observe() { # side B
  local run="$1" rc=0 pid
  prep_run "${run}"
  wait_get "${run}/plan.json" "run/${run}/plan.json" 120
  tools python bench_node.py observe --run "${run}" --side b --timeout 300 & pid=$!
  wait_file "run/${run}/ready-b" 60
  put "run/${run}/ready-b" "${run}/ready-b"
  wait_get "${run}/truth.json" "run/${run}/truth.json" 280
  wait "${pid}" || rc=$?
  finish_observe "${run}"
  return "${rc}"
}

# ---- failover soak (no epoch bump) -------------------------------------------
soak_prepare() {
  prep_run "$1"
  tools python soak_node.py prepare --run "$1" --rate "${2:-40}"
  put "run/$1/plan.json" "$1/plan.json"
}

soak_publish() { put "run/$1/watch-${SIDE}.json" "$1/watch-${SIDE}.json"; }

soak_run() { # soak_run <run> <seconds>: side A watches its cache and writes
  local run="$1" secs="$2" rc=0 wpid pid
  wait_get "${run}/ready-b" "run/${run}/ready-b" 180
  tools python soak_node.py watch --run "${run}" --side a --timeout $((secs + 480)) & pid=$!
  wait_file "run/${run}/ready-a" 60
  tools python soak_node.py write --run "${run}" --duration "${secs}" & wpid=$!
  wait_file "run/${run}/started" 60
  put "run/${run}/started" "${run}/started"
  wait "${wpid}"
  put "run/${run}/writes.json" "${run}/writes.json"
  put "run/${run}/truth.json" "${run}/truth.json"
  wait "${pid}" || rc=$?
  soak_publish "${run}"
  return "${rc}"
}

soak_watch() { # side B
  local run="$1" rc=0 pid
  prep_run "${run}"
  wait_get "${run}/plan.json" "run/${run}/plan.json" 120
  tools python soak_node.py watch --run "${run}" --side b --timeout 900 & pid=$!
  wait_file "run/${run}/ready-b" 60
  put "run/${run}/ready-b" "${run}/ready-b"
  wait_get "${run}/truth.json" "run/${run}/truth.json" 800
  wait "${pid}" || rc=$?
  soak_publish "${run}"
  return "${rc}"
}

cmd="${1:?command required}"; shift
case "${cmd}" in
  fetch-secrets)  fetch_secrets ;;
  up)             up ;;
  build)          build ;;
  acls)           acls ;;
  init-db)        init_db ;;
  register)       register ;;
  bump-epoch)     bump_epoch "$@" ;;
  stale-check)    stale_check ;;
  status)         status ;;
  connector)      connector_state ;;
  wait-connector) wait_connector "$@" ;;
  bench-prepare)  bench_prepare "$@" ;;
  bench-run)      bench_run "$@" ;;
  bench-observe)  bench_observe "$@" ;;
  tune)           tune "$@" ;;
  soak-stages)    soak_stages "$@" ;;
  soak-prepare)   soak_prepare "$@" ;;
  crash-writer)   admin python bench_node.py crash-writer ;;
  soak-run)       soak_run "$@" ;;
  soak-watch)     soak_watch "$@" ;;
  compose)        dc "$@" ;;
  *) echo "unknown command: ${cmd}" >&2; exit 2 ;;
esac
