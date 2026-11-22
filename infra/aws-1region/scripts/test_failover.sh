#!/usr/bin/env bash
# Failover test: Aurora writer failover, then the CDC_EPOCH runbook.
#   1. status before
#   2. fail over the writer (aws rds failover-db-cluster; locally: restart Postgres)
#   3. record whether the logical slot survived and the connector resumed (either
#      answer is fine: it is an observation, not a pass/fail criterion)
#   4. epoch runbook: new slot + topic prefix, CDC_EPOCH+1, on A then B
#   5. both caches must report the new epoch; a stale-epoch worker must refuse to start
# Follow with run_aws_benchmark.py to prove consistency under the new epoch.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PY="${PYTHON:-python}"
OUT="${OUT:-}"
report=()
say() { echo "$*"; report+=("$*"); }
die() { say "FAIL: $*"; finish 1; }
finish() {
  say "RESULT: $([[ $1 == 0 ]] && echo PASS || echo FAIL)"
  if [[ -n "${OUT}" ]]; then mkdir -p "$(dirname "${OUT}")"; printf '%s\n' "${report[@]}" > "${OUT}"; fi
  exit "$1"
}
node() { "${PY}" "${HERE}/cdc_remote.py" "$@"; }
field() { "${PY}" -c 'import json,sys; print(json.load(sys.stdin)[sys.argv[1]])' "$1" | tr -d '\r'; }
slot_state() { # slot_state <slot>: reads status JSON on stdin
  "${PY}" -c 'import json,sys
s=[x for x in json.load(sys.stdin)["slots"] if x["slot"]==sys.argv[1]]
print("%s active=%s wal_status=%s" % ("present", s[0]["active"], s[0]["wal_status"]) if s else "MISSING")' "$1" | tr -d '\r'
}

# ---- 1. before
before="$(node a status)" || die "status on A failed"
say "before A: ${before}"
epoch="$(field cache_epoch <<< "${before}")"
slot="$(field slot <<< "${before}")"
[[ "${epoch}" =~ ^[0-9]+$ ]] || die "unexpected epoch '${epoch}'"
new=$((epoch + 1))

# ---- 2. failover
t0=${SECONDS}
if [[ -n "${CDC_LOCAL:-}" ]]; then
  docker restart cdc-a-pg-1 > /dev/null
  say "local mode: restarted Postgres in place of an Aurora failover"
else
  : "${AWS_PROFILE:=cdc-cache-poc}"; export AWS_PROFILE
  nodes="$(bash "${HERE}/tf.sh" output -json nodes)"
  REGION="$(field region_a <<< "${nodes}")"
  CLUSTER="$(field cluster_id <<< "${nodes}")"
  writer() {
    aws rds describe-db-clusters --region "${REGION}" --db-cluster-identifier "${CLUSTER}" \
      --query 'DBClusters[0].DBClusterMembers[?IsClusterWriter].DBInstanceIdentifier | [0]' --output text | tr -d '\r'
  }
  old_writer="$(writer)"
  [[ -n "${old_writer}" && "${old_writer}" != None ]] || die "cannot determine the writer"
  aws rds failover-db-cluster --region "${REGION}" --db-cluster-identifier "${CLUSTER}" > /dev/null
  until [[ "$(writer)" != "${old_writer}" ]]; do
    (( SECONDS - t0 > 600 )) && die "writer did not change within 600s"
    sleep 2
  done
  say "writer ${old_writer} -> $(writer) after $((SECONDS - t0))s"
fi

# ---- 3. did the slot and connector survive? (observation)
survived=no
for _ in $(seq 60); do
  if now="$(node a status 2> /dev/null)" \
     && [[ "$(field connector <<< "${now}")" == RUNNING/RUNNING ]] \
     && [[ "$(slot_state "${slot}" <<< "${now}")" == *active=True* ]]; then
    survived=yes; break
  fi
  sleep 3
done
say "slot ${slot} after failover: $(slot_state "${slot}" <<< "${now:-${before}}"); connector resumed without intervention: ${survived} ($((SECONDS - t0))s after failover start)"

# ---- 4. epoch runbook
node a bump-epoch "${new}" > /dev/null || die "bump-epoch on A failed"
node b bump-epoch "${new}" > /dev/null || die "bump-epoch on B failed"
say "epoch ${epoch} -> ${new} on A and B"

# ---- 5. verify
for s in a b; do
  got=""
  for _ in $(seq 20); do
    got="$(node "${s}" status 2> /dev/null || true)"
    [[ -n "${got}" && "$(field cache_epoch <<< "${got}")" == "${new}" && "$(field worker <<< "${got}")" == running ]] && break
    sleep 3
  done
  say "after ${s^^}: ${got}"
  [[ -n "${got}" ]] || die "no status from ${s}"
  [[ "$(field cache_epoch <<< "${got}")" == "${new}" ]] || die "cache ${s} is not at epoch ${new}"
  [[ "$(field worker <<< "${got}")" == running ]] || die "worker ${s} is not running"
  stale="$(node "${s}" stale-check)" || die "stale-epoch worker on ${s} was not refused: ${stale}"
  say "stale ${s^^}: ${stale}"
done
[[ "$(node a connector | tr -d '\r\n')" == RUNNING/RUNNING ]] || die "new connector is not running"
finish 0
