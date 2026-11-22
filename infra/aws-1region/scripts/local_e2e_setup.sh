#!/usr/bin/env bash
# Assemble a local two-sided copy of the node bundle (no AWS) into $1.
# Postgres and Valkey containers replace Aurora/ElastiCache; a TCP proxy on its
# own Docker network replaces NLB + PrivateLink as the only A<->B path.
set -euo pipefail
DEST="${1:?usage: local_e2e_setup.sh <dir>}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/../../.." && pwd)"
HOSTN=kafka-a.cdc.internal
export MSYS_NO_PATHCONV=1

mkdir -p "${DEST}"/{a,b}/secrets/{kafka,pg} "${DEST}"/{a,b}/run "${DEST}"/{a,b}/worker "${DEST}/pki" "${DEST}/store"
for s in a b; do
  find "${HERE}/../node" -maxdepth 1 -type f -exec cp -t "${DEST}/${s}/" {} +
  cp "${ROOT}/debezium/register-connector.sh" "${DEST}/${s}/"
  cp "${ROOT}"/worker/*.py "${ROOT}"/worker/*.lua "${DEST}/${s}/worker/"
done

cd "${DEST}/pki"
if [[ ! -f server.pem ]]; then
  openssl req -x509 -newkey rsa:2048 -nodes -days 14 -subj "/CN=cdc-local-ca" -keyout ca.key -out ca.pem 2> /dev/null
  leaf() { # leaf <cn> <san> <out>
    openssl req -newkey rsa:2048 -nodes -subj "/CN=$1" -keyout "$3.key" -out "$3.csr" 2> /dev/null
    echo "subjectAltName=DNS:$2" > "$3.ext"
    openssl x509 -req -in "$3.csr" -CA ca.pem -CAkey ca.key -CAcreateserial -days 14 \
      -extfile "$3.ext" -out "$3.crt" 2> /dev/null
    cat "$3.key" "$3.crt" > "$3.pem"
  }
  leaf "${HOSTN}" "${HOSTN}" server
  leaf mm2-b mm2-b client
fi
cp ca.pem server.pem ../a/secrets/kafka/
cp ca.pem client.pem ../b/secrets/kafka/

cat > "${DEST}/a/.env" <<S
KAFKA_PRIVATELINK_HOST=${HOSTN}
KAFKA_TLS_BIND=127.0.0.1
KAFKA_HEAP_OPTS=-Xmx320m -Xms128m
CONNECT_HEAP_OPTS=-Xmx384m -Xms128m
CONNECT_PORT=18083
REDIS_HOST=cache
REDIS_PASSWORD=localtoken
REDIS_TLS=false
DEBEZIUM_PASSWORD=dbzlocal
MONITOR_PASSWORD=monlocal
APP_PASSWORD=applocal
MONITOR_DSN=postgresql://cdc_monitor:monlocal@pg:5432/inventory
APP_DSN=postgresql://cdc_app:applocal@pg:5432/inventory
SLOT_ADMIN_DSN=postgresql://debezium:dbzlocal@pg:5432/inventory
S
cat > "${DEST}/b/.env" <<S
KAFKA_PRIVATELINK_HOST=${HOSTN}
KAFKA_HEAP_OPTS=-Xmx256m -Xms128m
MM2_HEAP_OPTS=-Xmx256m -Xms128m
REDIS_HOST=cache
REDIS_PASSWORD=localtoken
REDIS_TLS=false
S
cat > "${DEST}/a/node.conf" <<S
SIDE=a
CDC_STORE=${DEST}/store
CONNECT_PORT=18083
DB_HOST=pg
DB_NAME=inventory
ADMIN_DSN=postgresql://postgres:postgres@pg:5432/inventory
S
cat > "${DEST}/b/node.conf" <<S
SIDE=b
CDC_STORE=${DEST}/store
KAFKA_PRIVATELINK_HOST=${HOSTN}
S
cat > "${DEST}/a/compose.override.yml" <<S
services:
  pg:
    image: postgres:16
    environment: {POSTGRES_PASSWORD: postgres, POSTGRES_DB: inventory}
    command: ["postgres", "-c", "wal_level=logical", "-c", "max_slot_wal_keep_size=2GB", "-c", "max_connections=200"]
  cache:
    image: valkey/valkey:8-alpine
    command: ["valkey-server", "--requirepass", "localtoken", "--maxmemory-policy", "volatile-lru"]
  privatelink:
    image: alpine/socat
    command: ["TCP-LISTEN:9094,fork,reuseaddr", "TCP:kafka:9094"]
    networks:
      default: {}
      plink: {aliases: ["${HOSTN}"]}
networks:
  plink: {name: cdc-plink}
S
cat > "${DEST}/b/compose.override.yml" <<S
services:
  cache:
    image: valkey/valkey:8-alpine
    command: ["valkey-server", "--requirepass", "localtoken", "--maxmemory-policy", "volatile-lru"]
  mm2:
    networks: [default, plink]
networks:
  plink: {name: cdc-plink, external: true}
S
echo "assembled ${DEST}"
