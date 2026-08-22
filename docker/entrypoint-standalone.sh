#!/usr/bin/env bash
set -euo pipefail

HTTP_PORT="${KAHUNA_HTTP_PORT:-8081}"
HTTPS_PORT="${KAHUNA_HTTPS_PORT:-8082}"
DATA_DIR="${KAHUNA_DATA_DIR:-/data}"
PARTITIONS="${KAHUNA_PARTITIONS:-3}"
GRPC_CLEARTEXT_PORT="${KAHUNA_GRPC_CLEARTEXT_PORT:-8083}"

mkdir -p "${DATA_DIR}/data" "${DATA_DIR}/wal"

# The cleartext HTTP/2 flag is only valid with a value. When the variable is empty, omit the
# flag so the command-line parser does not see a bare option.
H2C_ARGS=()
if [ -n "${GRPC_CLEARTEXT_PORT}" ]; then
  H2C_ARGS=(--grpc-cleartext-ports "${GRPC_CLEARTEXT_PORT}")
fi

exec dotnet /app/Kahuna.Server.dll \
  --raft-nodename kahuna1 \
  --raft-nodeid 1 \
  --raft-host 0.0.0.0 \
  --raft-port "${HTTPS_PORT}" \
  --http-ports "${HTTP_PORT}" \
  --https-ports "${HTTPS_PORT}" \
  "${H2C_ARGS[@]}" \
  --https-certificate /app/certificate.pfx \
  --initial-cluster-partitions "${PARTITIONS}" \
  --storage rocksdb \
  --storage-path "${DATA_DIR}/data" \
  --storage-revision v1 \
  --wal-storage rocksdb \
  --wal-path "${DATA_DIR}/wal" \
  --wal-revision v1 \
  --disable-wal-sync-writes \
  --raft-allow-insecure-certificate-validation
