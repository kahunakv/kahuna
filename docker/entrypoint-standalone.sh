#!/usr/bin/env bash
set -euo pipefail

HTTP_PORT="${KAHUNA_HTTP_PORT:-8081}"
HTTPS_PORT="${KAHUNA_HTTPS_PORT:-8082}"
DATA_DIR="${KAHUNA_DATA_DIR:-/data}"
PARTITIONS="${KAHUNA_PARTITIONS:-3}"

mkdir -p "${DATA_DIR}/data" "${DATA_DIR}/wal"

exec dotnet /app/Kahuna.Server.dll \
  --raft-nodename kahuna1 \
  --raft-nodeid 1 \
  --raft-host 0.0.0.0 \
  --raft-port "${HTTPS_PORT}" \
  --http-ports "${HTTP_PORT}" \
  --https-ports "${HTTPS_PORT}" \
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
