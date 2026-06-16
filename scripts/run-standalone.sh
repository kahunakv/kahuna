#!/bin/bash
#
# Runs a single Kahuna node as a self-contained, standalone cluster.
#
# Unlike scripts/run-node.sh (which joins a multi-node cluster via --initial-cluster),
# this omits the peer list entirely: the node starts with an empty static-discovery set
# and elects itself leader for every partition. Useful for local development and testing
# without spinning up the Docker cluster.
#
# Overridable via environment variables:
#   KAHUNA_STORAGE      storage backend: rocksdb (default, persistent) or memory (ephemeral)
#   KAHUNA_HTTP_PORT    HTTP port  (default 8081)
#   KAHUNA_HTTPS_PORT   HTTPS port (default 8082)
#   KAHUNA_PARTITIONS   initial cluster partitions (default 3; 128 is cluster-scale)
#   KAHUNA_DATA_DIR     base directory for data/WAL when using rocksdb (default /tmp/kahuna-standalone)
#
set -euo pipefail

cd "$(dirname "$0")/.."
REPO_ROOT="$(pwd)"

STORAGE="${KAHUNA_STORAGE:-rocksdb}"
HTTP_PORT="${KAHUNA_HTTP_PORT:-8081}"
HTTPS_PORT="${KAHUNA_HTTPS_PORT:-8082}"
PARTITIONS="${KAHUNA_PARTITIONS:-3}"
DATA_DIR="${KAHUNA_DATA_DIR:-/tmp/kahuna-standalone}"

PUBLISH_DIR="/tmp/kahuna-standalone-bin"

echo ">> Publishing Kahuna.Server to ${PUBLISH_DIR}"
dotnet publish Kahuna.Server/Kahuna.Server.csproj -c Release -o "${PUBLISH_DIR}"

cp "${REPO_ROOT}/certs/development-certificate.pfx" "${PUBLISH_DIR}/certificate.pfx"

cd "${PUBLISH_DIR}"

ARGS=(
  --raft-nodename kahuna1
  --raft-nodeid 1
  --raft-host 127.0.0.1
  --raft-port 8082
  --http-ports "${HTTP_PORT}"
  --https-ports "${HTTPS_PORT}"
  --https-certificate "${PUBLISH_DIR}/certificate.pfx"
  --initial-cluster-partitions "${PARTITIONS}"
)

if [ "${STORAGE}" = "memory" ]; then
  # Fully ephemeral: the embedded standalone engine supports an in-memory WAL too.
  echo ">> Storage: memory data + memory WAL (fully ephemeral)"
  ARGS+=(--storage memory --wal-storage memory)
else
  echo ">> Storage: rocksdb (persistent) at ${DATA_DIR}"
  # Create only the parent dirs; RocksDB creates each <path>/<revision> dir itself
  # and uses its existence to detect a pre-existing (versioned) store.
  mkdir -p "${DATA_DIR}/wal" "${DATA_DIR}/data"
  ARGS+=(
    --storage rocksdb --storage-path "${DATA_DIR}/data" --storage-revision v1
    --wal-storage rocksdb --wal-path "${DATA_DIR}/wal" --wal-revision v1
  )
fi

echo ">> Starting standalone node on https://127.0.0.1:${HTTPS_PORT} (http ${HTTP_PORT})"
exec dotnet Kahuna.Server.dll "${ARGS[@]}"
