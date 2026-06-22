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
#   KAHUNA_STORAGE              storage backend: rocksdb (default, persistent) or memory (ephemeral)
#   KAHUNA_HTTP_PORT            HTTP port  (default 8081)
#   KAHUNA_HTTPS_PORT           HTTPS port (default 8082)
#   KAHUNA_PARTITIONS           initial cluster partitions (default 3; 128 is cluster-scale)
#   KAHUNA_DATA_DIR             base directory for data/WAL when using rocksdb (default /tmp/kahuna-standalone)
#   KAHUNA_BACKUP_DIR           root directory for PITR backup artifacts; leave unset to disable backup
#   KAHUNA_RESTORE_STORAGE_PATH when set, use this directory as the storage path instead of DATA_DIR/data.
#                               Point this at the target-dir produced by "kahuna-control --restore".
#                               Must be used with the same KAHUNA_STORAGE type that created the backup.
#
set -euo pipefail

cd "$(dirname "$0")/.."
REPO_ROOT="$(pwd)"

STORAGE="${KAHUNA_STORAGE:-rocksdb}"
HTTP_PORT="${KAHUNA_HTTP_PORT:-8081}"
HTTPS_PORT="${KAHUNA_HTTPS_PORT:-8082}"
PARTITIONS="${KAHUNA_PARTITIONS:-3}"
DATA_DIR="${KAHUNA_DATA_DIR:-/tmp/kahuna-standalone}"
BACKUP_DIR="${KAHUNA_BACKUP_DIR:-}"
RESTORE_STORAGE_PATH="${KAHUNA_RESTORE_STORAGE_PATH:-}"

PUBLISH_DIR="/tmp/kahuna-standalone-bin"

echo ">> Publishing Kahuna.Server to ${PUBLISH_DIR}"
dotnet publish Kahuna.Server/Kahuna.Server.csproj -c Release -p:PublishReadyToRun=true -o "${PUBLISH_DIR}"

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
  # Determine the storage path: restored or fresh.
  if [ -n "${RESTORE_STORAGE_PATH}" ]; then
    echo ">> Storage: rocksdb (restored) from ${RESTORE_STORAGE_PATH}"
    STORAGE_PATH="${RESTORE_STORAGE_PATH}"
  else
    echo ">> Storage: rocksdb (persistent) at ${DATA_DIR}/data"
    STORAGE_PATH="${DATA_DIR}/data"
  fi

  # Create only the parent dirs; RocksDB creates each <path>/<revision> dir itself
  # and uses its existence to detect a pre-existing (versioned) store.
  mkdir -p "${DATA_DIR}/wal" "${STORAGE_PATH}"
  ARGS+=(
    --storage rocksdb --storage-path "${STORAGE_PATH}" --storage-revision v1
    --wal-storage rocksdb --wal-path "${DATA_DIR}/wal" --wal-revision v1
  )
fi

if [ -n "${BACKUP_DIR}" ]; then
  echo ">> Backup: PITR enabled at ${BACKUP_DIR}"
  mkdir -p "${BACKUP_DIR}"
  ARGS+=(--pitr-backup-dir "${BACKUP_DIR}")
else
  echo ">> Backup: disabled (set KAHUNA_BACKUP_DIR to enable PITR)"
fi

echo ">> Starting standalone node on https://127.0.0.1:${HTTPS_PORT} (http ${HTTP_PORT})"
exec dotnet Kahuna.Server.dll "${ARGS[@]}"
