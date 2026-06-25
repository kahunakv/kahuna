#!/bin/bash
#
# Runs a 3-node Kahuna cluster on localhost for local development and integration testing.
#
# Each node uses a distinct port pair (HTTP/HTTPS+Raft) and its own storage directory.
# Ctrl+C stops all three nodes cleanly.
#
# Overridable via environment variables:
#   KAHUNA_STORAGE      storage backend: rocksdb (default, persistent) or memory (ephemeral)
#   KAHUNA_PARTITIONS   initial partition count (default 3; 128 is cluster-scale)
#   KAHUNA_DATA_DIR     base directory for node data/WAL when using rocksdb
#                       (default /tmp/kahuna-cluster)
#   KAHUNA_WAL_INSTRUMENT  set to 1 to enable WAL double-fsync phase instrumentation; each node
#                       logs a per-phase snapshot (propose/commit/followerAppend) + fsync counters on Ctrl+C
#   KAHUNA_WAL_SINGLE_FSYNC  single-fsync fast path (ack on propose-quorum + lazy commit marker).
#                       ON by default now; set to 0 to A/B against the legacy two-fsync path.
#   KAHUNA_WIPE         set to 1 to delete the rocksdb store before starting (clean A/B state every run;
#                       also avoids replaying a store written by a different Kommander version)
#   KAHUNA_LINGER_MS    WAL group-commit linger window in ms (default 0 = disabled, the measured best
#                       pairing with the fast path). Raise to re-enable cross-write fsync coalescing.
#
# Port layout (all on 127.0.0.1):
#   Node 1 — HTTP :8081  HTTPS/Raft :8082
#   Node 2 — HTTP :8083  HTTPS/Raft :8084
#   Node 3 — HTTP :8085  HTTPS/Raft :8086
#
set -euo pipefail

cd "$(dirname "$0")/.."
REPO_ROOT="$(pwd)"

STORAGE="${KAHUNA_STORAGE:-rocksdb}"
PARTITIONS="${KAHUNA_PARTITIONS:-3}"
DATA_DIR="${KAHUNA_DATA_DIR:-/tmp/kahuna-cluster}"

PUBLISH_DIR="/tmp/kahuna-cluster-bin"
HOST="127.0.0.1"

# ── Build ────────────────────────────────────────────────────────────────────
echo ">> Publishing Kahuna.Server to ${PUBLISH_DIR}"
dotnet publish Kahuna.Server/Kahuna.Server.csproj -c Release -p:PublishReadyToRun=true -o "${PUBLISH_DIR}"
cp "${REPO_ROOT}/certs/development-certificate.pfx" "${PUBLISH_DIR}/certificate.pfx"

# ── Storage directories ───────────────────────────────────────────────────────
if [ "${STORAGE}" = "rocksdb" ]; then
    # KAHUNA_WIPE=1 deletes the persistent store before starting, so a benchmark A/B begins from a
    # clean, identical state every run. Without it the store is reused across runs (handy for dev,
    # but a confound for benchmarking — and replaying a store written by a different Kommander
    # version can wedge recovery). Only ever removes the cluster's own DATA_DIR.
    if [ "${KAHUNA_WIPE:-}" = "1" ]; then
        echo ">> KAHUNA_WIPE=1: removing ${DATA_DIR} for a clean store"
        rm -rf "${DATA_DIR}"
    fi
    echo ">> Storage: rocksdb (persistent) under ${DATA_DIR}"
    for n in 1 2 3; do
        # Create only the parent dirs; RocksDB creates each <path>/<revision> dir
        # itself and uses its existence to detect a pre-existing (versioned) store.
        mkdir -p "${DATA_DIR}/node${n}/wal" "${DATA_DIR}/node${n}/data"
    done
else
    echo ">> Storage: memory (fully ephemeral)"
fi

# ── Process tracking ──────────────────────────────────────────────────────────
pids=()

# Named FIFOs for per-node log labeling. Using FIFOs (rather than process
# substitution "> >(sed)") guarantees that $! after "dotnet ... &" is the dotnet
# PID, not a sed process substitution PID — which is what broke Ctrl+C on bash 3.2.
FIFO_DIR=$(mktemp -d)

cleanup() {
    echo ""
    echo ">> Stopping cluster (sending SIGTERM)..."
    if [ "${#pids[@]}" -gt 0 ]; then
        for pid in "${pids[@]}"; do
            kill -TERM "$pid" 2>/dev/null || true
        done
        # Give nodes up to 5 s for graceful shutdown, then force-kill survivors.
        local i=0
        while [ $i -lt 50 ]; do
            local alive=0
            for pid in "${pids[@]}"; do
                kill -0 "$pid" 2>/dev/null && alive=1 && break
            done
            [ $alive -eq 0 ] && break
            sleep 0.1
            i=$((i + 1))
        done
        for pid in "${pids[@]}"; do
            kill -KILL "$pid" 2>/dev/null || true
        done
        for pid in "${pids[@]}"; do
            wait "$pid" 2>/dev/null || true
        done
    fi
    rm -rf "$FIFO_DIR"
    echo ">> Cluster stopped."
}
trap cleanup SIGINT SIGTERM

# ── Node launcher ─────────────────────────────────────────────────────────────
# Usage: start_node <id> <http_port> <https_port> <peer1_addr> <peer2_addr>
start_node() {
    local id=$1 http_port=$2 https_port=$3 peer1=$4 peer2=$5

    local args=(
        --raft-nodename        "kahuna${id}"
        --raft-nodeid          "${id}"
        --raft-host            "${HOST}"
        --raft-port            "${https_port}"
        --http-ports           "${http_port}"
        --https-ports          "${https_port}"
        --https-certificate    "${PUBLISH_DIR}/certificate.pfx"
        --raft-allow-insecure-certificate-validation
        --initial-cluster      "${peer1}" "${peer2}"
        --initial-cluster-partitions "${PARTITIONS}"
    )

    # Diagnostic knob: force a single gRPC stream per peer to remove cross-stream reordering
    # of AppendLogs (set KAHUNA_GRPC_CHANNELS=1). Unset => server default (4).
    if [ -n "${KAHUNA_GRPC_CHANNELS:-}" ]; then
        args+=(--raft-grpc-channels-per-node "${KAHUNA_GRPC_CHANNELS}")
    fi

    # Diagnostic knob: KAHUNA_SHARED_POOL=0 reverts to the original one-OS-thread-per-partition
    # model (disables the shared executor pool). Honored via env in Program.cs because the CLI
    # bool is a bare switch and cannot express "false". Unset => server default (on).

    if [ "${STORAGE}" = "memory" ]; then
        args+=(--storage memory --wal-storage memory)
    else
        args+=(
            --storage          rocksdb
            --storage-path     "${DATA_DIR}/node${id}/data"
            --storage-revision v1
            --wal-storage      rocksdb
            --wal-path         "${DATA_DIR}/node${id}/wal"
            --wal-revision     v1
        )
    fi

    # Create named FIFOs for stdout/stderr so the sed labeling processes are
    # decoupled from dotnet. This keeps $! = dotnet PID after the "dotnet ... &"
    # line, which is required for cleanup to kill the right processes.
    # sed exits automatically once dotnet closes its end of the FIFO.
    local out_fifo="${FIFO_DIR}/node${id}_out"
    local err_fifo="${FIFO_DIR}/node${id}_err"
    mkfifo "$out_fifo" "$err_fifo"
    sed -u "s/^/[kahuna${id}] /"      < "$out_fifo"      &
    sed -u "s/^/[kahuna${id}] /" >&2  < "$err_fifo"      &

    # env passes the logging override directly to the child process, bypassing
    # the CommandLine argument parser which rejects unknown --Key:Sub=Value flags.
    # Dots in env var names require the `env` command; bash cannot export them directly.
    # Diagnostic: KAHUNA_RAFT_LOG=Debug surfaces Kommander proposal/replication logs so we can
    # see what is being written. Default Warning keeps the normal quiet output.
    # Diagnostic: KAHUNA_WAL_INSTRUMENT=1 brackets the node's lifetime as a WAL double-fsync
    # measurement window; each node logs a per-phase snapshot (propose/commit/followerAppend) on
    # graceful shutdown (Ctrl+C). Inert when unset.
    env "Logging__LogLevel__Kommander.IRaft=${KAHUNA_RAFT_LOG:-Warning}" \
        "KAHUNA_WAL_INSTRUMENT=${KAHUNA_WAL_INSTRUMENT:-}" \
        "KAHUNA_WAL_SINGLE_FSYNC=${KAHUNA_WAL_SINGLE_FSYNC:-}" \
        "KAHUNA_LINGER_MS=${KAHUNA_LINGER_MS:-}" \
        dotnet "${PUBLISH_DIR}/Kahuna.Server.dll" "${args[@]}" \
        > "$out_fifo" 2> "$err_fifo" &
    local pid=$!
    pids+=($pid)
    echo ">> kahuna${id} started (PID ${pid})  HTTP :${http_port}  HTTPS/Raft :${https_port}"
}

# ── Start the three nodes ────────────────────────────────────────────────────
# Each node's --initial-cluster lists the OTHER two nodes' Raft endpoints.
start_node 1  8081 8082  "${HOST}:8084" "${HOST}:8086"
start_node 2  8083 8084  "${HOST}:8082" "${HOST}:8086"
start_node 3  8085 8086  "${HOST}:8082" "${HOST}:8084"

echo ""
echo ">> 3-node cluster running. Press Ctrl+C to stop all nodes."
echo "   Node 1 — http://localhost:8081   https://localhost:8082"
echo "   Node 2 — http://localhost:8083   https://localhost:8084"
echo "   Node 3 — http://localhost:8085   https://localhost:8086"
echo ""

wait "${pids[@]}"
