#!/bin/bash
#
# Measures garbage-collector pauses of one standalone Kahuna node under kahuna-bench load.
#
# The script starts the node with scripts/run-standalone.sh, waits for it to answer, attaches
# dotnet-counters (System.Runtime meter, one sample per second, CSV), runs the benchmark client,
# optionally takes one heap dump (dotnet-gcdump) in the middle of the run, stops the node, and
# summarizes the counters with scripts/gc-summary.py. Every artifact lands in one directory so a
# baseline and a later run are compared with the same commands.
#
# Requirements: dotnet-counters and dotnet-gcdump on PATH (dotnet tool install -g dotnet-counters
# dotnet-gcdump), python3, curl, and a Release build of Kahuna.Benchmark
# (dotnet build Kahuna.Benchmark/Kahuna.Benchmark.csproj -c Release).
#
# Overridable via environment variables:
#   KAHUNA_STORAGE        storage backend for the node: memory (default) or rocksdb
#   KAHUNA_PARTITIONS     initial partitions (default 8)
#   KAHUNA_EXTRA_ARGS     extra server flags, e.g. "--pinned-value-payloads --trim-flushed-revisions"
#   KAHUNA_SKIP_PUBLISH   1 = reuse /tmp/kahuna-standalone-bin instead of publishing (default 0)
#   BENCH_WORKLOAD        kahuna-bench workload (default mixed)
#   BENCH_DURATION        measured seconds (default 30)
#   BENCH_WARMUP          warm-up seconds (default 5)
#   BENCH_CONCURRENCY     concurrent clients (default 128)
#   BENCH_KEY_SPACE       distinct keys (default 300000)
#   BENCH_VALUE_SIZE      value bytes (default 1024)
#   BENCH_DURABILITY      persistent (default) or ephemeral
#   BENCH_EXTRA_ARGS      extra kahuna-bench flags
#   GCDUMP_AT             seconds after the client starts to take one heap dump; empty = no dump
#   GC_OUT_DIR            artifact directory (default gc-runs/<UTC timestamp>)
#   GC_SERVER_ENV         extra environment for the server process, e.g. "DOTNET_GCgen0size=0x4000000"
#
set -euo pipefail

cd "$(dirname "$0")/.."
REPO_ROOT="$(pwd)"

STORAGE="${KAHUNA_STORAGE:-memory}"
PARTITIONS="${KAHUNA_PARTITIONS:-8}"
WORKLOAD="${BENCH_WORKLOAD:-mixed}"
DURATION="${BENCH_DURATION:-30}"
WARMUP="${BENCH_WARMUP:-5}"
CONCURRENCY="${BENCH_CONCURRENCY:-128}"
KEY_SPACE="${BENCH_KEY_SPACE:-300000}"
VALUE_SIZE="${BENCH_VALUE_SIZE:-1024}"
DURABILITY="${BENCH_DURABILITY:-persistent}"
BENCH_EXTRA="${BENCH_EXTRA_ARGS:-}"
GCDUMP_AT="${GCDUMP_AT:-}"
OUT_DIR="${GC_OUT_DIR:-${REPO_ROOT}/gc-runs/$(date -u +%Y%m%dT%H%M%SZ)}"
SERVER_ENV="${GC_SERVER_ENV:-}"

HTTP_PORT="${KAHUNA_HTTP_PORT:-8081}"
H2C_PORT="${KAHUNA_H2C_PORT:-8083}"
BENCH_DLL="${REPO_ROOT}/Kahuna.Benchmark/bin/Release/net10.0/Kahuna.Benchmark.dll"

for tool in dotnet-counters python3 curl; do
  command -v "${tool}" >/dev/null || { echo "!! ${tool} not found on PATH" >&2; exit 1; }
done
if [ -n "${GCDUMP_AT}" ]; then
  command -v dotnet-gcdump >/dev/null || { echo "!! dotnet-gcdump not found on PATH" >&2; exit 1; }
fi
if [ ! -f "${BENCH_DLL}" ]; then
  echo ">> Building Kahuna.Benchmark (Release)"
  dotnet build Kahuna.Benchmark/Kahuna.Benchmark.csproj -c Release -v quiet
fi

mkdir -p "${OUT_DIR}"
echo ">> Artifacts: ${OUT_DIR}"

{
  echo "date_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "git_head=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
  echo "git_dirty=$(git status --porcelain 2>/dev/null | grep -c . || true)"
  echo "storage=${STORAGE} partitions=${PARTITIONS} extra_args=${KAHUNA_EXTRA_ARGS:-} server_env=${SERVER_ENV}"
  echo "workload=${WORKLOAD} duration=${DURATION} warmup=${WARMUP} concurrency=${CONCURRENCY}"
  echo "key_space=${KEY_SPACE} value_size=${VALUE_SIZE} durability=${DURABILITY} bench_extra=${BENCH_EXTRA}"
  echo "host=$(uname -srm) cpus=$(getconf _NPROCESSORS_ONLN) mem_bytes=$(sysctl -n hw.memsize 2>/dev/null || grep MemTotal /proc/meminfo | awk '{print $2*1024}')"
  echo "dotnet=$(dotnet --version)"
} > "${OUT_DIR}/run.env"

SERVER_PID=""
COUNTERS_PID=""

cleanup() {
  if [ -n "${COUNTERS_PID}" ] && kill -0 "${COUNTERS_PID}" 2>/dev/null; then
    kill -INT "${COUNTERS_PID}" 2>/dev/null || true
    wait "${COUNTERS_PID}" 2>/dev/null || true
  fi
  if [ -n "${SERVER_PID}" ] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill -TERM "${SERVER_PID}" 2>/dev/null || true
    for _ in $(seq 1 50); do
      kill -0 "${SERVER_PID}" 2>/dev/null || break
      sleep 0.2
    done
    kill -KILL "${SERVER_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

echo ">> Starting the node (storage=${STORAGE}, partitions=${PARTITIONS})"
# Intentional word split of SERVER_ENV: it carries zero or more VAR=value pairs.
# shellcheck disable=SC2086
env ${SERVER_ENV} \
  KAHUNA_STORAGE="${STORAGE}" KAHUNA_PARTITIONS="${PARTITIONS}" KAHUNA_LOG_LEVEL="${KAHUNA_LOG_LEVEL:-Warning}" \
  KAHUNA_SKIP_PUBLISH="${KAHUNA_SKIP_PUBLISH:-0}" KAHUNA_EXTRA_ARGS="${KAHUNA_EXTRA_ARGS:-}" \
  KAHUNA_HTTP_PORT="${HTTP_PORT}" KAHUNA_H2C_PORT="${H2C_PORT}" \
  "${REPO_ROOT}/scripts/run-standalone.sh" > "${OUT_DIR}/server.log" 2>&1 &
SERVER_PID=$!

echo ">> Waiting for http://127.0.0.1:${HTTP_PORT}/v1/cluster/health"
for i in $(seq 1 600); do
  if curl -fsS "http://127.0.0.1:${HTTP_PORT}/v1/cluster/health" >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
    echo "!! the node exited before it became healthy; see ${OUT_DIR}/server.log" >&2
    exit 1
  fi
  if [ "${i}" = "600" ]; then
    echo "!! the node did not become healthy in 300 s" >&2
    exit 1
  fi
  sleep 0.5
done

# run-standalone.sh exec's into dotnet, so its PID is the server's PID once the publish step is done.
# Still, resolve the dotnet process by its listening port in case a shell layer remained.
DOTNET_PID="$(lsof -nP -iTCP:"${H2C_PORT}" -sTCP:LISTEN -t 2>/dev/null | head -n1 || true)"
DOTNET_PID="${DOTNET_PID:-${SERVER_PID}}"
echo "server_pid=${DOTNET_PID}" >> "${OUT_DIR}/run.env"
echo ">> Node is healthy (pid ${DOTNET_PID}); letting it settle for 3 s"
sleep 3

TOTAL_SECONDS=$((DURATION + WARMUP + 20))
COUNTERS_DURATION="$(printf '00:00:%02d:%02d' $((TOTAL_SECONDS / 60)) $((TOTAL_SECONDS % 60)))"
echo ">> Collecting System.Runtime counters for ${TOTAL_SECONDS} s -> counters.csv"
dotnet-counters collect -p "${DOTNET_PID}" --counters System.Runtime --format csv \
  -o "${OUT_DIR}/counters.csv" --refresh-interval 1 --duration "${COUNTERS_DURATION}" \
  > "${OUT_DIR}/counters.log" 2>&1 &
COUNTERS_PID=$!
sleep 2

echo ">> Running kahuna-bench (${WORKLOAD}, ${DURATION} s + ${WARMUP} s warm-up, ${CONCURRENCY} clients, ${KEY_SPACE} keys x ${VALUE_SIZE} B, ${DURABILITY})"
BENCH_START="$(date +%s)"
echo "bench_start_epoch=${BENCH_START}" >> "${OUT_DIR}/run.env"
# shellcheck disable=SC2086
dotnet "${BENCH_DLL}" -c "http://127.0.0.1:${H2C_PORT}" \
  --workload "${WORKLOAD}" --duration "${DURATION}" --warmup "${WARMUP}" --concurrency "${CONCURRENCY}" \
  --key-space "${KEY_SPACE}" --value-size "${VALUE_SIZE}" --durability "${DURABILITY}" \
  --format json --output "${OUT_DIR}/bench.json" ${BENCH_EXTRA} > "${OUT_DIR}/bench.log" 2>&1 &
BENCH_PID=$!

if [ -n "${GCDUMP_AT}" ]; then
  sleep "${GCDUMP_AT}"
  if kill -0 "${BENCH_PID}" 2>/dev/null; then
    echo ">> Taking a heap dump at +${GCDUMP_AT} s -> heap.gcdump (this induces one blocking collection)"
    echo "gcdump_epoch=$(date +%s)" >> "${OUT_DIR}/run.env"
    dotnet-gcdump collect -p "${DOTNET_PID}" -o "${OUT_DIR}/heap.gcdump" -t 120 > "${OUT_DIR}/gcdump.log" 2>&1 || \
      echo "!! gcdump failed; see ${OUT_DIR}/gcdump.log" >&2
  fi
fi

wait "${BENCH_PID}" || echo "!! kahuna-bench exited with an error; see ${OUT_DIR}/bench.log" >&2
echo "bench_end_epoch=$(date +%s)" >> "${OUT_DIR}/run.env"

echo ">> Benchmark finished; waiting for the counter session to close"
wait "${COUNTERS_PID}" 2>/dev/null || true
COUNTERS_PID=""

if [ -f "${OUT_DIR}/heap.gcdump" ]; then
  echo ">> Heap report -> heap-report.txt"
  dotnet-gcdump report "${OUT_DIR}/heap.gcdump" > "${OUT_DIR}/heap-report.txt" 2>&1 || true
fi

echo ">> Stopping the node"
cleanup
SERVER_PID=""

grep -E "Slow dispatch|took [0-9]+ms" "${OUT_DIR}/server.log" > "${OUT_DIR}/slow-dispatch.log" || true

echo
python3 "${REPO_ROOT}/scripts/gc-summary.py" "${OUT_DIR}" | tee "${OUT_DIR}/summary.txt"
