#!/usr/bin/env bash
#
# Start a server from a release archive and run one write and one read against it. The release
# workflow runs this on a runner of the archive's own platform before it attaches the archive to the
# release, so an archive that cannot load its native libraries (RocksDB, the .NET host) never ships.
#
# --version alone is not enough: it exits before the storage engine starts, so it does not load
# librocksdb. This script starts a standalone node, writes a persistent key over REST, reads it back
# over REST and with the archive's kahuna-cli (gRPC), and stops the node.
#
# Usage:
#   scripts/smoke-test-release.sh artifacts/release/kahuna-1.17.3-osx-arm64.tar.gz [http-port]

set -euo pipefail

ARCHIVE="${1:?usage: scripts/smoke-test-release.sh <archive.tar.gz> [http-port]}"
HTTP_PORT="${2:-18081}"
H2C_PORT="$((HTTP_PORT + 2))"

WORK="$(mktemp -d)"
SERVER_PID=""

cleanup() {
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill -TERM "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT

tar -xzf "$ARCHIVE" -C "$WORK"
APP_DIR="$(find "$WORK" -mindepth 1 -maxdepth 1 -type d | head -n1)"
SERVER="$APP_DIR/server/Kahuna.Server"
CLI="$APP_DIR/cli/Kahuna.Control"

# The command-line parser writes the version to stderr, after the banner on stdout.
echo "== $("$SERVER" --version 2>&1 | grep -m1 '^Kahuna.Server ')"
echo "== $("$CLI" --version 2>&1 | grep -m1 '^Kahuna.Control ')"

# KAHUNA_HOME keeps the storage and the write-ahead log inside the scratch directory. The directory
# does not exist yet, so the test also checks that the node creates it, as the Homebrew service needs.
export KAHUNA_HOME="$WORK/home"
"$SERVER" \
  --http-ports "$HTTP_PORT" \
  --grpc-cleartext-ports "$H2C_PORT" \
  --raft-port "$HTTP_PORT" \
  > "$WORK/server.log" 2>&1 &
SERVER_PID=$!

URL="http://localhost:$HTTP_PORT"

fail() {
  echo "error: $1" >&2
  cat "$WORK/server.log" >&2
  exit 1
}

# The health route returns 200 only when the node can serve requests.
ready=0
for _ in $(seq 1 90); do
  if curl -sf -o /dev/null "$URL/v1/cluster/health"; then
    ready=1
    break
  fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    fail "the server exited during startup"
  fi
  sleep 1
done

if [[ "$ready" != "1" ]]; then
  fail "the server did not become ready"
fi

post() {
  curl -s -H 'Content-Type: application/json' -d "$2" "$URL$1"
}

# "cmVsZWFzZQ==" is "release" in base64. Durability 1 is persistent, so the write goes through Raft
# and the RocksDB backend. Response type 0 is Set and 3 is Get.
SET_BODY='{"key":"smoke","value":"cmVsZWFzZQ==","durability":1}'
GET_BODY='{"key":"smoke","durability":1}'

set_ok=0
for _ in $(seq 1 10); do
  response="$(post /v1/kv/try-set "$SET_BODY")"
  echo "/v1/kv/try-set -> $response"
  if [[ "$response" == *'"type":0'* ]]; then
    set_ok=1
    break
  fi
  sleep 1
done

if [[ "$set_ok" != "1" ]]; then
  fail "/v1/kv/try-set did not return type 0 (Set)"
fi

response="$(post /v1/kv/try-get "$GET_BODY")"
echo "/v1/kv/try-get -> $response"
if [[ "$response" != *'"type":3'* || "$response" != *'"value":"cmVsZWFzZQ=="'* ]]; then
  fail "the value written was not read back"
fi

# The CLI reads the key over gRPC on the cleartext HTTP/2 port. It prints "r<revision> get <value>".
cli_output="$("$CLI" --connection-source "http://localhost:$H2C_PORT" --get smoke 2>&1)" || true
echo "kahuna-cli --get smoke -> $cli_output"
if [[ "$cli_output" != *"get release"* || "$cli_output" == *"not get"* ]]; then
  fail "kahuna-cli did not read the value back"
fi

if [[ ! -d "$KAHUNA_HOME/data" || ! -d "$KAHUNA_HOME/wal" ]]; then
  fail "the node did not create its data and wal directories under KAHUNA_HOME"
fi

echo "== smoke test passed"
