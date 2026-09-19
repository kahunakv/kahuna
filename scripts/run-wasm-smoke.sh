#!/usr/bin/env bash
#
# Thread-free build smoke check.
#
# Builds Kahuna.WasmSmoke for the default single-threaded browser-wasm runtime and runs it under
# Node.js. The app boots a single-node embedded Kahuna, waits for it to elect itself, runs a
# key-value transaction and a transaction script, reads both values back, and disposes the node. The
# check fails on a PlatformNotSupportedException (a Thread start or a blocking wait that the runtime
# refuses), on a stall (a deadlock), or on a wrong result.
#
# Needs the .NET 10 SDK and Node.js. It does not need the wasm-tools workload: the app does no
# native relinking or AOT.
#
# Usage:
#   scripts/run-wasm-smoke.sh              build (Release) and run
#   SMOKE_TIMEOUT=300 scripts/run-wasm-smoke.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_DIR="$REPO_ROOT/Kahuna.WasmSmoke"
CONFIGURATION="${CONFIGURATION:-Release}"
SMOKE_TIMEOUT="${SMOKE_TIMEOUT:-300}"

dotnet build "$PROJECT_DIR/Kahuna.WasmSmoke.csproj" -c "$CONFIGURATION"

APP_DIR="$PROJECT_DIR/bin/$CONFIGURATION/net10.0-browser/wwwroot"

if [ ! -f "$APP_DIR/_framework/dotnet.js" ]; then
  echo "run-wasm-smoke: no _framework/dotnet.js under $APP_DIR" >&2
  exit 1
fi

cp "$PROJECT_DIR/main.mjs" "$APP_DIR/main.mjs"

# A whole-process timeout on top of the app's own per-phase deadlines: a hang inside the runtime
# itself never reaches the app's deadline code. perl is used because macOS has no `timeout`.
cd "$APP_DIR"
exec perl -e 'alarm shift; exec @ARGV' "$SMOKE_TIMEOUT" node main.mjs
