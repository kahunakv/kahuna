#!/usr/bin/env bash
#
# Thread-free build smoke check.
#
# Publishes Kahuna.WasmSmoke for the default single-threaded browser-wasm runtime and runs it under
# Node.js. The app boots a single-node embedded Kahuna, waits for it to elect itself, runs a
# key-value transaction and a transaction script, reads both values back, and disposes the node. It
# then starts a 3-node in-memory cluster, writes through each node, checks that leadership stays
# stable for 60 s, stops the meta-partition leader, restarts it, and then isolates one node to show
# that a minority cannot commit while the majority elects a leader and commits. The check fails on a
# PlatformNotSupportedException (a Thread start or a blocking wait that the runtime refuses), on a
# stall (a deadlock), on an election while no fault is injected, or on a wrong result.
#
# Needs the .NET 11 SDK and Node.js. It does not need the wasm-tools workload: the app does no
# native relinking or AOT.
#
# Usage:
#   scripts/run-wasm-smoke.sh              publish (Release) for net10.0-browser and run
#   SMOKE_FRAMEWORK=net11.0-browser scripts/run-wasm-smoke.sh
#   SMOKE_TIMEOUT=300 scripts/run-wasm-smoke.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_DIR="$REPO_ROOT/Kahuna.WasmSmoke"
CONFIGURATION="${CONFIGURATION:-Release}"
SMOKE_TIMEOUT="${SMOKE_TIMEOUT:-300}"
SMOKE_FRAMEWORK="${SMOKE_FRAMEWORK:-net10.0-browser}"

# Publish, not build: from .NET 11 the build output keeps the runtime files out of wwwroot/_framework
# (only dotnet.js is there) and a development server supplies them, so Node.js cannot load the app
# from it. A publish writes the complete folder on every SDK. Trimming stays off so the app runs the
# same code as a build: the smoke check is about threads, not about what the trimmer removes.
PUBLISH_DIR="$PROJECT_DIR/bin/$CONFIGURATION/$SMOKE_FRAMEWORK/publish"

dotnet publish "$PROJECT_DIR/Kahuna.WasmSmoke.csproj" -c "$CONFIGURATION" \
  -p:KahunaSmokeFramework="$SMOKE_FRAMEWORK" -p:PublishTrimmed=false -o "$PUBLISH_DIR"

APP_DIR="$PUBLISH_DIR/wwwroot"

if [ ! -f "$APP_DIR/_framework/dotnet.js" ]; then
  echo "run-wasm-smoke: no _framework/dotnet.js under $APP_DIR" >&2
  exit 1
fi

cp "$PROJECT_DIR/main.mjs" "$APP_DIR/main.mjs"

# A whole-process timeout on top of the app's own per-phase deadlines: a hang inside the runtime
# itself never reaches the app's deadline code. perl is used because macOS has no `timeout`.
cd "$APP_DIR"
exec perl -e 'alarm shift; exec @ARGV' "$SMOKE_TIMEOUT" node main.mjs
