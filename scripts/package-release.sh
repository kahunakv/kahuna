#!/usr/bin/env bash
#
# Build one self-contained release archive: kahuna-<version>-<rid>.tar.gz, plus a .sha256 file
# beside it. The release workflow (.github/workflows/release.yml) runs this once per platform, and the
# Homebrew formula downloads the result. The archive holds two applications:
#
#   server/  the node (Kahuna.Server), run as kahuna-server
#   cli/     the command-line client (Kahuna.Control), run as kahuna-cli
#
# Each application is published into its own directory, because the two have different dependency
# graphs and one publish would overwrite files of the other in a shared directory.
#
# Self-contained means each application carries the .NET runtime, so the machine that installs it
# needs no .NET of its own, and a Homebrew upgrade of the `dotnet` formula cannot break an installed
# node. The server is not trimmed and not compiled ahead of time: the extension loader loads
# assemblies at run time, and trimming removes code those assemblies may need. ReadyToRun precompiles
# both applications, to cut JIT work on the first requests and on each CLI start.
#
# Usage:
#   scripts/package-release.sh <rid> [output-dir]
#   VERSION=1.17.3 scripts/package-release.sh osx-arm64 artifacts/release
#
# <rid> is a .NET runtime identifier: osx-arm64, osx-x64, linux-x64 or linux-arm64. The version comes
# from <Version> in Kahuna.Server/Kahuna.Server.csproj unless VERSION overrides it. scripts/set-version.sh
# writes the same version into both projects.

set -euo pipefail

RID="${1:?usage: scripts/package-release.sh <rid> [output-dir]}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT="$REPO_ROOT/Kahuna.Server/Kahuna.Server.csproj"
CLI_PROJECT="$REPO_ROOT/Kahuna.Control/Kahuna.Control.csproj"
OUTPUT="${2:-$REPO_ROOT/artifacts/release}"

if [[ -z "${VERSION:-}" ]]; then
  VERSION="$(grep -oE '<Version>[^<]+</Version>' "$PROJECT" | head -n1 | sed -E 's/<\/?Version>//g')"
fi

if [[ -z "$VERSION" ]]; then
  echo "error: could not determine version (set VERSION=...)" >&2
  exit 1
fi

NAME="kahuna-$VERSION-$RID"
mkdir -p "$OUTPUT"
OUTPUT="$(cd "$OUTPUT" && pwd)"
STAGE="$OUTPUT/$NAME"
ARCHIVE="$OUTPUT/$NAME.tar.gz"

echo "Archive: $NAME.tar.gz"
echo "Output:  $OUTPUT"
echo

rm -rf "$STAGE" "$ARCHIVE" "$ARCHIVE.sha256"

publish() {
  dotnet publish "$1" \
    --configuration Release \
    --runtime "$RID" \
    --self-contained \
    -p:Version="$VERSION" \
    -p:PublishReadyToRun=true \
    -p:RunAnalyzers=false \
    -o "$2"
}

publish "$PROJECT" "$STAGE/server"
publish "$CLI_PROJECT" "$STAGE/cli"

cp "$REPO_ROOT/README.md" "$REPO_ROOT/LICENSE" "$STAGE/"

# COPYFILE_DISABLE keeps macOS tar from adding AppleDouble (._*) entries for extended attributes.
COPYFILE_DISABLE=1 tar -czf "$ARCHIVE" -C "$OUTPUT" "$NAME"
rm -rf "$STAGE"

if command -v sha256sum >/dev/null 2>&1; then
  (cd "$OUTPUT" && sha256sum "$NAME.tar.gz" > "$NAME.tar.gz.sha256")
else
  (cd "$OUTPUT" && shasum -a 256 "$NAME.tar.gz" > "$NAME.tar.gz.sha256")
fi

echo
echo "Wrote $ARCHIVE"
cat "$ARCHIVE.sha256"
