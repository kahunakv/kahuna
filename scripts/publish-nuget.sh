#!/usr/bin/env bash
#
# Pack and publish the Kahuna.Server .NET global tool to nuget.org.
#
# Installed by end users as:
#   dotnet tool install -g Kahuna.Server
#   kahuna-server
#
# Tags pushed: the <Version> from Kahuna.Server/Kahuna.Server.csproj.
#
# Usage:
#   scripts/publish-nuget.sh                 # pack + push
#   VERSION=1.0.3 scripts/publish-nuget.sh   # override the version
#   PUSH=0 scripts/publish-nuget.sh          # pack locally only, do not push
#
# Requires: NUGET_API_KEY in the environment when PUSH=1.

set -euo pipefail

# Resolve repo root from this script's location so it works from any cwd.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

PROJECT="$REPO_ROOT/Kahuna.Server/Kahuna.Server.csproj"
OUTPUT="$REPO_ROOT/Kahuna.Server/nupkg"
PACKAGE_ID="Kahuna.Server"
SOURCE="${SOURCE:-https://api.nuget.org/v3/index.json}"
PUSH="${PUSH:-1}"

# Derive the version from the csproj <Version> unless overridden, so the NuGet version, the Docker
# tag and the assembly version cannot drift.
if [[ -z "${VERSION:-}" ]]; then
  VERSION="$(grep -oE '<Version>[^<]+</Version>' "$PROJECT" | head -n1 | sed -E 's/<\/?Version>//g')"
fi

if [[ -z "$VERSION" ]]; then
  echo "error: could not determine version (set VERSION=...)" >&2
  exit 1
fi

PACKAGE="$OUTPUT/$PACKAGE_ID.$VERSION.nupkg"

echo "Package:  $PACKAGE_ID"
echo "Version:  $VERSION"
echo "Source:   $SOURCE"
echo "Push:     $PUSH"
echo

# PackAsTool/PackageId live in the csproj — passing -p:PackageId here would flow the property into
# the whole restore graph and collide with the referenced projects.
rm -f "$PACKAGE"

set -x
dotnet pack "$PROJECT" -c Release -p:PackageVersion="$VERSION" -o "$OUTPUT"
set +x

if [[ ! -f "$PACKAGE" ]]; then
  echo "error: expected package not produced: $PACKAGE" >&2
  exit 1
fi

echo
echo "Packed: $PACKAGE ($(du -h "$PACKAGE" | cut -f1))"

if [[ "$PUSH" != "1" ]]; then
  echo "PUSH=0: skipping push."
  exit 0
fi

if [[ -z "${NUGET_API_KEY:-}" ]]; then
  echo "error: NUGET_API_KEY is not set (or use PUSH=0 to pack only)" >&2
  exit 1
fi

set -x
dotnet nuget push "$PACKAGE" --source "$SOURCE" --api-key "$NUGET_API_KEY" --skip-duplicate
set +x

echo
echo "Done: $PACKAGE_ID $VERSION"
