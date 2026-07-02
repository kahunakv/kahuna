#!/usr/bin/env bash
#
# Build and publish the standalone Kahuna server image to Docker Hub (kahunakv/kahuna).
#
# Tags pushed: the <Version> from Kahuna.Core/Kahuna.Core.csproj (e.g. 0.5.7) and "latest".
#
# Usage:
#   docker/publish.sh                 # build + push :<version> and :latest
#   VERSION=0.5.7 docker/publish.sh   # override the version tag
#   PLATFORMS=linux/amd64 docker/publish.sh   # restrict target platforms
#   PUSH=0 docker/publish.sh          # build locally only, do not push
#
# Requires: docker with buildx, and `docker login` already done for the kahunakv org.

set -euo pipefail

# Resolve repo root from this script's location so it works from any cwd.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

IMAGE="${IMAGE:-kahunakv/kahuna}"
DOCKERFILE="$SCRIPT_DIR/Dockerfile.standalone"
PLATFORMS="${PLATFORMS:-linux/amd64,linux/arm64}"
PUSH="${PUSH:-1}"

# Derive the version tag from the csproj <Version> unless overridden.
if [[ -z "${VERSION:-}" ]]; then
  VERSION="$(grep -oE '<Version>[^<]+</Version>' "$REPO_ROOT/Kahuna.Core/Kahuna.Core.csproj" \
    | head -n1 | sed -E 's/<\/?Version>//g')"
fi

if [[ -z "$VERSION" ]]; then
  echo "error: could not determine version (set VERSION=...)" >&2
  exit 1
fi

echo "Image:     $IMAGE"
echo "Version:   $VERSION"
echo "Platforms: $PLATFORMS"
echo "Push:      $PUSH"
echo

# Ensure a buildx builder exists for multi-platform builds.
if ! docker buildx inspect kahuna-builder >/dev/null 2>&1; then
  docker buildx create --name kahuna-builder --use
else
  docker buildx use kahuna-builder
fi

BUILD_ARGS=(
  --file "$DOCKERFILE"
  --tag "$IMAGE:$VERSION"
  --tag "$IMAGE:latest"
)

if [[ "$PUSH" == "1" ]]; then
  # Multi-platform images can only be exported by pushing to a registry.
  BUILD_ARGS+=(--platform "$PLATFORMS" --push)
else
  # --load imports into the local docker engine and supports one platform only.
  echo "PUSH=0: building for the local platform only (no push)."
  BUILD_ARGS+=(--load)
fi

set -x
docker buildx build "${BUILD_ARGS[@]}" "$REPO_ROOT"
set +x

echo
echo "Done: $IMAGE:$VERSION (and :latest)"
