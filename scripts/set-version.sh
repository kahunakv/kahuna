#!/usr/bin/env bash
#
# Set a single version across every packable project and pack the NuGet packages.
#
# A "packable project" here is a .csproj that carries a <PackageId> and does not opt out with
# <IsPackable>false</IsPackable>. Test projects and the vendored RadLine fork (no PackageId — it is
# consumed as a ProjectReference, never published) are skipped and reported.
#
# Usage:
#   scripts/set-version.sh 1.2.0                  # rewrite <Version> everywhere, then pack
#   scripts/set-version.sh 1.2.0 --no-pack        # only rewrite the csproj versions
#   scripts/set-version.sh 1.2.0 --dry-run        # show what would change, touch nothing
#   scripts/set-version.sh 1.2.0 -o /tmp/nupkg    # pack into a different output directory
#   scripts/set-version.sh 1.2.0 -c Debug         # pack a non-Release configuration
#
# Packages land in <repo>/nupkg by default. Pushing is left to scripts/publish-nuget.sh.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

VERSION=""
CONFIGURATION="Release"
OUTPUT="$REPO_ROOT/nupkg"
PACK=1
DRY_RUN=0

usage() {
  sed -n '2,20p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)          usage 0 ;;
    --no-pack)          PACK=0; shift ;;
    --dry-run)          DRY_RUN=1; shift ;;
    -c|--configuration) CONFIGURATION="$2"; shift 2 ;;
    -o|--output)        OUTPUT="$2"; shift 2 ;;
    -*)                 echo "error: unknown option: $1" >&2; usage 1 ;;
    *)
      if [[ -n "$VERSION" ]]; then
        echo "error: unexpected argument: $1" >&2
        usage 1
      fi
      VERSION="$1"; shift ;;
  esac
done

if [[ -z "$VERSION" ]]; then
  echo "error: no version given" >&2
  usage 1
fi

# Accept NuGet-shaped versions: 1.2.3, 1.2.3.4, 1.2.3-beta.1, 1.2.3+build.5 — reject typos early so
# a bad string does not get written into eight project files.
if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(\.[0-9]+)?(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$ ]]; then
  echo "error: '$VERSION' is not a valid NuGet version (expected e.g. 1.2.0, 1.2.0-rc.1)" >&2
  exit 1
fi

# Discover packable projects instead of hardcoding a list, so a new package cannot be forgotten.
PACKABLE=()
SKIPPED=()
while IFS= read -r project; do
  if grep -qE '<IsPackable>[[:space:]]*false[[:space:]]*</IsPackable>' "$project"; then
    SKIPPED+=("$project (IsPackable=false)")
    continue
  fi
  if ! grep -qE '<PackageId>' "$project"; then
    SKIPPED+=("$project (no PackageId)")
    continue
  fi
  PACKABLE+=("$project")
done < <(find "$REPO_ROOT" -name '*.csproj' -not -path '*/bin/*' -not -path '*/obj/*' | sort)

if [[ ${#PACKABLE[@]} -eq 0 ]]; then
  echo "error: no packable projects found under $REPO_ROOT" >&2
  exit 1
fi

echo "Version:       $VERSION"
echo "Configuration: $CONFIGURATION"
echo "Output:        $OUTPUT"
echo "Pack:          $PACK"
echo

for project in "${PACKABLE[@]}"; do
  rel="${project#"$REPO_ROOT"/}"
  package_id="$(grep -oE '<PackageId>[^<]+</PackageId>' "$project" | head -n1 | sed -E 's#</?PackageId>##g')"
  current="$(grep -oE '<Version>[^<]+</Version>' "$project" | head -n1 | sed -E 's#</?Version>##g')"

  printf '  %-34s %-18s %s -> %s\n' "$package_id" "$rel" "${current:-<none>}" "$VERSION"

  [[ "$DRY_RUN" -eq 1 ]] && continue

  if [[ -n "$current" ]]; then
    # Replace only the first <Version> so a stray version on a nested element is left alone.
    VERSION="$VERSION" perl -0777 -pi -e 's{<Version>[^<]*</Version>}{"<Version>$ENV{VERSION}</Version>"}se' "$project"
  else
    # No <Version> yet: put one right after <PackageId> so it lands in the packaging PropertyGroup.
    VERSION="$VERSION" perl -0777 -pi -e 's{(^([ \t]*)<PackageId>[^<]*</PackageId>[ \t]*\n)}{"$1$2<Version>$ENV{VERSION}</Version>\n"}sme' "$project"
  fi

  written="$(grep -oE '<Version>[^<]+</Version>' "$project" | head -n1 | sed -E 's#</?Version>##g')"
  if [[ "$written" != "$VERSION" ]]; then
    echo "error: failed to write <Version> into $rel (found '${written:-<none>}')" >&2
    exit 1
  fi
done

if [[ "$DRY_RUN" -eq 1 ]]; then
  echo
  echo "Dry run: nothing written."
fi

if [[ ${#SKIPPED[@]} -gt 0 ]]; then
  echo
  echo "Skipped (not packable):"
  for entry in "${SKIPPED[@]}"; do
    echo "  ${entry#"$REPO_ROOT"/}"
  done
fi

if [[ "$PACK" -ne 1 || "$DRY_RUN" -eq 1 ]]; then
  echo
  echo "Skipping pack."
  exit 0
fi

mkdir -p "$OUTPUT"

echo
echo "Packing ${#PACKABLE[@]} project(s)..."

PACKED=()
for project in "${PACKABLE[@]}"; do
  package_id="$(grep -oE '<PackageId>[^<]+</PackageId>' "$project" | head -n1 | sed -E 's#</?PackageId>##g')"
  expected="$OUTPUT/$package_id.$VERSION.nupkg"

  # Drop any same-version leftover so a stale package can't be mistaken for a fresh one.
  rm -f "$expected"

  echo
  echo "==> $package_id"
  # PackageId/PackAsTool stay in the csproj: passing them here would flow as global properties into
  # the whole restore graph and collide with the referenced projects.
  dotnet pack "$project" -c "$CONFIGURATION" -o "$OUTPUT"

  if [[ ! -f "$expected" ]]; then
    echo "error: expected package not produced: $expected" >&2
    exit 1
  fi
  PACKED+=("$expected")
done

echo
echo "Packed ${#PACKED[@]} package(s) into $OUTPUT:"
for package in "${PACKED[@]}"; do
  printf '  %-52s %s\n' "$(basename "$package")" "$(du -h "$package" | cut -f1)"
done
