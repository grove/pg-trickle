#!/usr/bin/env bash
# OPS-10-03: Update base image SHA256 digests in Dockerfiles.
#
# Run this script quarterly or when a PostgreSQL patch release is needed.
# It resolves the current linux/amd64 digest for postgres:18.3-bookworm
# and patches the relevant Dockerfiles in-place.
#
# Usage:
#   scripts/update_base_image_digests.sh
#
# Requirements:
#   - docker (with manifest inspect support)
#   - sed
#
# See also: CONTRIBUTING.md — "Updating base image digests"

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

BASE_IMAGE="postgres:18.3-bookworm"
TARGET_PLATFORM="linux/amd64"

echo "Resolving digest for ${BASE_IMAGE} (${TARGET_PLATFORM})..."
DIGEST=$(docker manifest inspect "${BASE_IMAGE}" 2>/dev/null \
  | python3 -c "
import sys, json
d = json.load(sys.stdin)
for m in d.get('manifests', []):
    p = m.get('platform', {})
    if p.get('os') == 'linux' and p.get('architecture') == 'amd64':
        print(m['digest'])
        break
")

if [[ -z "${DIGEST}" ]]; then
  echo "ERROR: Could not resolve digest for ${BASE_IMAGE}" >&2
  exit 1
fi

echo "Resolved: ${BASE_IMAGE}@${DIGEST}"

# Patch Dockerfiles — replace any existing @sha256:... pin or tag-only reference.
DOCKERFILES=(
  "${REPO_ROOT}/Dockerfile.demo"
  "${REPO_ROOT}/Dockerfile.ghcr"
  "${REPO_ROOT}/tests/Dockerfile.e2e"
)

PATTERN="FROM ${BASE_IMAGE%%@*}"   # strip any existing @sha256: suffix
REPLACEMENT="FROM ${BASE_IMAGE%%@*}@${DIGEST}"

for f in "${DOCKERFILES[@]}"; do
  if [[ ! -f "${f}" ]]; then
    echo "WARNING: ${f} not found, skipping"
    continue
  fi
  # Use perl for cross-platform in-place edit (macOS sed -i requires extension).
  perl -pi -e "s|FROM \Q${BASE_IMAGE%%@*}\E(@sha256:[a-f0-9]+)?|FROM ${BASE_IMAGE%%@*}@${DIGEST}|g" "${f}"
  echo "Patched ${f}"
done

echo ""
echo "Done. Commit the updated Dockerfiles:"
echo "  git add Dockerfile.demo Dockerfile.ghcr tests/Dockerfile.e2e"
echo "  git commit -m 'chore(docker): update base image digest to ${DIGEST}'"
