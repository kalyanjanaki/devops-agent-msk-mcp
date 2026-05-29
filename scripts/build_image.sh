#!/usr/bin/env bash
# Build the msk-mcp container image.
# Uses docker if available, else finch (Amazon's Docker-CLI-compatible runtime).
set -euo pipefail

cd "$(dirname "$0")/.."

if command -v docker >/dev/null 2>&1; then
  RUNTIME=docker
elif command -v finch >/dev/null 2>&1; then
  RUNTIME=finch
else
  echo "Neither docker nor finch is installed. Install one and retry." >&2
  exit 1
fi

TAG="${TAG:-msk-mcp:dev}"
# linux/amd64 explicit so the image runs on default Fargate (x86) regardless of
# what arch we're building from. Override with PLATFORM=linux/arm64 for Graviton.
PLATFORM="${PLATFORM:-linux/amd64}"

echo "Building $TAG for $PLATFORM using $RUNTIME ..."
"$RUNTIME" build --platform "$PLATFORM" -t "$TAG" .

echo "Built $TAG"
"$RUNTIME" images "$TAG"
