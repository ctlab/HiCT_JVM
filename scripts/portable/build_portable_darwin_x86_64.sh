#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export HICT_DARWIN_ARCH="${HICT_DARWIN_ARCH:-x86_64}"
export HICT_DARWIN_PLATFORM_DIR="${HICT_DARWIN_PLATFORM_DIR:-darwin_x86_64}"
export HICT_DARWIN_ARTIFACT_PLATFORM="${HICT_DARWIN_ARTIFACT_PLATFORM:-darwin-x86_64}"
exec "${SCRIPT_DIR}/build_portable_darwin_arm64.sh" "$@"
