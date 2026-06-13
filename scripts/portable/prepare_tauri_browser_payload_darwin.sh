#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
WEBUI_REPO_URL="${HICT_WEBUI_REPO_URL:-https://github.com/ctlab/HiCT_WebUI.git}"
WEBUI_REF="${HICT_WEBUI_REF:-same-as-jvm}"
DARWIN_ARCH="$(uname -m)"
case "${DARWIN_ARCH}" in
  arm64|aarch64) DARWIN_ARCH="arm64" ;;
  x86_64|amd64) DARWIN_ARCH="x86_64" ;;
  *) echo "Unsupported macOS browser architecture: ${DARWIN_ARCH}" >&2; exit 1 ;;
esac
PLATFORM="${HICT_BROWSER_PLATFORM:-darwin_${DARWIN_ARCH}}"
OUTPUT_DIR="${HICT_BROWSER_OUTPUT_DIR:-${PROJECT_DIR}/browsers-dist/${PLATFORM}/tauri}"
WORK_ROOT="${HICT_BROWSER_BUILD_ROOT:-${PROJECT_DIR}/build/tauri-browser}"

usage() {
  cat <<'EOF'
Prepare the macOS Tauri browser payload consumed by HiCT portable packages.

Environment overrides:
  HICT_WEBUI_DIR=/path/to/HiCT_WebUI      Use an existing checkout.
  HICT_WEBUI_REF=<ref>                    HiCT_WebUI branch/tag/ref to clone; same-as-jvm by default.
  HICT_WEBUI_REPO_URL=<url>               HiCT_WebUI Git URL.
  HICT_BROWSER_PLATFORM=darwin_arm64      Override target payload platform.
  HICT_BROWSER_OUTPUT_DIR=<dir>           Payload output directory.
  HICT_SKIP_NPM_INSTALL=1                 Reuse existing HiCT_WebUI node_modules.
EOF
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
  exit 0
fi

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

require_cmd cargo
require_cmd git
require_cmd node
require_cmd npm

current_jvm_ref() {
  if [[ -n "${GITHUB_REF_NAME:-}" ]]; then
    printf '%s\n' "${GITHUB_REF_NAME}"
    return
  fi
  git -C "${PROJECT_DIR}" branch --show-current 2>/dev/null | grep -v '^$' || printf '%s\n' master
}

resolve_webui_source() {
  if [[ -n "${HICT_WEBUI_DIR:-}" ]]; then
    printf '%s\n' "$(cd "${HICT_WEBUI_DIR}" && pwd)"
    return
  fi

  local sibling="${PROJECT_DIR}/../HiCT_WebUI"
  if [[ -f "${sibling}/package.json" ]]; then
    printf '%s\n' "$(cd "${sibling}" && pwd)"
    return
  fi

  local requested_ref="${WEBUI_REF}"
  if [[ "${requested_ref}" == "same-as-jvm" ]]; then
    requested_ref="$(current_jvm_ref)"
  fi

  local checkout_dir="${WORK_ROOT}/HiCT_WebUI"
  rm -rf "${checkout_dir}"
  mkdir -p "${WORK_ROOT}"
  if git clone --depth 1 --branch "${requested_ref}" "${WEBUI_REPO_URL}" "${checkout_dir}"; then
    printf '%s\n' "${checkout_dir}"
    return
  fi

  if [[ "${requested_ref}" != "master" ]]; then
    echo "HiCT_WebUI ref '${requested_ref}' was not found; falling back to master." >&2
    rm -rf "${checkout_dir}"
    git clone --depth 1 --branch master "${WEBUI_REPO_URL}" "${checkout_dir}"
    printf '%s\n' "${checkout_dir}"
    return
  fi

  echo "Could not clone HiCT_WebUI ref '${requested_ref}'." >&2
  exit 1
}

WEBUI_DIR="$(resolve_webui_source)"
echo "[tauri-browser] Using HiCT_WebUI source: ${WEBUI_DIR}"
echo "[tauri-browser] Writing payload to: ${OUTPUT_DIR}"

(
  cd "${WEBUI_DIR}"
  if [[ "${HICT_SKIP_NPM_INSTALL:-0}" == "1" ]]; then
    echo "[tauri-browser] Reusing existing HiCT_WebUI node_modules."
  else
    if [[ -f package-lock.json ]]; then
      npm ci
    else
      npm install
    fi
  fi
  npm run build:web
)

node "${WEBUI_DIR}/scripts/build-tauri-browser-payload.mjs" --platform "${PLATFORM}" --output "${OUTPUT_DIR}"
