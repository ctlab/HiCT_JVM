#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
WORK_DIR="${WORK_DIR:-/tmp/mimalloc-build-linux-x86_64}"
OUTPUT_DIR="${OUTPUT_DIR:-${WORK_DIR}/install}"
SOURCE_DIR="${SOURCE_DIR:-${WORK_DIR}/src}"
BUILD_DIR="${BUILD_DIR:-${WORK_DIR}/build}"
MIMALLOC_REF="${MIMALLOC_REF:-v2.2.4}"
MIMALLOC_REPO_URL="${MIMALLOC_REPO_URL:-https://github.com/microsoft/mimalloc.git}"
BUILD_JOBS="${BUILD_JOBS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)}"
MACOS_DEPLOYMENT_TARGET="${MACOS_DEPLOYMENT_TARGET:-}"

usage() {
  cat <<EOF
Build mimalloc as a static library for Linux portable releases.

Environment overrides:
  WORK_DIR=/tmp/mimalloc-build-linux-x86_64
  OUTPUT_DIR=/tmp/mimalloc-build-linux-x86_64/install
  SOURCE_DIR=/tmp/mimalloc-build-linux-x86_64/src
  BUILD_DIR=/tmp/mimalloc-build-linux-x86_64/build
  MIMALLOC_REF=v2.2.4
  MIMALLOC_REPO_URL=https://github.com/microsoft/mimalloc.git
  BUILD_JOBS=8

The script prints the install prefix on stdout when the static library is ready.
EOF
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
  exit 0
fi

for cmd in git cmake ninja clang readelf; do
  command -v "${cmd}" >/dev/null 2>&1 || {
    echo "Required command not found: ${cmd}" >&2
    exit 1
  }
done

if [[ -f "${OUTPUT_DIR}/share/doc/mimalloc/build-info.txt" ]] && grep -qx "ref=${MIMALLOC_REF}" "${OUTPUT_DIR}/share/doc/mimalloc/build-info.txt" && [[ -f "${OUTPUT_DIR}/lib/libmimalloc.a" ]]; then
  printf '%s\n' "${OUTPUT_DIR}"
  exit 0
fi

rm -rf "${BUILD_DIR}"
mkdir -p "${WORK_DIR}" "${OUTPUT_DIR}"

if [[ ! -d "${SOURCE_DIR}/.git" ]]; then
  rm -rf "${SOURCE_DIR}"
  git clone --depth 1 --branch "${MIMALLOC_REF}" "${MIMALLOC_REPO_URL}" "${SOURCE_DIR}"
else
  git -C "${SOURCE_DIR}" fetch --tags --force origin "${MIMALLOC_REF}" || git -C "${SOURCE_DIR}" fetch --tags --force origin
  git -C "${SOURCE_DIR}" checkout --force "${MIMALLOC_REF}"
fi

cmake_args=(
  -S "${SOURCE_DIR}"
  -B "${BUILD_DIR}"
  -G Ninja
  -DCMAKE_BUILD_TYPE=Release
  -DCMAKE_INSTALL_PREFIX="${OUTPUT_DIR}"
  -DMI_BUILD_SHARED=OFF
  -DMI_BUILD_STATIC=ON
  -DMI_BUILD_TESTS=OFF
  -DMI_BUILD_TOOLS=OFF
  -DMI_BUILD_OBJECT=OFF
)
if [[ -n "${MACOS_DEPLOYMENT_TARGET}" ]]; then
  cmake_args+=(-DCMAKE_OSX_DEPLOYMENT_TARGET="${MACOS_DEPLOYMENT_TARGET}")
fi
cmake "${cmake_args[@]}" >&2
cmake --build "${BUILD_DIR}" -j"${BUILD_JOBS}" >&2
cmake --install "${BUILD_DIR}" >&2

mkdir -p "${OUTPUT_DIR}/share/doc/mimalloc"
MIMALLOC_ARCHIVE="$(find "${BUILD_DIR}" -type f \( -name 'libmimalloc.a' -o -name 'libmimalloc-secure.a' -o -name 'libmimalloc-static.a' \) | sort | tail -n 1)"
if [[ -z "${MIMALLOC_ARCHIVE}" || ! -f "${MIMALLOC_ARCHIVE}" ]]; then
  echo "Failed to locate a static mimalloc archive under ${BUILD_DIR}" >&2
  exit 1
fi
mkdir -p "${OUTPUT_DIR}/lib"
cp -f "${MIMALLOC_ARCHIVE}" "${OUTPUT_DIR}/lib/"
{
  echo "project=mimalloc"
  echo "repository=${MIMALLOC_REPO_URL}"
  echo "ref=${MIMALLOC_REF}"
  echo "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo
  echo "[file]"
  file "${OUTPUT_DIR}/lib/$(basename "${MIMALLOC_ARCHIVE}")"
  echo
  echo "[readelf]"
  readelf -h "${OUTPUT_DIR}/lib/$(basename "${MIMALLOC_ARCHIVE}")" | head -n 40 || true
} > "${OUTPUT_DIR}/share/doc/mimalloc/build-info.txt"

printf '%s\n' "${OUTPUT_DIR}"
