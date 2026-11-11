#!/usr/bin/env bash
set -euo pipefail

export VERTXWEB_ENVIRONMENT="dev"
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)

# Versioned build artifacts are intentionally retained by Gradle. Resolve one
# JAR explicitly so stale artifacts cannot become accidental CLI arguments.
FAT_JAR=""
for candidate in "${SCRIPT_DIR}"/build/libs/hict_server-*-fat.jar; do
  [[ -f "${candidate}" ]] || continue
  if [[ -z "${FAT_JAR}" || "${candidate}" -nt "${FAT_JAR}" ]]; then
    FAT_JAR="${candidate}"
  fi
done

if [[ -z "${FAT_JAR}" ]]; then
  printf 'No HiCT fat JAR found under %s/build/libs. Run ./gradlew jar first.\n' "${SCRIPT_DIR}" >&2
  exit 1
fi

printf 'Starting HiCT development server from %s\n' "${FAT_JAR}"
VXPORT="${VXPORT:-5000}" \
DATA_DIR="${DATA_DIR:-/mnt/Models/HiCT/data/}" \
TILE_SIZE="${TILE_SIZE:-256}" \
HICT_WORKERS_TOTAL_MAX="${HICT_WORKERS_TOTAL_MAX:-256}" \
  exec java -jar "${FAT_JAR}" "$@"
