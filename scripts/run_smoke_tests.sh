#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JVM_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
WEBUI_DIR="$(cd "${JVM_DIR}/../HiCT_WebUI" && pwd)"
export GRADLE_USER_HOME="${GRADLE_USER_HOME:-/tmp/hict_gradle_home}"
mkdir -p "${GRADLE_USER_HOME}"

echo "[smoke] Building JVM module"
cd "${JVM_DIR}"
./gradlew compileJava
echo "[smoke] Running JVM tests"
./gradlew test

if [[ -d "${WEBUI_DIR}" ]]; then
  echo "[smoke] Type-checking and building WebUI"
  cd "${WEBUI_DIR}"
  npm run type-check
  npm run build-only
fi

echo "[smoke] OK"
