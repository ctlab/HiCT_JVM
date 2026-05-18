#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DIST_ROOT="${HICT_PORTABLE_DIST_DIR:-${PROJECT_DIR}/build/portable}"
PLATFORM="linux-x86_64"
APP_NAME="HiCT"
VERSION="$(tr -d '[:space:]' < "${PROJECT_DIR}/version.txt")"
APP_DIR="${DIST_ROOT}/${APP_NAME}-${VERSION}-${PLATFORM}"
ARTIFACT_DIR="${PROJECT_DIR}/build/distributions"
RUNTIME_MODULES="${HICT_RUNTIME_MODULES:-java.se,jdk.charsets,jdk.crypto.ec,jdk.localedata,jdk.unsupported,jdk.zipfs}"

usage() {
  cat <<'EOF'
Build a self-contained Linux HiCT portable package with an embedded Java runtime.

Outputs:
  build/distributions/HiCT-<version>-linux-x86_64.run
  build/distributions/HiCT-<version>-linux-x86_64.tar.gz
  build/distributions/HiCT-<version>-linux-x86_64.sha256

Environment overrides:
  HICT_SKIP_GRADLE=1                 Reuse an existing build/libs/*-fat.jar.
  HICT_RUNTIME_MODULES=<modules>     Override jlink modules.
  HICT_PORTABLE_DIST_DIR=<dir>       Override staging directory.

The .run file is a transparent shell script with a tar.gz payload appended. It
extracts to the user's cache and sets DATA_DIR to the directory containing the
.run file unless DATA_DIR was explicitly provided.
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

require_cmd bash
require_cmd tar
require_cmd sha256sum
require_cmd awk
require_cmd grep
require_cmd tail

JLINK="${JAVA_HOME:-}/bin/jlink"
if [[ ! -x "${JLINK}" ]]; then
  JLINK="$(command -v jlink || true)"
fi
if [[ -z "${JLINK}" || ! -x "${JLINK}" ]]; then
  echo "Missing jlink. Use a full JDK, not a JRE." >&2
  exit 1
fi
JAR_TOOL="${JAVA_HOME:-}/bin/jar"
if [[ ! -x "${JAR_TOOL}" ]]; then
  JAR_TOOL="$(command -v jar || true)"
fi
if [[ -z "${JAR_TOOL}" || ! -x "${JAR_TOOL}" ]]; then
  echo "Missing jar. Use a full JDK, not a JRE." >&2
  exit 1
fi

if [[ "${HICT_SKIP_GRADLE:-0}" != "1" ]]; then
  (cd "${PROJECT_DIR}" && ./gradlew shadowJar)
fi

FAT_JAR="$(find "${PROJECT_DIR}/build/libs" -maxdepth 1 -type f -name '*-fat.jar' | sort | tail -n 1)"
if [[ -z "${FAT_JAR}" || ! -f "${FAT_JAR}" ]]; then
  echo "Fat JAR was not found under ${PROJECT_DIR}/build/libs" >&2
  exit 1
fi
if ! "${JAR_TOOL}" tf "${FAT_JAR}" | grep -qx 'webui/index.html'; then
  echo "Fat JAR does not contain webui/index.html; portable packages require a baked-in HiCT_WebUI build." >&2
  exit 1
fi

rm -rf "${APP_DIR}"
mkdir -p \
  "${APP_DIR}/bin" \
  "${APP_DIR}/lib" \
  "${APP_DIR}/licenses" \
  "${ARTIFACT_DIR}"

cp "${FAT_JAR}" "${APP_DIR}/lib/hict.jar"
cp "${PROJECT_DIR}/LICENSE" "${APP_DIR}/licenses/HiCT_JVM_LICENSE"
if [[ -f "${PROJECT_DIR}/../HiCT_WebUI/LICENSE" ]]; then
  cp "${PROJECT_DIR}/../HiCT_WebUI/LICENSE" "${APP_DIR}/licenses/HiCT_WebUI_LICENSE"
fi

(
  cd "${APP_DIR}"
  "${JAR_TOOL}" xf "${APP_DIR}/lib/hict.jar" webui
)

cat > "${APP_DIR}/licenses/PORTABLE_DISTRIBUTION_NOTICE.txt" <<'EOF'
HiCT portable distribution notice
=================================

This package is assembled from:

  - HiCT_JVM and HiCT_WebUI, redistributed under their bundled MIT licenses.
  - The HiCT_JVM fat JAR, which contains Java dependencies and preserves the
    upstream META-INF license/notice files included in those dependency JARs.
  - A jlink runtime image created from the release JDK. The runtime/legal
    directory is intentionally kept intact and must remain with redistributed
    packages. For Eclipse Temurin/OpenJDK runtimes this includes GPLv2 with
    Classpath Exception notices and third-party runtime notices.
  - Optional bundled hictk resources inside hict.jar when toolchains-dist was
    prepared before packaging. hictk is redistributed under its MIT license and
    should be cited when .hic conversion is used.

The Linux .run artifact is a transparent shell wrapper with an appended tar.gz
payload. It is used to keep the release inspectable and avoid opaque native
self-extracting packers.
EOF

"${JLINK}" \
  --add-modules "${RUNTIME_MODULES}" \
  --strip-debug \
  --no-header-files \
  --no-man-pages \
  --compress=zip-6 \
  --output "${APP_DIR}/runtime"

cat > "${APP_DIR}/bin/hict" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

SCRIPT_PATH="${BASH_SOURCE[0]}"
while [[ -L "${SCRIPT_PATH}" ]]; do
  SCRIPT_DIR="$(cd -P "$(dirname "${SCRIPT_PATH}")" && pwd)"
  SCRIPT_PATH="$(readlink "${SCRIPT_PATH}")"
  [[ "${SCRIPT_PATH}" != /* ]] && SCRIPT_PATH="${SCRIPT_DIR}/${SCRIPT_PATH}"
done

APP_HOME="$(cd -P "$(dirname "${SCRIPT_PATH}")/.." && pwd)"

if [[ -z "${DATA_DIR:-}" ]]; then
  if [[ -n "${HICT_PORTABLE_DATA_DIR:-}" ]]; then
    export DATA_DIR="${HICT_PORTABLE_DATA_DIR}"
  else
    export DATA_DIR="${APP_HOME}"
  fi
fi
if [[ -z "${WEBUI_ROOT:-}" && -d "${APP_HOME}/webui" ]]; then
  export WEBUI_ROOT="${APP_HOME}/webui"
fi

mkdir -p "${DATA_DIR}"

JAVA_OPTS=()
if [[ -n "${HICT_JAVA_OPTS:-}" ]]; then
  # shellcheck disable=SC2206
  JAVA_OPTS=(${HICT_JAVA_OPTS})
fi

exec "${APP_HOME}/runtime/bin/java" "${JAVA_OPTS[@]}" -jar "${APP_HOME}/lib/hict.jar" "$@"
EOF
chmod +x "${APP_DIR}/bin/hict"

cat > "${APP_DIR}/README_PORTABLE.txt" <<EOF
HiCT portable Linux package
===========================

Run:
  ./bin/hict
  ./bin/hict --help
  ./bin/hict start-server
  ./bin/hict convert --help

The package includes:
  - HiCT_JVM fat JAR, including the built HiCT_WebUI resources
  - extracted HiCT_WebUI assets used as WEBUI_ROOT for robust portable serving
  - a jlink runtime built from the JDK used by the release runner
  - HiCT license files
  - the runtime/legal directory generated by jlink

DATA_DIR defaults:
  - extracted app directory when running ./bin/hict directly
  - directory containing the .run file when running the .run wrapper
  - explicit DATA_DIR always wins

Java runtime notices:
  The embedded runtime keeps its jlink-generated legal/ directory intact. For
  Temurin/OpenJDK builds this includes the OpenJDK GPLv2 + Classpath Exception
  notices and third-party notices shipped with the runtime.
EOF

TAR_PATH="${ARTIFACT_DIR}/${APP_NAME}-${VERSION}-${PLATFORM}.tar.gz"
RUN_PATH="${ARTIFACT_DIR}/${APP_NAME}-${VERSION}-${PLATFORM}.run"
SHA_PATH="${ARTIFACT_DIR}/${APP_NAME}-${VERSION}-${PLATFORM}.sha256"
PAYLOAD_PATH="${DIST_ROOT}/${APP_NAME}-${VERSION}-${PLATFORM}.payload.tar.gz"

tar -C "${DIST_ROOT}" -czf "${TAR_PATH}" "${APP_NAME}-${VERSION}-${PLATFORM}"
cp "${TAR_PATH}" "${PAYLOAD_PATH}"
PAYLOAD_SHA="$(sha256sum "${PAYLOAD_PATH}" | awk '{print $1}')"

cat > "${RUN_PATH}" <<EOF
#!/usr/bin/env bash
set -euo pipefail

APP_NAME="${APP_NAME}"
APP_VERSION="${VERSION}"
APP_PLATFORM="${PLATFORM}"
PAYLOAD_SHA256="${PAYLOAD_SHA}"

SELF_PATH="\$(readlink -f "\$0" 2>/dev/null || realpath "\$0" 2>/dev/null || printf '%s\n' "\$0")"
SELF_DIR="\$(cd "\$(dirname "\${SELF_PATH}")" && pwd)"
MARKER="__HICT_PAYLOAD_BELOW__"

usage() {
  cat <<'EOU'
HiCT portable launcher

Usage:
  ./HiCT-<version>-linux-x86_64.run [HiCT CLI args...]
  ./HiCT-<version>-linux-x86_64.run --hict-extract-only <directory>

DATA_DIR defaults to the directory containing this .run file. Set DATA_DIR
explicitly to use a different data directory.
EOU
}

if [[ "\${1:-}" == "--hict-run-help" ]]; then
  usage
  exit 0
fi

payload_line="\$(awk "/^\${MARKER}\$/ { print NR + 1; exit 0; }" "\${SELF_PATH}")"
if [[ -z "\${payload_line}" ]]; then
  echo "Cannot find embedded HiCT payload." >&2
  exit 1
fi

if [[ "\${1:-}" == "--hict-extract-only" ]]; then
  if [[ -z "\${2:-}" ]]; then
    echo "--hict-extract-only requires a target directory." >&2
    exit 1
  fi
  mkdir -p "\$2"
  tail -n +"\${payload_line}" "\${SELF_PATH}" | tar -xzf - -C "\$2"
  echo "Extracted HiCT to \$2/${APP_NAME}-${VERSION}-${PLATFORM}"
  exit 0
fi

home_dir="\${HOME:-/tmp}"
preferred_cache_root="\${XDG_CACHE_HOME:-\${home_dir}/.cache}/hict/portable"
cache_probe="\${preferred_cache_root}/.hict-write-test-\$\$"
if mkdir -p "\${preferred_cache_root}" 2>/dev/null && touch "\${cache_probe}" 2>/dev/null; then
  rm -f "\${cache_probe}"
  cache_root="\${preferred_cache_root}"
else
  cache_root="\${TMPDIR:-/tmp}/hict-\${USER:-user}/portable"
  mkdir -p "\${cache_root}"
fi
extract_root="\${cache_root}/${APP_NAME}-${VERSION}-${PLATFORM}-\${PAYLOAD_SHA256}"
app_home="\${extract_root}/${APP_NAME}-${VERSION}-${PLATFORM}"
marker_file="\${extract_root}/.payload.sha256"

if [[ ! -x "\${app_home}/bin/hict" || ! -f "\${marker_file}" || "\$(cat "\${marker_file}" 2>/dev/null || true)" != "\${PAYLOAD_SHA256}" ]]; then
  rm -rf "\${extract_root}"
  mkdir -p "\${extract_root}"
  tail -n +"\${payload_line}" "\${SELF_PATH}" | tar -xzf - -C "\${extract_root}"
  printf '%s\n' "\${PAYLOAD_SHA256}" > "\${marker_file}"
fi

export HICT_PORTABLE_DATA_DIR="\${DATA_DIR:-\${SELF_DIR}}"
exec "\${app_home}/bin/hict" "\$@"

__HICT_PAYLOAD_BELOW__
EOF
cat "${PAYLOAD_PATH}" >> "${RUN_PATH}"
chmod +x "${RUN_PATH}"

(
  cd "${ARTIFACT_DIR}"
  sha256sum "$(basename "${RUN_PATH}")" "$(basename "${TAR_PATH}")" > "${SHA_PATH}"
)

echo "Built ${RUN_PATH}"
echo "Built ${TAR_PATH}"
echo "Wrote ${SHA_PATH}"
