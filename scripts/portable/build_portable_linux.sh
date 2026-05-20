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
RUN_PAYLOAD_XZ_THREADS="${HICT_RUN_PAYLOAD_XZ_THREADS:-2}"

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
  HICT_RUN_PAYLOAD_XZ_THREADS=<n>    xz threads for the .run payload, default 2.

The .run file is a transparent shell script with a tar.xz payload appended. It
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
require_cmd gzip
require_cmd xz
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
  (cd "${PROJECT_DIR}" && ./gradlew -PrequireBundledWebUI=true shadowJar)
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
if [[ -f "${PROJECT_DIR}/toolchains-dist/linux_x86_64/manifest.json" ]] &&
   ! "${JAR_TOOL}" tf "${FAT_JAR}" | grep -qx 'toolchains/linux_x86_64/manifest.json'; then
  echo "toolchains-dist/linux_x86_64 exists, but the fat JAR does not contain the bundled Linux hictk manifest." >&2
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
  "${JAR_TOOL}" xf "${APP_DIR}/lib/hict.jar" webui toolchains
)
if [[ -f "${PROJECT_DIR}/toolchains-dist/linux_x86_64/manifest.json" ]]; then
  chmod 0755 "${APP_DIR}/toolchains/linux_x86_64/bin/hictk" 2>/dev/null || true
  if [[ ! -x "${APP_DIR}/toolchains/linux_x86_64/bin/hictk" ]]; then
    echo "Portable package is missing executable bundled hictk at toolchains/linux_x86_64/bin/hictk." >&2
    exit 1
  fi
  "${APP_DIR}/toolchains/linux_x86_64/bin/hictk" --version >/dev/null
fi

if [[ -d "${PROJECT_DIR}/browsers-dist/linux_x86_64" ]] &&
   find "${PROJECT_DIR}/browsers-dist/linux_x86_64" -name manifest.json -type f | grep -q .; then
  mkdir -p "${APP_DIR}/browsers"
  cp -a "${PROJECT_DIR}/browsers-dist/linux_x86_64" "${APP_DIR}/browsers/"
  while IFS= read -r BROWSER_MANIFEST; do
    BROWSER_ROOT="$(dirname "${BROWSER_MANIFEST}")"
    BROWSER_COMMAND="$(grep -E '"command"[[:space:]]*:' "${BROWSER_MANIFEST}" | head -n 1 | sed -E 's/.*"command"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/')"
    if [[ -z "${BROWSER_COMMAND}" || ! -f "${BROWSER_ROOT}/${BROWSER_COMMAND}" ]]; then
      echo "${BROWSER_MANIFEST} must contain a valid relative command path." >&2
      exit 1
    fi
    chmod 0755 "${BROWSER_ROOT}/${BROWSER_COMMAND}" 2>/dev/null || true
  done < <(find "${APP_DIR}/browsers/linux_x86_64" -name manifest.json -type f | sort)
fi

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
    prepared before packaging. Portable packages also extract the platform
    hictk payload under toolchains/ and set HICT_TOOLCHAIN_DIR at launch time.
    hictk is redistributed under its MIT license and should be cited when .hic
    conversion is used.
  - Optional bundled browser resources under browsers/ when browsers-dist was
    prepared before packaging. HiCT does not download browser binaries during
    packaging; browser payloads must be curated with their upstream license,
    trademark, and update requirements before redistribution.

The Linux .run artifact is a transparent shell wrapper with an appended tar.xz
payload. It is used to keep the release inspectable, reduce bundled-browser
release size, and avoid opaque native self-extracting packers.
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
export HICT_APP_HOME="${APP_HOME}"
export HICT_JAR_PATH="${APP_HOME}/lib/hict.jar"
if [[ -z "${WEBUI_ROOT:-}" && -d "${APP_HOME}/webui" ]]; then
  export WEBUI_ROOT="${APP_HOME}/webui"
fi
if [[ -z "${HICT_TOOLCHAIN_DIR:-}" && -f "${APP_HOME}/toolchains/linux_x86_64/manifest.json" ]]; then
  export HICT_TOOLCHAIN_DIR="${APP_HOME}/toolchains/linux_x86_64"
fi
if [[ -z "${HICT_BROWSER_DIR:-}" && -d "${APP_HOME}/browsers/linux_x86_64" ]]; then
  export HICT_BROWSER_DIR="${APP_HOME}/browsers/linux_x86_64"
fi

warn_missing_tauri_webview_dependencies() {
  local browser_root="${HICT_BROWSER_DIR:-${APP_HOME}/browsers/linux_x86_64}"
  if [[ ! -d "${browser_root}" ]] || ! grep -R -q '"engine"[[:space:]]*:[[:space:]]*"tauri-system-webview"' "${browser_root}" 2>/dev/null; then
    return 0
  fi
  if command -v ldconfig >/dev/null 2>&1 && ldconfig -p 2>/dev/null | grep -Eq 'libwebkit2gtk-4\.1\.so|libwebkitgtk-6\.0\.so'; then
    return 0
  fi
  if find /usr/lib /usr/lib64 /lib /lib64 \( -name 'libwebkit2gtk-4.1.so*' -o -name 'libwebkitgtk-6.0.so*' \) 2>/dev/null | grep -q .; then
    return 0
  fi
  cat >&2 <<'EOW'
WARNING: HiCT includes the small Tauri WebView browser, but Linux WebKitGTK
runtime libraries were not detected. The launcher will try Tauri first and then
fall back to Electron or the system browser if available.

Install WebKitGTK for your distribution if the bundled Tauri browser does not
open:

  Debian/Ubuntu: sudo apt-get install libwebkit2gtk-4.1-0 libjavascriptcoregtk-4.1-0 libgtk-3-0
  Fedora/RHEL:   sudo dnf install webkit2gtk4.1 gtk3
  Arch Linux:    sudo pacman -S webkit2gtk-4.1 gtk3
  openSUSE:      sudo zypper install libwebkit2gtk-4_1-0 gtk3

EOW
}
warn_missing_tauri_webview_dependencies

export HICT_BIND_HOST="${HICT_BIND_HOST:-127.0.0.1}"
if [[ "$#" -eq 0 ]]; then
  export HICT_LAUNCHER_MODE="${HICT_LAUNCHER_MODE:-gui}"
fi

mkdir -p "${DATA_DIR}"
cd "${DATA_DIR}"
export DATA_DIR="$(pwd -P)"

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
  ./bin/hict launcher
  ./bin/hict --help
  ./bin/hict start-server
  ./bin/hict convert --help

The package includes:
  - HiCT_JVM fat JAR, including the built HiCT_WebUI resources
  - extracted HiCT_WebUI assets used as WEBUI_ROOT for robust portable serving
  - extracted bundled hictk payload under toolchains/ when release packaging
    was built with .hic conversion support
  - optional bundled browser payload under browsers/ when browsers-dist was
    prepared before packaging
  - a jlink runtime built from the JDK used by the release runner
  - HiCT license files
  - the runtime/legal directory generated by jlink

With no arguments, ./bin/hict opens the graphical launcher. Explicit CLI
subcommands keep the traditional command-line behavior.

DATA_DIR defaults:
  - extracted app directory when running ./bin/hict directly
  - directory containing the .run file when running the .run wrapper
  - explicit DATA_DIR always wins

The launcher enters DATA_DIR before Java starts, so file dialogs and relative
paths begin from the portable data location.

HICT_BIND_HOST defaults to 127.0.0.1 in this portable launcher. Set
HICT_BIND_HOST=0.0.0.0 explicitly if remote machines must connect to this HiCT
server.

Java runtime notices:
  The embedded runtime keeps its jlink-generated legal/ directory intact. For
  Temurin/OpenJDK builds this includes the OpenJDK GPLv2 + Classpath Exception
  notices and third-party notices shipped with the runtime.
EOF

TAR_PATH="${ARTIFACT_DIR}/${APP_NAME}-${VERSION}-${PLATFORM}.tar.gz"
RUN_PATH="${ARTIFACT_DIR}/${APP_NAME}-${VERSION}-${PLATFORM}.run"
SHA_PATH="${ARTIFACT_DIR}/${APP_NAME}-${VERSION}-${PLATFORM}.sha256"
PAYLOAD_PATH="${DIST_ROOT}/${APP_NAME}-${VERSION}-${PLATFORM}.payload.tar.xz"

tar -C "${DIST_ROOT}" -cf - "${APP_NAME}-${VERSION}-${PLATFORM}" | gzip -9 > "${TAR_PATH}"
tar -C "${DIST_ROOT}" -cf - "${APP_NAME}-${VERSION}-${PLATFORM}" | xz -9e -T"${RUN_PAYLOAD_XZ_THREADS}" > "${PAYLOAD_PATH}"
PAYLOAD_SHA="$(sha256sum "${PAYLOAD_PATH}" | awk '{print $1}')"
BUNDLED_TAURI_BROWSER="0"
if [[ -d "${APP_DIR}/browsers/linux_x86_64" ]] &&
   grep -R -q '"engine"[[:space:]]*:[[:space:]]*"tauri-system-webview"' "${APP_DIR}/browsers/linux_x86_64" 2>/dev/null; then
  BUNDLED_TAURI_BROWSER="1"
fi

cat > "${RUN_PATH}" <<EOF
#!/usr/bin/env bash
set -euo pipefail

APP_NAME="${APP_NAME}"
APP_VERSION="${VERSION}"
APP_PLATFORM="${PLATFORM}"
PAYLOAD_SHA256="${PAYLOAD_SHA}"
BUNDLED_TAURI_BROWSER="${BUNDLED_TAURI_BROWSER}"

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

require_runtime_cmd() {
  if command -v "\$1" >/dev/null 2>&1; then
    return 0
  fi
  cat >&2 <<EOM
HiCT portable launcher cannot start because required command '\$1' is not available.

Install the standard archive/shell utilities for your Linux distribution, then
run this file again. Common commands:

  Debian/Ubuntu: sudo apt-get install coreutils gawk tar xz-utils
  Fedora/RHEL:   sudo dnf install coreutils gawk tar xz
  Arch Linux:    sudo pacman -S coreutils gawk tar xz
  openSUSE:      sudo zypper install coreutils gawk tar xz

If this machine is locked down, use the .tar.gz portable artifact instead and
extract it on a machine that has these standard tools.
EOM
  exit 127
}

if [[ "\${1:-}" == "--hict-run-help" ]]; then
  usage
  exit 0
fi

require_runtime_cmd awk
require_runtime_cmd tail
require_runtime_cmd tar
require_runtime_cmd xz
require_runtime_cmd mkdir
require_runtime_cmd touch
require_runtime_cmd cat

warn_missing_tauri_webview_dependencies() {
  if [[ "\${BUNDLED_TAURI_BROWSER}" != "1" ]]; then
    return 0
  fi
  if command -v ldconfig >/dev/null 2>&1 && ldconfig -p 2>/dev/null | grep -Eq 'libwebkit2gtk-4\.1\.so|libwebkitgtk-6\.0\.so'; then
    return 0
  fi
  if find /usr/lib /usr/lib64 /lib /lib64 \( -name 'libwebkit2gtk-4.1.so*' -o -name 'libwebkitgtk-6.0.so*' \) 2>/dev/null | grep -q .; then
    return 0
  fi
  cat >&2 <<'EOW'
WARNING: This HiCT package includes the small Tauri WebView browser, but Linux
WebKitGTK runtime libraries were not detected before launch. HiCT can still run:
the launcher will try Tauri first and then fall back to Electron or the system
browser if available.

Install WebKitGTK for your distribution if the bundled Tauri browser does not
open:

  Debian/Ubuntu: sudo apt-get install libwebkit2gtk-4.1-0 libjavascriptcoregtk-4.1-0 libgtk-3-0
  Fedora/RHEL:   sudo dnf install webkit2gtk4.1 gtk3
  Arch Linux:    sudo pacman -S webkit2gtk-4.1 gtk3
  openSUSE:      sudo zypper install libwebkit2gtk-4_1-0 gtk3

EOW
}
warn_missing_tauri_webview_dependencies

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
  tail -n +"\${payload_line}" "\${SELF_PATH}" | xz -dc | tar -xf - -C "\$2"
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
  tail -n +"\${payload_line}" "\${SELF_PATH}" | xz -dc | tar -xf - -C "\${extract_root}"
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
