#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DIST_ROOT="${HICT_PORTABLE_DIST_DIR:-${PROJECT_DIR}/build/portable}"
PLATFORM="linux-x86_64"
APP_NAME="HiCT"
VERSION="$(tr -d '[:space:]' < "${PROJECT_DIR}/version.txt")"
ABI_SUFFIX=""
if [[ "${HICT_LINUX_ABI_MODE:-glibc217}" == "musl-static" ]]; then
  ABI_SUFFIX="-musl"
fi
APP_DIR="${DIST_ROOT}/${APP_NAME}-${VERSION}${ABI_SUFFIX}-${PLATFORM}"
PACKAGE_NAME="${PACKAGE_NAME:-${APP_NAME}-${VERSION}${ABI_SUFFIX}-${PLATFORM}}"
ARTIFACT_DIR="${PROJECT_DIR}/build/distributions"
RUNTIME_MODULES="${HICT_RUNTIME_MODULES:-java.se,jdk.charsets,jdk.crypto.ec,jdk.localedata,jdk.management,jdk.unsupported,jdk.zipfs}"
RUN_PAYLOAD_XZ_THREADS="${HICT_RUN_PAYLOAD_XZ_THREADS:-2}"

usage() {
  cat <<'EOF'
Build a self-contained Linux HiCT portable package with an embedded Java runtime.

Outputs:
  build/distributions/HiCT-<version>[-musl]-linux-x86_64.run
  build/distributions/HiCT-<version>[-musl]-linux-x86_64.tar.gz
  build/distributions/HiCT-<version>[-musl]-linux-x86_64.sha256

Environment overrides:
  HICT_SKIP_GRADLE=1                 Reuse an existing build/libs/*-fat.jar.
  HICT_RUNTIME_MODULES=<modules>     Override jlink modules.
  HICT_PORTABLE_DIST_DIR=<dir>       Override staging directory.
  HICT_RUN_PAYLOAD_XZ_THREADS=<n>    xz threads for the .run payload, default 2.

The .run file is a transparent shell script with a tar.xz payload appended. It
extracts to the user's cache and sets DATA_DIR to the directory containing the
.run file unless DATA_DIR was explicitly provided.
The .run wrapper also accepts --help to print launcher usage without starting HiCT.
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
  (
    export HICT_JHDF5_RUNTIME_PLATFORMS="${HICT_JHDF5_RUNTIME_PLATFORMS:-amd64-Linux}"
    export HICT_VERIFY_BUNDLED_JHDF5_SCOPE="${HICT_VERIFY_BUNDLED_JHDF5_SCOPE:-runtime}"
    cd "${PROJECT_DIR}" && ./gradlew -PrequireBundledWebUI=true verifyBundledJhdf5Payload shadowJar
  )
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
  chmod 0755 "${APP_DIR}/toolchains/linux_x86_64/bin/minimap2" 2>/dev/null || true
  chmod 0755 "${APP_DIR}/toolchains/linux_x86_64/bin/mm2plus-avx2" 2>/dev/null || true
  chmod 0755 "${APP_DIR}/toolchains/linux_x86_64/bin/mm2plus-avx512" 2>/dev/null || true
  if grep -q '"hictk"' "${PROJECT_DIR}/toolchains-dist/linux_x86_64/manifest.json"; then
    if [[ ! -x "${APP_DIR}/toolchains/linux_x86_64/bin/hictk" ]]; then
      echo "Portable package is missing executable bundled hictk at toolchains/linux_x86_64/bin/hictk." >&2
      exit 1
    fi
    "${APP_DIR}/toolchains/linux_x86_64/bin/hictk" --version >/dev/null
  fi
  if grep -q '"minimap2"' "${PROJECT_DIR}/toolchains-dist/linux_x86_64/manifest.json"; then
    if [[ ! -x "${APP_DIR}/toolchains/linux_x86_64/bin/minimap2" ]]; then
      echo "Portable package is missing executable bundled minimap2 at toolchains/linux_x86_64/bin/minimap2." >&2
      exit 1
    fi
    "${APP_DIR}/toolchains/linux_x86_64/bin/minimap2" --version >/dev/null
  fi
  if grep -q '"mm2plus_avx2"' "${PROJECT_DIR}/toolchains-dist/linux_x86_64/manifest.json"; then
    if [[ ! -x "${APP_DIR}/toolchains/linux_x86_64/bin/mm2plus-avx2" ]]; then
      echo "Portable package is missing executable bundled mm2-plus AVX2 at toolchains/linux_x86_64/bin/mm2plus-avx2." >&2
      exit 1
    fi
    "${APP_DIR}/toolchains/linux_x86_64/bin/mm2plus-avx2" --version >/dev/null || "${APP_DIR}/toolchains/linux_x86_64/bin/mm2plus-avx2" --help >/dev/null || true
  fi
  if grep -q '"mm2plus_avx512"' "${PROJECT_DIR}/toolchains-dist/linux_x86_64/manifest.json"; then
    if [[ ! -x "${APP_DIR}/toolchains/linux_x86_64/bin/mm2plus-avx512" ]]; then
      echo "Portable package is missing executable bundled mm2-plus AVX-512 at toolchains/linux_x86_64/bin/mm2plus-avx512." >&2
      exit 1
    fi
  fi
fi

if [[ -d "${PROJECT_DIR}/browsers-dist/linux_x86_64" ]] &&
   find "${PROJECT_DIR}/browsers-dist/linux_x86_64" -name manifest.json -type f | grep -q .; then
  mkdir -p "${APP_DIR}/browsers"
  cp -a "${PROJECT_DIR}/browsers-dist/linux_x86_64" "${APP_DIR}/browsers/"
  if [[ -f "${APP_DIR}/browsers/linux_x86_64/manifest.json" ]] &&
     find "${APP_DIR}/browsers/linux_x86_64" -mindepth 2 -name manifest.json -type f | grep -q .; then
    rm -f "${APP_DIR}/browsers/linux_x86_64/manifest.json"
    rm -rf "${APP_DIR}/browsers/linux_x86_64/app"
  fi
  while IFS= read -r BROWSER_MANIFEST; do
    BROWSER_ROOT="$(dirname "${BROWSER_MANIFEST}")"
    BROWSER_COMMAND="$(grep -E '"command"[[:space:]]*:' "${BROWSER_MANIFEST}" | head -n 1 | sed -E 's/.*"command"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/')"
    BROWSER_COMMAND_TARGET="${BROWSER_ROOT}/${BROWSER_COMMAND}"
    if [[ -n "${BROWSER_COMMAND}" && "${BROWSER_COMMAND}" != /* && ! -f "${BROWSER_COMMAND_TARGET}" ]]; then
      BROWSER_COMMAND_BASENAME="${BROWSER_COMMAND##*/}"
      if [[ -n "${BROWSER_COMMAND_BASENAME}" && -f "${BROWSER_ROOT}/${BROWSER_COMMAND_BASENAME}" ]]; then
        BROWSER_COMMAND="${BROWSER_COMMAND_BASENAME}"
        BROWSER_COMMAND_TARGET="${BROWSER_ROOT}/${BROWSER_COMMAND}"
        sed -i -E 's#("command"[[:space:]]*:[[:space:]]*")[^"]+(")#\1'"${BROWSER_COMMAND}"'\2#' "${BROWSER_MANIFEST}"
      fi
    fi
    if [[ -z "${BROWSER_COMMAND}" || "${BROWSER_COMMAND}" = /* || ! -f "${BROWSER_COMMAND_TARGET}" ]]; then
      echo "${BROWSER_MANIFEST} must contain a valid relative command path; command='${BROWSER_COMMAND}' resolved='${BROWSER_COMMAND_TARGET}'." >&2
      echo "Available files near manifest:" >&2
      find "${BROWSER_ROOT}" -maxdepth 2 -type f -printf '  %P\n' 2>/dev/null | sort | head -n 80 >&2 || true
      exit 1
    fi
    chmod 0755 "${BROWSER_COMMAND_TARGET}" 2>/dev/null || true
  done < <(find "${APP_DIR}/browsers/linux_x86_64" -name manifest.json -type f | sort)
fi

if [[ -x "${APP_DIR}/bin/hict" ]]; then
  "${APP_DIR}/bin/hict" --help >/dev/null
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
if [[ -z "${HICT_JHDF5_NATIVES_ARCHIVE:-}" ]]; then
  HICT_JHDF5_NATIVES_CANDIDATE="$(find "${APP_HOME}/lib" -maxdepth 1 -type f -name 'sis-jhdf5-*-natives.tar.gz' | sort | tail -n 1)"
  if [[ -n "${HICT_JHDF5_NATIVES_CANDIDATE}" ]]; then
    export HICT_JHDF5_NATIVES_ARCHIVE="${HICT_JHDF5_NATIVES_CANDIDATE}"
  fi
fi
if [[ -z "${WEBUI_ROOT:-}" && -d "${APP_HOME}/webui" ]]; then
  export WEBUI_ROOT="${APP_HOME}/webui"
fi
if [[ -z "${HICT_TOOLCHAIN_DIR:-}" && -f "${APP_HOME}/toolchains/linux_x86_64/manifest.json" ]]; then
  export HICT_TOOLCHAIN_DIR="${APP_HOME}/toolchains/linux_x86_64"
fi
if [[ -z "${HICT_BROWSER_DIR:-}" && -d "${APP_HOME}/browsers/linux_x86_64" ]]; then
  export HICT_BROWSER_DIR="${APP_HOME}/browsers/linux_x86_64"
fi

can_execute_from_directory() {
  local directory="$1"
  mkdir -p "${directory}" 2>/dev/null || return 1
  local probe="${directory}/.hict-exec-test-$$.sh"
  printf '#!/bin/sh\nexit 0\n' > "${probe}" 2>/dev/null || return 1
  chmod 0700 "${probe}" 2>/dev/null || {
    rm -f "${probe}" 2>/dev/null || true
    return 1
  }
  "${probe}" >/dev/null 2>&1
  local status=$?
  rm -f "${probe}" 2>/dev/null || true
  return "${status}"
}

select_runtime_temp_dir() {
  local local_temp="${DATA_DIR}/tmp"
  local fallback_temp="${TMPDIR:-/tmp}/hict-${USER:-user}/runtime"
  local candidates=()
  [[ -n "${HICT_TEMP_DIR:-}" ]] && candidates+=("${HICT_TEMP_DIR}")
  [[ -n "${HICT_EXEC_TEMP_DIR:-}" ]] && candidates+=("${HICT_EXEC_TEMP_DIR}")
  candidates+=("${local_temp}" "${fallback_temp}" "${APP_HOME}/tmp")

  local candidate
  for candidate in "${candidates[@]}"; do
    [[ -z "${candidate}" ]] && continue
    if can_execute_from_directory "${candidate}"; then
      printf '%s\n' "$(cd "${candidate}" && pwd -P)"
      return 0
    fi
    echo "WARNING: Runtime temp candidate is not executable, trying fallback: ${candidate}" >&2
  done
  echo "No executable runtime temp directory is available. Check DATA_DIR/tmp, HICT_TEMP_DIR, and /tmp." >&2
  return 1
}

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
export HICT_TEMP_DIR="$(select_runtime_temp_dir)"
if [[ "${HICT_TEMP_DIR}" != "${DATA_DIR}/tmp" ]]; then
  echo "WARNING: Using fallback runtime temp directory because DATA_DIR/tmp is not executable or not usable: ${HICT_TEMP_DIR}" >&2
fi
export TMP="${TMP:-${HICT_TEMP_DIR}}"
export TEMP="${TEMP:-${HICT_TEMP_DIR}}"

JAVA_OPTS=()
if [[ -n "${HICT_JAVA_OPTS:-}" ]]; then
  # shellcheck disable=SC2206
  JAVA_OPTS=(${HICT_JAVA_OPTS})
fi

if [[ -n "${HICT_JAVA_OPTS:-}" ]]; then
  exec "${APP_HOME}/runtime/bin/java" "-Djava.io.tmpdir=${HICT_TEMP_DIR}" "${JAVA_OPTS[@]}" -jar "${APP_HOME}/lib/hict.jar" "$@"
else
  exec "${APP_HOME}/runtime/bin/java" "-Djava.io.tmpdir=${HICT_TEMP_DIR}" -jar "${APP_HOME}/lib/hict.jar" "$@"
fi
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
  - optional split JHDF5 native archive under lib/ when the release uses the
    slim JHDF5 jar packaging
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

TAR_PATH="${ARTIFACT_DIR}/${APP_NAME}-${VERSION}${ABI_SUFFIX}-${PLATFORM}.tar.gz"
RUN_PATH="${ARTIFACT_DIR}/${APP_NAME}-${VERSION}${ABI_SUFFIX}-${PLATFORM}.run"
SHA_PATH="${ARTIFACT_DIR}/${APP_NAME}-${VERSION}${ABI_SUFFIX}-${PLATFORM}.sha256"
PAYLOAD_PATH="${DIST_ROOT}/${APP_NAME}-${VERSION}${ABI_SUFFIX}-${PLATFORM}.payload.tar.xz"

tar -C "${DIST_ROOT}" -cf - "${PACKAGE_NAME}" | gzip -9 > "${TAR_PATH}"
tar -C "${DIST_ROOT}" -cf - "${PACKAGE_NAME}" | xz -9e -T"${RUN_PAYLOAD_XZ_THREADS}" > "${PAYLOAD_PATH}"
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
APP_PACKAGE_NAME="${PACKAGE_NAME:-${APP_NAME}-${VERSION}${ABI_SUFFIX}-${PLATFORM}}"
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
  ./HiCT-<version>-linux-x86_64.run --help
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

  Debian/Ubuntu: sudo apt-get install coreutils tar xz-utils
  Fedora/RHEL:   sudo dnf install coreutils tar xz
  Arch Linux:    sudo pacman -S coreutils tar xz
  openSUSE:      sudo zypper install coreutils tar xz

If this machine is locked down, use the .tar.gz portable artifact instead and
extract it on a machine that has these standard tools.
EOM
  exit 127
}

if [[ "\${1:-}" == "--hict-run-help" || "\${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

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

print_banner() {
  cat <<'EOB'
HiCT - Hi-C scaffolding and visualization workstation
Copyright (c) 2021-2026 Aleksandr Serdiukov, Anton Zamyatin,
Aleksandr Sinitsyn, Vitalii Dravgelis, and CT Lab ITMO University.
License: MIT. Bundled third-party tools keep their own licenses.

Preparing the portable HiCT package. The first start extracts the application
payload and can take a while. Please keep this Terminal window open.

EOB
}

can_execute_from_directory() {
  local directory="\$1"
  mkdir -p "\${directory}" 2>/dev/null || return 1
  local probe="\${directory}/.hict-exec-test-\$\$.sh"
  printf '#!/bin/sh\nexit 0\n' > "\${probe}" 2>/dev/null || return 1
  chmod 0700 "\${probe}" 2>/dev/null || {
    rm -f "\${probe}" 2>/dev/null || true
    return 1
  }
  "\${probe}" >/dev/null 2>&1
  local status=\$?
  rm -f "\${probe}" 2>/dev/null || true
  return "\${status}"
}

find_payload_line() {
  local line_number=0
  local line
  while IFS= read -r line; do
    line_number=\$((line_number + 1))
    if [[ "\${line}" == "\${MARKER}" ]]; then
      echo \$((line_number + 1))
      return 0
    fi
  done < "\${SELF_PATH}"
  return 1
}

payload_line="\$(find_payload_line || true)"
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
  echo "Extracting HiCT to \$2. Please wait..." >&2
  tail -n +"\${payload_line}" "\${SELF_PATH}" | xz -dc | tar -xf - -C "\$2"
  echo "Extracted HiCT to \$2/\${APP_PACKAGE_NAME}"
  exit 0
fi

print_banner

home_dir="\${HOME:-/tmp}"
local_cache_root="\${SELF_DIR}/HiCT.portable/payloads"
preferred_cache_root="\${XDG_CACHE_HOME:-\${home_dir}/.cache}/hict/portable"
fallback_cache_root="\${TMPDIR:-/tmp}/hict-\${USER:-user}/portable"
cache_root=""
for candidate_cache_root in "\${local_cache_root}" "\${fallback_cache_root}" "\${preferred_cache_root}"; do
  if can_execute_from_directory "\${candidate_cache_root}"; then
    cache_root="\${candidate_cache_root}"
    break
  fi
  echo "WARNING: HiCT payload cache candidate is not executable, trying fallback: \${candidate_cache_root}" >&2
done
if [[ -z "\${cache_root}" ]]; then
  echo "No executable HiCT payload cache directory is available. Check the .run directory, XDG cache, and /tmp." >&2
  exit 1
fi
if [[ "\${cache_root}" != "\${local_cache_root}" ]]; then
  export HICT_PORTABLE_NOTICE="The directory containing the .run file could not be used as an executable payload cache; using \${cache_root}."
fi
extract_root="\${cache_root}/\${APP_PACKAGE_NAME}-\${PAYLOAD_SHA256}"
app_home="\${extract_root}/\${APP_PACKAGE_NAME}"
marker_file="\${extract_root}/.payload.sha256"

if [[ ! -x "\${app_home}/bin/hict" || ! -f "\${marker_file}" || "\$(cat "\${marker_file}" 2>/dev/null || true)" != "\${PAYLOAD_SHA256}" ]]; then
  rm -rf "\${extract_root}"
  mkdir -p "\${extract_root}"
  echo "Extracting HiCT to \${extract_root}. Please wait..." >&2
  tail -n +"\${payload_line}" "\${SELF_PATH}" | xz -dc | tar -xf - -C "\${extract_root}"
  printf '%s\n' "\${PAYLOAD_SHA256}" > "\${marker_file}"
fi

export HICT_PORTABLE_DATA_DIR="\${DATA_DIR:-\${SELF_DIR}}"
exec "\${app_home}/bin/hict" "\$@"

__HICT_PAYLOAD_BELOW__
EOF
cat "${PAYLOAD_PATH}" >> "${RUN_PATH}"
chmod +x "${RUN_PATH}"
if [[ -x "${RUN_PATH}" ]]; then
  "${RUN_PATH}" --help >/dev/null
  RUN_EXTRACT_TEST_DIR="$(mktemp -d "${TMPDIR:-/tmp}/hict-run-extract-test.XXXXXX")"
  "${RUN_PATH}" --hict-extract-only "${RUN_EXTRACT_TEST_DIR}" >/dev/null
  test -x "${RUN_EXTRACT_TEST_DIR}/${PACKAGE_NAME}/bin/hict"
  rm -rf "${RUN_EXTRACT_TEST_DIR}"
  RUN_SMOKE_TEST_DIR="$(mktemp -d "${TMPDIR:-/tmp}/hict-run-smoke-test.XXXXXX")"
  cleanup_run_smoke_test() {
    rm -rf "${RUN_SMOKE_TEST_DIR}"
  }
  trap cleanup_run_smoke_test EXIT
  mkdir -p "${RUN_SMOKE_TEST_DIR}/data" "${RUN_SMOKE_TEST_DIR}/cache" "${RUN_SMOKE_TEST_DIR}/tmp"
  perl -e 'alarm shift @ARGV; exec @ARGV or die $!' 600 env \
    DATA_DIR="${RUN_SMOKE_TEST_DIR}/data" \
    HICT_PORTABLE_DATA_DIR="${RUN_SMOKE_TEST_DIR}/data" \
    XDG_CACHE_HOME="${RUN_SMOKE_TEST_DIR}/cache" \
    TMPDIR="${RUN_SMOKE_TEST_DIR}/tmp" \
    HICT_LAUNCHER_MODE=cli \
    "${RUN_PATH}" check-toolchains --require-hdf5-native --check-available-natives --quiet
  rm -rf "${RUN_SMOKE_TEST_DIR}"
  trap - EXIT
fi

(
  cd "${ARTIFACT_DIR}"
  sha256sum "$(basename "${RUN_PATH}")" "$(basename "${TAR_PATH}")" > "${SHA_PATH}"
)

echo "Built ${RUN_PATH}"
echo "Built ${TAR_PATH}"
echo "Wrote ${SHA_PATH}"
