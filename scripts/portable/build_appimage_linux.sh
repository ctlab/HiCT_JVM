#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DIST_ROOT="${HICT_PORTABLE_DIST_DIR:-${PROJECT_DIR}/build/portable}"
ARTIFACT_DIR="${PROJECT_DIR}/build/distributions"
TOOLS_DIR="${PROJECT_DIR}/build/tools"
APP_NAME="HiCT"
VERSION="$(tr -d '[:space:]' < "${PROJECT_DIR}/version.txt")"
PORTABLE_PLATFORM="linux-x86_64"
APPIMAGE_ARCH="x86_64"
DESKTOP_ID="org.itmo.ctlab.hict"
PORTABLE_APP_DIR="${DIST_ROOT}/${APP_NAME}-${VERSION}-${PORTABLE_PLATFORM}"
APPDIR="${DIST_ROOT}/${APP_NAME}-${VERSION}.AppDir"
APPIMAGE_PATH="${ARTIFACT_DIR}/${APP_NAME}-${VERSION}-${APPIMAGE_ARCH}.AppImage"
SHA_PATH="${ARTIFACT_DIR}/${APP_NAME}-${VERSION}-${APPIMAGE_ARCH}.AppImage.sha256"
APPIMAGETOOL_URL="${APPIMAGETOOL_URL:-https://github.com/AppImage/AppImageKit/releases/download/continuous/appimagetool-x86_64.AppImage}"
APPIMAGE_RUNTIME_URL="${APPIMAGE_RUNTIME_URL:-https://github.com/AppImage/type2-runtime/releases/download/continuous/runtime-x86_64}"
APPIMAGE_COMPRESSION="${APPIMAGE_COMPRESSION:-gzip}"
APPIMAGE_ZSTD_LEVEL="${APPIMAGE_ZSTD_LEVEL:-19}"
APPIMAGE_MKSQUASHFS_BLOCK_SIZE="${APPIMAGE_MKSQUASHFS_BLOCK_SIZE:-1048576}"
APPIMAGE_VALIDATE="${APPIMAGE_VALIDATE:-0}"
HICT_APPIMAGE_BUNDLE_GLIBC="${HICT_APPIMAGE_BUNDLE_GLIBC:-1}"
GLIBC_DIR="${APPDIR}/usr/lib/hict/glibc"

usage() {
  cat <<'EOF'
Build a Linux AppImage for HiCT from the portable Linux app directory.

Outputs:
  build/distributions/HiCT-<version>-x86_64.AppImage
  build/distributions/HiCT-<version>-x86_64.AppImage.sha256

Environment overrides:
  APPIMAGETOOL=/path/to/appimagetool     Use an existing appimagetool binary.
  APPIMAGETOOL_URL=<url>                 Override the official appimagetool URL.
  APPIMAGE_RUNTIME=/path/to/runtime      Use an existing AppImage runtime stub.
  APPIMAGE_RUNTIME_URL=<url>             Override the static type2 runtime URL.
  APPIMAGE_RUNTIME=none                  Use appimagetool's embedded runtime instead.
  APPIMAGE_COMPRESSION=gzip|zstd|xz      SquashFS compression, gzip by default.
  APPIMAGE_ZSTD_LEVEL=<1..22>            zstd compression level, 19 by default.
  APPIMAGE_MKSQUASHFS_BLOCK_SIZE=<bytes> SquashFS block size, 1048576 by default.
  APPIMAGE_VALIDATE=1                    Enable appstream validation, disabled by default.
  HICT_APPIMAGE_BUNDLE_GLIBC=0            Disable bundled glibc/ld-linux runtime, enabled by default.
  HICT_SKIP_PORTABLE=1                   Reuse an existing build/portable app.
  HICT_PORTABLE_DIST_DIR=<dir>           Override staging directory.
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
require_cmd cp
require_cmd chmod
require_cmd file
require_cmd find
require_cmd ldd
require_cmd mkdir
require_cmd readelf
require_cmd readlink
require_cmd sed
require_cmd sha256sum
require_cmd sort

is_elf_file() {
  [[ -f "$1" ]] && file -b "$1" 2>/dev/null | grep -q 'ELF'
}

copy_glibc_runtime_library() {
  local source_path="$1"
  [[ -n "${source_path}" && "${source_path}" == /* && -e "${source_path}" ]] || return 0
  local real_source
  real_source="$(readlink -f "${source_path}" 2>/dev/null || printf '%s\n' "${source_path}")"
  [[ -f "${real_source}" ]] || return 0
  local target="${GLIBC_DIR}/$(basename "${real_source}")"
  if [[ ! -e "${target}" ]]; then
    cp -L "${real_source}" "${target}"
    chmod 0755 "${target}" 2>/dev/null || true
    APPIMAGE_GLIBC_QUEUE+=("${target}")
    APPIMAGE_GLIBC_SOURCE_DIRS["$(dirname "${real_source}")"]=1
  fi
}

queue_elf_for_glibc_scan() {
  local candidate="$1"
  is_elf_file "${candidate}" || return 0
  local real_candidate
  real_candidate="$(readlink -f "${candidate}" 2>/dev/null || printf '%s\n' "${candidate}")"
  [[ -n "${APPIMAGE_GLIBC_SCANNED["${real_candidate}"]:-}" ]] && return 0
  APPIMAGE_GLIBC_QUEUE+=("${candidate}")
}

copy_glibc_dependencies_for_elf() {
  local elf="$1"
  local interpreter
  interpreter="$(readelf -l "${elf}" 2>/dev/null | sed -n 's#.*Requesting program interpreter: \(.*\)]#\1#p' | head -n 1)"
  copy_glibc_runtime_library "${interpreter}"

  while IFS= read -r dependency; do
    copy_glibc_runtime_library "${dependency}"
  done < <(
    ldd "${elf}" 2>/dev/null |
      sed -n \
        -e 's/^[[:space:]]*[^[:space:]]\+[[:space:]]*=>[[:space:]]*\(\/[^[:space:]]*\).*/\1/p' \
        -e 's/^[[:space:]]*\(\/[^[:space:]]*\).*/\1/p'
  )
}

copy_common_glibc_dlopen_libraries() {
  local source_dir library_name candidate
  local common_libraries=(
    ld-linux-x86-64.so.2
    libc.so.6
    libm.so.6
    libdl.so.2
    libpthread.so.0
    librt.so.1
    libresolv.so.2
    libanl.so.1
    libnss_files.so.2
    libnss_dns.so.2
    libnss_compat.so.2
    libgcc_s.so.1
    libstdc++.so.6
  )
  local fallback_dirs=(
    /lib64
    /lib/x86_64-linux-gnu
    /usr/lib/x86_64-linux-gnu
  )
  for source_dir in "${!APPIMAGE_GLIBC_SOURCE_DIRS[@]}" "${fallback_dirs[@]}"; do
    [[ -d "${source_dir}" ]] || continue
    for library_name in "${common_libraries[@]}"; do
      candidate="${source_dir}/${library_name}"
      [[ -e "${candidate}" ]] && copy_glibc_runtime_library "${candidate}"
    done
  done
}

bundle_appimage_glibc_runtime() {
  rm -rf "${GLIBC_DIR}"
  mkdir -p "${GLIBC_DIR}"
  declare -g -a APPIMAGE_GLIBC_QUEUE=()
  declare -g -A APPIMAGE_GLIBC_SCANNED=()
  declare -g -A APPIMAGE_GLIBC_SOURCE_DIRS=()

  while IFS= read -r -d '' candidate; do
    queue_elf_for_glibc_scan "${candidate}"
  done < <(find "${APPDIR}/usr/lib/hict" -type f -print0)

  local elf real_elf
  while [[ ${#APPIMAGE_GLIBC_QUEUE[@]} -gt 0 ]]; do
    elf="${APPIMAGE_GLIBC_QUEUE[0]}"
    APPIMAGE_GLIBC_QUEUE=("${APPIMAGE_GLIBC_QUEUE[@]:1}")
    is_elf_file "${elf}" || continue
    real_elf="$(readlink -f "${elf}" 2>/dev/null || printf '%s\n' "${elf}")"
    [[ -n "${APPIMAGE_GLIBC_SCANNED["${real_elf}"]:-}" ]] && continue
    APPIMAGE_GLIBC_SCANNED["${real_elf}"]=1
    copy_glibc_dependencies_for_elf "${elf}"
  done

  copy_common_glibc_dlopen_libraries
  while [[ ${#APPIMAGE_GLIBC_QUEUE[@]} -gt 0 ]]; do
    elf="${APPIMAGE_GLIBC_QUEUE[0]}"
    APPIMAGE_GLIBC_QUEUE=("${APPIMAGE_GLIBC_QUEUE[@]:1}")
    is_elf_file "${elf}" || continue
    real_elf="$(readlink -f "${elf}" 2>/dev/null || printf '%s\n' "${elf}")"
    [[ -n "${APPIMAGE_GLIBC_SCANNED["${real_elf}"]:-}" ]] && continue
    APPIMAGE_GLIBC_SCANNED["${real_elf}"]=1
    copy_glibc_dependencies_for_elf "${elf}"
  done

  [[ -x "${GLIBC_DIR}/ld-linux-x86-64.so.2" ]] || { echo "Bundled glibc runtime is missing ld-linux-x86-64.so.2" >&2; exit 1; }
  [[ -f "${GLIBC_DIR}/libc.so.6" ]] || { echo "Bundled glibc runtime is missing libc.so.6" >&2; exit 1; }
  [[ -f "${GLIBC_DIR}/libm.so.6" ]] || { echo "Bundled glibc runtime is missing libm.so.6" >&2; exit 1; }
  find "${GLIBC_DIR}" -maxdepth 1 -type f -printf '%f\n' | sort > "${GLIBC_DIR}/GLIBC_RUNTIME_MANIFEST.txt"
}

install_glibc_exec_wrapper() {
  local target="$1"
  [[ -f "${target}" && -x "${target}" ]] || return 0
  is_elf_file "${target}" || return 0
  case "${target}" in
    "${GLIBC_DIR}"/*|*/.glibc-wrapped/*) return 0 ;;
  esac

  local target_dir target_name wrapped_dir wrapped_target
  target_dir="$(dirname "${target}")"
  target_name="$(basename "${target}")"
  wrapped_dir="${target_dir}/.glibc-wrapped"
  wrapped_target="${wrapped_dir}/${target_name}"
  mkdir -p "${wrapped_dir}"
  mv "${target}" "${wrapped_target}"
  chmod 0755 "${wrapped_target}" 2>/dev/null || true
  cat > "${target}" <<'EOF'
#!/bin/sh
set -eu

find_app_home() {
  dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
  while [ "${dir}" != "/" ]; do
    if [ -x "${dir}/glibc/ld-linux-x86-64.so.2" ] && [ -d "${dir}/runtime" ]; then
      printf '%s\n' "${dir}"
      return 0
    fi
    dir="$(dirname -- "${dir}")"
  done
  return 1
}

APP_HOME="${HICT_APP_HOME:-$(find_app_home)}"
GLIBC_DIR="${APP_HOME}/glibc"
LOADER="${GLIBC_DIR}/ld-linux-x86-64.so.2"
SELF_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
ORIGINAL="${SELF_DIR}/.glibc-wrapped/$(basename -- "$0")"
LIB_PATH="${SELF_DIR}:${APP_HOME}/runtime/lib:${APP_HOME}/runtime/lib/server:${APP_HOME}/lib:${APP_HOME}/toolchains/linux_x86_64/lib:${GLIBC_DIR}"
export HICT_APP_HOME="${APP_HOME}"
LOADER_LIBRARY_PATH="${LIB_PATH}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
exec "${LOADER}" --library-path "${LOADER_LIBRARY_PATH}" "${ORIGINAL}" "$@"
EOF
  chmod 0755 "${target}"
}

install_appimage_glibc_launchers() {
  local launcher="${APPDIR}/usr/lib/hict/bin/hict-appimage-glibc"
  cat > "${launcher}" <<'EOF'
#!/bin/sh
set -eu

SCRIPT_PATH="$0"
APP_HOME="$(CDPATH= cd -- "$(dirname -- "${SCRIPT_PATH}")/.." && pwd -P)"
GLIBC_DIR="${APP_HOME}/glibc"
LOADER="${GLIBC_DIR}/ld-linux-x86-64.so.2"
JAVA_BIN="${APP_HOME}/runtime/bin/java"
LIB_PATH="${APP_HOME}/runtime/lib:${APP_HOME}/runtime/lib/server:${APP_HOME}/lib:${APP_HOME}/toolchains/linux_x86_64/lib:${GLIBC_DIR}"

if [ -z "${DATA_DIR:-}" ]; then
  if [ -n "${HICT_PORTABLE_DATA_DIR:-}" ]; then
    DATA_DIR="${HICT_PORTABLE_DATA_DIR}"
  else
    DATA_DIR="${APP_HOME}"
  fi
  export DATA_DIR
fi
mkdir -p "${DATA_DIR}"
cd "${DATA_DIR}"
DATA_DIR="$(pwd -P)"
export DATA_DIR

can_execute_from_directory() {
  directory="$1"
  mkdir -p "${directory}" 2>/dev/null || return 1
  probe="${directory}/.hict-exec-test-$$.sh"
  printf '#!/bin/sh\nexit 0\n' > "${probe}" 2>/dev/null || return 1
  chmod 0700 "${probe}" 2>/dev/null || {
    rm -f "${probe}" 2>/dev/null || true
    return 1
  }
  "${probe}" >/dev/null 2>&1
  status=$?
  rm -f "${probe}" 2>/dev/null || true
  return "${status}"
}

select_runtime_temp_dir() {
  for candidate in "${HICT_TEMP_DIR:-}" "${HICT_EXEC_TEMP_DIR:-}" "${DATA_DIR}/tmp" "${TMPDIR:-/tmp}/hict-${USER:-user}/runtime" "${APP_HOME}/tmp"; do
    [ -n "${candidate}" ] || continue
    if can_execute_from_directory "${candidate}"; then
      (cd "${candidate}" && pwd -P)
      return 0
    fi
    echo "WARNING: Runtime temp candidate is not executable, trying fallback: ${candidate}" >&2
  done
  echo "No executable runtime temp directory is available. Check DATA_DIR/tmp, HICT_TEMP_DIR, and /tmp." >&2
  return 1
}

export HICT_APP_HOME="${APP_HOME}"
export HICT_JAR_PATH="${APP_HOME}/lib/hict.jar"
if [ -z "${HICT_JHDF5_NATIVES_ARCHIVE:-}" ]; then
  for candidate in "${APP_HOME}"/lib/sis-jhdf5-*-natives.tar.gz; do
    if [ -f "${candidate}" ]; then
      export HICT_JHDF5_NATIVES_ARCHIVE="${candidate}"
      break
    fi
  done
fi
if [ -z "${WEBUI_ROOT:-}" ] && [ -d "${APP_HOME}/webui" ]; then
  export WEBUI_ROOT="${APP_HOME}/webui"
fi
if [ -z "${HICT_TOOLCHAIN_DIR:-}" ] && [ -f "${APP_HOME}/toolchains/linux_x86_64/manifest.json" ]; then
  export HICT_TOOLCHAIN_DIR="${APP_HOME}/toolchains/linux_x86_64"
fi
if [ -z "${HICT_BROWSER_DIR:-}" ] && [ -d "${APP_HOME}/browsers/linux_x86_64" ]; then
  export HICT_BROWSER_DIR="${APP_HOME}/browsers/linux_x86_64"
fi
if [ -z "${HICT_BIND_HOST:-}" ]; then
  export HICT_BIND_HOST="127.0.0.1"
fi
if [ "$#" -eq 0 ]; then
  export HICT_LAUNCHER_MODE="${HICT_LAUNCHER_MODE:-gui}"
fi

HICT_TEMP_DIR="$(select_runtime_temp_dir)"
export HICT_TEMP_DIR
export TMP="${TMP:-${HICT_TEMP_DIR}}"
export TEMP="${TEMP:-${HICT_TEMP_DIR}}"
LOADER_LIBRARY_PATH="${LIB_PATH}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"

if [ -n "${HICT_JAVA_OPTS:-}" ]; then
  # shellcheck disable=SC2086
  set -- ${HICT_JAVA_OPTS} -jar "${APP_HOME}/lib/hict.jar" "$@"
else
  set -- -jar "${APP_HOME}/lib/hict.jar" "$@"
fi

exec "${LOADER}" --library-path "${LOADER_LIBRARY_PATH}" "${JAVA_BIN}" "-Djava.io.tmpdir=${HICT_TEMP_DIR}" "$@"
EOF
  chmod 0755 "${launcher}"

  if [[ -d "${APPDIR}/usr/lib/hict/toolchains/linux_x86_64/bin" ]]; then
    while IFS= read -r -d '' candidate; do
      install_glibc_exec_wrapper "${candidate}"
    done < <(find "${APPDIR}/usr/lib/hict/toolchains/linux_x86_64/bin" -maxdepth 1 -type f -perm -0100 -print0)
  fi
  if [[ -d "${APPDIR}/usr/lib/hict/browsers/linux_x86_64" ]]; then
    while IFS= read -r -d '' candidate; do
      install_glibc_exec_wrapper "${candidate}"
    done < <(find "${APPDIR}/usr/lib/hict/browsers/linux_x86_64" -type f -perm -0100 -print0)
  fi
}

if [[ "${HICT_SKIP_PORTABLE:-0}" != "1" || ! -x "${PORTABLE_APP_DIR}/bin/hict" ]]; then
  "${SCRIPT_DIR}/build_portable_linux.sh"
fi

if [[ ! -x "${PORTABLE_APP_DIR}/bin/hict" ]]; then
  echo "Portable app was not found at ${PORTABLE_APP_DIR}" >&2
  exit 1
fi
if [[ ! -f "${PORTABLE_APP_DIR}/webui/index.html" ]]; then
  echo "Portable app does not contain extracted WebUI assets." >&2
  exit 1
fi

rm -rf "${APPDIR}"
mkdir -p \
  "${APPDIR}/usr/lib/hict" \
  "${APPDIR}/usr/share/metainfo" \
  "${APPDIR}/usr/share/applications" \
  "${APPDIR}/usr/share/icons/hicolor/scalable/apps" \
  "${ARTIFACT_DIR}"

cp -a "${PORTABLE_APP_DIR}/." "${APPDIR}/usr/lib/hict/"

cat > "${APPDIR}/AppRun" <<'EOF'
#!/bin/sh
set -eu

APPDIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
if [ -z "${DATA_DIR:-}" ]; then
  if [ -n "${APPIMAGE:-}" ]; then
    DATA_DIR="$(CDPATH= cd -- "$(dirname -- "${APPIMAGE}")" && pwd -P)"
  else
    DATA_DIR="${APPDIR}"
  fi
  export DATA_DIR
fi
if [ -z "${WEBUI_ROOT:-}" ] && [ -d "${APPDIR}/usr/lib/hict/webui" ]; then
  export WEBUI_ROOT="${APPDIR}/usr/lib/hict/webui"
fi
export HICT_APP_HOME="${APPDIR}/usr/lib/hict"
export HICT_JAR_PATH="${APPDIR}/usr/lib/hict/lib/hict.jar"
if [ -z "${HICT_BROWSER_DIR:-}" ] && [ -d "${APPDIR}/usr/lib/hict/browsers/linux_x86_64" ]; then
  export HICT_BROWSER_DIR="${APPDIR}/usr/lib/hict/browsers/linux_x86_64"
fi

if [ "${HICT_APPIMAGE_FORCE_BUNDLED_GLIBC:-0}" != "1" ] &&
   [ -x "${APPDIR}/usr/lib/hict/runtime/bin/java" ] &&
   "${APPDIR}/usr/lib/hict/runtime/bin/java" -version >/dev/null 2>&1; then
  exec "${APPDIR}/usr/lib/hict/bin/hict" "$@"
fi

if [ -x "${APPDIR}/usr/lib/hict/bin/hict-appimage-glibc" ]; then
  exec "${APPDIR}/usr/lib/hict/bin/hict-appimage-glibc" "$@"
fi
exec "${APPDIR}/usr/lib/hict/bin/hict" "$@"
EOF
chmod +x "${APPDIR}/AppRun"

cat > "${APPDIR}/${DESKTOP_ID}.desktop" <<EOF
[Desktop Entry]
Type=Application
Name=HiCT
Comment=Hi-C scaffolding and visualization workstation
Exec=HiCT
Icon=hict
Categories=Science;
Terminal=false
EOF
cp "${APPDIR}/${DESKTOP_ID}.desktop" "${APPDIR}/usr/share/applications/${DESKTOP_ID}.desktop"

cat > "${APPDIR}/usr/share/metainfo/${DESKTOP_ID}.appdata.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<component type="desktop-application">
  <id>${DESKTOP_ID}</id>
  <name>HiCT</name>
  <summary>Hi-C scaffolding and visualization workstation</summary>
  <metadata_license>MIT</metadata_license>
  <project_license>MIT</project_license>
  <url type="homepage">https://github.com/ctlab/HiCT_JVM</url>
  <description>
    <p>HiCT is a portable workstation for Hi-C map visualization, assembly review, and conversion workflows.</p>
  </description>
  <launchable type="desktop-id">${DESKTOP_ID}.desktop</launchable>
  <provides>
    <binary>hict</binary>
  </provides>
  <categories>
    <category>Science</category>
  </categories>
  <releases>
    <release version="${VERSION}" date="$(date -u +%Y-%m-%d)"/>
  </releases>
</component>
EOF

cat > "${APPDIR}/usr/share/icons/hicolor/scalable/apps/hict.svg" <<'EOF'
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 128 128">
  <rect width="128" height="128" rx="24" fill="#101820"/>
  <path d="M23 104 104 23" stroke="#18b6f6" stroke-width="10" stroke-linecap="round"/>
  <path d="M31 31h22v66H31zM60 31h22v66H60zM89 31h8v66h-8z" fill="#f4f8fb" opacity=".9"/>
  <path d="M24 104h80" stroke="#f03a47" stroke-width="8" stroke-linecap="round"/>
</svg>
EOF
cp "${APPDIR}/usr/share/icons/hicolor/scalable/apps/hict.svg" "${APPDIR}/hict.svg"
cp "${APPDIR}/usr/share/icons/hicolor/scalable/apps/hict.svg" "${APPDIR}/.DirIcon"

if [[ "${HICT_APPIMAGE_BUNDLE_GLIBC}" == "1" || "${HICT_APPIMAGE_BUNDLE_GLIBC}" == "true" || "${HICT_APPIMAGE_BUNDLE_GLIBC}" == "yes" ]]; then
  bundle_appimage_glibc_runtime
  install_appimage_glibc_launchers
  {
    echo
    echo "AppImage bundled glibc runtime"
    echo "=============================="
    echo
    echo "This AppImage includes a glibc dynamic loader and runtime library closure"
    echo "under usr/lib/hict/glibc. AppRun starts HiCT through that bundled loader,"
    echo "and bundled executable tool/browser payloads are wrapped to do the same."
    echo "The glibc runtime is redistributed under the GNU LGPL/GPL notices supplied"
    echo "by the build distribution. Keep the bundled glibc manifest and license files"
    echo "with redistributed AppImage artifacts."
  } >> "${APPDIR}/usr/lib/hict/licenses/PORTABLE_DISTRIBUTION_NOTICE.txt"
  if [[ -f /usr/share/doc/libc6/copyright ]]; then
    cp /usr/share/doc/libc6/copyright "${APPDIR}/usr/lib/hict/licenses/glibc-copyright"
  fi
fi

if [[ -n "${APPIMAGETOOL:-}" ]]; then
  APPIMAGETOOL_BIN="${APPIMAGETOOL}"
else
  APPIMAGETOOL_BIN="$(command -v appimagetool || true)"
fi

if [[ -z "${APPIMAGETOOL_BIN}" ]]; then
  require_cmd curl
  mkdir -p "${TOOLS_DIR}"
  APPIMAGETOOL_BIN="${TOOLS_DIR}/appimagetool-${APPIMAGE_ARCH}.AppImage"
  if [[ ! -x "${APPIMAGETOOL_BIN}" ]]; then
    curl -L --fail --retry 3 -o "${APPIMAGETOOL_BIN}" "${APPIMAGETOOL_URL}"
    chmod +x "${APPIMAGETOOL_BIN}"
  fi
fi

APPIMAGE_RUNTIME_FILE="${APPIMAGE_RUNTIME:-}"
if [[ "${APPIMAGE_RUNTIME_FILE}" == "none" || "${APPIMAGE_RUNTIME_FILE}" == "0" ]]; then
  APPIMAGE_RUNTIME_FILE=""
elif [[ -z "${APPIMAGE_RUNTIME_FILE}" ]]; then
  require_cmd curl
  mkdir -p "${TOOLS_DIR}"
  APPIMAGE_RUNTIME_FILE="${TOOLS_DIR}/runtime-${APPIMAGE_ARCH}"
  if [[ ! -s "${APPIMAGE_RUNTIME_FILE}" ]]; then
    curl -L --fail --retry 3 -o "${APPIMAGE_RUNTIME_FILE}" "${APPIMAGE_RUNTIME_URL}"
    chmod +x "${APPIMAGE_RUNTIME_FILE}"
  fi
fi

rm -f "${APPIMAGE_PATH}" "${SHA_PATH}"
APPIMAGETOOL_ARGS=(
  --comp "${APPIMAGE_COMPRESSION}"
  --mksquashfs-opt -b
  --mksquashfs-opt "${APPIMAGE_MKSQUASHFS_BLOCK_SIZE}"
)
if [[ "${APPIMAGE_COMPRESSION}" == "xz" ]]; then
  APPIMAGETOOL_ARGS+=(
    --mksquashfs-opt -Xdict-size
    --mksquashfs-opt "100%"
    --mksquashfs-opt -Xbcj
    --mksquashfs-opt x86
  )
elif [[ "${APPIMAGE_COMPRESSION}" == "zstd" ]]; then
  APPIMAGETOOL_ARGS+=(
    --mksquashfs-opt -Xcompression-level
    --mksquashfs-opt "${APPIMAGE_ZSTD_LEVEL}"
  )
fi
if [[ "${APPIMAGE_VALIDATE}" != "1" ]]; then
  APPIMAGETOOL_ARGS=(-n "${APPIMAGETOOL_ARGS[@]}")
fi
if [[ -n "${APPIMAGE_RUNTIME_FILE}" ]]; then
  APPIMAGETOOL_ARGS+=(--runtime-file "${APPIMAGE_RUNTIME_FILE}")
fi

ARCH="${APPIMAGE_ARCH}" APPIMAGE_EXTRACT_AND_RUN=1 "${APPIMAGETOOL_BIN}" "${APPIMAGETOOL_ARGS[@]}" "${APPDIR}" "${APPIMAGE_PATH}"

APPIMAGE_SMOKE_TEST_DIR="$(mktemp -d "${TMPDIR:-/tmp}/hict-appimage-smoke-test.XXXXXX")"
cleanup_appimage_smoke_test() {
  rm -rf "${APPIMAGE_SMOKE_TEST_DIR}"
}
trap cleanup_appimage_smoke_test EXIT
mkdir -p "${APPIMAGE_SMOKE_TEST_DIR}/data" "${APPIMAGE_SMOKE_TEST_DIR}/cache" "${APPIMAGE_SMOKE_TEST_DIR}/tmp"
perl -e 'alarm shift @ARGV; exec @ARGV or die $!' 60 env \
  APPIMAGE_EXTRACT_AND_RUN=1 \
  DATA_DIR="${APPIMAGE_SMOKE_TEST_DIR}/data" \
  HICT_PORTABLE_DATA_DIR="${APPIMAGE_SMOKE_TEST_DIR}/data" \
  XDG_CACHE_HOME="${APPIMAGE_SMOKE_TEST_DIR}/cache" \
  TMPDIR="${APPIMAGE_SMOKE_TEST_DIR}/tmp" \
  HICT_LAUNCHER_MODE=cli \
  "${APPIMAGE_PATH}" check-toolchains --require-hdf5-native --check-available-natives --quiet
rm -rf "${APPIMAGE_SMOKE_TEST_DIR}"
trap - EXIT

(
  cd "${ARTIFACT_DIR}"
  sha256sum "$(basename "${APPIMAGE_PATH}")" > "${SHA_PATH}"
)

echo "Built ${APPIMAGE_PATH}"
echo "Wrote ${SHA_PATH}"
