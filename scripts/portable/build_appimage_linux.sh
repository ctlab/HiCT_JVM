#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DIST_ROOT="${HICT_PORTABLE_DIST_DIR:-${PROJECT_DIR}/build/portable}"
ARTIFACT_DIR="${PROJECT_DIR}/build/distributions"
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

usage() {
  cat <<'EOF'
Build a Linux AppImage for HiCT from the portable Linux app directory.

Outputs:
  build/distributions/HiCT-<version>-x86_64.AppImage
  build/distributions/HiCT-<version>-x86_64.AppImage.sha256

Environment overrides:
  APPIMAGETOOL=/path/to/appimagetool     Use an existing appimagetool binary.
  APPIMAGETOOL_URL=<url>                 Override the official appimagetool URL.
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
require_cmd mkdir
require_cmd sha256sum

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
#!/usr/bin/env bash
set -euo pipefail

APPDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -z "${DATA_DIR:-}" ]]; then
  if [[ -n "${APPIMAGE:-}" ]]; then
    export DATA_DIR="$(cd "$(dirname "${APPIMAGE}")" && pwd)"
  else
    export DATA_DIR="${APPDIR}"
  fi
fi
if [[ -z "${WEBUI_ROOT:-}" && -d "${APPDIR}/usr/lib/hict/webui" ]]; then
  export WEBUI_ROOT="${APPDIR}/usr/lib/hict/webui"
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
Terminal=true
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

if [[ -n "${APPIMAGETOOL:-}" ]]; then
  APPIMAGETOOL_BIN="${APPIMAGETOOL}"
else
  APPIMAGETOOL_BIN="$(command -v appimagetool || true)"
fi

if [[ -z "${APPIMAGETOOL_BIN}" ]]; then
  require_cmd curl
  TOOLS_DIR="${PROJECT_DIR}/build/tools"
  mkdir -p "${TOOLS_DIR}"
  APPIMAGETOOL_BIN="${TOOLS_DIR}/appimagetool-${APPIMAGE_ARCH}.AppImage"
  if [[ ! -x "${APPIMAGETOOL_BIN}" ]]; then
    curl -L --fail --retry 3 -o "${APPIMAGETOOL_BIN}" "${APPIMAGETOOL_URL}"
    chmod +x "${APPIMAGETOOL_BIN}"
  fi
fi

rm -f "${APPIMAGE_PATH}" "${SHA_PATH}"
ARCH="${APPIMAGE_ARCH}" APPIMAGE_EXTRACT_AND_RUN=1 "${APPIMAGETOOL_BIN}" "${APPDIR}" "${APPIMAGE_PATH}"

(
  cd "${ARTIFACT_DIR}"
  sha256sum "$(basename "${APPIMAGE_PATH}")" > "${SHA_PATH}"
)

echo "Built ${APPIMAGE_PATH}"
echo "Wrote ${SHA_PATH}"
