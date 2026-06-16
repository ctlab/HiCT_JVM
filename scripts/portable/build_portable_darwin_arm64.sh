#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DIST_ROOT="${HICT_PORTABLE_DIST_DIR:-${PROJECT_DIR}/build/portable}"
DARWIN_ARCH="${HICT_DARWIN_ARCH:-arm64}"
case "${DARWIN_ARCH}" in
  arm64) ;;
  x86_64|amd64) DARWIN_ARCH="x86_64" ;;
  *) echo "Unsupported HICT_DARWIN_ARCH=${DARWIN_ARCH}; expected arm64 or x86_64." >&2; exit 1 ;;
esac
TOOLCHAIN_PLATFORM="${HICT_DARWIN_PLATFORM_DIR:-darwin_${DARWIN_ARCH}}"
PLATFORM="${HICT_DARWIN_ARTIFACT_PLATFORM:-darwin-${DARWIN_ARCH}}"
MACOS_DEPLOYMENT_TARGET="${MACOS_DEPLOYMENT_TARGET:-12.0}"
APP_NAME="HiCT"
VERSION="$(tr -d '[:space:]' < "${PROJECT_DIR}/version.txt")"
APP_DIR="${DIST_ROOT}/${APP_NAME}-${VERSION}-${PLATFORM}"
ARTIFACT_DIR="${PROJECT_DIR}/build/distributions"
RUNTIME_MODULES="${HICT_RUNTIME_MODULES:-java.se,jdk.charsets,jdk.crypto.ec,jdk.localedata,jdk.management,jdk.unsupported,jdk.zipfs}"
RUN_PAYLOAD_XZ_THREADS="${HICT_RUN_PAYLOAD_XZ_THREADS:-2}"
LAUNCHER_SOURCE="${SCRIPT_DIR}/macos_launcher/HiCTDarwinLauncher.cpp"
LAUNCHER_BASENAME="hict-darwin-${DARWIN_ARCH}"
LAUNCHER_BINARY="${APP_DIR}/bin/${LAUNCHER_BASENAME}"

usage() {
  cat <<'EOF'
Build a self-contained macOS HiCT portable package with an embedded Java runtime.

Outputs:
  build/distributions/HiCT-<version>-${PLATFORM}.run
  build/distributions/HiCT-<version>-${PLATFORM}.tar.gz
  build/distributions/HiCT-<version>-${PLATFORM}.sha256

Environment overrides:
  HICT_SKIP_GRADLE=1                 Reuse an existing build/libs/*-fat.jar.
  HICT_RUNTIME_MODULES=<modules>     Override jlink modules.
  HICT_PORTABLE_DIST_DIR=<dir>       Override staging directory.
  HICT_RUN_PAYLOAD_XZ_THREADS=<n>    xz threads for the .run payload, default 2.

The macOS .run wrapper accepts --help and --hict-extract-only <directory>.
If Gatekeeper quarantines the file after download, clear it with:
  xattr -d com.apple.quarantine ./HiCT-*.run
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
require_cmd shasum
require_cmd awk
require_cmd grep
require_cmd tail
require_cmd clang++
require_cmd codesign

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


resolve_jhdf5_for_portable_release() {
  local mode="${HICT_JHDF5_SOURCE_MODE:-artifact}"
  case "${mode}" in
    maven|maven-central|published)
      echo "[darwin/portable] HICT_JHDF5_SOURCE_MODE=${mode}; intentionally using Maven JHDF5."
      export HICT_USE_MAVEN_JHDF5=1
      export HICT_REQUIRE_BUNDLED_JHDF5=0
      return 0
      ;;
    local)
      local local_jar="${HICT_JHDF5_LOCAL_JAR:-src/main/resources/libs/${HICT_JHDF5_JAR_NAME:-sis-jhdf5-19.04.1.jar}}"
      if [[ ! -f "${PROJECT_DIR}/${local_jar}" && ! -f "${local_jar}" ]]; then
        echo "HICT_JHDF5_SOURCE_MODE=local but ${local_jar} does not exist. Use artifact/release mode, or set HICT_JHDF5_SOURCE_MODE=maven intentionally." >&2
        exit 1
      fi
      export HICT_REQUIRE_BUNDLED_JHDF5=1
      return 0
      ;;
    artifact|release|auto|"")
      local resolver="${PROJECT_DIR}/scripts/ci/resolve_jhdf5_jar.sh"
      if [[ ! -x "${resolver}" ]]; then
        echo "Missing executable ${resolver}; cannot resolve bundled JHDF5 artifact for macOS portable release." >&2
        echo "Set HICT_JHDF5_SOURCE_MODE=maven to intentionally use Maven JHDF5 instead." >&2
        exit 1
      fi
      export HICT_JHDF5_SOURCE_MODE="${mode:-artifact}"
      "${resolver}"
      local local_jar="${HICT_JHDF5_LOCAL_JAR:-src/main/resources/libs/${HICT_JHDF5_JAR_NAME:-sis-jhdf5-19.04.1.jar}}"
      if [[ -f "${PROJECT_DIR}/${local_jar}" || -f "${local_jar}" ]]; then
        export HICT_REQUIRE_BUNDLED_JHDF5=1
      else
        echo "::warning::JHDF5 snapshot artifact was not resolved for this macOS runner; using Maven-provided cisd:jhdf5:19.04.1."
        export HICT_JHDF5_SOURCE_MODE=maven
        export HICT_USE_MAVEN_JHDF5=1
        export HICT_REQUIRE_BUNDLED_JHDF5=0
      fi
      return 0
      ;;
    *)
      echo "Unsupported HICT_JHDF5_SOURCE_MODE=${mode}; use artifact, release, local, or maven." >&2
      exit 1
      ;;
  esac
}

darwin_jhdf5_platform_dir() {
  if [[ "${DARWIN_ARCH}" == "x86_64" ]]; then
    printf '%s\n' "x86_64-Mac OS X"
  else
    printf '%s\n' "aarch64-Mac OS X"
  fi
}

darwin_jhdf5_archive_name() {
  printf '%s\n' "${HICT_JHDF5_NATIVES_ARCHIVE_NAME:-sis-jhdf5-19.04.1-natives.tar.gz}"
}

resolve_darwin_jhdf5_archive_source() {
  local archive_name
  archive_name="$(darwin_jhdf5_archive_name)"
  local candidates=(
    "${PROJECT_DIR}/build/jhdf5-runtime/${archive_name}"
    "${PROJECT_DIR}/build/resources/main/libs/${archive_name}"
    "${HICT_JHDF5_NATIVES_ARCHIVE:-}"
    "${PROJECT_DIR}/${HICT_JHDF5_NATIVES_ARCHIVE:-}"
    "${PROJECT_DIR}/src/main/resources/libs/${archive_name}"
    "${PROJECT_DIR}/.gradle/jhdf5/${archive_name}"
  )
  local candidate
  for candidate in "${candidates[@]}"; do
    if [[ -n "${candidate}" && -f "${candidate}" ]]; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done
  return 1
}

archive_contains_darwin_jhdf5_payload() {
  local archive="$1"
  local platform_dir
  platform_dir="$(darwin_jhdf5_platform_dir)"
  tar -tzf "${archive}" | grep -E -qx "native/jhdf5/${platform_dir}/libhdf5(\\.[0-9]+(\\.[0-9]+)*)?\\.dylib" &&
    tar -tzf "${archive}" | grep -qx "native/jhdf5/${platform_dir}/libjhdf5.jnilib"
}

fat_jar_contains_darwin_jhdf5_payload() {
  if [[ "${DARWIN_ARCH}" == "x86_64" ]]; then
    "${JAR_TOOL}" tf "${FAT_JAR}" | grep -E -qx 'resources/libs/(osx_64|macos_64|darwin_x86_64)/libhdf5(\.[0-9]+(\.[0-9]+)*)?\.dylib'
  else
    "${JAR_TOOL}" tf "${FAT_JAR}" | grep -E -qx 'resources/libs/(osx_arm64|macos_arm64|darwin_arm64)/libhdf5(\.[0-9]+(\.[0-9]+)*)?\.dylib'
  fi
}

prepare_darwin_jhdf5_sidecar_archive() {
  local source_archive
  source_archive="$(resolve_darwin_jhdf5_archive_source || true)"
  if [[ -z "${source_archive}" ]]; then
    if fat_jar_contains_darwin_jhdf5_payload; then
      return 0
    fi
    echo "Cannot find JHDF5 native archive for macOS ${DARWIN_ARCH}; macOS portable packages require the JHDF5/HDF5 dylib tree." >&2
    exit 1
  fi
  if ! archive_contains_darwin_jhdf5_payload "${source_archive}"; then
    echo "JHDF5 native archive ${source_archive} does not contain the required macOS ${DARWIN_ARCH} HDF5/JHDF5 dylibs." >&2
    exit 1
  fi

  local platform_dir
  platform_dir="$(darwin_jhdf5_platform_dir)"
  local archive_name
  archive_name="$(darwin_jhdf5_archive_name)"
  local tmp
  tmp="$(mktemp -d)"
  tar -xzf "${source_archive}" -C "${tmp}" "native/jhdf5/${platform_dir}"
  tar -czf "${APP_DIR}/lib/${archive_name}" -C "${tmp}" "native/jhdf5/${platform_dir}"
  rm -rf "${tmp}"
  echo "[darwin/portable] Bundled platform JHDF5 native archive: lib/${archive_name} (${platform_dir})"
}

if [[ "${HICT_SKIP_GRADLE:-0}" != "1" ]]; then
  resolve_jhdf5_for_portable_release
  # macOS portable packages intentionally embed only the target platform's
  # JHDF5/HDF5 runtime sidecar.  The separately published fat JAR remains
  # universal and is validated by the Linux release build.
  export HICT_VERIFY_BUNDLED_JHDF5_SCOPE="${HICT_VERIFY_BUNDLED_JHDF5_SCOPE:-runtime}"
  (cd "${PROJECT_DIR}" && ./gradlew -PrequireBundledWebUI=true verifyBundledJhdf5Payload shadowJar)
fi

HICT_DARWIN_ARCH="${DARWIN_ARCH}" HICT_DARWIN_PLATFORM_DIR="${TOOLCHAIN_PLATFORM}" "${SCRIPT_DIR}/../toolchains/build_darwin_toolchains.sh"

FAT_JAR="$(find "${PROJECT_DIR}/build/libs" -maxdepth 1 -type f -name '*-fat.jar' | sort | tail -n 1)"
if [[ -z "${FAT_JAR}" || ! -f "${FAT_JAR}" ]]; then
  echo "Fat JAR was not found under ${PROJECT_DIR}/build/libs" >&2
  exit 1
fi
if ! "${JAR_TOOL}" tf "${FAT_JAR}" | grep -qx 'webui/index.html'; then
  echo "Fat JAR does not contain webui/index.html; portable packages require a baked-in HiCT_WebUI build." >&2
  exit 1
fi
DARWIN_JHDF5_ARCHIVE_SOURCE="$(resolve_darwin_jhdf5_archive_source || true)"
if ! fat_jar_contains_darwin_jhdf5_payload; then
  if [[ -z "${DARWIN_JHDF5_ARCHIVE_SOURCE}" ]] || ! archive_contains_darwin_jhdf5_payload "${DARWIN_JHDF5_ARCHIVE_SOURCE}"; then
    echo "Neither the fat JAR nor the split JHDF5 native archive contains a supported macOS ${DARWIN_ARCH} HDF5/JHDF5 dylib tree." >&2
    exit 1
  fi
fi

rm -rf "${APP_DIR}"
mkdir -p \
  "${APP_DIR}/bin" \
  "${APP_DIR}/lib" \
  "${APP_DIR}/licenses" \
  "${APP_DIR}/toolchains" \
  "${APP_DIR}/share/doc" \
  "${ARTIFACT_DIR}"

cp "${FAT_JAR}" "${APP_DIR}/lib/hict.jar"
prepare_darwin_jhdf5_sidecar_archive
cp "${PROJECT_DIR}/LICENSE" "${APP_DIR}/licenses/HiCT_JVM_LICENSE"
if [[ -f "${PROJECT_DIR}/../HiCT_WebUI/LICENSE" ]]; then
  cp "${PROJECT_DIR}/../HiCT_WebUI/LICENSE" "${APP_DIR}/licenses/HiCT_WebUI_LICENSE"
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
  - Optional bundled hictk, minimap2, and mm2-plus resources under toolchains/
    when toolchains-dist was prepared before packaging. hictk is redistributed
    under its MIT license and should be cited when .hic conversion is used.
  - Optional bundled browser resources under browsers/ when browsers-dist was
    prepared before packaging. Browser payloads must be redistributed with their
    upstream license, trademark, and update requirements.

License locations inside this package:

  - licenses/ contains HiCT and portable distribution notices.
  - runtime/legal/ contains the bundled Java runtime notices.
  - toolchains/ contains bundled external command-line tools, when present.
  - browsers/ contains bundled browser launcher resources, when present.

The macOS .run artifact is a transparent shell wrapper with an appended tar.xz
payload. It is used to keep the release inspectable and avoid opaque native
self-extracting packers.
EOF

(
  cd "${APP_DIR}"
  "${JAR_TOOL}" xf "${APP_DIR}/lib/hict.jar" webui
)

if [[ -d "${PROJECT_DIR}/toolchains-dist/${TOOLCHAIN_PLATFORM}" ]]; then
  cp -a "${PROJECT_DIR}/toolchains-dist/${TOOLCHAIN_PLATFORM}" "${APP_DIR}/toolchains/"
fi
if [[ -f "${APP_DIR}/toolchains/${TOOLCHAIN_PLATFORM}/manifest.json" ]]; then
  for tool in hictk minimap2 mm2plus mm2plus-avx2; do
    chmod 0755 "${APP_DIR}/toolchains/${TOOLCHAIN_PLATFORM}/bin/${tool}" 2>/dev/null || true
  done
  "${APP_DIR}/toolchains/${TOOLCHAIN_PLATFORM}/bin/hictk" --help >/dev/null
  "${APP_DIR}/toolchains/${TOOLCHAIN_PLATFORM}/bin/minimap2" --help >/dev/null
  "${APP_DIR}/toolchains/${TOOLCHAIN_PLATFORM}/bin/mm2plus-avx2" --help >/dev/null || true
fi
if [[ -d "${PROJECT_DIR}/browsers-dist/${TOOLCHAIN_PLATFORM}" ]] &&
   find "${PROJECT_DIR}/browsers-dist/${TOOLCHAIN_PLATFORM}" -name manifest.json -type f | grep -q .; then
  mkdir -p "${APP_DIR}/browsers"
  cp -a "${PROJECT_DIR}/browsers-dist/${TOOLCHAIN_PLATFORM}" "${APP_DIR}/browsers/"
fi

"${JLINK}" \
  --add-modules "${RUNTIME_MODULES}" \
  --strip-debug \
  --no-header-files \
  --no-man-pages \
  --compress=zip-6 \
  --output "${APP_DIR}/runtime"

cat > "${LAUNCHER_SOURCE}.tmp.cpp" <<'EOF'
#include <cstdlib>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <limits.h>
#include <mach-o/dyld.h>
#include <string>
#include <system_error>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;

static std::string executable_path() {
  uint32_t size = 0;
  _NSGetExecutablePath(nullptr, &size);
  std::string path(size, '\0');
  if (_NSGetExecutablePath(path.data(), &size) != 0) {
    return {};
  }
  char resolved[PATH_MAX];
  if (realpath(path.c_str(), resolved) != nullptr) {
    return resolved;
  }
  path.resize(std::char_traits<char>::length(path.c_str()));
  return path;
}

static std::string parent_dir(const std::string& path) {
  return fs::path(path).parent_path().string();
}

static void set_env_if_unset(const char* key, const std::string& value) {
  if (std::getenv(key) == nullptr) {
    setenv(key, value.c_str(), 1);
  }
}

static void set_env_if_unset_existing_dir(const char* key, const std::vector<std::string>& candidates) {
  if (std::getenv(key) != nullptr) {
    return;
  }
  for (const auto& candidate : candidates) {
    std::error_code ec;
    if (fs::is_directory(candidate, ec)) {
      setenv(key, candidate.c_str(), 1);
      return;
    }
  }
}

static std::string selected_data_dir(const std::string& app_home) {
  if (const char* data_dir = std::getenv("DATA_DIR"); data_dir != nullptr && *data_dir != '\0') {
    return data_dir;
  }
  if (const char* portable_dir = std::getenv("HICT_PORTABLE_DATA_DIR"); portable_dir != nullptr && *portable_dir != '\0') {
    return portable_dir;
  }
  return app_home;
}

static std::string selected_temp_dir(const std::string& data_dir) {
  const std::vector<std::string> candidates = {
    std::getenv("HICT_TEMP_DIR") ? std::getenv("HICT_TEMP_DIR") : "",
    std::getenv("HICT_EXEC_TEMP_DIR") ? std::getenv("HICT_EXEC_TEMP_DIR") : "",
    data_dir + "/tmp",
    std::getenv("TMPDIR") ? std::getenv("TMPDIR") : "",
    "/tmp/hict-" + std::to_string(getuid()) + "/runtime",
  };
  for (const auto& candidate : candidates) {
    if (candidate.empty()) {
      continue;
    }
    std::error_code ec;
    fs::create_directories(candidate, ec);
    if (ec) {
      continue;
    }
    if (access(candidate.c_str(), W_OK | X_OK) == 0) {
      return candidate;
    }
  }
  return data_dir + "/tmp";
}

static int launch_java(const std::string& java_path, const std::vector<std::string>& args) {
  std::vector<char*> argv;
  argv.reserve(args.size() + 2);
  argv.push_back(const_cast<char*>(java_path.c_str()));
  for (const auto& arg : args) {
    argv.push_back(const_cast<char*>(arg.c_str()));
  }
  argv.push_back(nullptr);
  execv(java_path.c_str(), argv.data());
  std::perror("execv");
  return 127;
}

int main(int argc, char* argv[]) {
  const std::string exe_path = executable_path();
  if (exe_path.empty()) {
    std::cerr << "Failed to resolve HiCT launcher path.\n";
    return 1;
  }

  const std::string bin_dir = parent_dir(exe_path);
  const std::string app_home = parent_dir(bin_dir);
  const std::string data_dir = selected_data_dir(app_home);
  const std::string temp_dir = selected_temp_dir(data_dir);

  set_env_if_unset("DATA_DIR", data_dir);
  set_env_if_unset("HICT_APP_HOME", app_home);
  set_env_if_unset("HICT_JAR_PATH", app_home + "/lib/hict.jar");
  for (const auto& entry : fs::directory_iterator(app_home + "/lib")) {
    const auto file_name = entry.path().filename().string();
    if (entry.is_regular_file() &&
        file_name.rfind("sis-jhdf5-", 0) == 0 &&
        file_name.size() >= std::string("-natives.tar.gz").size() &&
        file_name.substr(file_name.size() - std::string("-natives.tar.gz").size()) == "-natives.tar.gz") {
      set_env_if_unset("HICT_JHDF5_NATIVES_ARCHIVE", entry.path().string());
      break;
    }
  }
  if (fs::exists(app_home + "/webui")) {
    set_env_if_unset("WEBUI_ROOT", app_home + "/webui");
  }
  set_env_if_unset_existing_dir("HICT_TOOLCHAIN_DIR", {
    app_home + "/toolchains/darwin_arm64",
    app_home + "/toolchains/darwin_x86_64"
  });
  set_env_if_unset_existing_dir("HICT_BROWSER_DIR", {
    app_home + "/browsers/darwin_arm64",
    app_home + "/browsers/darwin_x86_64"
  });
  set_env_if_unset("HICT_TEMP_DIR", temp_dir);
  set_env_if_unset("TMPDIR", temp_dir);
  set_env_if_unset("TMP", temp_dir);
  set_env_if_unset("TEMP", temp_dir);
  set_env_if_unset("HICT_BIND_HOST", "127.0.0.1");
  if (argc == 1) {
    set_env_if_unset("HICT_LAUNCHER_MODE", "gui");
  }

  const std::string java_path = app_home + "/runtime/bin/java";
  if (access(java_path.c_str(), X_OK) != 0) {
    std::cerr << "Missing embedded Java runtime: " << java_path << "\n";
    return 1;
  }

  std::vector<std::string> args = {
    "-Djava.io.tmpdir=" + temp_dir,
    "-jar",
    app_home + "/lib/hict.jar",
  };
  for (int i = 1; i < argc; ++i) {
    args.emplace_back(argv[i]);
  }
  return launch_java(java_path, args);
}
EOF

clang++ -std=c++17 -O2 -arch "${DARWIN_ARCH}" -mmacosx-version-min="${MACOS_DEPLOYMENT_TARGET}" "${LAUNCHER_SOURCE}.tmp.cpp" -o "${LAUNCHER_BINARY}"
rm -f "${LAUNCHER_SOURCE}.tmp.cpp"
codesign -s - --force --timestamp=none "${LAUNCHER_BINARY}"

adhoc_sign_macho_tree() {
  local root="$1"
  [[ -d "${root}" ]] || return 0
  while IFS= read -r -d '' f; do
    if file "${f}" | grep -q 'Mach-O'; then
      codesign -s - --force --timestamp=none "${f}" >/dev/null 2>&1 || {
        echo "Failed to ad-hoc sign ${f}" >&2
        return 1
      }
    fi
  done < <(find "${root}" -type f -print0)
}

adhoc_sign_macho_tree "${APP_DIR}/runtime"
adhoc_sign_macho_tree "${APP_DIR}/toolchains"
adhoc_sign_macho_tree "${APP_DIR}/browsers"

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
exec "${APP_HOME}/bin/__HICT_DARWIN_LAUNCHER__" "$@"
EOF
python3 - "${APP_DIR}/bin/hict" "${LAUNCHER_BASENAME}" <<'PY'
from pathlib import Path
import sys
path = Path(sys.argv[1])
launcher = sys.argv[2]
text = path.read_text(encoding="utf-8")
path.write_text(text.replace("__HICT_DARWIN_LAUNCHER__", launcher), encoding="utf-8")
PY
chmod +x "${APP_DIR}/bin/hict"

cat > "${APP_DIR}/README_PORTABLE.txt" <<EOF
HiCT portable macOS package
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
  - a signed native ${DARWIN_ARCH} launcher at bin/${LAUNCHER_BASENAME}
  - a jlink runtime built from the JDK used by the release runner
  - optional bundled browser resources under browsers/ when browsers-dist was
    prepared before packaging
  - HiCT license files and portable third-party notice under licenses/
  - the runtime/legal directory generated by jlink

License and third-party notice locations:
  - licenses/PORTABLE_DISTRIBUTION_NOTICE.txt explains bundled components
  - runtime/legal/ contains Java runtime notices
  - toolchains/ contains external command-line tools, when present
  - browsers/ contains bundled browser launcher resources, when present

With no arguments, ./bin/hict opens the graphical launcher. Explicit CLI
subcommands keep the traditional command-line behavior.

DATA_DIR defaults to the extracted app directory when running ./bin/hict
directly. The optional single-file .run wrapper sets DATA_DIR to the directory
containing the .run file. Explicit DATA_DIR always wins.

If Gatekeeper quarantines the .run file after download, clear it with:
  xattr -d com.apple.quarantine ./HiCT-*.run
EOF

TAR_PATH="${ARTIFACT_DIR}/${APP_NAME}-${VERSION}-${PLATFORM}.tar.gz"
RUN_PATH="${ARTIFACT_DIR}/${APP_NAME}-${VERSION}-${PLATFORM}.run"
SHA_PATH="${ARTIFACT_DIR}/${APP_NAME}-${VERSION}-${PLATFORM}.sha256"
PAYLOAD_PATH="${DIST_ROOT}/${APP_NAME}-${VERSION}-${PLATFORM}.payload.tar.xz"

tar -C "${DIST_ROOT}" -cf - "${APP_NAME}-${VERSION}-${PLATFORM}" | gzip -9 > "${TAR_PATH}"
tar -C "${DIST_ROOT}" -cf - "${APP_NAME}-${VERSION}-${PLATFORM}" | xz -9e -T"${RUN_PAYLOAD_XZ_THREADS}" > "${PAYLOAD_PATH}"
PAYLOAD_SHA="$(shasum -a 256 "${PAYLOAD_PATH}" | awk '{print $1}')"

cat > "${RUN_PATH}" <<EOF
#!/usr/bin/env bash
set -euo pipefail

APP_NAME="${APP_NAME}"
APP_VERSION="${VERSION}"
APP_PLATFORM="${PLATFORM}"
PAYLOAD_SHA256="${PAYLOAD_SHA}"

SELF_PATH="\${BASH_SOURCE[0]}"
while [[ -L "\${SELF_PATH}" ]]; do
  SELF_DIR="\$(cd -P "\$(dirname "\${SELF_PATH}")" && pwd)"
  SELF_PATH="\$(readlink "\${SELF_PATH}")"
  [[ "\${SELF_PATH}" != /* ]] && SELF_PATH="\${SELF_DIR}/\${SELF_PATH}"
done
SELF_DIR="\$(cd -P "\$(dirname "\${SELF_PATH}")" && pwd)"
MARKER="__HICT_PAYLOAD_BELOW__"

usage() {
  cat <<'EOU'
HiCT portable launcher

Usage:
  ./HiCT-<version>-${PLATFORM}.run [HiCT CLI args...]
  ./HiCT-<version>-${PLATFORM}.run --help
  ./HiCT-<version>-${PLATFORM}.run --hict-extract-only <directory>

DATA_DIR defaults to the directory containing this .run file. Set DATA_DIR
explicitly to use a different data directory.
EOU
}

if [[ "\${1:-}" == "--help" || "\${1:-}" == "--hict-run-help" ]]; then
  usage
  exit 0
fi

require_runtime_cmd() {
  if command -v "\$1" >/dev/null 2>&1; then
    return 0
  fi
  echo "HiCT portable launcher cannot start because required command '\$1' is not available." >&2
  exit 127
}

require_runtime_cmd tail
require_runtime_cmd tar
require_runtime_cmd xz
require_runtime_cmd mkdir
require_runtime_cmd touch
require_runtime_cmd cat

print_banner() {
  cat <<'EOB'
HiCT - Hi-C scaffolding and visualization workstation
Copyright (c) 2021-2026 Aleksandr Serdiukov, Anton Zamyatin,
Aleksandr Sinitsyn, Vitalii Dravgelis, and CT Lab ITMO University.
License: MIT. Bundled third-party tools keep their own licenses.
See licenses/PORTABLE_DISTRIBUTION_NOTICE.txt after extraction for license
locations and third-party redistribution notes.

Preparing the portable HiCT package. The first start extracts the application
payload and can take a while. Please keep this Terminal window open.

EOB
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

extract_to() {
  local target_dir="\$1"
  mkdir -p "\${target_dir}"
  echo "Extracting HiCT to \${target_dir}. Please wait..." >&2
  tail -n +"\${payload_line}" "\${SELF_PATH}" | xz -dc | tar -xf - -C "\${target_dir}"
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

if [[ "\${1:-}" == "--hict-extract-only" ]]; then
  if [[ -z "\${2:-}" ]]; then
    echo "--hict-extract-only requires a target directory." >&2
    exit 1
  fi
  extract_to "\$2"
  echo "Extracted HiCT to \$2/${APP_NAME}-${VERSION}-${PLATFORM}"
  exit 0
fi

print_banner

if [[ -z "\${DATA_DIR:-}" ]]; then
  if [[ -n "\${HICT_PORTABLE_DATA_DIR:-}" ]]; then
    export DATA_DIR="\${HICT_PORTABLE_DATA_DIR}"
  else
    export DATA_DIR="\${SELF_DIR}"
  fi
fi

local_cache_root="\${SELF_DIR}/HiCT.portable/payloads"
preferred_cache_root="\${XDG_CACHE_HOME:-\${HOME:-/tmp}/.cache}/hict/portable"
fallback_cache_root="\${TMPDIR:-/tmp}/hict-\${USER:-user}/portable"
cache_root=""
for candidate_cache_root in "\${local_cache_root}" "\${fallback_cache_root}" "\${preferred_cache_root}"; do
  if can_execute_from_directory "\${candidate_cache_root}"; then
    cache_root="\${candidate_cache_root}"
    break
  fi
done
if [[ -z "\${cache_root}" ]]; then
  echo "No executable HiCT payload cache directory is available. Check the .run directory, XDG cache, and /tmp." >&2
  exit 1
fi
if [[ "\${cache_root}" != "\${local_cache_root}" ]]; then
  export HICT_PORTABLE_NOTICE="The directory containing the .run file could not be used as an executable payload cache; using \${cache_root}."
fi
extract_root="\${cache_root}/${APP_NAME}-${VERSION}-${PLATFORM}-\${PAYLOAD_SHA256}"
app_home="\${extract_root}/${APP_NAME}-${VERSION}-${PLATFORM}"
if [[ ! -x "\${app_home}/bin/${LAUNCHER_BASENAME}" ]]; then
  rm -rf "\${extract_root}"
  extract_to "\${extract_root}"
fi

export HICT_APP_HOME="\${app_home}"
export HICT_JAR_PATH="\${app_home}/lib/hict.jar"
if [[ -z "\${HICT_JHDF5_NATIVES_ARCHIVE:-}" ]]; then
  HICT_JHDF5_NATIVES_CANDIDATE="\$(find "\${app_home}/lib" -maxdepth 1 -type f -name 'sis-jhdf5-*-natives.tar.gz' | sort | tail -n 1)"
  if [[ -n "\${HICT_JHDF5_NATIVES_CANDIDATE}" ]]; then
    export HICT_JHDF5_NATIVES_ARCHIVE="\${HICT_JHDF5_NATIVES_CANDIDATE}"
  fi
fi
if [[ -z "\${WEBUI_ROOT:-}" && -d "\${app_home}/webui" ]]; then
  export WEBUI_ROOT="\${app_home}/webui"
fi
if [[ -z "\${HICT_BIND_HOST:-}" ]]; then
  export HICT_BIND_HOST="127.0.0.1"
fi
if [[ "\$#" -eq 0 ]]; then
  export HICT_LAUNCHER_MODE="\${HICT_LAUNCHER_MODE:-gui}"
fi

exec "\${app_home}/bin/${LAUNCHER_BASENAME}" "\$@"

__HICT_PAYLOAD_BELOW__
EOF
cat "${PAYLOAD_PATH}" >> "${RUN_PATH}"
chmod +x "${RUN_PATH}"

{
  echo "project=HiCT"
  echo "platform=${PLATFORM}"
  echo "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "launcher=${LAUNCHER_BINARY}"
  echo
  echo "[file]"
  file "${LAUNCHER_BINARY}"
  echo
  echo "[codesign]"
  codesign -dv --verbose=2 "${LAUNCHER_BINARY}" 2>&1 || true
} > "${APP_DIR}/share/doc/macos-build-info.txt"

if [[ -x "${LAUNCHER_BINARY}" ]]; then
  "${LAUNCHER_BINARY}" --help >/dev/null
fi
if [[ -x "${APP_DIR}/bin/hict" ]]; then
  "${APP_DIR}/bin/hict" --help >/dev/null
fi
"${RUN_PATH}" --help >/dev/null
RUN_EXTRACT_TEST_DIR="$(mktemp -d "${TMPDIR:-/tmp}/hict-run-extract-test.XXXXXX")"
"${RUN_PATH}" --hict-extract-only "${RUN_EXTRACT_TEST_DIR}" >/dev/null
test -x "${RUN_EXTRACT_TEST_DIR}/${APP_NAME}-${VERSION}-${PLATFORM}/bin/${LAUNCHER_BASENAME}"
rm -rf "${RUN_EXTRACT_TEST_DIR}"
RUN_SMOKE_TEST_DIR="$(mktemp -d "${TMPDIR:-/tmp}/hict-run-smoke-test.XXXXXX")"
cleanup_run_smoke_test() {
  rm -rf "${RUN_SMOKE_TEST_DIR}"
}
trap cleanup_run_smoke_test EXIT
mkdir -p "${RUN_SMOKE_TEST_DIR}/data" "${RUN_SMOKE_TEST_DIR}/cache" "${RUN_SMOKE_TEST_DIR}/tmp"
perl -e 'alarm shift @ARGV; exec @ARGV or die $!' 60 env \
  DATA_DIR="${RUN_SMOKE_TEST_DIR}/data" \
  HICT_PORTABLE_DATA_DIR="${RUN_SMOKE_TEST_DIR}/data" \
  XDG_CACHE_HOME="${RUN_SMOKE_TEST_DIR}/cache" \
  TMPDIR="${RUN_SMOKE_TEST_DIR}/tmp" \
  HICT_LAUNCHER_MODE=cli \
  "${RUN_PATH}" check-toolchains --require-hdf5-native --check-available-natives --quiet
rm -rf "${RUN_SMOKE_TEST_DIR}"
trap - EXIT

{
  echo "$(shasum -a 256 "${TAR_PATH}" | awk '{print $1}')  $(basename "${TAR_PATH}")"
  echo "$(shasum -a 256 "${RUN_PATH}" | awk '{print $1}')  $(basename "${RUN_PATH}")"
} > "${SHA_PATH}"

echo "Built ${TAR_PATH}"
echo "Built ${RUN_PATH}"
echo "Wrote ${SHA_PATH}"
