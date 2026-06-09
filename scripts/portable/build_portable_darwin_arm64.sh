#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DIST_ROOT="${HICT_PORTABLE_DIST_DIR:-${PROJECT_DIR}/build/portable}"
PLATFORM="darwin-arm64"
APP_NAME="HiCT"
VERSION="$(tr -d '[:space:]' < "${PROJECT_DIR}/version.txt")"
APP_DIR="${DIST_ROOT}/${APP_NAME}-${VERSION}-${PLATFORM}"
ARTIFACT_DIR="${PROJECT_DIR}/build/distributions"
RUNTIME_MODULES="${HICT_RUNTIME_MODULES:-java.se,jdk.charsets,jdk.crypto.ec,jdk.localedata,jdk.management,jdk.unsupported,jdk.zipfs}"
RUN_PAYLOAD_XZ_THREADS="${HICT_RUN_PAYLOAD_XZ_THREADS:-2}"
LAUNCHER_SOURCE="${SCRIPT_DIR}/macos_launcher/HiCTDarwinLauncher.cpp"
LAUNCHER_BINARY="${APP_DIR}/bin/hict-darwin-arm64"

usage() {
  cat <<'EOF'
Build a self-contained macOS arm64 HiCT portable package with an embedded Java runtime.

Outputs:
  build/distributions/HiCT-<version>-darwin-arm64.run
  build/distributions/HiCT-<version>-darwin-arm64.tar.gz
  build/distributions/HiCT-<version>-darwin-arm64.sha256

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

rm -rf "${APP_DIR}"
mkdir -p \
  "${APP_DIR}/bin" \
  "${APP_DIR}/lib" \
  "${APP_DIR}/licenses" \
  "${APP_DIR}/share/doc" \
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

"${JLINK}" \
  --add-modules "${RUNTIME_MODULES}" \
  --strip-debug \
  --no-header-files \
  --no-man-pages \
  --compress=zip-6 \
  --output "${APP_DIR}/runtime"

cat > "${LAUNCHER_SOURCE}.tmp" <<'EOF'
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
  if (fs::exists(app_home + "/webui")) {
    set_env_if_unset("WEBUI_ROOT", app_home + "/webui");
  }
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

clang++ -std=c++17 -O2 -arch arm64 -mmacosx-version-min=12.0 "${LAUNCHER_SOURCE}.tmp" -o "${LAUNCHER_BINARY}"
rm -f "${LAUNCHER_SOURCE}.tmp"
codesign -s - --force --timestamp=none "${LAUNCHER_BINARY}"

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
exec "${APP_HOME}/bin/hict-darwin-arm64" "$@"
EOF
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
  - extracted HiCT_WebUI assets used as WEBUI_ROOT for robust portable serving
  - a signed native Apple Silicon launcher at bin/hict-darwin-arm64
  - a jlink runtime built from the JDK used by the release runner
  - HiCT license files
  - the runtime/legal directory generated by jlink

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
  ./HiCT-<version>-darwin-arm64.run [HiCT CLI args...]
  ./HiCT-<version>-darwin-arm64.run --help
  ./HiCT-<version>-darwin-arm64.run --hict-extract-only <directory>

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

require_runtime_cmd awk
require_runtime_cmd tail
require_runtime_cmd tar
require_runtime_cmd xz
require_runtime_cmd mkdir
require_runtime_cmd touch
require_runtime_cmd cat

payload_line="\$(awk "/^\${MARKER}\$/ { print NR + 1; exit 0; }" "\${SELF_PATH}")"
if [[ -z "\${payload_line}" ]]; then
  echo "Cannot find embedded HiCT payload." >&2
  exit 1
fi

extract_to() {
  local target_dir="\$1"
  mkdir -p "\${target_dir}"
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
if [[ ! -x "\${app_home}/bin/hict-darwin-arm64" ]]; then
  rm -rf "\${extract_root}"
  extract_to "\${extract_root}"
fi

export HICT_APP_HOME="\${app_home}"
export HICT_JAR_PATH="\${app_home}/lib/hict.jar"
if [[ -z "\${WEBUI_ROOT:-}" && -d "\${app_home}/webui" ]]; then
  export WEBUI_ROOT="\${app_home}/webui"
fi
if [[ -z "\${HICT_BIND_HOST:-}" ]]; then
  export HICT_BIND_HOST="127.0.0.1"
fi
if [[ "$#" -eq 0 ]]; then
  export HICT_LAUNCHER_MODE="\${HICT_LAUNCHER_MODE:-gui}"
fi

exec "\${app_home}/bin/hict-darwin-arm64" "$@"
EOF
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

{
  echo "$(shasum -a 256 "${TAR_PATH}" | awk '{print $1}')  $(basename "${TAR_PATH}")"
  echo "$(shasum -a 256 "${RUN_PATH}" | awk '{print $1}')  $(basename "${RUN_PATH}")"
} > "${SHA_PATH}"

echo "Built ${TAR_PATH}"
echo "Built ${RUN_PATH}"
echo "Wrote ${SHA_PATH}"
