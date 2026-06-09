#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
OUTPUT_DIR="${OUTPUT_DIR:-${PROJECT_DIR}/toolchains-dist/linux_x86_64}"
WORK_DIR="${WORK_DIR:-/tmp/hictk-build-linux-x86_64}"
CONAN_HOME_DIR="${HICTK_CONAN_HOME:-${CONAN_HOME:-${WORK_DIR}/conan-home}}"
REPO_URL="${HICTK_REPO_URL:-https://github.com/paulsengroup/hictk.git}"
REF="${HICTK_REF:-latest}"
RUN_TESTS="${RUN_TESTS:-0}"
ENABLE_MOSTLY_STATIC_RUNTIME="${ENABLE_MOSTLY_STATIC_RUNTIME:-0}"
ENABLE_STATIC_MUSL="${HICT_STATIC_MUSL:-0}"
ENABLE_MIMALLOC="${HICT_STATIC_MIMALLOC:-0}"
COMPILER="${COMPILER:-gcc}"
BUILD_JOBS="${BUILD_JOBS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)}"

usage() {
  cat <<'EOF'
Build an official hictk release for bundling into HiCT_JVM.

Environment overrides:
  HICTK_REF=v2.2.0                     Build a specific official tag. Default: latest tag.
  OUTPUT_DIR=/path/to/toolchains-dist/linux_x86_64
  WORK_DIR=/tmp/hictk-build-linux-x86_64
  HICTK_CONAN_HOME=/path/to/cacheable/conan-home
  COMPILER=gcc|clang                   Default: gcc
  RUN_TESTS=1                          Run ctest after building.
  ENABLE_MOSTLY_STATIC_RUNTIME=1       Add -static-libstdc++ -static-libgcc on GCC builds.
  HICT_STATIC_MUSL=1                   Prefer musl compiler wrappers and full static linking.
  HICT_STATIC_MIMALLOC=1               Link mimalloc statically into the final executable.
  BUILD_JOBS=8                         Parallelism for Conan/CMake builds.

Example:
  HICTK_REF=v2.2.0 RUN_TESTS=1 ./scripts/toolchains/build_hictk_linux.sh
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

require_cmd git
require_cmd python3
require_cmd g++
require_cmd cmake
require_cmd ninja
require_cmd ldd
require_cmd file
if [[ "${ENABLE_STATIC_MUSL}" == "1" || "${ENABLE_MIMALLOC}" == "1" ]]; then
  require_cmd pkg-config
fi

if [[ "${COMPILER}" == "clang" ]]; then
  require_cmd clang
  require_cmd clang++
fi
if [[ "${ENABLE_STATIC_MUSL}" == "1" ]]; then
  require_cmd clang
  require_cmd clang++
fi

resolve_latest_ref() {
  git ls-remote --refs --tags "${REPO_URL}" 'v*' \
    | awk '{print $2}' \
    | sed 's#refs/tags/##' \
    | sort -V \
    | tail -n 1
}

if [[ "${REF}" == "latest" ]]; then
  REF="$(resolve_latest_ref)"
fi

if [[ -z "${REF}" ]]; then
  echo "Failed to resolve an official hictk tag from ${REPO_URL}" >&2
  exit 1
fi

echo "[hictk/linux] Building ${REF} into ${OUTPUT_DIR}"
echo "[hictk/linux] Using Conan home ${CONAN_HOME_DIR}"

SOURCE_DIR="${WORK_DIR}/src"
VENV_DIR="${WORK_DIR}/venv"
BUILD_DIR="${SOURCE_DIR}/build"
STAGE_DIR="${WORK_DIR}/stage"

rm -rf "${WORK_DIR}"
mkdir -p "${WORK_DIR}" "${OUTPUT_DIR}" "${CONAN_HOME_DIR}"

python3 -m venv "${VENV_DIR}"
"${VENV_DIR}/bin/python" -m pip install --upgrade pip setuptools wheel
"${VENV_DIR}/bin/python" -m pip install 'conan>=2' 'cmake>=3.25' ninja

"${VENV_DIR}/bin/git" --version >/dev/null 2>&1 || true

git clone --depth 1 --branch "${REF}" "${REPO_URL}" "${SOURCE_DIR}"

export PATH="${VENV_DIR}/bin:${PATH}"
export CONAN_HOME="${CONAN_HOME_DIR}"
export CONAN_CPU_COUNT="${BUILD_JOBS}"
export CMAKE_BUILD_PARALLEL_LEVEL="${BUILD_JOBS}"

conan profile detect --force >/dev/null

if [[ "${COMPILER}" == "clang" ]]; then
  export CC=clang
  export CXX=clang++
elif [[ "${ENABLE_STATIC_MUSL}" == "1" ]]; then
  export CC=clang
  export CXX=clang++
  export CFLAGS="${CFLAGS:-} --target=x86_64-linux-musl"
  export CXXFLAGS="${CXXFLAGS:-} --target=x86_64-linux-musl"
else
  export CC=gcc
  export CXX=g++
fi

cd "${SOURCE_DIR}"

conan install --build=missing \
  -pr default \
  -s build_type=Release \
  -s compiler.cppstd=17 \
  --output-folder="${BUILD_DIR}" \
  .

linker_flags=""
if [[ "${ENABLE_MOSTLY_STATIC_RUNTIME}" == "1" && ( "${COMPILER}" == "gcc" || "${ENABLE_STATIC_MUSL}" == "1" ) ]]; then
  linker_flags="-static-libstdc++ -static-libgcc"
fi
if [[ "${ENABLE_STATIC_MUSL}" == "1" ]]; then
  linker_flags="${linker_flags} -static -fuse-ld=lld"
fi
if [[ "${ENABLE_MIMALLOC}" == "1" ]]; then
  linker_flags="${linker_flags} -Wl,--whole-archive -lmimalloc -Wl,--no-whole-archive"
fi

cmake \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_PREFIX_PATH="${BUILD_DIR}" \
  -DHICTK_ENABLE_TESTING=$([[ "${RUN_TESTS}" == "1" ]] && echo ON || echo OFF) \
  -DHICTK_ENABLE_FUZZY_TESTING=OFF \
  -DHICTK_BUILD_BENCHMARKS=OFF \
  -DHICTK_BUILD_EXAMPLES=OFF \
  -DHICTK_BUILD_TOOLS=ON \
  -DHICTK_DOWNLOAD_TEST_DATASET=OFF \
  -DHICTK_WITH_ARROW=OFF \
  -DHICTK_WITH_EIGEN=OFF \
  -DBUILD_SHARED_LIBS=OFF \
  -DCMAKE_C_COMPILER="${CC}" \
  -DCMAKE_CXX_COMPILER="${CXX}" \
  -DCMAKE_EXE_LINKER_FLAGS="${linker_flags}" \
  -G Ninja \
  -S "${SOURCE_DIR}" \
  -B "${BUILD_DIR}"

cmake --build "${BUILD_DIR}"

if [[ "${RUN_TESTS}" == "1" ]]; then
  ctest --test-dir "${BUILD_DIR}" --output-on-failure -j "${BUILD_JOBS}"
fi

rm -rf "${STAGE_DIR}"
cmake --install "${BUILD_DIR}" --prefix "${STAGE_DIR}" --component Runtime
if [[ -x "${STAGE_DIR}/bin/hictk" ]]; then
  "${STAGE_DIR}/bin/hictk" --help >/dev/null
fi

mkdir -p "${STAGE_DIR}/share/doc/hictk"
cp "${SOURCE_DIR}/CITATION.cff" "${STAGE_DIR}/share/doc/hictk/CITATION.cff"

{
  echo "project=hictk"
  echo "repository=${REPO_URL}"
  echo "ref=${REF}"
  echo "platform=linux_x86_64"
  echo "compiler=${CXX}"
  echo "build_shared_libs=OFF"
  echo "cpu_flag_policy=generic official hictk Release build; no AVX-specific hictk executable is produced so the same payload remains portable across x86-64 hosts."
  echo "hictk_dependencies_linking=static_or_embedded_per_upstream"
  echo "static_runtime=$([[ "${ENABLE_STATIC_MUSL}" == "1" ]] && echo musl || echo host_glibc)"
  echo "mimalloc_linking=$([[ "${ENABLE_MIMALLOC}" == "1" ]] && echo static || echo disabled)"
  echo "runtime_linker_flags=${linker_flags:-<default>}"
  echo "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo
  echo "[file]"
  file "${STAGE_DIR}/bin/hictk"
  echo
  echo "[ldd]"
  ldd "${STAGE_DIR}/bin/hictk" || true
} > "${STAGE_DIR}/share/doc/hictk/build-info.txt"

cat > "${STAGE_DIR}/manifest.json" <<EOF
{
  "id": "hictk-${REF}-linux-x86_64",
  "commands": {
    "hictk": "bin/hictk"
  },
  "files": [
    "bin/hictk",
    "share/licenses/hictk/LICENSE",
    "share/doc/hictk/CITATION.cff",
    "share/doc/hictk/build-info.txt"
  ],
  "notices": [
    "This HiCT build bundles an official hictk source build produced from ${REF}.",
    "HiCT performs .hic conversion by invoking the bundled hictk executable; no Python runtime is required for this path.",
    "hictk is redistributed under its MIT license. Keep the bundled license and citation files with released artifacts."
  ],
  "citations": [
    "hictk: Rossini R, Paulsen J. hictk: blazing fast toolkit to work with .hic and .cool files. Bioinformatics. 2024;40(7):btae408. doi:10.1093/bioinformatics/btae408."
  ],
  "limitations": [
    "Upstream hictk builds embed their own third-party dependencies, but Linux libc/libstdc++ ABI compatibility still depends on the target system.",
    "This payload was compiled on Linux and should only be bundled into Linux fat-JAR releases."
  ]
}
EOF

rm -rf "${OUTPUT_DIR}"
mkdir -p "${OUTPUT_DIR}"
cp -a "${STAGE_DIR}/." "${OUTPUT_DIR}/"

echo "[hictk/linux] Bundled payload prepared at ${OUTPUT_DIR}"
echo "[hictk/linux] Run ./gradlew shadowJar from HiCT_JVM to embed it into the fat JAR."
