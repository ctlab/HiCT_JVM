#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
OUTPUT_DIR="${OUTPUT_DIR:-${PROJECT_DIR}/toolchains-dist/darwin_arm64}"
WORK_DIR="${WORK_DIR:-/tmp/hict-toolchains-darwin-arm64}"
CONAN_HOME_DIR="${HICTK_CONAN_HOME:-${CONAN_HOME:-${WORK_DIR}/conan-home}}"
HICTK_REF="${HICTK_REF:-latest}"
MINIMAP2_REF="${MINIMAP2_REF:-v2.31}"
MM2PLUS_REF="${MM2PLUS_REF:-v1.2}"
HICTK_REPO_URL="${HICTK_REPO_URL:-https://github.com/paulsengroup/hictk.git}"
MINIMAP2_REPO_URL="${MINIMAP2_REPO_URL:-https://github.com/lh3/minimap2.git}"
MM2PLUS_REPO_URL="${MM2PLUS_REPO_URL:-https://github.com/at-cg/mm2-plus.git}"
BUILD_JOBS="${BUILD_JOBS:-$(sysctl -n hw.ncpu 2>/dev/null || echo 4)}"
MACOS_DEPLOYMENT_TARGET="${MACOS_DEPLOYMENT_TARGET:-12.0}"

usage() {
  cat <<EOF
Build the Darwin arm64 hictk/minimap2/mm2-plus toolchain payload for HiCT.

Environment overrides:
  HICTK_REF=tag|latest
  MINIMAP2_REF=tag
  MM2PLUS_REF=tag
  OUTPUT_DIR=/path/to/toolchains-dist/darwin_arm64
  WORK_DIR=/tmp/hict-toolchains-darwin-arm64
  HICTK_CONAN_HOME=/path/to/conan-cache
  BUILD_JOBS=8
  MACOS_DEPLOYMENT_TARGET=12.0

The script reuses an existing payload when the refs recorded in the build-info
files match the requested refs.
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
require_cmd cmake
require_cmd ninja
require_cmd make
require_cmd clang
require_cmd clang++
require_cmd file
require_cmd otool

if [[ "${HICTK_REF}" == "latest" ]]; then
  HICTK_REF="$(git ls-remote --refs --tags "${HICTK_REPO_URL}" 'v*' | awk '{print $2}' | sed 's#refs/tags/##' | sort -V | tail -n 1)"
fi

if [[ -z "${HICTK_REF}" ]]; then
  echo "Failed to resolve an official hictk tag from ${HICTK_REPO_URL}" >&2
  exit 1
fi

echo "[darwin/toolchains] Building refs hictk=${HICTK_REF} minimap2=${MINIMAP2_REF} mm2-plus=${MM2PLUS_REF}"
echo "[darwin/toolchains] Using Conan home ${CONAN_HOME_DIR}"

rm -rf "${WORK_DIR}"
mkdir -p "${WORK_DIR}" "${OUTPUT_DIR}" "${CONAN_HOME_DIR}"

export CC=clang
export CXX=clang++
export MACOSX_DEPLOYMENT_TARGET="${MACOS_DEPLOYMENT_TARGET}"
export CMAKE_OSX_ARCHITECTURES=arm64
export CMAKE_OSX_DEPLOYMENT_TARGET="${MACOS_DEPLOYMENT_TARGET}"
export CONAN_HOME="${CONAN_HOME_DIR}"
export CONAN_CPU_COUNT="${BUILD_JOBS}"
export CMAKE_BUILD_PARALLEL_LEVEL="${BUILD_JOBS}"

SOURCE_HICTK="${WORK_DIR}/hictk-src"
SOURCE_MINIMAP2="${WORK_DIR}/minimap2-src"
SOURCE_MM2PLUS="${WORK_DIR}/mm2plus-src"

build_hictk() {
  local stage_dir="${WORK_DIR}/hictk-stage"
  local build_dir="${WORK_DIR}/hictk-build"
  local build_info="${OUTPUT_DIR}/share/doc/hictk/build-info.txt"
  if [[ -x "${OUTPUT_DIR}/bin/hictk" ]] && [[ -f "${build_info}" ]] && grep -qx "ref=${HICTK_REF}" "${build_info}"; then
    echo "[darwin/toolchains] Reusing cached hictk payload"
    return 0
  fi

  if [[ ! -d "${SOURCE_HICTK}/.git" ]]; then
    git clone --depth 1 --branch "${HICTK_REF}" "${HICTK_REPO_URL}" "${SOURCE_HICTK}"
  else
    git -C "${SOURCE_HICTK}" fetch --tags --force origin "${HICTK_REF}" || git -C "${SOURCE_HICTK}" fetch --tags --force origin
    git -C "${SOURCE_HICTK}" checkout --force "${HICTK_REF}"
  fi

  python3 -m venv "${WORK_DIR}/hictk-venv"
  "${WORK_DIR}/hictk-venv/bin/python" -m pip install --upgrade pip setuptools wheel
  "${WORK_DIR}/hictk-venv/bin/python" -m pip install 'conan>=2' 'cmake>=3.25' ninja
  export PATH="${WORK_DIR}/hictk-venv/bin:${PATH}"

  conan profile detect --force >/dev/null
  conan install --build=missing \
    -pr default \
    -s build_type=Release \
    -s compiler.cppstd=17 \
    --output-folder="${build_dir}" \
    "${SOURCE_HICTK}"

  rm -rf "${stage_dir}"
  cmake_args=(
    -DCMAKE_BUILD_TYPE=Release
    -DCMAKE_PREFIX_PATH="${build_dir}"
    -DHICTK_ENABLE_TESTING=OFF
    -DHICTK_ENABLE_FUZZY_TESTING=OFF
    -DHICTK_BUILD_BENCHMARKS=OFF
    -DHICTK_BUILD_EXAMPLES=OFF
    -DHICTK_BUILD_TOOLS=ON
    -DHICTK_DOWNLOAD_TEST_DATASET=OFF
    -DHICTK_WITH_ARROW=OFF
    -DHICTK_WITH_EIGEN=OFF
    -DBUILD_SHARED_LIBS=OFF
    -DCMAKE_C_COMPILER="${CC}"
    -DCMAKE_CXX_COMPILER="${CXX}"
    -DCMAKE_OSX_ARCHITECTURES=arm64
    -DCMAKE_OSX_DEPLOYMENT_TARGET="${MACOS_DEPLOYMENT_TARGET}"
    -G Ninja
    -S "${SOURCE_HICTK}"
    -B "${build_dir}"
  )
  if [[ -f "${build_dir}/conan_toolchain.cmake" ]]; then
    cmake_args+=(-DCMAKE_TOOLCHAIN_FILE="${build_dir}/conan_toolchain.cmake")
  fi
  cmake "${cmake_args[@]}"

  cmake --build "${build_dir}"
  cmake --install "${build_dir}" --prefix "${stage_dir}" --component Runtime
  mkdir -p "${stage_dir}/share/doc/hictk"
  cp "${SOURCE_HICTK}/CITATION.cff" "${stage_dir}/share/doc/hictk/CITATION.cff"

  {
    echo "project=hictk"
    echo "repository=${HICTK_REPO_URL}"
    echo "ref=${HICTK_REF}"
    echo "platform=darwin_arm64"
    echo "compiler=$(clang++ --version | head -n 1)"
    echo "build_shared_libs=OFF"
    echo "runtime_linking=system_dylib"
    echo "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo
    echo "[file]"
    file "${stage_dir}/bin/hictk"
    echo
    echo "[otool]"
    otool -L "${stage_dir}/bin/hictk" || true
  } > "${stage_dir}/share/doc/hictk/build-info.txt"

  mkdir -p "${OUTPUT_DIR}/bin" "${OUTPUT_DIR}/share/licenses/hictk" "${OUTPUT_DIR}/share/doc/hictk"
  rm -f "${OUTPUT_DIR}/bin/hictk"
  rm -rf "${OUTPUT_DIR}/share/licenses/hictk" "${OUTPUT_DIR}/share/doc/hictk"
  cp -a "${stage_dir}/." "${OUTPUT_DIR}/"
  "${OUTPUT_DIR}/bin/hictk" --help >/dev/null
}

build_minimap2() {
  local stage_dir="${WORK_DIR}/minimap2-stage"
  local build_info="${OUTPUT_DIR}/share/doc/minimap2/build-info.txt"
  if [[ -x "${OUTPUT_DIR}/bin/minimap2" ]] && [[ -f "${build_info}" ]] && grep -qx "ref=${MINIMAP2_REF}" "${build_info}"; then
    echo "[darwin/toolchains] Reusing cached minimap2 payload"
    return 0
  fi

  if [[ ! -d "${SOURCE_MINIMAP2}/.git" ]]; then
    git clone --filter=blob:none "${MINIMAP2_REPO_URL}" "${SOURCE_MINIMAP2}"
  fi
  git -C "${SOURCE_MINIMAP2}" fetch --tags --force origin "${MINIMAP2_REF}" || git -C "${SOURCE_MINIMAP2}" fetch --tags --force origin
  git -C "${SOURCE_MINIMAP2}" checkout --force "${MINIMAP2_REF}"

  make -C "${SOURCE_MINIMAP2}" clean >/dev/null 2>&1 || true
  make -C "${SOURCE_MINIMAP2}" -j"${BUILD_JOBS}" \
    CC="${CC}" \
    aarch64=1 \
    arm_neon=1 \
    CFLAGS="-O3 -DNDEBUG -arch arm64 -mmacosx-version-min=${MACOS_DEPLOYMENT_TARGET}"

  rm -rf "${stage_dir}"
  mkdir -p "${stage_dir}/bin" "${stage_dir}/share/licenses/minimap2" "${stage_dir}/share/doc/minimap2"
  install -m 0755 "${SOURCE_MINIMAP2}/minimap2" "${stage_dir}/bin/minimap2"
  if [[ -f "${SOURCE_MINIMAP2}/LICENSE.txt" ]]; then
    install -m 0644 "${SOURCE_MINIMAP2}/LICENSE.txt" "${stage_dir}/share/licenses/minimap2/LICENSE.txt"
  elif [[ -f "${SOURCE_MINIMAP2}/LICENSE" ]]; then
    install -m 0644 "${SOURCE_MINIMAP2}/LICENSE" "${stage_dir}/share/licenses/minimap2/LICENSE"
  fi

  {
    echo "project=minimap2"
    echo "repository=${MINIMAP2_REPO_URL}"
    echo "ref=${MINIMAP2_REF}"
    echo "platform=darwin_arm64"
    echo "compiler=$(clang --version | head -n 1)"
    echo "cpu_flag_policy=generic official minimap2 build; upstream SSE2/SSE4.1 dispatch objects retain their fixed target flags."
    echo "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo
    echo "[file]"
    file "${stage_dir}/bin/minimap2"
    echo
    echo "[otool]"
    otool -L "${stage_dir}/bin/minimap2" || true
  } > "${stage_dir}/share/doc/minimap2/build-info.txt"

  rm -rf "${OUTPUT_DIR}/bin/minimap2" "${OUTPUT_DIR}/share/licenses/minimap2" "${OUTPUT_DIR}/share/doc/minimap2"
  mkdir -p "${OUTPUT_DIR}"
  cp -a "${stage_dir}/bin" "${OUTPUT_DIR}/"
  cp -a "${stage_dir}/share" "${OUTPUT_DIR}/"
  "${OUTPUT_DIR}/bin/minimap2" --help >/dev/null || true
}

build_mm2plus() {
  local stage_dir="${WORK_DIR}/mm2plus-stage"
  local build_info="${OUTPUT_DIR}/share/doc/mm2plus/build-info.txt"
  if [[ -f "${build_info}" ]] && grep -qx "ref=${MM2PLUS_REF}" "${build_info}" && [[ -e "${OUTPUT_DIR}/bin/mm2plus" ]]; then
    echo "[darwin/toolchains] Reusing cached mm2-plus payload"
    return 0
  fi

  if [[ ! -d "${SOURCE_MM2PLUS}/.git" ]]; then
    git clone --filter=blob:none "${MM2PLUS_REPO_URL}" "${SOURCE_MM2PLUS}"
  fi
  git -C "${SOURCE_MM2PLUS}" fetch --tags --force origin "${MM2PLUS_REF}" || git -C "${SOURCE_MM2PLUS}" fetch --tags --force origin
  git -C "${SOURCE_MM2PLUS}" checkout --force "${MM2PLUS_REF}"

python3 - "${SOURCE_MM2PLUS}/Makefile" <<'PY'
import pathlib
import sys
path = pathlib.Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
old = "$(CXX) -c $(CPPFLAGS) $(INCLUDES) $< -o $@"
new = "$(CXX) -c $(CPPFLAGS) $(EXTRAFLAGS) $(INCLUDES) $< -o $@"
if old in text and new not in text:
    path.write_text(text.replace(old, new), encoding="utf-8")
PY
python3 - "${SOURCE_MM2PLUS}/Makefile" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
updated = text.replace("-fopenmp", "")
updated = updated.replace("-lgomp", "")
updated = updated.replace("-D_GLIBCXX_PARALLEL", "")
if updated != text:
    path.write_text(updated, encoding="utf-8")
PY
mkdir -p "${WORK_DIR}/compat"
cat > "${WORK_DIR}/compat/omp.h" <<'EOF'
#ifndef HICT_DARWIN_MM2PLUS_OMP_H
#define HICT_DARWIN_MM2PLUS_OMP_H

typedef int omp_lock_t;
typedef int omp_nest_lock_t;

static inline int omp_get_thread_num(void) { return 0; }
static inline int omp_get_num_threads(void) { return 1; }
static inline int omp_get_max_threads(void) { return 1; }
static inline int omp_get_num_procs(void) { return 1; }
static inline int omp_in_parallel(void) { return 0; }
static inline int omp_get_dynamic(void) { return 0; }
static inline int omp_get_nested(void) { return 0; }
static inline int omp_get_thread_limit(void) { return 1; }
static inline int omp_get_level(void) { return 0; }
static inline int omp_get_ancestor_thread_num(int) { return 0; }
static inline int omp_get_team_size(int) { return 1; }
static inline int omp_get_active_level(void) { return 0; }
static inline int omp_in_final(void) { return 1; }
static inline void omp_set_num_threads(int) {}
static inline void omp_set_dynamic(int) {}
static inline void omp_set_nested(int) {}
static inline void omp_set_schedule(int, int) {}
static inline void omp_get_schedule(int *, int *) {}
static inline void omp_set_max_active_levels(int) {}
static inline int omp_get_max_active_levels(void) { return 1; }
static inline void omp_init_lock(omp_lock_t *) {}
static inline void omp_destroy_lock(omp_lock_t *) {}
static inline void omp_set_lock(omp_lock_t *) {}
static inline void omp_unset_lock(omp_lock_t *) {}
static inline int omp_test_lock(omp_lock_t *) { return 1; }
static inline void omp_init_nest_lock(omp_nest_lock_t *) {}
static inline void omp_destroy_nest_lock(omp_nest_lock_t *) {}
static inline void omp_set_nest_lock(omp_nest_lock_t *) {}
static inline void omp_unset_nest_lock(omp_nest_lock_t *) {}
static inline int omp_test_nest_lock(omp_nest_lock_t *) { return 1; }
static inline double omp_get_wtime(void) { return 0.0; }
static inline double omp_get_wtick(void) { return 1.0; }

#endif
EOF
cat > "${SOURCE_MM2PLUS}/src/parallel_sort.cpp" <<'EOF'
#include <algorithm>
#include "mmpriv.h"

void parallel_sort(mm128_t* z, size_t n_u, int32_t) {
    std::stable_sort(z, z + n_u, [](const mm128_t &a, const mm128_t &b) { return a.x < b.x; });
}
EOF

  local built=0
  make -C "${SOURCE_MM2PLUS}" clean >/dev/null 2>&1 || true
  if make -C "${SOURCE_MM2PLUS}" -j"${BUILD_JOBS}" \
      base=1 avx=0 \
      aarch64=1 \
      arm_neon=1 \
      CXX="${CXX}" \
      CC="${CC}" \
      EXTRAFLAGS="-I${WORK_DIR}/compat -arch arm64 -mmacosx-version-min=${MACOS_DEPLOYMENT_TARGET}"; then
    if [[ -x "${SOURCE_MM2PLUS}/mm2plus" ]]; then
      built=1
    fi
  fi

  rm -rf "${stage_dir}"
  mkdir -p "${stage_dir}/bin" "${stage_dir}/share/licenses/mm2plus" "${stage_dir}/share/doc/mm2plus"

  if [[ "${built}" == "1" ]]; then
    install -m 0755 "${SOURCE_MM2PLUS}/mm2plus" "${stage_dir}/bin/mm2plus"
  else
    cat > "${stage_dir}/bin/mm2plus" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${SCRIPT_DIR}/minimap2" "$@"
EOF
    chmod +x "${stage_dir}/bin/mm2plus"
  fi
  cat > "${stage_dir}/bin/mm2plus-avx2" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${SCRIPT_DIR}/mm2plus" "$@"
EOF
  cat > "${stage_dir}/bin/mm2plus-avx512" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${SCRIPT_DIR}/mm2plus" "$@"
EOF
  chmod +x "${stage_dir}/bin/mm2plus" "${stage_dir}/bin/mm2plus-avx2" "${stage_dir}/bin/mm2plus-avx512"

  if [[ -f "${SOURCE_MM2PLUS}/LICENSE.txt" ]]; then
    install -m 0644 "${SOURCE_MM2PLUS}/LICENSE.txt" "${stage_dir}/share/licenses/mm2plus/LICENSE.txt"
  elif [[ -f "${SOURCE_MM2PLUS}/LICENSE" ]]; then
    install -m 0644 "${SOURCE_MM2PLUS}/LICENSE" "${stage_dir}/share/licenses/mm2plus/LICENSE"
  fi
  if [[ -f "${SOURCE_MM2PLUS}/README.md" ]]; then
    install -m 0644 "${SOURCE_MM2PLUS}/README.md" "${stage_dir}/share/doc/mm2plus/README.md"
  fi

  {
    echo "project=mm2-plus"
    echo "repository=${MM2PLUS_REPO_URL}"
    echo "ref=${MM2PLUS_REF}"
    echo "platform=darwin_arm64"
    echo "compiler=$(clang++ --version | head -n 1)"
    echo "build_mode=$([[ "${built}" == "1" ]] && echo native || echo fallback_wrapper)"
    echo "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo
    echo "[file]"
    file "${stage_dir}/bin/mm2plus"
    echo
    echo "[otool]"
    if [[ "${built}" == "1" ]]; then
      otool -L "${stage_dir}/bin/mm2plus" || true
    fi
  } > "${stage_dir}/share/doc/mm2plus/build-info.txt"

  mkdir -p "${OUTPUT_DIR}/bin" "${OUTPUT_DIR}/share/licenses/mm2plus" "${OUTPUT_DIR}/share/doc/mm2plus"
  cp -a "${stage_dir}/bin/mm2plus" "${OUTPUT_DIR}/bin/mm2plus"
  cp -a "${stage_dir}/bin/mm2plus-avx2" "${OUTPUT_DIR}/bin/mm2plus-avx2"
  cp -a "${stage_dir}/bin/mm2plus-avx512" "${OUTPUT_DIR}/bin/mm2plus-avx512"
  cp -a "${stage_dir}/share/licenses/mm2plus/." "${OUTPUT_DIR}/share/licenses/mm2plus/" 2>/dev/null || true
  cp -a "${stage_dir}/share/doc/mm2plus/." "${OUTPUT_DIR}/share/doc/mm2plus/"
  "${OUTPUT_DIR}/bin/mm2plus-avx2" --help >/dev/null || true
}

build_hictk
build_minimap2
build_mm2plus

cat > "${OUTPUT_DIR}/manifest.json" <<EOF
{
  "id": "hict-toolchain-darwin-arm64-hictk-minimap2-mm2plus-${HICTK_REF}-${MINIMAP2_REF}-${MM2PLUS_REF}",
  "commands": {
    "hictk": "bin/hictk",
    "minimap2": "bin/minimap2",
    "mm2plus_avx2": "bin/mm2plus-avx2",
    "mm2plus_avx512": "bin/mm2plus-avx512"
  },
  "files": [
    "bin/hictk",
    "bin/minimap2",
    "bin/mm2plus",
    "bin/mm2plus-avx2",
    "bin/mm2plus-avx512",
    "share/licenses/hictk/LICENSE",
    "share/doc/hictk/CITATION.cff",
    "share/doc/hictk/build-info.txt",
    "share/licenses/minimap2/LICENSE.txt",
    "share/licenses/minimap2/LICENSE",
    "share/doc/minimap2/build-info.txt",
    "share/licenses/mm2plus/LICENSE.txt",
    "share/licenses/mm2plus/LICENSE",
    "share/doc/mm2plus/README.md",
    "share/doc/mm2plus/build-info.txt"
  ],
  "notices": [
    "This HiCT build bundles an official hictk source build produced from ${HICTK_REF}.",
    "This HiCT build bundles an official minimap2 source build produced from ${MINIMAP2_REF}.",
    "This HiCT build bundles mm2-plus for macOS arm64; when a native build is unavailable, the bundled mm2-plus entrypoints fall back to minimap2-compatible behavior to keep the package runnable.",
    "HiCT performs .hic conversion by invoking the bundled hictk executable; no Python runtime is required for this path."
  ],
  "citations": [
    "hictk: Rossini R, Paulsen J. hictk: blazing fast toolkit to work with .hic and .cool files. Bioinformatics. 2024;40(7):btae408. doi:10.1093/bioinformatics/btae408.",
    "minimap2: Li H. Minimap2: pairwise alignment for nucleotide sequences. Bioinformatics. 2018;34(18):3094-3100."
  ],
  "limitations": [
    "This payload was compiled on macOS arm64 and should only be bundled into macOS arm64 portable releases.",
    "The mm2-plus macOS payload uses a generic arm64 build when upstream source support allows it, otherwise it falls back to compatibility wrappers over minimap2."
  ]
}
EOF

mkdir -p "${OUTPUT_DIR}/share/doc/darwin_toolchains"
{
  echo "hictk_ref=${HICTK_REF}"
  echo "minimap2_ref=${MINIMAP2_REF}"
  echo "mm2plus_ref=${MM2PLUS_REF}"
  echo "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} > "${OUTPUT_DIR}/share/doc/darwin_toolchains/build-info.txt"

echo "[darwin/toolchains] Bundled payload prepared at ${OUTPUT_DIR}"
