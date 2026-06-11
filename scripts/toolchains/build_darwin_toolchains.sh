#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DARWIN_ARCH="${HICT_DARWIN_ARCH:-arm64}"
case "${DARWIN_ARCH}" in
  arm64) CONAN_DARWIN_ARCH="armv8" ;;
  x86_64|amd64) DARWIN_ARCH="x86_64"; CONAN_DARWIN_ARCH="x86_64" ;;
  *) echo "Unsupported HICT_DARWIN_ARCH=${DARWIN_ARCH}; expected arm64 or x86_64." >&2; exit 1 ;;
esac
PLATFORM_DIR="${HICT_DARWIN_PLATFORM_DIR:-darwin_${DARWIN_ARCH}}"
OUTPUT_DIR="${OUTPUT_DIR:-${PROJECT_DIR}/toolchains-dist/${PLATFORM_DIR}}"
WORK_DIR="${WORK_DIR:-/tmp/hict-toolchains-${PLATFORM_DIR}}"
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
Build the Darwin hictk/minimap2/mm2-plus toolchain payload for HiCT.

Environment overrides:
  HICT_DARWIN_ARCH=arm64|x86_64
  HICTK_REF=tag|latest
  MINIMAP2_REF=tag
  MM2PLUS_REF=tag
  OUTPUT_DIR=/path/to/toolchains-dist/${PLATFORM_DIR}
  WORK_DIR=/tmp/hict-toolchains-${PLATFORM_DIR}
  HICTK_CONAN_HOME=/path/to/conan-cache
  BUILD_JOBS=8
  MACOS_DEPLOYMENT_TARGET=12.0
EOF
}
if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then usage; exit 0; fi
require_cmd() { command -v "$1" >/dev/null 2>&1 || { echo "Missing required command: $1" >&2; exit 1; }; }
for cmd in git python3 cmake ninja make clang clang++ file otool codesign; do require_cmd "${cmd}"; done
adhoc_sign_if_macho() { local path="$1"; [[ -f "${path}" ]] || return 0; if file "${path}" | grep -q 'Mach-O'; then codesign -s - --force --timestamp=none "${path}" >/dev/null 2>&1; fi; }

conan_apple_clang_version() {
  local version
  version="$(${CXX:-clang++} --version 2>/dev/null | awk '/Apple clang version/ {print $4; exit}')"
  version="${version%%.*}"
  [[ -n "${version}" ]] || version="17"
  printf '%s\n' "${version}"
}

sanitize_conan_home() {
  mkdir -p "${CONAN_HOME_DIR}" "${CONAN_HOME_DIR}/profiles"
  # Older iterations of this workflow cached Conan files containing the removed
  # Conan 2.29+ key tools.apple:deployment_target. Conan validates cached
  # global.conf/profile confs before resolving packages, so merely removing the
  # command-line -c option is not enough when a restored cache contains it.
  while IFS= read -r -d '' file; do
    if grep -q 'tools\.apple:deployment_target' "${file}" 2>/dev/null; then
      echo "[darwin/toolchains] Removing obsolete tools.apple:deployment_target from ${file}"
      python3 - "${file}" <<'PY_CLEAN_CONAN'
import pathlib, sys
path = pathlib.Path(sys.argv[1])
lines = path.read_text(encoding='utf-8', errors='ignore').splitlines()
lines = [line for line in lines if 'tools.apple:deployment_target' not in line]
path.write_text('\n'.join(lines) + ('\n' if lines else ''), encoding='utf-8')
PY_CLEAN_CONAN
    fi
  done < <(find "${CONAN_HOME_DIR}" -type f \( -name 'global.conf' -o -path '*/profiles/*' \) -print0 2>/dev/null || true)
}

write_conan_profiles() {
  local compiler_version host_profile build_profile
  compiler_version="$(conan_apple_clang_version)"
  host_profile="${WORK_DIR}/conan-profile-host"
  build_profile="${WORK_DIR}/conan-profile-build"
  cat > "${host_profile}" <<EOF_PROFILE
[settings]
os=Macos
os.version=${MACOS_DEPLOYMENT_TARGET}
arch=${CONAN_DARWIN_ARCH}
compiler=apple-clang
compiler.version=${compiler_version}
compiler.libcxx=libc++
compiler.cppstd=17
build_type=Release

[conf]
tools.cmake.cmaketoolchain:generator=Ninja
EOF_PROFILE
  cp "${host_profile}" "${build_profile}"
  echo "${host_profile};${build_profile}"
}
if [[ "${HICTK_REF}" == "latest" ]]; then HICTK_REF="$(git ls-remote --refs --tags "${HICTK_REPO_URL}" 'v*' | awk '{print $2}' | sed 's#refs/tags/##' | sort -V | tail -n 1)"; fi
[[ -n "${HICTK_REF}" ]] || { echo "Failed to resolve an official hictk tag from ${HICTK_REPO_URL}" >&2; exit 1; }
echo "[darwin/toolchains] Building ${PLATFORM_DIR}: hictk=${HICTK_REF} minimap2=${MINIMAP2_REF} mm2-plus=${MM2PLUS_REF}"
echo "[darwin/toolchains] Using Conan home ${CONAN_HOME_DIR}"
rm -rf "${WORK_DIR}"; mkdir -p "${WORK_DIR}" "${OUTPUT_DIR}" "${CONAN_HOME_DIR}"
export CC=clang CXX=clang++ MACOSX_DEPLOYMENT_TARGET="${MACOS_DEPLOYMENT_TARGET}" CMAKE_OSX_ARCHITECTURES="${DARWIN_ARCH}" CMAKE_OSX_DEPLOYMENT_TARGET="${MACOS_DEPLOYMENT_TARGET}" CONAN_HOME="${CONAN_HOME_DIR}" CONAN_CPU_COUNT="${BUILD_JOBS}" CMAKE_BUILD_PARALLEL_LEVEL="${BUILD_JOBS}"
SOURCE_HICTK="${WORK_DIR}/hictk-src"; SOURCE_MINIMAP2="${WORK_DIR}/minimap2-src"; SOURCE_MM2PLUS="${WORK_DIR}/mm2plus-src"

build_hictk() {
  local stage_dir="${WORK_DIR}/hictk-stage" build_dir="${WORK_DIR}/hictk-build" build_info="${OUTPUT_DIR}/share/doc/hictk/build-info.txt"
  if [[ -x "${OUTPUT_DIR}/bin/hictk" && -f "${build_info}" ]] && grep -qx "ref=${HICTK_REF}" "${build_info}" && grep -qx "platform=${PLATFORM_DIR}" "${build_info}" && grep -qx "macos_deployment_target=${MACOS_DEPLOYMENT_TARGET}" "${build_info}"; then echo "[darwin/toolchains] Reusing cached hictk payload"; return 0; fi
  if [[ ! -d "${SOURCE_HICTK}/.git" ]]; then git clone --depth 1 --branch "${HICTK_REF}" "${HICTK_REPO_URL}" "${SOURCE_HICTK}"; else git -C "${SOURCE_HICTK}" fetch --tags --force origin "${HICTK_REF}" || git -C "${SOURCE_HICTK}" fetch --tags --force origin; git -C "${SOURCE_HICTK}" checkout --force "${HICTK_REF}"; fi
  python3 -m venv "${WORK_DIR}/hictk-venv"; "${WORK_DIR}/hictk-venv/bin/python" -m pip install --upgrade pip setuptools wheel; "${WORK_DIR}/hictk-venv/bin/python" -m pip install 'conan>=2,<2.30' 'cmake>=3.25,<4.0' ninja; export PATH="${WORK_DIR}/hictk-venv/bin:${PATH}"
  sanitize_conan_home
  IFS=';' read -r conan_host_profile conan_build_profile <<< "$(write_conan_profiles)"
  echo "[darwin/toolchains] Conan version: $(conan --version)"
  echo "[darwin/toolchains] Conan host profile:"; sed 's/^/[darwin\/toolchains]   /' "${conan_host_profile}"
  # Do not use Conan's detected default profile here. It can be restored from
  # cache and may still contain removed conf keys such as tools.apple:deployment_target.
  # The deployment target is represented by the supported Macos os.version
  # setting and by MACOSX_DEPLOYMENT_TARGET/CMAKE_OSX_DEPLOYMENT_TARGET.
  conan install --build=missing -pr:h "${conan_host_profile}" -pr:b "${conan_build_profile}" --output-folder="${build_dir}" "${SOURCE_HICTK}"
  rm -rf "${stage_dir}"
  cmake_args=(-DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH="${build_dir}" -DHICTK_ENABLE_TESTING=OFF -DHICTK_ENABLE_FUZZY_TESTING=OFF -DHICTK_BUILD_BENCHMARKS=OFF -DHICTK_BUILD_EXAMPLES=OFF -DHICTK_BUILD_TOOLS=ON -DHICTK_DOWNLOAD_TEST_DATASET=OFF -DHICTK_WITH_ARROW=OFF -DHICTK_WITH_EIGEN=OFF -DBUILD_SHARED_LIBS=OFF -DCMAKE_C_COMPILER="${CC}" -DCMAKE_CXX_COMPILER="${CXX}" -DCMAKE_OSX_ARCHITECTURES="${DARWIN_ARCH}" -DCMAKE_OSX_DEPLOYMENT_TARGET="${MACOS_DEPLOYMENT_TARGET}" -G Ninja -S "${SOURCE_HICTK}" -B "${build_dir}")
  [[ -f "${build_dir}/conan_toolchain.cmake" ]] && cmake_args+=(-DCMAKE_TOOLCHAIN_FILE="${build_dir}/conan_toolchain.cmake")
  cmake "${cmake_args[@]}"; cmake --build "${build_dir}"; cmake --install "${build_dir}" --prefix "${stage_dir}" --component Runtime
  mkdir -p "${stage_dir}/share/doc/hictk"; cp "${SOURCE_HICTK}/CITATION.cff" "${stage_dir}/share/doc/hictk/CITATION.cff"; adhoc_sign_if_macho "${stage_dir}/bin/hictk"
  { echo "project=hictk"; echo "repository=${HICTK_REPO_URL}"; echo "ref=${HICTK_REF}"; echo "platform=${PLATFORM_DIR}"; echo "arch=${DARWIN_ARCH}"; echo "macos_deployment_target=${MACOS_DEPLOYMENT_TARGET}"; echo "compiler=$(clang++ --version | head -n 1)"; echo "build_shared_libs=OFF"; echo "runtime_linking=system_dylib"; echo "codesign=adhoc"; echo "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"; echo; echo "[file]"; file "${stage_dir}/bin/hictk"; echo; echo "[otool]"; otool -L "${stage_dir}/bin/hictk" || true; echo; echo "[codesign]"; codesign -dv --verbose=2 "${stage_dir}/bin/hictk" 2>&1 || true; } > "${stage_dir}/share/doc/hictk/build-info.txt"
  mkdir -p "${OUTPUT_DIR}"; cp -a "${stage_dir}/." "${OUTPUT_DIR}/"; "${OUTPUT_DIR}/bin/hictk" --help >/dev/null
}

build_minimap2() {
  local stage_dir="${WORK_DIR}/minimap2-stage" build_info="${OUTPUT_DIR}/share/doc/minimap2/build-info.txt"
  if [[ -x "${OUTPUT_DIR}/bin/minimap2" && -f "${build_info}" ]] && grep -qx "ref=${MINIMAP2_REF}" "${build_info}" && grep -qx "platform=${PLATFORM_DIR}" "${build_info}"; then echo "[darwin/toolchains] Reusing cached minimap2 payload"; return 0; fi
  [[ -d "${SOURCE_MINIMAP2}/.git" ]] || git clone --filter=blob:none "${MINIMAP2_REPO_URL}" "${SOURCE_MINIMAP2}" || git clone "${MINIMAP2_REPO_URL}" "${SOURCE_MINIMAP2}"
  git -C "${SOURCE_MINIMAP2}" fetch --tags --force origin "${MINIMAP2_REF}" || git -C "${SOURCE_MINIMAP2}" fetch --tags --force origin; git -C "${SOURCE_MINIMAP2}" checkout --force "${MINIMAP2_REF}"; make -C "${SOURCE_MINIMAP2}" clean >/dev/null 2>&1 || true
  if [[ "${DARWIN_ARCH}" == "arm64" ]]; then make -C "${SOURCE_MINIMAP2}" -j"${BUILD_JOBS}" CC="${CC}" aarch64=1 arm_neon=1 CFLAGS="-O3 -DNDEBUG -arch arm64 -mmacosx-version-min=${MACOS_DEPLOYMENT_TARGET}"; else make -C "${SOURCE_MINIMAP2}" -j"${BUILD_JOBS}" CC="${CC}" CFLAGS="-O3 -DNDEBUG -arch x86_64 -mmacosx-version-min=${MACOS_DEPLOYMENT_TARGET}"; fi
  rm -rf "${stage_dir}"; mkdir -p "${stage_dir}/bin" "${stage_dir}/share/licenses/minimap2" "${stage_dir}/share/doc/minimap2"; install -m 0755 "${SOURCE_MINIMAP2}/minimap2" "${stage_dir}/bin/minimap2"; adhoc_sign_if_macho "${stage_dir}/bin/minimap2"
  [[ -f "${SOURCE_MINIMAP2}/LICENSE.txt" ]] && install -m 0644 "${SOURCE_MINIMAP2}/LICENSE.txt" "${stage_dir}/share/licenses/minimap2/LICENSE.txt" || { [[ -f "${SOURCE_MINIMAP2}/LICENSE" ]] && install -m 0644 "${SOURCE_MINIMAP2}/LICENSE" "${stage_dir}/share/licenses/minimap2/LICENSE" || true; }
  { echo "project=minimap2"; echo "repository=${MINIMAP2_REPO_URL}"; echo "ref=${MINIMAP2_REF}"; echo "platform=${PLATFORM_DIR}"; echo "arch=${DARWIN_ARCH}"; echo "compiler=$(clang --version | head -n 1)"; echo "codesign=adhoc"; echo "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"; echo; echo "[file]"; file "${stage_dir}/bin/minimap2"; echo; echo "[otool]"; otool -L "${stage_dir}/bin/minimap2" || true; echo; echo "[codesign]"; codesign -dv --verbose=2 "${stage_dir}/bin/minimap2" 2>&1 || true; } > "${stage_dir}/share/doc/minimap2/build-info.txt"
  cp -a "${stage_dir}/bin" "${OUTPUT_DIR}/"; cp -a "${stage_dir}/share" "${OUTPUT_DIR}/"; "${OUTPUT_DIR}/bin/minimap2" --help >/dev/null || true
}

patch_mm2plus_for_no_openmp() {
  python3 - "${SOURCE_MM2PLUS}/Makefile" <<'PY'
import pathlib, sys
path = pathlib.Path(sys.argv[1]); text = path.read_text(encoding="utf-8")
old = "$(CXX) -c $(CPPFLAGS) $(INCLUDES) $< -o $@"; new = "$(CXX) -c $(CPPFLAGS) $(EXTRAFLAGS) $(INCLUDES) $< -o $@"
if old in text and new not in text: text = text.replace(old, new)
text = text.replace("-fopenmp", "").replace("-lgomp", "").replace("-D_GLIBCXX_PARALLEL", "")
text = text.replace(" -Wl,-Bstatic -lz -Wl,-Bdynamic ", " -lz ")
text = text.replace("-Wl,-Bstatic", "").replace("-Wl,-Bdynamic", "")
text = text.replace(" -Wl,-Bstatic  -lz -Wl,-Bdynamic ", " -lz ")
path.write_text(text, encoding="utf-8")
PY
  mkdir -p "${WORK_DIR}/compat"
  cat > "${WORK_DIR}/compat/omp.h" <<'EOF'
#ifndef HICT_DARWIN_MM2PLUS_OMP_H
#define HICT_DARWIN_MM2PLUS_OMP_H
typedef int omp_lock_t; typedef int omp_nest_lock_t;
static inline int omp_get_thread_num(void) { return 0; }
static inline int omp_get_num_threads(void) { return 1; }
static inline int omp_get_max_threads(void) { return 1; }
static inline int omp_get_num_procs(void) { return 1; }
static inline int omp_in_parallel(void) { return 0; }
static inline double omp_get_wtime(void) { return 0.0; }
static inline double omp_get_wtick(void) { return 1.0; }
#endif
EOF
  cat > "${SOURCE_MM2PLUS}/src/parallel_sort.cpp" <<'EOF'
#include <algorithm>
#include "mmpriv.h"
void parallel_sort(mm128_t* z, size_t n_u, int32_t) { std::stable_sort(z, z + n_u, [](const mm128_t &a, const mm128_t &b) { return a.x < b.x; }); }
EOF
}

build_mm2plus() {
  local stage_dir="${WORK_DIR}/mm2plus-stage" build_info="${OUTPUT_DIR}/share/doc/mm2plus/build-info.txt"
  if [[ -f "${build_info}" ]] && grep -qx "ref=${MM2PLUS_REF}" "${build_info}" && grep -qx "platform=${PLATFORM_DIR}" "${build_info}" && [[ -e "${OUTPUT_DIR}/bin/mm2plus-avx2" ]]; then echo "[darwin/toolchains] Reusing cached mm2-plus payload"; return 0; fi
  [[ -d "${SOURCE_MM2PLUS}/.git" ]] || git clone --filter=blob:none "${MM2PLUS_REPO_URL}" "${SOURCE_MM2PLUS}" || git clone "${MM2PLUS_REPO_URL}" "${SOURCE_MM2PLUS}"
  git -C "${SOURCE_MM2PLUS}" fetch --tags --force origin "${MM2PLUS_REF}" || git -C "${SOURCE_MM2PLUS}" fetch --tags --force origin; git -C "${SOURCE_MM2PLUS}" checkout --force "${MM2PLUS_REF}"; patch_mm2plus_for_no_openmp
  rm -rf "${stage_dir}"; mkdir -p "${stage_dir}/bin" "${stage_dir}/share/licenses/mm2plus" "${stage_dir}/share/doc/mm2plus"
  if [[ "${DARWIN_ARCH}" == "arm64" ]]; then
    make -C "${SOURCE_MM2PLUS}" clean >/dev/null 2>&1 || true
    if make -C "${SOURCE_MM2PLUS}" -j"${BUILD_JOBS}" base=1 avx=0 aarch64=1 arm_neon=1 CXX="${CXX}" CC="${CC}" EXTRAFLAGS="-I${WORK_DIR}/compat -arch arm64 -mmacosx-version-min=${MACOS_DEPLOYMENT_TARGET}"; then install -m 0755 "${SOURCE_MM2PLUS}/mm2plus" "${stage_dir}/bin/mm2plus"; adhoc_sign_if_macho "${stage_dir}/bin/mm2plus"; fi
    if [[ ! -x "${stage_dir}/bin/mm2plus" ]]; then printf '%s\n' '#!/usr/bin/env bash' 'set -euo pipefail' 'SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"' 'exec "${SCRIPT_DIR}/minimap2" "$@"' > "${stage_dir}/bin/mm2plus"; chmod +x "${stage_dir}/bin/mm2plus"; fi
    for variant in avx2; do printf '%s\n' '#!/usr/bin/env bash' 'set -euo pipefail' 'SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"' 'exec "${SCRIPT_DIR}/mm2plus" "$@"' > "${stage_dir}/bin/mm2plus-${variant}"; chmod +x "${stage_dir}/bin/mm2plus-${variant}"; done
  else
    for variant in avx2; do
      local flags="-I${WORK_DIR}/compat -arch x86_64 -mmacosx-version-min=${MACOS_DEPLOYMENT_TARGET} -mavx2"
      make -C "${SOURCE_MM2PLUS}" clean >/dev/null 2>&1 || true
      if make -C "${SOURCE_MM2PLUS}" -j"${BUILD_JOBS}" base=1 avx=1 CXX="${CXX}" CC="${CC}" EXTRAFLAGS="${flags}"; then install -m 0755 "${SOURCE_MM2PLUS}/mm2plus" "${stage_dir}/bin/mm2plus-${variant}"; adhoc_sign_if_macho "${stage_dir}/bin/mm2plus-${variant}"; else echo "::warning::mm2-plus ${variant} build failed on ${PLATFORM_DIR}; creating minimap2 fallback wrapper."; printf '%s\n' '#!/usr/bin/env bash' 'set -euo pipefail' 'SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"' 'exec "${SCRIPT_DIR}/minimap2" "$@"' > "${stage_dir}/bin/mm2plus-${variant}"; chmod +x "${stage_dir}/bin/mm2plus-${variant}"; fi
    done
    cp "${stage_dir}/bin/mm2plus-avx2" "${stage_dir}/bin/mm2plus" 2>/dev/null || true
  fi
  [[ -f "${SOURCE_MM2PLUS}/LICENSE.txt" ]] && install -m 0644 "${SOURCE_MM2PLUS}/LICENSE.txt" "${stage_dir}/share/licenses/mm2plus/LICENSE.txt" || { [[ -f "${SOURCE_MM2PLUS}/LICENSE" ]] && install -m 0644 "${SOURCE_MM2PLUS}/LICENSE" "${stage_dir}/share/licenses/mm2plus/LICENSE" || true; }
  [[ -f "${SOURCE_MM2PLUS}/README.md" ]] && install -m 0644 "${SOURCE_MM2PLUS}/README.md" "${stage_dir}/share/doc/mm2plus/README.md"
  { echo "project=mm2-plus"; echo "repository=${MM2PLUS_REPO_URL}"; echo "ref=${MM2PLUS_REF}"; echo "platform=${PLATFORM_DIR}"; echo "arch=${DARWIN_ARCH}"; echo "macos_deployment_target=${MACOS_DEPLOYMENT_TARGET}"; echo "compiler=$(clang++ --version | head -n 1)"; echo "openmp=disabled_serial_compatibility"; echo "codesign=adhoc_for_macho_outputs"; echo "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"; echo; echo "[files]"; find "${stage_dir}/bin" -maxdepth 1 -type f -print -exec file {} \;; } > "${stage_dir}/share/doc/mm2plus/build-info.txt"
  mkdir -p "${OUTPUT_DIR}/bin" "${OUTPUT_DIR}/share/licenses/mm2plus" "${OUTPUT_DIR}/share/doc/mm2plus"; cp -a "${stage_dir}/bin/." "${OUTPUT_DIR}/bin/"; cp -a "${stage_dir}/share/licenses/mm2plus/." "${OUTPUT_DIR}/share/licenses/mm2plus/" 2>/dev/null || true; cp -a "${stage_dir}/share/doc/mm2plus/." "${OUTPUT_DIR}/share/doc/mm2plus/"; "${OUTPUT_DIR}/bin/mm2plus-avx2" --help >/dev/null || true
}

build_hictk; build_minimap2; build_mm2plus
cat > "${OUTPUT_DIR}/manifest.json" <<EOF
{
  "id": "hict-toolchain-${PLATFORM_DIR}-hictk-minimap2-mm2plus-${HICTK_REF}-${MINIMAP2_REF}-${MM2PLUS_REF}",
  "commands": { "hictk": "bin/hictk", "minimap2": "bin/minimap2", "mm2plus_avx2": "bin/mm2plus-avx2", "mm2plus": "bin/mm2plus" },
  "files": ["bin/hictk", "bin/minimap2", "bin/mm2plus", "bin/mm2plus-avx2", "share/licenses/hictk/LICENSE", "share/doc/hictk/CITATION.cff", "share/doc/hictk/build-info.txt", "share/licenses/minimap2/LICENSE.txt", "share/licenses/minimap2/LICENSE", "share/doc/minimap2/build-info.txt", "share/licenses/mm2plus/LICENSE.txt", "share/licenses/mm2plus/LICENSE", "share/doc/mm2plus/README.md", "share/doc/mm2plus/build-info.txt"],
  "notices": ["This HiCT build bundles an official hictk source build produced from ${HICTK_REF}.", "This HiCT build bundles an official minimap2 source build produced from ${MINIMAP2_REF}.", "This HiCT build bundles mm2-plus for ${PLATFORM_DIR}; when a native build is unavailable, the bundled mm2-plus entrypoints fall back to minimap2-compatible behavior to keep the package runnable.", "HiCT performs .hic conversion by invoking the bundled hictk executable; no Python runtime is required for this path."],
  "citations": ["hictk: Rossini R, Paulsen J. hictk: blazing fast toolkit to work with .hic and .cool files. Bioinformatics. 2024;40(7):btae408. doi:10.1093/bioinformatics/btae408.", "minimap2: Li H. Minimap2: pairwise alignment for nucleotide sequences. Bioinformatics. 2018;34(18):3094-3100."],
  "limitations": ["This payload was compiled on ${PLATFORM_DIR} and should only be bundled into matching macOS portable releases.", "The mm2-plus macOS payload uses native builds when upstream source support allows it, otherwise it falls back to compatibility wrappers over minimap2."]
}
EOF
mkdir -p "${OUTPUT_DIR}/share/doc/darwin_toolchains"
{ echo "platform=${PLATFORM_DIR}"; echo "arch=${DARWIN_ARCH}"; echo "macos_deployment_target=${MACOS_DEPLOYMENT_TARGET}"; echo "hictk_ref=${HICTK_REF}"; echo "minimap2_ref=${MINIMAP2_REF}"; echo "mm2plus_ref=${MM2PLUS_REF}"; echo "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"; } > "${OUTPUT_DIR}/share/doc/darwin_toolchains/build-info.txt"
echo "[darwin/toolchains] Bundled payload prepared at ${OUTPUT_DIR}"
