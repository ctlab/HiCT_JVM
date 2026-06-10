#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
OUTPUT_DIR="${OUTPUT_DIR:-${PROJECT_DIR}/toolchains-dist/linux_x86_64}"
WORK_DIR="${WORK_DIR:-/tmp/mm2plus-build-linux-x86_64}"
REF="${MM2PLUS_REF:-v1.2}"
REPO_URL="${MM2PLUS_REPO_URL:-https://github.com/at-cg/mm2-plus.git}"
CXX_BIN="${CXX:-g++}"
CC_BIN="${CC:-gcc}"
ENABLE_STATIC_MUSL="${HICT_STATIC_MUSL:-0}"
ENABLE_MIMALLOC="${HICT_STATIC_MIMALLOC:-0}"
BUILD_JOBS="${BUILD_JOBS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || nproc 2>/dev/null || echo 4)}"

usage() {
  cat <<USAGE
Build mm2-plus for bundling into HiCT_JVM.

Environment:
  MM2PLUS_REF=$REF
  MM2PLUS_REPO_URL=$REPO_URL
  OUTPUT_DIR=$OUTPUT_DIR
  WORK_DIR=$WORK_DIR
  CXX=$CXX_BIN
  CC=$CC_BIN
  HICT_STATIC_MUSL=1
  HICT_STATIC_MIMALLOC=1
  BUILD_JOBS=$BUILD_JOBS

The script augments an existing hictk/minimap2 toolchain payload when present,
then rewrites manifest.json to expose hictk, minimap2 and mm2-plus variants.
USAGE
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
  exit 0
fi

for cmd in git make "${CXX_BIN}" "${CC_BIN}" python3 file; do
  command -v "${cmd}" >/dev/null 2>&1 || { echo "Required command not found: ${cmd}" >&2; exit 1; }
done
if [[ "${ENABLE_STATIC_MUSL}" == "1" || "${ENABLE_MIMALLOC}" == "1" ]]; then
  for cmd in clang clang++ readelf; do
    command -v "${cmd}" >/dev/null 2>&1 || { echo "Required command not found: ${cmd}" >&2; exit 1; }
  done
fi

echo "[mm2plus/linux] Building ${REF} into ${OUTPUT_DIR}"
mkdir -p "${WORK_DIR}" "${OUTPUT_DIR}/bin" "${OUTPUT_DIR}/share/licenses/mm2plus" "${OUTPUT_DIR}/share/doc/mm2plus"
BUILD_INFO="${OUTPUT_DIR}/share/doc/mm2plus/build-info.txt"

SOURCE_DIR="${WORK_DIR}/src"
if [[ ! -d "${SOURCE_DIR}/.git" ]]; then
  rm -rf "${SOURCE_DIR}"
  git clone --filter=blob:none "${REPO_URL}" "${SOURCE_DIR}" || git clone "${REPO_URL}" "${SOURCE_DIR}"
fi

git -C "${SOURCE_DIR}" fetch --tags --force origin "${REF}" || git -C "${SOURCE_DIR}" fetch --tags --force origin
git -C "${SOURCE_DIR}" checkout --force "${REF}"

python3 - "${SOURCE_DIR}/Makefile" <<'PY'
import pathlib, sys
path = pathlib.Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
patterns = [
    ("$(CXX) -c $(CPPFLAGS) $(INCLUDES) $< -o $@", "$(CXX) -c $(CPPFLAGS) $(EXTRAFLAGS) $(INCLUDES) $< -o $@"),
    ("$(CC) -c $(CPPFLAGS) $(INCLUDES) $< -o $@", "$(CC) -c $(CPPFLAGS) $(EXTRAFLAGS) $(INCLUDES) $< -o $@"),
    ("$(CC) -c $(CFLAGS) $(INCLUDES) $< -o $@", "$(CC) -c $(CFLAGS) $(EXTRAFLAGS) $(INCLUDES) $< -o $@"),
]
patched = 0
for old, new in patterns:
    count = text.count(old)
    if count:
        text = text.replace(old, new)
        patched += count
path.write_text(text, encoding="utf-8")
if patched:
    print(f"[mm2plus/linux] Patched {patched} Makefile object rule(s) to include EXTRAFLAGS.")
else:
    print("[mm2plus/linux] Makefile object rules already include EXTRAFLAGS or use an upstream layout not matching known patterns.")
PY

MIMALLOC_LINK_ARGS=()
if [[ "${ENABLE_MIMALLOC}" == "1" ]]; then
  MIMALLOC_PREFIX="$(WORK_DIR="${WORK_DIR}/mimalloc" OUTPUT_DIR="${WORK_DIR}/mimalloc/install" BUILD_JOBS="${BUILD_JOBS}" bash "${SCRIPT_DIR}/build_mimalloc_static.sh")"
  MIMALLOC_ARCHIVE="$(find "${MIMALLOC_PREFIX}/lib" -maxdepth 1 -type f -name 'libmimalloc*.a' | sort | head -n 1)"
  if [[ -z "${MIMALLOC_ARCHIVE}" || ! -f "${MIMALLOC_ARCHIVE}" ]]; then
    echo "mimalloc static archive was not found under ${MIMALLOC_PREFIX}/lib" >&2
    exit 1
  fi
  MIMALLOC_LINK_ARGS=("-Wl,--whole-archive" "${MIMALLOC_ARCHIVE}" "-Wl,--no-whole-archive")
fi

if [[ "${ENABLE_STATIC_MUSL}" == "1" ]]; then
  python3 - "${SOURCE_DIR}/Makefile" <<'PY'
import pathlib, sys
path = pathlib.Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
updated = text.replace("-fopenmp", "").replace("-lgomp", "").replace("-D_GLIBCXX_PARALLEL", "")
if updated != text:
    path.write_text(updated, encoding="utf-8")
PY
  mkdir -p "${WORK_DIR}/compat"
  cat > "${WORK_DIR}/compat/omp.h" <<'EOF'
#ifndef HICT_MM2PLUS_STATIC_MUSL_OMP_H
#define HICT_MM2PLUS_STATIC_MUSL_OMP_H

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
  cat > "${SOURCE_DIR}/src/parallel_sort.cpp" <<'EOF'
#include <algorithm>
#include "mmpriv.h"

void parallel_sort(mm128_t* z, size_t n_u, int32_t) {
    std::stable_sort(z, z + n_u, [](const mm128_t &a, const mm128_t &b) { return a.x < b.x; });
}
EOF
fi

cpu_has_flag() {
  local flag="$1"
  [[ -r /proc/cpuinfo ]] && grep -m1 -qw -- "${flag}" /proc/cpuinfo
}

can_execute_variant_here() {
  local variant="$1"
  case "${variant}" in
    avx2) cpu_has_flag avx2 ;;
    avx512) cpu_has_flag avx512f ;;
    *) return 0 ;;
  esac
}

build_variant() {
  local variant="$1" flags="$2" output="${OUTPUT_DIR}/bin/mm2plus-${variant}"
  if [[ -x "${output}" && "${HICT_REBUILD_MM2PLUS:-0}" != "1" ]] && grep -qx "ref=${REF}" "${BUILD_INFO}" 2>/dev/null; then
    echo "[mm2plus/linux] Reusing existing ${output}"
    return 0
  fi

  make -C "${SOURCE_DIR}" clean >/dev/null 2>&1 || true
  echo "[mm2plus/linux] Compiling ${variant} with EXTRAFLAGS=${flags}"

  local ldflags libs
  if [[ "${ENABLE_STATIC_MUSL}" == "1" ]]; then
    ldflags="-static -fuse-ld=lld ${HICT_MUSL_PREFIX:+-L${HICT_MUSL_PREFIX}/lib}"
    libs="-static -fuse-ld=lld ${HICT_MUSL_PREFIX:+-L${HICT_MUSL_PREFIX}/lib} -Wl,-Bstatic -lz -lm -lpthread ${MIMALLOC_LINK_ARGS[*]}"
  else
    ldflags="-fopenmp"
    libs="-fopenmp -lz -lm -lpthread -ldl"
  fi

  if make -C "${SOURCE_DIR}" -j"${BUILD_JOBS}" base=1 avx=1 CC="${CC_BIN}" CXX="${CXX_BIN}" EXTRAFLAGS="${flags}" LDFLAGS="${ldflags}" LIBS="${libs}"; then
    install -m 0755 "${SOURCE_DIR}/mm2plus" "${output}"
    if [[ "${ENABLE_STATIC_MUSL}" == "1" ]] && readelf -d "${output}" 2>/dev/null | grep -q 'NEEDED'; then
      echo "mm2-plus ${variant} is still dynamically linked; static musl packaging failed." >&2
      rm -f "${output}"
      return 1
    fi
    if can_execute_variant_here "${variant}"; then
      "${output}" --help >/dev/null || true
    else
      echo "::warning::Skipping ${variant} smoke execution on this runner because required CPU flags are absent."
    fi
    return 0
  fi
  echo "::warning::mm2-plus ${variant} build failed; dotplot generation can still use minimap2 or another mm2-plus variant." >&2
  rm -f "${output}"
  return 1
}

variant_failures=0
MM2PLUS_TARGET_FLAGS=""
if [[ "${ENABLE_STATIC_MUSL}" == "1" ]]; then
  MM2PLUS_TARGET_FLAGS="--target=x86_64-alpine-linux-musl -I${WORK_DIR}/compat ${HICT_MUSL_PREFIX:+-I${HICT_MUSL_PREFIX}/include}"
fi
build_variant "avx2" "${MM2PLUS_TARGET_FLAGS} -mavx2" || variant_failures=$((variant_failures + 1))
build_variant "avx512" "${MM2PLUS_TARGET_FLAGS} -mavx512f -mavx512dq -mavx512bw -mavx512vl -mavx2" || variant_failures=$((variant_failures + 1))

if [[ -f "${SOURCE_DIR}/LICENSE.txt" ]]; then
  install -m 0644 "${SOURCE_DIR}/LICENSE.txt" "${OUTPUT_DIR}/share/licenses/mm2plus/LICENSE.txt"
elif [[ -f "${SOURCE_DIR}/LICENSE" ]]; then
  install -m 0644 "${SOURCE_DIR}/LICENSE" "${OUTPUT_DIR}/share/licenses/mm2plus/LICENSE"
fi
[[ -f "${SOURCE_DIR}/README.md" ]] && install -m 0644 "${SOURCE_DIR}/README.md" "${OUTPUT_DIR}/share/doc/mm2plus/README.md"

{
  echo "project=mm2-plus"
  echo "repository=${REPO_URL}"
  echo "ref=${REF}"
  echo "platform=linux_x86_64"
  echo "compiler=$("${CXX_BIN}" --version | head -n 1)"
  echo "avx2_extraflags=-mavx2"
  echo "avx512_extraflags=-mavx512f -mavx512dq -mavx512bw -mavx512vl -mavx2"
  echo "cpu_flag_policy=EXTRAFLAGS are applied to generic and AVX objects; OpenMP is disabled for static musl builds and replaced with a serial compatibility path."
  echo "static_runtime=$([[ "${ENABLE_STATIC_MUSL}" == "1" ]] && echo musl || echo host_glibc)"
  echo "mimalloc_linking=$([[ "${ENABLE_MIMALLOC}" == "1" ]] && echo static || echo disabled)"
  echo "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  for variant in avx2 avx512; do
    binary="${OUTPUT_DIR}/bin/mm2plus-${variant}"
    if [[ -x "${binary}" ]]; then
      echo
      echo "[${variant} file]"
      file "${binary}"
      echo
      echo "[${variant} ldd]"
      ldd "${binary}" || true
      echo
      echo "[${variant} smoke]"
      if can_execute_variant_here "${variant}"; then
        "${binary}" --help >/dev/null && echo "help OK" || echo "help returned non-zero"
      else
        echo "skipped because this runner lacks required CPU flags"
      fi
    fi
  done
} > "${BUILD_INFO}"

python3 - "${OUTPUT_DIR}" "${REF}" <<'PY'
import json, pathlib, sys
root = pathlib.Path(sys.argv[1]); ref = sys.argv[2]
commands = {}; files = []; notices = []; citations = []; limitations = []
def add_file(path: str) -> None:
    if (root / path).is_file() and path not in files:
        files.append(path)
if (root / "bin/hictk").is_file():
    commands["hictk"] = "bin/hictk"
    for p in ("bin/hictk", "share/licenses/hictk/LICENSE", "share/doc/hictk/CITATION.cff", "share/doc/hictk/build-info.txt"): add_file(p)
    notices.extend(["This HiCT build bundles an official hictk source build.", "HiCT performs .hic conversion and .cool/.mcool dotplot loading by invoking the bundled hictk executable; no Python runtime is required for these paths.", "hictk is redistributed under its MIT license. Keep the bundled license and citation files with released artifacts."])
    citations.append("hictk: Rossini R, Paulsen J. hictk: blazing fast toolkit to work with .hic and .cool files. Bioinformatics. 2024;40(7):btae408. doi:10.1093/bioinformatics/btae408.")
if (root / "bin/minimap2").is_file():
    commands["minimap2"] = "bin/minimap2"
    for p in ("bin/minimap2", "share/licenses/minimap2/LICENSE.txt", "share/licenses/minimap2/LICENSE", "share/doc/minimap2/build-info.txt"): add_file(p)
    notices.extend(["This HiCT build bundles an official minimap2 source build.", "HiCT self-dotplot generation can use minimap2 for self-alignment and built-in Java/native post-processing instead of Python/Cooler.", "minimap2 is redistributed under its MIT license. Keep the bundled license and citation files with released artifacts."])
    citations.append("minimap2: Li H. Minimap2: pairwise alignment for nucleotide sequences. Bioinformatics. 2018;34(18):3094-3100.")
for key, path in {"mm2plus_avx2": "bin/mm2plus-avx2", "mm2plus_avx512": "bin/mm2plus-avx512"}.items():
    if (root / path).is_file(): commands[key] = path; add_file(path)
if "mm2plus_avx2" in commands or "mm2plus_avx512" in commands:
    for p in ("share/licenses/mm2plus/LICENSE.txt", "share/licenses/mm2plus/LICENSE", "share/doc/mm2plus/README.md", "share/doc/mm2plus/build-info.txt"): add_file(p)
    notices.extend([f"This HiCT build bundles mm2-plus source builds produced from {ref}.", "HiCT self-dotplot generation can use mm2-plus as an accelerated minimap2-compatible aligner when selected.", "mm2-plus is redistributed with its upstream license file. Keep the bundled license and citation files with released artifacts."])
    citations.append("mm2-plus: Ghanshyam Chandra, Md Vasimuddin, Sanchit Misra and Chirag Jain. Accelerating whole-genome alignment in the age of complete genome assemblies. bioRxiv 2024. doi:10.1101/2024.11.25.625328.")
    limitations.append("Static musl mm2-plus builds disable OpenMP and use a serial compatibility path to avoid host runtime dependencies.")
else:
    limitations.append("No mm2-plus executable was built; HiCT self-dotplot generation will fall back to minimap2 when available.")
manifest = {"id": f"hict-toolchain-linux-x86_64-hictk-minimap2-mm2plus-{ref}", "commands": commands, "files": files, "notices": notices, "citations": citations, "limitations": limitations + ["This payload was compiled on Linux and should only be bundled into Linux fat-JAR releases."]}
(root / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
PY

if [[ "${HICT_REQUIRE_MM2PLUS_VARIANTS:-0}" == "1" && "${variant_failures}" -ne 0 ]]; then
  echo "[mm2plus/linux] HICT_REQUIRE_MM2PLUS_VARIANTS=1 and ${variant_failures} mm2-plus variant(s) failed." >&2
  exit 1
fi

echo "[mm2plus/linux] Bundled mm2-plus payload prepared at ${OUTPUT_DIR}"
