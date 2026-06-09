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
ENABLE_STATIC_MUSL="0"
ENABLE_MIMALLOC="0"

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

The script augments an existing hictk/minimap2 toolchain payload when present,
then rewrites manifest.json to expose hictk, minimap2 and mm2-plus variants.
USAGE
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
  exit 0
fi

for cmd in git make "${CXX_BIN}" "${CC_BIN}" python3; do
  command -v "${cmd}" >/dev/null 2>&1 || {
    echo "Required command not found: ${cmd}" >&2
    exit 1
  }
done

echo "[mm2plus/linux] Building ${REF} into ${OUTPUT_DIR}"
mkdir -p "${WORK_DIR}" "${OUTPUT_DIR}/bin" "${OUTPUT_DIR}/share/licenses/mm2plus" "${OUTPUT_DIR}/share/doc/mm2plus"
BUILD_INFO="${OUTPUT_DIR}/share/doc/mm2plus/build-info.txt"

SOURCE_DIR="${WORK_DIR}/src"
if [[ ! -d "${SOURCE_DIR}/.git" ]]; then
  rm -rf "${SOURCE_DIR}"
  git clone --filter=blob:none "${REPO_URL}" "${SOURCE_DIR}"
fi

git -C "${SOURCE_DIR}" fetch --tags --force origin "${REF}" || git -C "${SOURCE_DIR}" fetch --tags --force origin
git -C "${SOURCE_DIR}" checkout --force "${REF}"

python3 - "${SOURCE_DIR}/Makefile" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
old = "$(CXX) -c $(CPPFLAGS) $(INCLUDES) $< -o $@"
new = "$(CXX) -c $(CPPFLAGS) $(EXTRAFLAGS) $(INCLUDES) $< -o $@"
count = text.count(old)
if count:
    path.write_text(text.replace(old, new), encoding="utf-8")
    print(f"[mm2plus/linux] Patched {count} Makefile generic object rule(s) to include EXTRAFLAGS.")
elif new in text:
    print("[mm2plus/linux] Makefile generic object rules already include EXTRAFLAGS.")
else:
    print("[mm2plus/linux] Makefile generic object rule pattern was not found; upstream layout may have changed.")
PY
if [[ "${ENABLE_STATIC_MUSL}" == "1" ]]; then
  python3 - "${SOURCE_DIR}/Makefile" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
updated = text.replace("-fopenmp", "")
updated = updated.replace("-lgomp", "")
if updated != text:
    path.write_text(updated, encoding="utf-8")
PY
fi

build_variant() {
  local variant="$1"
  local flags="$2"
  local output="${OUTPUT_DIR}/bin/mm2plus-${variant}"

  if [[ -x "${output}" && "${HICT_REBUILD_MM2PLUS:-0}" != "1" ]] && grep -qx "ref=${REF}" "${BUILD_INFO}" 2>/dev/null; then
    echo "[mm2plus/linux] Reusing existing ${output}"
    return 0
  fi
  if [[ -x "${output}" && "${HICT_REBUILD_MM2PLUS:-0}" != "1" ]]; then
    echo "[mm2plus/linux] Existing ${output} was built for a different or unknown ref; rebuilding."
  fi

  make -C "${SOURCE_DIR}" clean >/dev/null 2>&1 || true
  echo "[mm2plus/linux] Compiling ${variant} with EXTRAFLAGS=${flags}"
  if make -C "${SOURCE_DIR}" -j"$(nproc)" base=1 avx=1 CC="${CC_BIN}" CXX="${CXX_BIN}" EXTRAFLAGS="${flags}" \
      LDFLAGS="-fopenmp" \
      LIBS="-fopenmp -lz -lm -lpthread -ldl"; then
    install -m 0755 "${SOURCE_DIR}/mm2plus" "${output}"
    "${output}" --help >/dev/null || true
    return 0
  fi
  echo "::warning::mm2-plus ${variant} build failed; dotplot generation can still use minimap2 or another mm2-plus variant." >&2
  rm -f "${output}"
  return 1
}

variant_failures=0
build_variant "avx2" "-mavx2" || variant_failures=$((variant_failures + 1))
build_variant "avx512" "-mavx512f -mavx512dq -mavx512bw -mavx512vl -mavx2" || variant_failures=$((variant_failures + 1))

if [[ -f "${SOURCE_DIR}/LICENSE.txt" ]]; then
  install -m 0644 "${SOURCE_DIR}/LICENSE.txt" "${OUTPUT_DIR}/share/licenses/mm2plus/LICENSE.txt"
elif [[ -f "${SOURCE_DIR}/LICENSE" ]]; then
  install -m 0644 "${SOURCE_DIR}/LICENSE" "${OUTPUT_DIR}/share/licenses/mm2plus/LICENSE"
fi
if [[ -f "${SOURCE_DIR}/README.md" ]]; then
  install -m 0644 "${SOURCE_DIR}/README.md" "${OUTPUT_DIR}/share/doc/mm2plus/README.md"
fi

{
  echo "project=mm2-plus"
  echo "repository=${REPO_URL}"
  echo "ref=${REF}"
  echo "platform=linux_x86_64"
  echo "compiler=$("${CXX_BIN}" --version | head -n 1)"
  echo "avx2_extraflags=-mavx2"
  echo "avx512_extraflags=-mavx512f -mavx512dq -mavx512bw -mavx512vl -mavx2"
  echo "cpu_flag_policy=EXTRAFLAGS are applied to generic and AVX/OpenMP objects; upstream SSE2/SSE4.1 dispatch objects intentionally retain their fixed target flags."
  echo "static_runtime=host_glibc"
  echo "mimalloc_linking=disabled"
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
      echo "[${variant} version]"
      if [[ "${variant}" == "avx512" ]]; then
        echo "version check skipped to avoid executing AVX-512 code on runners without AVX-512 support"
      else
        "${binary}" --version || true
      fi
    fi
  done
} > "${BUILD_INFO}"

python3 - "${OUTPUT_DIR}" "${REF}" <<'PY'
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
ref = sys.argv[2]

commands = {}
files = []
notices = []
citations = []
limitations = []

def add_file(path: str) -> None:
    if (root / path).is_file() and path not in files:
        files.append(path)

if (root / "bin/hictk").is_file():
    commands["hictk"] = "bin/hictk"
    for p in ("bin/hictk", "share/licenses/hictk/LICENSE", "share/doc/hictk/CITATION.cff", "share/doc/hictk/build-info.txt"):
        add_file(p)
    notices.extend([
        "This HiCT build bundles an official hictk source build.",
        "HiCT performs .hic conversion and .cool/.mcool dotplot loading by invoking the bundled hictk executable; no Python runtime is required for these paths.",
        "hictk is redistributed under its MIT license. Keep the bundled license and citation files with released artifacts.",
    ])
    citations.append("hictk: Rossini R, Paulsen J. hictk: blazing fast toolkit to work with .hic and .cool files. Bioinformatics. 2024;40(7):btae408. doi:10.1093/bioinformatics/btae408.")
    limitations.append("Upstream hictk builds embed their own third-party dependencies, but Linux libc/libstdc++ ABI compatibility still depends on the target system.")

if (root / "bin/minimap2").is_file():
    commands["minimap2"] = "bin/minimap2"
    for p in ("bin/minimap2", "share/licenses/minimap2/LICENSE.txt", "share/licenses/minimap2/LICENSE", "share/doc/minimap2/build-info.txt"):
        add_file(p)
    notices.extend([
        "This HiCT build bundles an official minimap2 source build.",
        "HiCT self-dotplot generation can use minimap2 for self-alignment and built-in Java/native post-processing instead of Python/Cooler.",
        "minimap2 is redistributed under its MIT license. Keep the bundled license and citation files with released artifacts.",
    ])
    citations.append("minimap2: Li H. Minimap2: pairwise alignment for nucleotide sequences. Bioinformatics. 2018;34(18):3094-3100.")
    limitations.append("This payload was compiled as a static musl binary for Linux x86_64 and should not depend on the host glibc runtime.")

mm2plus_variants = {
    "mm2plus_avx2": "bin/mm2plus-avx2",
    "mm2plus_avx512": "bin/mm2plus-avx512",
}
available_mm2plus = []
for key, path in mm2plus_variants.items():
    if (root / path).is_file():
        commands[key] = path
        add_file(path)
        available_mm2plus.append(key)

if available_mm2plus:
    for p in ("share/licenses/mm2plus/LICENSE.txt", "share/licenses/mm2plus/LICENSE", "share/doc/mm2plus/README.md", "share/doc/mm2plus/build-info.txt"):
        add_file(p)
    notices.extend([
        f"This HiCT build bundles mm2-plus source builds produced from {ref}.",
        "HiCT self-dotplot generation can use mm2-plus as an accelerated minimap2-compatible aligner when selected.",
        "mm2-plus is redistributed with its upstream license file. Keep the bundled license and citation files with released artifacts.",
    ])
    citations.append("mm2-plus: Ghanshyam Chandra, Md Vasimuddin, Sanchit Misra and Chirag Jain. Accelerating whole-genome alignment in the age of complete genome assemblies. bioRxiv 2024. doi:10.1101/2024.11.25.625328.")
    limitations.append("This payload was compiled as a dynamically linked Linux x86_64 binary and depends on the host glibc/libstdc++ runtime.")
else:
    limitations.append("No mm2-plus executable was built; HiCT self-dotplot generation will fall back to minimap2 when available.")

manifest = {
    "id": f"hict-toolchain-linux-x86_64-hictk-minimap2-mm2plus-{ref}",
    "commands": commands,
    "files": files,
    "notices": notices,
    "citations": citations,
    "limitations": limitations + ["This payload was compiled on Linux and should only be bundled into Linux fat-JAR releases."],
}
(root / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
PY

if [[ "${HICT_REQUIRE_MM2PLUS_VARIANTS:-0}" == "1" && "${variant_failures}" -gt 0 ]]; then
  echo "[mm2plus/linux] HICT_REQUIRE_MM2PLUS_VARIANTS=1 and ${variant_failures} mm2-plus variant(s) failed." >&2
  exit 1
fi

if [[ "${variant_failures}" -ge 2 ]]; then
  echo "[mm2plus/linux] No mm2-plus variant was built." >&2
  exit 1
fi

echo "[mm2plus/linux] Bundled mm2-plus payload prepared at ${OUTPUT_DIR}"
