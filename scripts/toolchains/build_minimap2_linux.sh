#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
OUTPUT_DIR="${OUTPUT_DIR:-${PROJECT_DIR}/toolchains-dist/linux_x86_64}"
WORK_DIR="${WORK_DIR:-/tmp/minimap2-build-linux-x86_64}"
REF="${MINIMAP2_REF:-v2.31}"
REPO_URL="${MINIMAP2_REPO_URL:-https://github.com/lh3/minimap2.git}"

usage() {
  cat <<USAGE
Build minimap2 for bundling into HiCT_JVM.

Environment:
  MINIMAP2_REF=$REF
  MINIMAP2_REPO_URL=$REPO_URL
  OUTPUT_DIR=$OUTPUT_DIR
  WORK_DIR=$WORK_DIR

The script augments an existing hictk toolchain payload when present, then rewrites
manifest.json to expose both hictk and minimap2 to HiCT.
USAGE
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
  exit 0
fi

for cmd in git make gcc python3; do
  command -v "${cmd}" >/dev/null 2>&1 || {
    echo "Required command not found: ${cmd}" >&2
    exit 1
  }
done

echo "[minimap2/linux] Building ${REF} into ${OUTPUT_DIR}"
mkdir -p "${WORK_DIR}" "${OUTPUT_DIR}/bin" "${OUTPUT_DIR}/share/licenses/minimap2" "${OUTPUT_DIR}/share/doc/minimap2"

SOURCE_DIR="${WORK_DIR}/src"
if [[ ! -d "${SOURCE_DIR}/.git" ]]; then
  rm -rf "${SOURCE_DIR}"
  git clone --filter=blob:none "${REPO_URL}" "${SOURCE_DIR}"
fi

git -C "${SOURCE_DIR}" fetch --tags --force origin "${REF}" || git -C "${SOURCE_DIR}" fetch --tags --force origin
git -C "${SOURCE_DIR}" checkout --force "${REF}"

make -C "${SOURCE_DIR}" clean >/dev/null 2>&1 || true
make -C "${SOURCE_DIR}" -j"$(nproc)" CFLAGS="${CFLAGS:--O3 -DNDEBUG}" LDFLAGS="${LDFLAGS:--static-libgcc}"

install -m 0755 "${SOURCE_DIR}/minimap2" "${OUTPUT_DIR}/bin/minimap2"
if [[ -f "${SOURCE_DIR}/LICENSE.txt" ]]; then
  install -m 0644 "${SOURCE_DIR}/LICENSE.txt" "${OUTPUT_DIR}/share/licenses/minimap2/LICENSE.txt"
elif [[ -f "${SOURCE_DIR}/LICENSE" ]]; then
  install -m 0644 "${SOURCE_DIR}/LICENSE" "${OUTPUT_DIR}/share/licenses/minimap2/LICENSE"
fi

{
  echo "project=minimap2"
  echo "repository=${REPO_URL}"
  echo "ref=${REF}"
  echo "platform=linux_x86_64"
  echo "compiler=$(gcc --version | head -n 1)"
  echo "cpu_flag_policy=generic official minimap2 build; upstream SSE2/SSE4.1 dispatch objects retain their fixed target flags."
  echo "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo
  echo "[file]"
  file "${OUTPUT_DIR}/bin/minimap2"
  echo
  echo "[ldd]"
  ldd "${OUTPUT_DIR}/bin/minimap2" || true
} > "${OUTPUT_DIR}/share/doc/minimap2/build-info.txt"

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
        f"This HiCT build bundles an official minimap2 source build produced from {ref}.",
        "HiCT self-dotplot generation uses minimap2 for self-alignment and built-in Java/native post-processing instead of Python/Cooler.",
        "minimap2 is redistributed under its MIT license. Keep the bundled license and citation files with released artifacts.",
    ])
    citations.append("minimap2: Li H. Minimap2: pairwise alignment for nucleotide sequences. Bioinformatics. 2018;34(18):3094-3100.")

manifest = {
    "id": f"hict-toolchain-linux-x86_64-hictk-minimap2-{ref}",
    "commands": commands,
    "files": files,
    "notices": notices,
    "citations": citations,
    "limitations": limitations + ["This payload was compiled on Linux and should only be bundled into Linux fat-JAR releases."],
}
(root / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
PY

echo "[minimap2/linux] Bundled minimap2 payload prepared at ${OUTPUT_DIR}"
