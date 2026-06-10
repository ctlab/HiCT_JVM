#!/usr/bin/env bash
set -euo pipefail

component="${1:-}"
if [[ -z "${component}" || "${component}" == "--help" || "${component}" == "-h" ]]; then
  cat <<'USAGE'
Usage: scripts/ci/validate_linux_component.sh <component>

Components:
  hict-native
  hictk
  minimap2
  mm2plus
  fatjar
  portable
USAGE
  exit $([[ -z "${component}" ]] && echo 1 || echo 0)
fi

abi_mode="${HICT_LINUX_ABI_MODE:-glibc217}"
max_glibc="${HICT_GLIBC_MAX:-2.17}"

version_gt() {
  local a="$1" b="$2"
  [[ "$(printf '%s\n%s\n' "$b" "$a" | sort -V | tail -n 1)" == "$a" && "$a" != "$b" ]]
}

scan_glibc_floor() {
  local root="$1"
  local bad=0
  [[ -e "${root}" ]] || { echo "::error::Missing path for GLIBC scan: ${root}"; return 1; }
  while IFS= read -r -d '' f; do
    case "${f}" in
      */browsers/*|*/FreeBSD/*|*/SunOS/*|*/Linux/arm/*|*/Linux/armv6/*|*/Linux/armv7/*|*/Linux/aarch64/*|*/Linux/ppc/*|*/Linux/ppc64/*|*/Linux/ppc64le/*|*/Linux/s390x/*|*/Linux/riscv64/*)
        continue
        ;;
    esac
    file -b "${f}" 2>/dev/null | grep -q 'ELF.*x86-64' || continue
    while read -r sym file_path; do
      [[ -z "${sym:-}" ]] && continue
      local ver="${sym#GLIBC_}"
      if version_gt "${ver}" "${max_glibc}"; then
        echo "::error::${file_path} requires ${sym}, above GLIBC_${max_glibc}"
        bad=1
      fi
    done < <(
      objdump -T "${f}" 2>/dev/null |
        awk -v f="${f}" '/\*UND\*/ && match($0, /\(GLIBC_[^)]+\)/) { v=substr($0, RSTART+1, RLENGTH-2); print v, f }'
    )
  done < <(find "${root}" -type f -print0)
  return "${bad}"
}

validate_static_musl_binary() {
  local label="$1" path="$2"
  [[ -x "${path}" ]] || { echo "::error::${label} is missing or not executable: ${path}"; return 1; }
  if readelf -d "${path}" 2>/dev/null | grep -q 'NEEDED'; then
    echo "::error::${label} is dynamically linked, but linux_abi_mode=musl-static requires a static executable: ${path}"
    return 1
  fi
  if objdump -T "${path}" 2>/dev/null | grep -q 'GLIBC_'; then
    echo "::error::${label} references GLIBC symbols, but linux_abi_mode=musl-static requires no host glibc dependency: ${path}"
    return 1
  fi
}

validate_glibc_binary() {
  local label="$1" path="$2"
  [[ -x "${path}" ]] || { echo "::error::${label} is missing or not executable: ${path}"; return 1; }
  scan_glibc_floor "${path}"
}

validate_tool_binary() {
  local label="$1" path="$2"
  if [[ "${abi_mode}" == "musl-static" ]]; then
    validate_static_musl_binary "${label}" "${path}"
  else
    validate_glibc_binary "${label}" "${path}"
  fi
}

case "${component}" in
  hict-native)
    root="build/native-processing/resources/natives/linux_64"
    [[ -d "${root}" ]] || { echo "::error::HiCT native processing output directory is missing: ${root}"; exit 1; }
    find "${root}" -path '*/native/*.so' -type f | grep -q . || { echo "::error::No HiCT native processing .so files found in ${root}"; exit 1; }
    scan_glibc_floor "${root}"
    ;;
  hictk)
    validate_tool_binary "hictk" "toolchains-dist/linux_x86_64/bin/hictk"
    toolchains-dist/linux_x86_64/bin/hictk --version >/dev/null || toolchains-dist/linux_x86_64/bin/hictk --help >/dev/null
    ;;
  minimap2)
    validate_tool_binary "minimap2" "toolchains-dist/linux_x86_64/bin/minimap2"
    toolchains-dist/linux_x86_64/bin/minimap2 --version >/dev/null || toolchains-dist/linux_x86_64/bin/minimap2 --help >/dev/null
    ;;
  mm2plus)
    validate_tool_binary "mm2-plus AVX2" "toolchains-dist/linux_x86_64/bin/mm2plus-avx2"
    validate_tool_binary "mm2-plus AVX-512" "toolchains-dist/linux_x86_64/bin/mm2plus-avx512"
    if grep -qw avx2 /proc/cpuinfo 2>/dev/null; then
      toolchains-dist/linux_x86_64/bin/mm2plus-avx2 --version >/dev/null || toolchains-dist/linux_x86_64/bin/mm2plus-avx2 --help >/dev/null || true
    else
      echo "::warning::Skipping mm2-plus AVX2 execution because runner CPU does not advertise avx2. Static/ABI checks passed."
    fi
    if grep -qw avx512f /proc/cpuinfo 2>/dev/null; then
      toolchains-dist/linux_x86_64/bin/mm2plus-avx512 --version >/dev/null || toolchains-dist/linux_x86_64/bin/mm2plus-avx512 --help >/dev/null || true
    else
      echo "::warning::Skipping mm2-plus AVX-512 execution because runner CPU does not advertise avx512f. Static/ABI checks passed."
    fi
    ;;
  fatjar)
    fat_jar="$(find build/libs -maxdepth 1 -type f -name '*-fat.jar' | sort | tail -n 1)"
    [[ -n "${fat_jar}" && -f "${fat_jar}" ]] || { echo "::error::Fat JAR was not found under build/libs."; exit 1; }
    jar tf "${fat_jar}" | grep -qx 'webui/index.html' || { echo "::error::Fat JAR does not contain webui/index.html."; exit 1; }
    if [[ -f toolchains-dist/linux_x86_64/manifest.json ]]; then
      jar tf "${fat_jar}" | grep -qx 'toolchains/linux_x86_64/manifest.json' || { echo "::error::Fat JAR does not contain bundled Linux toolchain manifest."; exit 1; }
    fi
    ;;
  portable)
    root="$(find build/portable -mindepth 1 -maxdepth 1 -type d -name 'HiCT-*-linux-x86_64' | sort | tail -n 1)"
    [[ -n "${root}" && -d "${root}" ]] || { echo "::error::Portable Linux root was not found under build/portable."; exit 1; }
    scan_glibc_floor "${root}"
    ;;
  *) echo "Unsupported component: ${component}" >&2; exit 1 ;;
esac
