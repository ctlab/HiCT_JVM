#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/ci/linux_container_build.sh <target>

Targets:
  hict-native   Build HiCT JNI/native-processing libraries in manylinux2014.
  hictk         Build bundled hictk in Docker for the selected Linux ABI mode.
  minimap2      Build bundled minimap2 in Docker for the selected Linux ABI mode.
  mm2plus       Build bundled mm2-plus in Docker for the selected Linux ABI mode.
  fatjar        Build the HiCT fat JAR in manylinux2014 using already prepared WebUI resources.

Environment:
  HICT_LINUX_ABI_MODE=glibc217|musl-static   Default: glibc217
  HICTK_REF=v2.2.0
  MINIMAP2_REF=v2.31
  MM2PLUS_REF=v1.2
  HICTK_CONAN_HOME=/path/to/conan/cache
  GRADLE_USER_HOME=/path/to/.gradle
  GRADLE_OPTS=-Dorg.gradle.daemon=false

No host JDK is mounted into the containers. Gradle/JVM work installs or reuses
Temurin JDK 21 inside .cache/container-jdks/temurin-21-linux-x64.
USAGE
}

target="${1:-}"
if [[ -z "${target}" || "${target}" == "--help" || "${target}" == "-h" ]]; then
  usage
  exit $([[ -z "${target}" ]] && echo 1 || echo 0)
fi

case "${target}" in
  hict-native|hictk|minimap2|mm2plus|fatjar) ;;
  *) echo "Unsupported Linux container build target: ${target}" >&2; usage >&2; exit 1 ;;
esac

repo_root="${GITHUB_WORKSPACE:-$(pwd -P)}"
cd "${repo_root}"

abi_mode="${HICT_LINUX_ABI_MODE:-glibc217}"
case "${abi_mode}" in
  glibc217|musl-static) ;;
  *) echo "Unsupported HICT_LINUX_ABI_MODE=${abi_mode}; expected glibc217 or musl-static." >&2; exit 1 ;;
esac

mkdir -p \
  "${repo_root}/.cache/container-jdks" \
  "${repo_root}/.cache/hictk-conan/linux_x86_64-${abi_mode}" \
  "${repo_root}/.gradle" \
  "${repo_root}/toolchains-dist/linux_x86_64"

common_docker_args=(
  --rm -i
  -v "${repo_root}:${repo_root}"
  -w "${repo_root}"
  -e GITHUB_WORKSPACE="${repo_root}"
  -e GRADLE_USER_HOME="${GRADLE_USER_HOME:-${repo_root}/.gradle}"
  -e GRADLE_OPTS="${GRADLE_OPTS:--Dorg.gradle.daemon=false}"
  -e HICTK_REF="${HICTK_REF:-v2.2.0}"
  -e MINIMAP2_REF="${MINIMAP2_REF:-v2.31}"
  -e MM2PLUS_REF="${MM2PLUS_REF:-v1.2}"
  -e HICT_WEBUI_REF="${HICT_WEBUI_REF:-same-as-jvm}"
  -e HICT_LINUX_ABI_MODE="${abi_mode}"
  -e HICTK_CONAN_HOME="${HICTK_CONAN_HOME:-${repo_root}/.cache/hictk-conan/linux_x86_64-${abi_mode}}"
  -e HICT_REQUIRE_NATIVE_PROCESSING_VARIANTS="${HICT_REQUIRE_NATIVE_PROCESSING_VARIANTS:-1}"
  -e HICT_REQUIRE_MM2PLUS_VARIANTS="${HICT_REQUIRE_MM2PLUS_VARIANTS:-1}"
  -e HICTK_CONAN_BUILD_POLICY="${HICTK_CONAN_BUILD_POLICY:-*}"
  -e HOST_UID="$(id -u)"
  -e HOST_GID="$(id -g)"
)

run_manylinux() {
  local ml_target="$1"
  docker run "${common_docker_args[@]}" \
    -e HICT_CONTAINER_TARGET="${ml_target}" \
    -e HICT_STATIC_MUSL="0" \
    -e HICT_STATIC_MIMALLOC="0" \
    quay.io/pypa/manylinux2014_x86_64:latest \
    bash -s <<'MANYLINUX'
set -euo pipefail

yum install -y \
  autoconf automake binutils bzip2 bzip2-devel ca-certificates curl diffutils file findutils \
  gcc gcc-c++ git gzip libtool m4 make openssl-devel patch perl-core perl-IPC-Cmd pkgconfig \
  tar unzip which xz xz-devel zip zlib-devel

perl -MIPC::Cmd -e 1
command -v m4 >/dev/null
m4 --version | head -n 1

enable_devtoolset() {
  local candidate=""
  for candidate in /opt/rh/devtoolset-10/enable /opt/rh/devtoolset-9/enable /opt/rh/devtoolset-8/enable; do
    if [[ -f "${candidate}" ]]; then
      echo "Sourcing ${candidate}"
      set +u
      source "${candidate}"
      set -u
      return 0
    fi
  done
  echo "No devtoolset enable script found; using default compiler."
}
enable_devtoolset

find_manylinux_python() {
  local candidate=""
  for candidate in \
    /opt/python/cp313-cp313/bin/python3 /opt/python/cp313-cp313/bin/python \
    /opt/python/cp312-cp312/bin/python3 /opt/python/cp312-cp312/bin/python \
    /opt/python/cp311-cp311/bin/python3 /opt/python/cp311-cp311/bin/python \
    /opt/python/cp310-cp310/bin/python3 /opt/python/cp310-cp310/bin/python \
    /opt/python/cp39-cp39/bin/python3 /opt/python/cp39-cp39/bin/python \
    /opt/python/cp38-cp38/bin/python3 /opt/python/cp38-cp38/bin/python; do
    if [[ -e "${candidate}" ]]; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done
  find /opt/python -maxdepth 4 \( -name python3 -o -name python \) -print 2>/dev/null | sort -V | tail -n 1
}

PYBIN="$(find_manylinux_python || true)"
if [[ -z "${PYBIN}" || ! -e "${PYBIN}" ]]; then
  echo "Unable to find a manylinux Python under /opt/python." >&2
  find /opt/python -maxdepth 3 -print 2>/dev/null | sort >&2 || true
  exit 1
fi
ln -sfn "${PYBIN}" /usr/local/bin/python3

install_temurin_jdk21() {
  local jdk_root="${GITHUB_WORKSPACE}/.cache/container-jdks/temurin-21-linux-x64"
  local tmp_root="${jdk_root}.tmp"
  local tarball="/tmp/temurin-jdk21-linux-x64.tar.gz"
  if [[ ! -x "${jdk_root}/bin/java" || ! -x "${jdk_root}/bin/javac" || ! -x "${jdk_root}/bin/jlink" ]]; then
    echo "Installing Temurin JDK 21 inside manylinux container: ${jdk_root}"
    rm -rf "${tmp_root}"
    mkdir -p "${tmp_root}" "$(dirname "${jdk_root}")"
    curl --fail --location --retry 5 --retry-delay 2 --output "${tarball}" \
      "https://api.adoptium.net/v3/binary/latest/21/ga/linux/x64/jdk/hotspot/normal/eclipse"
    tar -xzf "${tarball}" -C "${tmp_root}" --strip-components=1
    rm -rf "${jdk_root}"
    mv "${tmp_root}" "${jdk_root}"
    rm -f "${tarball}"
  else
    echo "Using cached Temurin JDK 21 inside manylinux container: ${jdk_root}"
  fi
  export JAVA_HOME="${jdk_root}"
  export PATH="${JAVA_HOME}/bin:${PATH}"
}

export PATH="/usr/local/bin:$(dirname "${PYBIN}"):${PATH}"
python3 --version
gcc --version | head -n 1
g++ --version | head -n 1

python3 -m ensurepip --upgrade >/dev/null 2>&1 || true
python3 -m pip install --upgrade pip
python3 -m pip install "cmake>=3.25" ninja "conan>=2"

chmod +x gradlew \
  scripts/toolchains/build_hictk_linux.sh \
  scripts/toolchains/build_minimap2_linux.sh \
  scripts/toolchains/build_mm2plus_linux.sh

case "${HICT_CONTAINER_TARGET}" in
  hict-native)
    install_temurin_jdk21
    java -version
    javac -version
    ./gradlew verifyNativeProcessingBuild --no-daemon
    ;;
  hictk)
    conan remove 'm4/*' -c || true
    HICT_STATIC_MUSL=0 HICT_STATIC_MIMALLOC=0 ENABLE_MOSTLY_STATIC_RUNTIME=1 ./scripts/toolchains/build_hictk_linux.sh
    ;;
  minimap2)
    HICT_STATIC_MUSL=0 HICT_STATIC_MIMALLOC=0 ./scripts/toolchains/build_minimap2_linux.sh
    ;;
  mm2plus)
    HICT_REQUIRE_MM2PLUS_VARIANTS=1 HICT_STATIC_MUSL=0 HICT_STATIC_MIMALLOC=0 CC=gcc CXX=g++ ./scripts/toolchains/build_mm2plus_linux.sh
    ;;
  fatjar)
    install_temurin_jdk21
    java -version
    javac -version
    ./gradlew -PrequireBundledWebUI=true shadowJar -x buildWebUI -x copyWebUI --no-daemon
    ./gradlew --stop || true
    ;;
  *) echo "Unknown manylinux target: ${HICT_CONTAINER_TARGET}" >&2; exit 1 ;;
esac

chown -R "${HOST_UID}:${HOST_GID}" build toolchains-dist .cache .gradle src/main/resources/webui 2>/dev/null || true
MANYLINUX
}

run_alpine_musl() {
  local alpine_target="$1"
  docker run "${common_docker_args[@]}" \
    -e HICT_CONTAINER_TARGET="${alpine_target}" \
    -e HICT_STATIC_MUSL="1" \
    -e HICT_STATIC_MIMALLOC="1" \
    alpine:3.20 \
    sh -euxc '
      apk add --no-cache \
        bash binutils bzip2-dev bzip2-static ca-certificates clang cmake coreutils curl file \
        g++ gcc git libc-dev linux-headers lld make musl-dev ninja openssl-dev openssl-libs-static \
        m4 patch perl perl-utils py3-pip py3-virtualenv python3 tar unzip xz xz-dev zip zlib-dev zlib-static
      exec bash -s
    ' <<'ALPINE'
set -euo pipefail

export HICT_STATIC_MUSL=1
export HICT_STATIC_MIMALLOC=1
export CC=clang
export CXX=clang++
export COMPILER=clang
export HICTK_CONAN_BUILD_POLICY="${HICTK_CONAN_BUILD_POLICY:-*}"
export PATH="/usr/local/bin:${PATH}"

python3 --version
clang --version | head -n 1
clang++ --version | head -n 1

# Alpine's Clang must use the Alpine target triple to discover crtbeginS.o/libgcc.
echo 'int main(void) { return 0; }' > /tmp/hict-musl-smoke.c
clang --target=x86_64-alpine-linux-musl -static /tmp/hict-musl-smoke.c -o /tmp/hict-musl-smoke
/tmp/hict-musl-smoke

chmod +x \
  scripts/toolchains/build_hictk_linux.sh \
  scripts/toolchains/build_minimap2_linux.sh \
  scripts/toolchains/build_mm2plus_linux.sh

case "${HICT_CONTAINER_TARGET}" in
  hictk)
    python3 -m venv /tmp/hict-conan-probe
    /tmp/hict-conan-probe/bin/python -m pip install --upgrade pip 'conan>=2' >/dev/null
    /tmp/hict-conan-probe/bin/conan remove 'm4/*' -c || true
    HICT_STATIC_MUSL=1 HICT_STATIC_MIMALLOC=1 ENABLE_MOSTLY_STATIC_RUNTIME=1 ./scripts/toolchains/build_hictk_linux.sh
    ;;
  minimap2)
    HICT_STATIC_MUSL=1 HICT_STATIC_MIMALLOC=1 ./scripts/toolchains/build_minimap2_linux.sh
    ;;
  mm2plus)
    HICT_REQUIRE_MM2PLUS_VARIANTS=1 HICT_SKIP_MM2PLUS_AVX512=1 HICT_STATIC_MUSL=1 HICT_STATIC_MIMALLOC=1 CC=clang CXX=clang++ ./scripts/toolchains/build_mm2plus_linux.sh
    ;;
  *) echo "Unknown Alpine musl target: ${HICT_CONTAINER_TARGET}" >&2; exit 1 ;;
esac

for binary in toolchains-dist/linux_x86_64/bin/hictk toolchains-dist/linux_x86_64/bin/minimap2 toolchains-dist/linux_x86_64/bin/mm2plus-avx2 toolchains-dist/linux_x86_64/bin/mm2plus-avx512; do
  if [[ -x "${binary}" ]]; then
    if readelf -d "${binary}" 2>/dev/null | grep -q 'NEEDED'; then
      echo "::warning::${binary} is dynamically linked in musl-static context; artifact remains available for inspection."
      continue
    fi
    if objdump -T "${binary}" 2>/dev/null | grep -q 'GLIBC_'; then
      echo "::warning::${binary} references GLIBC_ symbols in musl-static context; artifact remains available for inspection."
      continue
    fi
  fi
done

chown -R "${HOST_UID}:${HOST_GID}" toolchains-dist .cache 2>/dev/null || true
ALPINE
}

case "${target}" in
  hict-native|fatjar)
    run_manylinux "${target}"
    ;;
  hictk|minimap2|mm2plus)
    if [[ "${abi_mode}" == "musl-static" ]]; then
      run_alpine_musl "${target}"
    else
      run_manylinux "${target}"
    fi
    ;;
esac
