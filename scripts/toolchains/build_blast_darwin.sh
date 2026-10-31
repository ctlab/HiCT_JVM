#!/usr/bin/env bash
# NCBI's prebuilt BLAST 2.16.0 requires macOS 14. Build the pinned source for
# HiCT's deployment target instead. No Homebrew runtime libraries are needed.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../portable/macos_target.sh"
source_root="$(cd "$1" && pwd)"
mkdir -p "$2"
output_root="$(cd "$2" && pwd)"
arch="${HICT_DARWIN_ARCH:-$(uname -m)}"
case "$arch" in
  arm64|x86_64) ;;
  *) echo "Unsupported BLAST macOS architecture: $arch" >&2; exit 1 ;;
esac
jobs="${HICT_BUILD_JOBS:-$(sysctl -n hw.ncpu)}"
[[ "$jobs" =~ ^[1-9][0-9]*$ ]] || { echo 'HICT_BUILD_JOBS must be positive' >&2; exit 1; }
export SDKROOT="$(xcrun --sdk macosx --show-sdk-path)"
export CC="$(xcrun --find clang)" CXX="$(xcrun --find clang++)"
target_flags="-arch $arch -mmacosx-version-min=$MACOSX_DEPLOYMENT_TARGET -isysroot $SDKROOT"
# NCBI builds its bundled zlib without running zlib's configure. Darwin has
# unistd.h, but does not set Linux's _LARGEFILE64_SOURCE fallback that includes
# it in zconf.h. Supply zlib's supported feature setting for every C unit.
export CFLAGS="${CFLAGS:-} $target_flags -DHAVE_UNISTD_H=1"
export CXXFLAGS="${CXXFLAGS:-} $target_flags"
export LDFLAGS="${LDFLAGS:-} $target_flags"
echo "Building BLAST for macOS $MACOSX_DEPLOYMENT_TARGET ($arch), jobs=$jobs"
cd "$source_root/c++"
python3 "$SCRIPT_DIR/patch_blast_source.py" "$source_root/c++" \
  --report "$output_root/share/doc/blast-source-patches.json"
# Check every bundled zlib C unit, including gzip read/write/seek code, against
# this runner's SDK before the full build. Force the newer Apple SDK macro so
# older SDKs exercise that guard too. CFLAGS uses compiler word splitting.
echo 'Checking all bundled zlib C sources with strict POSIX declaration checks'
"$CC" -std=gnu18 $CFLAGS -DTARGET_OS_MAC=1 -Werror=implicit-function-declaration \
  -I"$source_root/c++/include/util/compress/zlib" -fsyntax-only \
  "$source_root"/c++/src/util/compress/zlib/*.c
# NCBI recommends --without-openmp for Apple Clang. BLAST's own -num_threads
# remains available via --with-mt; disabling OpenMP does not serialize searches.
# SQLite is required by BLAST. Use Apple's system library through the SDK,
# not a Homebrew installation that may require a newer macOS release.
bash ./configure --with-build-root=HiCTBlast --without-debug --without-sse42 \
  --with-bin-release --with-mt --without-openmp --with-static --without-dll \
  --without-boost --without-icu --without-expat --without-libxml --without-libxslt \
  --without-libexslt --without-curl \
  --without-vdb --without-ngs --without-gnutls --without-openssl \
  --without-zstd --without-bz2 --without-z --without-pcre \
  --without-lzo --without-gmp --without-gcrypt --without-nettle --without-lmdb \
  --without-libdw --without-backward-cpp --without-krb5 --without-python \
  --without-perl --without-sybase --without-mysql --without-bdb --without-caution \
  --with-sqlite3=/usr
make -C HiCTBlast/build -j "$jobs" all_r
[[ -x HiCTBlast/bin/blastn ]] || { echo 'NCBI build did not produce blastn; check unmet requirements in the build log' >&2; exit 1; }
mkdir -p "$output_root/bin" "$output_root/share/doc"
install -m 0755 HiCTBlast/bin/blastn "$output_root/bin/blastn"
codesign --force --sign - "$output_root/bin/blastn"
"$output_root/bin/blastn" -version
python3 "$SCRIPT_DIR/../ci/check_macos_compatibility.py" "$output_root" \
  --target "$MACOSX_DEPLOYMENT_TARGET" --arch "$arch" \
  --report "$output_root/share/doc/blast-macos-compatibility.json"
