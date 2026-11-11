#!/usr/bin/env bash
# Run inside manylinux2014_x86_64. Source archive must be the checksum-pinned release.
set -euo pipefail
source_root=$1
output_root=$2
mkdir -p "$output_root/bin"
sqlite_options=()
if [[ -d /opt/_internal/sqlite3 ]]; then sqlite_options=(--with-sqlite3=/opt/_internal/sqlite3); fi
cd "$source_root/c++"
CXXFLAGS="${CXXFLAGS:-} -fcoroutines" bash ./configure --with-build-root=HiCTBlast --without-debug --without-sse42 \
  --with-static --without-vdb --without-ngs --without-gnutls --without-openssl \
  --without-zstd --without-bz2 --without-z --without-pcre \
  --without-lzo --without-gmp --without-gcrypt --without-nettle --without-lmdb \
  --without-libdw --without-backward-cpp --without-krb5 --without-python \
  --without-perl --without-sybase --without-mysql --without-bdb --without-caution "${sqlite_options[@]}"
make -C HiCTBlast/build -j "${HICT_BUILD_JOBS:-4}" all_r
cp HiCTBlast/bin/blastn "$output_root/bin/blastn"
# manylinux supplies SQLite outside the default search path. Carry its runtime
# and OpenMP alongside blastn; keep the baseline system C/C++ runtimes external.
mkdir -p "$output_root/lib/blast"
ldd "$output_root/bin/blastn" | awk '/libsqlite3\.so|libgomp\.so/ {print $3}' | while IFS= read -r library; do
  [[ -f "$library" ]] || { echo "Missing BLAST runtime: $library" >&2; exit 1; }
  cp -L "$library" "$output_root/lib/blast/$(basename "$library")"
done
if compgen -G "$output_root/lib/blast/*" >/dev/null; then
  patchelf --set-rpath '$ORIGIN/../lib/blast' "$output_root/bin/blastn"
fi
"$output_root/bin/blastn" -version
