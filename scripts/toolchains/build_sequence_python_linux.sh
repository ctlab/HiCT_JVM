#!/usr/bin/env bash
# manylinux's wheel-building Python is static. Use a pinned shared CPython whose
# x86_64 GNU baseline is glibc 2.17; native adapter builds/tests still run here.
set -euo pipefail
prefix="${1:?Provide a new installation prefix}"
if [[ -e "$prefix" ]]; then
  grep -q 'sha256:1dbaa624a09e15afe7efbdac08d42993135a68db8d34f986ef6977a6d77bdc3c' "$prefix/hict-runtime-source.txt"
  "$prefix/bin/python3.12" -c 'import ssl,sqlite3,ctypes,bz2,lzma'
  exit 0
fi
export PATH="/opt/python/cp312-cp312/bin:$PATH"
python3 - "$prefix" <<'PYTHON'
import hashlib,pathlib,sys,tarfile,urllib.request,tempfile,shutil
root=pathlib.Path(tempfile.mkdtemp(prefix='hict-shared-runtime-'));archive=root/'python.tar.gz'
url='https://github.com/astral-sh/python-build-standalone/releases/download/20260211/cpython-3.12.12%2B20260211-x86_64-unknown-linux-gnu-install_only_stripped.tar.gz'
with urllib.request.urlopen(url,timeout=120) as src,archive.open('wb') as out:shutil.copyfileobj(src,out)
expected='1dbaa624a09e15afe7efbdac08d42993135a68db8d34f986ef6977a6d77bdc3c'
assert hashlib.sha256(archive.read_bytes()).hexdigest()==expected,'Shared Python checksum mismatch'
with tarfile.open(archive) as tar:tar.extractall(root,filter='data')
(root/'python').rename(sys.argv[1])
(pathlib.Path(sys.argv[1])/'hict-runtime-source.txt').write_text(url+'\nsha256:'+expected+'\n')
PYTHON
"$prefix/bin/python3.12" -c 'import ssl,sqlite3,ctypes,bz2,lzma;print(ssl.OPENSSL_VERSION, sqlite3.sqlite_version)'
