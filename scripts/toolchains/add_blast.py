"""Pinned local BLAST+ for the Linux portable payload (glibc 2.17 source build).

Other platforms keep SeqKit/ModDotPlot and can select an externally installed
BLAST via HICT_BLASTN_BIN. Do not mislabel NCBI's glibc 2.28 binaries as manylinux.
"""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import urllib.request

VERSION = '2.16.0'
SOURCE_SHA256 = '17c93cf009721023e5aecf5753f9c6a255d157561638b91b3ad7276fd6950c2b'
SOURCE_URL = 'https://ftp.ncbi.nlm.nih.gov/blast/executables/blast+/2.16.0/ncbi-blast-2.16.0+-src.tar.gz'
ROOT = Path(__file__).resolve().parents[2]


def add_blast(payload, work, manylinux):
    if sys.platform != 'linux' or os.uname().machine != 'x86_64':
        return None
    archive = work / 'blast-source.tar.gz'
    with urllib.request.urlopen(SOURCE_URL, timeout=60) as response, archive.open('xb') as output:
        shutil.copyfileobj(response, output)
    with archive.open('rb') as stream:
        if hashlib.file_digest(stream, 'sha256').hexdigest() != SOURCE_SHA256:
            raise RuntimeError('BLAST source checksum differs from pinned release')
    source_dir = work / 'blast-source'; source_dir.mkdir()
    with tarfile.open(archive) as source:
        source.extractall(source_dir, filter='data')
    source = source_dir / 'ncbi-blast-2.16.0+-src'
    build = work / 'blast-build'
    subprocess.run(['bash', str(ROOT / 'scripts/toolchains/build_blast_linux.sh'), str(source), str(build)], check=True)
    target = payload / 'bin/blastn'; shutil.copy2(build / 'bin/blastn', target)
    if (build / 'lib/blast').exists(): shutil.copytree(build / 'lib/blast', payload / 'lib/blast')
    notices = payload / 'share/licenses/sequence-analysis/blast'; shutil.copytree(ROOT / 'third_party/blast', notices)
    # Retain the upstream public-domain notice and every third-party notice
    # supplied in the source distribution, including embedded license headers.
    for file in (source / 'c++').rglob('*'):
        if not file.is_file() or 'HiCTBlast' in file.parts:
            continue
        relative = file.relative_to(source / 'c++')
        if any(word in file.name.lower() for word in ('license','licence','copying','copyright','notice','authors')):
            dest = notices / relative; dest.parent.mkdir(parents=True, exist_ok=True); shutil.copy2(file,dest)
    with (notices / 'embedded-notices.txt').open('w') as out:
        for directory in ['src/connect/mbedtls', 'src/util/regexp', 'src/util/compress', 'include/util/bitset', 'include/misc/jsonwrapp']:
            for file in (source / 'c++' / directory).rglob('*'):
                if not file.is_file() or file.suffix.lower() not in ('.c','.h','.hpp','.cpp','.txt','.generic'): continue
                prefix = '\n'.join(file.read_text(errors='replace').splitlines()[:160])
                if any(word in prefix.lower() for word in ('redistribution', 'permission', 'licence', 'license')):
                    out.write('\nSOURCE: '+str(file.relative_to(source / 'c++'))+'\n'+prefix+'\n')
    docs = payload / 'share/doc/sequence-analysis'
    result = smoke(target, work / 'blast-smoke')
    result.update(version=VERSION, source=SOURCE_URL, sourceSha256=SOURCE_SHA256,
                  linuxAbi='manylinux2014' if manylinux else 'host-development',
                  packaging='Linux x86_64 only; other platforms require HICT_BLASTN_BIN')
    (docs / 'blast-smoke.json').write_text(json.dumps(result,indent=2))
    return 'bin/blastn'


def smoke(executable, work):
    """A real insertion-containing alignment, not only --version."""
    import random
    work.mkdir(parents=True)
    rng=random.Random(17); query=''.join(rng.choice('ACGT') for _ in range(220))
    subject=query[:100]+'AAAAA'+query[100:]
    (work/'q.fa').write_text('>q\n'+query+'\n'); (work/'s.fa').write_text('>s\n'+subject+'\n')
    env=dict(os.environ,BLAST_USAGE_REPORT='false')
    result=subprocess.run([str(executable),'-query',str(work/'q.fa'),'-subject',str(work/'s.fa'),'-task','blastn',
      '-dust','no','-outfmt','6 qseqid sseqid pident length gapopen'],env=env,check=True,capture_output=True,text=True,timeout=60)
    rows=[r.split('\t') for r in result.stdout.splitlines()]
    if not any(int(r[3])>=220 and int(r[4])>0 for r in rows): raise RuntimeError('BLAST gapped alignment smoke failed')
    version=subprocess.check_output([str(executable),'-version'],text=True)
    return dict(passed=True,versionOutput=version,alignments=rows)
