#!/usr/bin/env python3
"""Build pinned native tools in a fresh directory; publish only after smoke tests.

Use Python 3.12 and Go 1.25.5. Linux release builds MUST run in manylinux2014;
host builds are explicitly marked as development builds. No existing payload or
source file is overwritten. Dependencies and their licence texts are inventoried.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]
MOD = '1616ae94f5b7e22190a8eb50b6df2e3798a18d0b'
SEQ = 'd13b5fa388cc869de05abe1bdb07980eef5efb4e'


def run(args, **kwargs):
    print('+', ' '.join(map(str, args)), flush=True)
    return subprocess.run(list(map(str, args)), check=True, **kwargs)


def digest(path):
    with open(path, 'rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def checkout(url, commit, target, local=None):
    run(['git', 'clone', '--no-checkout', local or url, target])
    run(['git', 'checkout', '--detach', commit], cwd=target)
    actual = run(['git', 'rev-parse', 'HEAD'], cwd=target, capture_output=True, text=True).stdout.strip()
    if actual != commit:
        raise RuntimeError('Unexpected source revision')


def json_sequence(text):
    decoder = json.JSONDecoder()
    while text.strip():
        text = text.lstrip()
        item, end = decoder.raw_decode(text)
        yield item
        text = text[end:]


def copy_go_notices(source, dest, env):
    run(['go', 'mod', 'download'], cwd=source, env=env)
    # The module graph also contains unused/test-only modules with no extracted
    # directory. Inventory the modules that actually supply linked packages.
    packages = json_sequence(run(['go', 'list', '-deps', '-json', './seqkit'], cwd=source,
                                 env=env, capture_output=True, text=True).stdout)
    modules = {p['Module']['Path']: p['Module'] for p in packages if 'Module' in p}.values()
    records = []
    for module in modules:
        if module.get('Main'):
            continue
        info = module.get('Replace', module)
        folder = Path(info['Dir'])
        notices = [p for p in folder.rglob('*') if p.is_file() and
                   any(term in part.lower() for part in p.relative_to(folder).parts
                       for term in ('license', 'licence', 'copying', 'notice', 'copyright', 'authors'))]
        if not any(any(term in p.name.lower() for term in ('license', 'licence', 'copying')) for p in notices):
            raise RuntimeError('Missing dependency licence: ' + info['Path'])
        target = dest / (info['Path'].replace('/', '_') + '@' + info['Version'])
        for path in notices:
            out = target / path.relative_to(folder)
            out.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, out)
        records.append({k: info.get(k) for k in ('Path', 'Version', 'Sum', 'GoModSum')})
    return records


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--work', type=Path)
    parser.add_argument('--seqkit-source')
    parser.add_argument('--moddotplot-source')
    parser.add_argument('--manylinux2014', action='store_true')
    args = parser.parse_args()
    if sys.version_info[:2] != (3, 12):
        parser.error('Build with Python 3.12')
    if args.output.exists():
        parser.error('Use a new output directory; existing artifacts are preserved')
    if args.manylinux2014 and not Path('/etc/os-release').read_text().find('CentOS') >= 0:
        # manylinux2014 currently uses CentOS 7; fail rather than mislabel Ubuntu.
        parser.error('--manylinux2014 requires the manylinux2014 container')
    if args.manylinux2014:
        # The standalone interpreter was built with clang; compile extensions
        # with the compiler supplied by manylinux, not that build-host setting.
        os.environ['CC'] = 'gcc'
        os.environ['CXX'] = 'g++'
    work = args.work or Path(tempfile.mkdtemp(prefix='hict-sequence-build-'))
    work = work.resolve()
    if args.work:
        work.mkdir(parents=True, exist_ok=False)
    payload = work / 'payload'
    (payload / 'bin').mkdir(parents=True)
    docs = payload / 'share/doc/sequence-analysis'
    docs.mkdir(parents=True)
    notices = payload / 'share/licenses/sequence-analysis'
    notices.mkdir(parents=True)
    seq, mod = work / 'seqkit', work / 'moddotplot'
    checkout('https://github.com/shenwei356/seqkit.git', SEQ, seq, args.seqkit_source)
    checkout('https://github.com/marbl/ModDotPlot.git', MOD, mod, args.moddotplot_source)
    goenv = dict(os.environ, CGO_ENABLED='0', GOAMD64='v1', GOTOOLCHAIN='go1.25.5')
    go_version = run(['go', 'version'], env=goenv, capture_output=True, text=True).stdout.strip()
    suffix = '.exe' if os.name == 'nt' else ''
    seqbin = payload / ('bin/seqkit' + suffix)
    run(['go', 'build', '-trimpath', '-buildvcs=true', '-o', seqbin, './seqkit'], cwd=seq, env=goenv)
    modules = copy_go_notices(seq, notices / 'go-modules', goenv)
    goroot = Path(run(['go', 'env', 'GOROOT'], env=goenv, capture_output=True, text=True).stdout.strip())
    for name in ('LICENSE', 'PATENTS'):
        if (goroot / name).is_file():
            shutil.copy2(goroot / name, notices / ('Go-' + name))
    (notices / 'FreeType-attribution.txt').write_text(
        'Freetype-Go is redistributed under the FreeType License (FTL) option. '
        'Portions of this software are copyright The FreeType Project (www.freetype.org). All rights reserved. '
        'See go-modules/github.com_golang_freetype*/licenses/ftl.txt.\n')
    for name in ('seqkit', 'moddotplot'):
        shutil.copytree(ROOT / 'third_party' / name, notices / name)
    run([sys.executable, '-m', 'venv', work / 'venv'])
    py = work / ('venv/Scripts/python.exe' if os.name == 'nt' else 'venv/bin/python')
    # pip records archive hashes and exact origins for every resolved wheel.
    run([py, '-m', 'pip', 'install', '--report', docs / 'python-install.json',
         '-r', ROOT / 'scripts/sequence_analysis/requirements-build.txt'])
    run([py, '-m', 'pip', 'install', '--no-deps', '--no-build-isolation',
         '--report', docs / 'moddotplot-install.json', mod])
    # Capture installed distribution notices, including bundled native libraries.
    collect = r'''
import importlib.metadata as im,json,pathlib,shutil,sys
out=pathlib.Path(sys.argv[1]); records=[]
for dist in im.distributions():
 name=dist.metadata['Name']; copied=[]
 for f in dist.files or []:
  if any(part.lower().startswith(('license','licence','copying','notice','copyright','authors')) for part in f.parts):
   src=pathlib.Path(dist.locate_file(f))
   if src.is_file():
    # Dist metadata paths cannot escape their own notice directory.
    target=out/name/pathlib.Path(*[p for p in f.parts if p not in ('.','..')]);target.parent.mkdir(parents=True,exist_ok=True)
    shutil.copy2(src,target);copied.append(str(target.relative_to(out)))
 if not copied: raise SystemExit('No licence notice found for '+name)
 records.append(dict(name=name,version=dist.version,license=dist.metadata.get('License-Expression',dist.metadata.get('License')),notices=copied))
(out/'inventory.json').write_text(json.dumps(records,indent=2))
'''
    run([py, '-c', collect, notices / 'python'])
    # CPython itself is embedded by PyInstaller; include its complete PSF notices.
    runtime_source = Path(sys.base_prefix) / 'hict-runtime-source.txt'
    if runtime_source.is_file():
        shutil.copy2(runtime_source, docs / 'shared-python-source.txt')
    license_text = run([py, '-c', 'import sys; print(sys.copyright)'], capture_output=True, text=True).stdout
    (notices / 'CPython-copyright.txt').write_text(license_text)
    candidates = [Path(sys.base_prefix) / p for p in ('LICENSE.txt', 'LICENSE', 'lib/python3.12/LICENSE.txt', 'share/doc/python3.12/LICENSE')]
    cpython_license = next((p for p in candidates if p.is_file()), None)
    if cpython_license:
        shutil.copy2(cpython_license, notices / 'CPython-LICENSE.txt')
    else:
        import urllib.request
        url = f'https://raw.githubusercontent.com/python/cpython/v{platform.python_version()}/LICENSE'
        (notices / 'CPython-LICENSE.txt').write_bytes(urllib.request.urlopen(url, timeout=60).read())
    run([py, '-m', 'PyInstaller', '--noconfirm', '--onedir', '--name', 'hict-moddotplot',
         '--distpath', payload / 'bin', '--workpath', work / 'pyinstaller', '--specpath', work,
         '--hidden-import', 'moddotplot._nthash', '--collect-data', 'moddotplot',
         '--exclude-module', 'matplotlib', '--exclude-module', 'IPython', '--exclude-module', 'pytest',
         ROOT / 'scripts/sequence_analysis/hict_moddotplot.py'])
    adapter = 'bin/hict-moddotplot/hict-moddotplot' + suffix
    run([py, ROOT / 'scripts/sequence_analysis/smoke_tools.py', '--seqkit', seqbin,
         '--adapter', payload / adapter, '--output', docs / 'smoke-test.json'])
    from add_blast import add_blast
    blast_command = add_blast(payload, work, args.manylinux2014)
    if args.manylinux2014:
        abi = {}
        for file in payload.rglob('*'):
            if not file.is_file():
                continue
            with file.open('rb') as stream:
                if stream.read(4) != b'\x7fELF':
                    continue
            info = run(['readelf', '--version-info', file], capture_output=True, text=True).stdout
            versions = {tuple(map(int, v.split('.'))) for v in re.findall(r'GLIBC_(\d+(?:\.\d+)+)', info)}
            maximum = max(versions, default=(0,))
            if maximum > (2, 17):
                raise RuntimeError(f'{file} requires GLIBC {maximum}, newer than manylinux2014')
            abi[str(file.relative_to(payload))] = '.'.join(map(str, maximum))
        (docs / 'glibc-abi.json').write_text(json.dumps(abi, indent=2))
    info = dict(schema='hict-sequence-tools-v1', seqkitCommit=SEQ, moddotplotCommit=MOD,
                python=platform.python_version(), go=go_version, platform=platform.platform(),
                linuxAbi='manylinux2014' if args.manylinux2014 else 'host-development' if sys.platform == 'linux' else None,
                cpuPolicy='baseline; CGO_DISABLED Go runtime and upstream ntHash kernel; no artificial AVX variants',
                goModules=modules)
    (docs / 'build-info.json').write_text(json.dumps(info, indent=2))
    files = sorted(str(p.relative_to(payload)).replace('\\', '/') for p in payload.rglob('*') if p.is_file())
    manifest = dict(platform=sys.platform, commands={'seqkit': 'bin/seqkit' + suffix, 'hict-moddotplot': adapter}, files=files,
                    sha256={p: digest(payload / p) for p in files},
                    notices=['SeqKit (MIT) and ModDotPlot/ntHash (MIT), with third-party notices under share/licenses/sequence-analysis.',
                             'ModDotPlot computational adapter, not the upstream plotting application.'],
                    citations=['Sweeten AP et al. Bioinformatics 2024;40(8):btae493. doi:10.1093/bioinformatics/btae493.',
                               'Shen W et al. iMeta 2024:e191. doi:10.1002/imt2.191.'])
    if blast_command:
        manifest['commands']['blastn'] = blast_command
        manifest['notices'].append('NCBI BLAST+ 2.16.0: public-domain NCBI code with bundled third-party notices; local searches only. Linux x86_64 portable payload.')
        manifest['citations'].append('Camacho C et al. BMC Bioinformatics 2009;10:421. doi:10.1186/1471-2105-10-421.')
    (payload / 'manifest.json').write_text(json.dumps(manifest, indent=2))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(payload, args.output)
    print('Verified sequence payload:', args.output, flush=True)


if __name__ == '__main__':
    main()
