#!/usr/bin/env python3
"""Build all tools for this host, or reuse a source-keyed, checksummed payload.

Linux builds use manylinux2014 Docker. macOS/Windows use the same native build
scripts as CI. Nothing is published until every required command is present;
previous payloads and interrupted builds are retained in .cache/bundled-tools.
--seal is for CI after its platform jobs have built and merged their payloads.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import tempfile
import time
from merge_sequence_payload import digest, merge

ROOT = Path(__file__).resolve().parents[2]
IMAGE = 'quay.io/pypa/manylinux2014_x86_64@sha256:f6a6f153c9cf45470507ebbc27013e5db14ce21331b62fd2c540561f25e5f0a5'
REFS = {'HICTK_REF': 'v2.2.0', 'MINIMAP2_REF': 'v2.31', 'MM2PLUS_REF': 'v1.2'}


def host_platform():
    arch = {'amd64': 'x86_64', 'aarch64': 'arm64'}.get(platform.machine().lower(), platform.machine().lower())
    name = {'Linux': 'linux', 'Darwin': 'darwin', 'Windows': 'windows'}.get(platform.system())
    result = f'{name}_{arch}'
    if result not in ('linux_x86_64', 'windows_x86_64', 'darwin_arm64', 'darwin_x86_64'):
        raise RuntimeError('Bundled tools are not supported on ' + result)
    return result


def source_key(root=ROOT):
    files = sorted(p for folder in ('scripts/toolchains', 'scripts/sequence_analysis', 'third_party')
                   for p in (root / folder).rglob('*') if p.is_file() and p.suffix != '.pyc' and '__pycache__' not in p.parts)
    values = {str(p.relative_to(root)): digest(p) for p in files}
    values['linux_container_build'] = digest(root / 'scripts/ci/linux_container_build.sh')
    values['centos_mirrors'] = digest(root / 'scripts/ci/configure_centos_mirrors.py')
    values['refs'] = {k: os.environ.get(k, v) for k, v in REFS.items()}
    values['abi'] = os.environ.get('HICT_LINUX_ABI_MODE', 'glibc217')
    return hashlib.sha256(json.dumps(values, sort_keys=True).encode()).hexdigest()


def required_commands(name):
    commands = {'hictk', 'minimap2', 'mm2plus_avx2', 'seqkit', 'hict-moddotplot'}
    if not name.endswith('arm64'):
        commands.add('mm2plus_avx512')
    if name == 'linux_x86_64':
        commands.add('blastn')
    return commands


def verify(target, name):
    manifest = json.loads((target / 'manifest.json').read_text(encoding='utf-8'))
    missing = required_commands(name) - manifest.get('commands', {}).keys()
    if missing:
        raise ValueError('Missing bundled commands: ' + ', '.join(sorted(missing)))
    for entry in manifest['files']:
        path = target / entry
        if Path(entry).is_absolute() or '..' in Path(entry).parts or not path.resolve().is_relative_to(target.resolve()):
            raise ValueError('Unsafe tool path: ' + entry)
        expected = manifest.get('sha256', {}).get(entry)
        if not expected or digest(path) != expected:
            raise ValueError('Missing checksum or corrupt tool file: ' + entry)
    for command in required_commands(name):
        entry = manifest['commands'][command]
        if entry not in manifest['files']:
            raise ValueError('Command is not checksummed: ' + command)
        if os.name != 'nt' and not os.access(target / entry, os.X_OK):
            raise ValueError('Command is not executable: ' + command)
    return manifest


def seal(target, name):
    verify(target, name)
    value = dict(platform=name, sourceKey=source_key(), manifestSha256=digest(target / 'manifest.json'))
    temporary = target / 'prepared-tools.json.tmp'
    temporary.write_text(json.dumps(value, indent=2), encoding='utf-8')
    temporary.replace(target / 'prepared-tools.json')
    print('Verified all bundled commands and payload hashes:', target, flush=True)


def cached(target, name):
    try:
        state = json.loads((target / 'prepared-tools.json').read_text(encoding='utf-8'))
        if state != dict(platform=name, sourceKey=source_key(), manifestSha256=digest(target / 'manifest.json')):
            return False
        verify(target, name)
        return True
    except (ValueError, OSError, KeyError):
        return False


def run(args, **kwargs):
    print('+', ' '.join(str(a) for a in args), flush=True)
    subprocess.run([str(a) for a in args], check=True, **kwargs)


def build(name, target):
    cache = ROOT / '.cache/bundled-tools'
    cache.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix='build-', dir=cache))
    for folder in ('scripts', 'third_party'):
        shutil.copytree(ROOT / folder, stage / folder, ignore=shutil.ignore_patterns('__pycache__'))
    env = dict(os.environ, **{k: os.environ.get(k, v) for k, v in REFS.items()})
    env.pop('GITHUB_WORKSPACE', None)
    env['PYTHONUTF8'] = '1'
    env['HICT_REQUIRE_MM2PLUS_VARIANTS'] = '1'
    output = stage / 'toolchains-dist' / name
    sequence = stage / 'sequence-tools-dist' / name
    if name == 'linux_x86_64':
        if not shutil.which('docker'):
            raise RuntimeError('Linux native tools require Docker (manylinux2014). Install Docker and allow your user to run it, then retry ./gradlew natives.')
        for tool in ('hictk', 'minimap2', 'mm2plus'):
            run(['bash', 'scripts/ci/linux_container_build.sh', tool], cwd=stage, env=env)
        # Download the exact Go release inside the container; no host compiler or
        # interpreter leaks into the portable ABI. Go's official checksum pins it.
        run(['docker', 'run', '--rm', '-v', f'{stage}:{stage}', '-w', stage, IMAGE,
             'bash', '-euxc', '''export PATH=/opt/python/cp312-cp312/bin:$PATH
python3 - <<'GO'
import hashlib,json,urllib.request,tarfile
version='go1.25.5'
with urllib.request.urlopen('https://go.dev/dl/?mode=json&include=all') as r: releases=json.load(r)
item=next(f for v in releases if v['version']==version for f in v['files'] if f['os']=='linux' and f['arch']=='amd64' and f['kind']=='archive')
p='/tmp/hict-go.tar.gz'; urllib.request.urlretrieve('https://go.dev/dl/'+item['filename'],p)
with open(p,'rb') as f: assert hashlib.file_digest(f,'sha256').hexdigest()==item['sha256']
with tarfile.open(p) as t: t.extractall('/tmp/hict-go', filter='data')
GO
export PATH=/tmp/hict-go/go/bin:$PATH
bash scripts/toolchains/build_sequence_python_linux.sh /tmp/hict-shared-python
/tmp/hict-shared-python/bin/python3.12 scripts/toolchains/build_sequence_tools.py --manylinux2014 --output sequence-tools-dist/linux_x86_64
'''], cwd=stage, env=env)
    else:
        env['OUTPUT_DIR'] = str(output)
        if name.startswith('windows'):
            for tool in ('hictk', 'minimap2', 'mm2plus'):
                env['WORK_DIR'] = str(stage / ('work-' + tool))
                run(['powershell', '-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', stage / f'scripts/toolchains/build_{tool}_windows.ps1'], cwd=stage, env=env)
        else:
            env.update(HICT_DARWIN_ARCH=name.split('_', 1)[1], HICT_DARWIN_PLATFORM_DIR=name, WORK_DIR=str(stage / 'work-tools'))
            run(['bash', stage / 'scripts/toolchains/build_darwin_toolchains.sh'], cwd=stage, env=env)
        run([sys.executable, stage / 'scripts/toolchains/build_sequence_tools.py', '--output', sequence], cwd=stage, env=env)
    merge(sequence, output)
    seal(output, name)
    target.parent.mkdir(parents=True, exist_ok=True)
    backup = cache / ('previous-' + name + '-' + str(time.time_ns()))
    if target.exists():
        target.rename(backup)
    try:
        output.rename(target)
    except BaseException:
        if backup.exists():
            backup.rename(target)
        raise
    print('Complete bundled toolchain:', target, flush=True)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--seal', action='store_true', help='Verify a payload just built by CI and record its source key.')
    p.add_argument('--verify', action='store_true', help='Verify all commands and hashes without building or adopting a cache.')
    p.add_argument('--platform', default=host_platform())
    p.add_argument('--target', type=Path)
    args = p.parse_args()
    target = (args.target or ROOT / 'toolchains-dist' / args.platform).resolve()
    if args.seal:
        seal(target, args.platform)
    elif args.verify:
        verify(target, args.platform)
    elif cached(target, args.platform):
        print('Reusing source-matched, verified bundled tools:', target)
    else:
        build(args.platform, target)


if __name__ == '__main__':
    main()
