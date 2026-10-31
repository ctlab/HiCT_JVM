#!/usr/bin/env python3
"""Reject bundled Mach-O binaries requiring an OS newer than the release target.

Uses Apple's lipo/otool, including for native libraries nested in JARs and
tarballs. This is a deployment-header check, not a substitute for testing the
same release on the oldest supported macOS.
"""
import argparse
import json
from pathlib import Path
import re
import shutil
import subprocess
import tarfile
import tempfile
import zipfile

MACHO = {bytes.fromhex(s) for s in (
    'feedface', 'cefaedfe', 'feedfacf', 'cffaedfe',
    'cafebabe', 'bebafeca', 'cafebabf', 'bfbafeca')}


def version(value):
    if not re.fullmatch(r'\d+\.\d+(?:\.\d+)?', value):
        raise ValueError(f'Invalid macOS version: {value}')
    parts = tuple(map(int, value.split('.')))
    return parts + (0,) * (3 - len(parts))


def minimum_versions(load_commands):
    versions = []
    for block in re.split(r'Load command \d+', load_commands):
        if re.search(r'\bcmd LC_BUILD_VERSION\b', block):
            # Reject iOS/Catalyst libraries accidentally staged as macOS.
            if not re.search(r'\bplatform (?:1|MACOS|macos)\b', block):
                raise ValueError('Non-macOS LC_BUILD_VERSION')
            match = re.search(r'\bminos (\d+\.\d+(?:\.\d+)?)', block)
        elif re.search(r'\bcmd LC_VERSION_MIN_MACOSX\b', block):
            match = re.search(r'\bversion (\d+\.\d+(?:\.\d+)?)', block)
        else:
            continue
        if match:
            versions.append(match.group(1))
    if not versions:
        raise ValueError('No macOS minimum-version load command')
    return versions


class Auditor:
    def __init__(self, target, arch):
        self.target = version(target)
        self.arch = arch
        self.binaries = []
        self.errors = []

    def inspect_binary(self, path, label):
        try:
            arches = subprocess.check_output(['lipo', '-archs', str(path)], text=True).split()
            if self.arch not in arches:
                return  # Fat JARs can also contain libraries for another CPU.
            commands = subprocess.check_output(
                ['otool', '-arch', self.arch, '-l', str(path)], text=True)
            minima = minimum_versions(commands)
            self.binaries.append(dict(path=label, architecture=self.arch, minimumVersions=minima))
            if any(version(v) > self.target for v in minima):
                self.errors.append(f'{label}: requires macOS {max(minima, key=version)}')
            links = subprocess.check_output(
                ['otool', '-arch', self.arch, '-L', str(path)], text=True)
            for dependency in re.findall(r'^\s+(/\S+) \(', links, re.MULTILINE):
                if not dependency.startswith(('/usr/lib/', '/System/Library/')):
                    self.errors.append(f'{label}: non-system absolute dependency {dependency}')
        except (subprocess.CalledProcessError, ValueError) as error:
            self.errors.append(f'{label}: {error}')

    def scan(self, stream, label, depth=0):
        prefix = stream.read(4)
        # Java class files share the 32-bit universal Mach-O magic.
        native = prefix in MACHO and not label.endswith('.class')
        archive = label.endswith(('.jar', '.zip', '.tar.gz', '.tgz', '.tar.xz'))
        if not native and not archive:
            return
        if depth > 8:
            raise ValueError(f'Archive nesting is too deep: {label}')
        # Never extract archive paths; only one anonymous member lives on disk.
        with tempfile.NamedTemporaryFile() as temporary:
            temporary.write(prefix)
            shutil.copyfileobj(stream, temporary)
            temporary.flush()
            if native:
                self.inspect_binary(Path(temporary.name), label)
            elif label.endswith(('.jar', '.zip')):
                with zipfile.ZipFile(temporary.name) as archive_file:
                    for member in archive_file.infolist():
                        if not member.is_dir():
                            with archive_file.open(member) as child:
                                self.scan(child, f'{label}!{member.filename}', depth + 1)
            else:
                with tarfile.open(temporary.name, mode='r|*') as archive_file:
                    for member in archive_file:
                        if member.isfile():
                            with archive_file.extractfile(member) as child:
                                self.scan(child, f'{label}!{member.name}', depth + 1)

    def check_tree(self, root):
        for path in sorted(root.rglob('*')):
            if path.is_file() and not path.is_symlink():
                with path.open('rb') as stream:
                    self.scan(stream, str(path.relative_to(root)))
        if not self.binaries:
            self.errors.append(f'No {self.arch} Mach-O binaries found')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('--target', default='12.3', type=version)
    parser.add_argument('--arch', required=True, choices=('arm64', 'x86_64'))
    parser.add_argument('--report', required=True, type=Path)
    args = parser.parse_args()
    target = '.'.join(map(str, args.target))
    for tool in ('lipo', 'otool'):
        if not shutil.which(tool):
            parser.error(f'{tool} is required; run this audit on macOS')
    auditor = Auditor(target, args.arch)
    auditor.check_tree(args.root)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(dict(target=target, architecture=args.arch,
        binaries=auditor.binaries, errors=auditor.errors), indent=2) + '\n')
    print(f'macOS {target} audit: {len(auditor.binaries)} binaries; {len(auditor.errors)} errors')
    if auditor.errors:
        raise SystemExit('\n'.join(auditor.errors))


if __name__ == '__main__':
    main()
