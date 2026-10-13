#!/usr/bin/env python3
"""Verify and merge an independently built sequence payload into toolchains.

Content-addressed subdirectories avoid clobbering tools from a previous build.
Existing command, citation and licence entries are preserved.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shutil


def digest(path):
    # Portable packaging jobs use the runner's Python (Ubuntu 22.04 has 3.10),
    # independently of the Python 3.12 used to build the frozen native adapter.
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(8 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def merge(source, target):
    manifest_bytes = (source / 'manifest.json').read_bytes()
    extra = json.loads(manifest_bytes)
    prefix = 'sequence/' + hashlib.sha256(manifest_bytes).hexdigest()[:20]
    for name in extra['files']:
        path = source / name
        if Path(name).is_absolute() or '..' in Path(name).parts or not path.resolve().is_relative_to(source.resolve()):
            raise ValueError('Unsafe payload file path: ' + name)
        if digest(path) != extra['sha256'][name]:
            raise ValueError('Payload checksum mismatch: ' + name)
    for command in extra['commands'].values():
        if command not in extra['files']:
            raise ValueError('Command missing from checked payload: ' + command)
    target.mkdir(parents=True, exist_ok=True)
    path = target / 'manifest.json'
    base = json.loads(path.read_text()) if path.exists() else dict(commands={}, files=[], notices=[], citations=[])
    for name in extra['files']:
        dest = target / prefix / name
        if dest.exists() and digest(dest) != extra['sha256'][name]:
            raise ValueError('Existing content-addressed file differs: ' + str(dest))
    for name in extra['files']:
        dest = target / prefix / name
        if not dest.exists():
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source / name, dest)
    for key, value in extra['commands'].items():
        base.setdefault('commands', {})[key] = prefix + '/' + value
        # ZIP/JAR extraction may lose mode bits. Mark commands explicitly.
        command = target / prefix / value
        command.chmod(command.stat().st_mode | 0o111)
    for key in ('notices', 'citations'):
        base[key] = list(dict.fromkeys(base.get(key, []) + extra.get(key, [])))
    # Retain old files on disk for rollback, but package only the active sequence
    # payload. Otherwise every cache refresh silently grows the release JAR.
    base['files'] = list(dict.fromkeys([p for p in base.get('files', []) if not p.startswith('sequence/')] + [prefix + '/' + p for p in extra['files']]))
    # Pin existing aligners too: their release label can survive an ABI rebuild.
    # The runtime extraction cache includes this manifest's content digest.
    for name in base['files']:
        if Path(name).is_absolute() or '..' in Path(name).parts:
            raise ValueError('Unsafe existing payload file path: ' + name)
    base['sha256'] = {name: digest(target / name) for name in base['files']}
    base['sequencePayloadSha256'] = hashlib.sha256(manifest_bytes).hexdigest()
    temporary = path.with_suffix('.json.tmp')
    temporary.write_text(json.dumps(base, indent=2), encoding='utf-8')
    temporary.replace(path)
    print('Verified and merged', len(extra['files']), 'sequence payload files into', target)


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source', required=True, type=Path)
    p.add_argument('--target', required=True, type=Path)
    a = p.parse_args()
    merge(a.source, a.target)
