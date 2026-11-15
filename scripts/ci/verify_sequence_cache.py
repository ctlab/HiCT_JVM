#!/usr/bin/env python3
"""Fail closed on incomplete or modified cached sequence payloads."""
import argparse
import hashlib
import json
from pathlib import Path


def verify(root, component='all'):
    root = root.resolve()
    manifest = json.loads((root / 'manifest.json').read_text())
    files = manifest['files']
    if not files or len(files) != len(set(files)):
        raise ValueError('Empty or duplicate payload inventory')
    if set(files) != set(manifest['sha256']):
        raise ValueError('Checksum inventory differs')
    actual = {p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_file()}
    if actual != set(files) | {'manifest.json'}:
        raise ValueError('Missing or unlisted payload files')
    for name in files:
        path = root / name
        if Path(name).is_absolute() or '..' in Path(name).parts or not path.resolve().is_relative_to(root):
            raise ValueError('Unsafe payload path')
        with path.open('rb') as stream:
            digest = hashlib.file_digest(stream, 'sha256').hexdigest()
        if digest != manifest['sha256'][name]:
            raise ValueError('Checksum mismatch: ' + name)
    required = ('blastn',) if component == 'blast' else ('seqkit', 'moddotplot', 'hict-moddotplot')
    if component == 'all':
        required += ('blastn',)
    for name in required:
        if manifest['commands'].get(name) not in files:
            raise ValueError('Missing command: ' + name)
    print('Verified', len(files), 'sequence payload files')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    parser.add_argument('--component', choices=('all', 'sequence', 'blast'), default='all')
    args = parser.parse_args()
    verify(args.directory, args.component)
