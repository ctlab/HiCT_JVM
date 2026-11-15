#!/usr/bin/env python3
"""Combine independently verified components without overwriting any payload file."""
import json
from pathlib import Path
import shutil
import sys
from verify_sequence_cache import verify


def assemble(target):
    components = [(Path(str(target) + '-' + kind), kind) for kind in ('sequence', 'blast')]
    manifests = []
    for root, kind in components:
        verify(root, kind)
        manifests.append(json.loads((root / 'manifest.json').read_text()))
    if len({m['platform'] for m in manifests}) != 1:
        raise ValueError('Component platforms differ')
    combined = dict(platform=manifests[0]['platform'], commands={}, files=[], sha256={}, notices=[], citations=[])
    for manifest in manifests:
        if set(combined['files']) & set(manifest['files']) or set(combined['commands']) & set(manifest['commands']):
            raise ValueError('Component file or command collision')
        for key in ('commands', 'sha256'):
            combined[key].update(manifest[key])
        for key in ('files', 'notices', 'citations'):
            combined[key].extend(manifest.get(key, []))
    target.mkdir(parents=True, exist_ok=False)
    for (root, _), manifest in zip(components, manifests):
        for name in manifest['files']:
            dest = target / name
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(root / name, dest)
    (target / 'manifest.json').write_text(json.dumps(combined, indent=2))
    verify(target)


if __name__ == '__main__':
    assemble(Path(sys.argv[1]))
