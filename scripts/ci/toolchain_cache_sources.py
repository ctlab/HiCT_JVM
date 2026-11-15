#!/usr/bin/env python3
"""Fingerprint upstream refs so a moved tag cannot reuse an older binary cache."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
import re
from pathlib import Path
import subprocess

TOOLS = {'HICTK': ('paulsengroup/hictk', 'v2.2.0'),
         'MINIMAP2': ('lh3/minimap2', 'v2.31'),
         'MM2PLUS': ('at-cg/mm2-plus', 'v1.2')}


def resolve(item):
    name, (repository, default) = item
    ref = os.environ.get(name + '_REF', default)
    url = os.environ.get(name + '_REPO_URL', 'https://github.com/' + repository + '.git')
    if re.fullmatch(r'[0-9a-fA-F]{40}', ref):
        return name, dict(repository=url, ref=ref, revisions=[ref.lower()])
    patterns = ['refs/tags/v*'] if name == 'HICTK' and ref == 'latest' else ([ref, ref + '^{}'] if ref.startswith('refs/') else ['refs/tags/' + ref, 'refs/tags/' + ref + '^{}', 'refs/heads/' + ref])
    result = subprocess.run(['git', 'ls-remote', '--exit-code', url, *patterns],
                            check=True, capture_output=True, text=True, timeout=120)
    lines = sorted(result.stdout.splitlines())
    if not lines:
        raise ValueError('No upstream revision for ' + name + ': ' + ref)
    return name, dict(repository=url, ref=ref, revisions=lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--verify', action='store_true')
    args = parser.parse_args()
    with ThreadPoolExecutor(max_workers=3) as pool:
        sources = dict(pool.map(resolve, TOOLS.items()))
    data = json.dumps(sources, sort_keys=True)
    snapshot = Path('build/ci/toolchain-cache-sources.json')
    if args.verify:
        if snapshot.read_text() != data:
            raise RuntimeError('Upstream tool sources changed during this build; rerun before caching or publishing')
    else:
        snapshot.parent.mkdir(parents=True, exist_ok=True)
        snapshot.write_text(data)
        with Path(os.environ['GITHUB_OUTPUT']).open('a') as output:
            output.write('sources=' + hashlib.sha256(data.encode()).hexdigest() + '\n')


if __name__ == '__main__':
    main()
