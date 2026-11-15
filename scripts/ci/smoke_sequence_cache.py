#!/usr/bin/env python3
"""Re-execute known-answer fixtures against cached native executables."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts/toolchains'))
from add_blast import smoke


def main(root):
    root = root.resolve()
    commands = json.loads((root / 'manifest.json').read_text())['commands']
    with tempfile.TemporaryDirectory(prefix='hict-cache-smoke-') as tmp:
        work = Path(tmp)
        if 'seqkit' in commands:
            subprocess.run([sys.executable, str(ROOT / 'scripts/sequence_analysis/smoke_tools.py'),
                            '--packaged-only', '--seqkit', str(root / commands['seqkit']),
                            '--adapter', str(root / commands['hict-moddotplot']),
                            '--moddotplot', str(root / commands['moddotplot']),
                            '--output', str(work / 'sequence.json')], check=True, timeout=300)
        if 'blastn' in commands:
            smoke(root / commands['blastn'], work / 'blast')
    print('Cached native tools passed known-answer fixtures')


if __name__ == '__main__':
    main(Path(sys.argv[1]))
