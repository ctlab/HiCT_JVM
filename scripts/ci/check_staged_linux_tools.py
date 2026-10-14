#!/usr/bin/env python3
"""Exercise the real Docker bootstrap from a scripts-only build stage.

Compilation is replaced by executable probes; image setup, target dispatch,
permissions and the absent Gradle wrapper are real. Full native builds and
packaged binary smoke tests run separately. Keep the stage and logs as evidence.
"""
import argparse
import os
from pathlib import Path
import shutil
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    stage = args.output.resolve()
    stage.mkdir(parents=True, exist_ok=False)
    root = Path(__file__).resolve().parents[2]
    shutil.copytree(root / 'scripts', stage / 'scripts',
                    ignore=shutil.ignore_patterns('__pycache__'))
    assert not (stage / 'gradlew').exists()
    env = dict(os.environ)
    env.pop('GITHUB_WORKSPACE', None)
    env['HICT_LINUX_ABI_MODE'] = 'glibc217'
    for tool in ('hictk', 'minimap2', 'mm2plus'):
        probe = stage / f'scripts/toolchains/build_{tool}_linux.sh'
        probe.write_text('#!/usr/bin/env bash\nset -euo pipefail\n'
                         'mkdir -p toolchains-dist/linux_x86_64/bin\n'
                         f'printf "%s\\n" "{tool}" > "toolchains-dist/linux_x86_64/bin/{tool}"\n'
                         f'printf "%s\\n" "{tool}" > "{tool}.started"\n')
        # The container bootstrap must make these executable itself.
        probe.chmod(0o644)
    for tool in ('hictk', 'minimap2', 'mm2plus'):
        with (stage / f'{tool}.log').open('w') as log:
            subprocess.run(['bash', 'scripts/ci/linux_container_build.sh', tool],
                           cwd=stage, env=env, stdout=log,
                           stderr=subprocess.STDOUT, check=True)
        if (stage / f'{tool}.started').read_text().strip() != tool:
            raise RuntimeError(f'Target did not execute: {tool}')
        output = stage / 'toolchains-dist/linux_x86_64/bin' / tool
        with output.open('a') as stream:
            stream.write('Host-side merge can update this output.\n')
        if output.stat().st_uid != os.getuid():
            raise RuntimeError(f'Container output belongs to another host UID: {output}')
        print(f'Scripts-only Docker stage passed: {tool}', flush=True)


if __name__ == '__main__':
    main()
