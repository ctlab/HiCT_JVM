#!/usr/bin/env python3
"""Verify the actual JAR payload, then run its extracted tools without overrides."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import zipfile
from prepare_bundled_tools import host_platform, required_commands


def verify_archive(jar, name):
    with zipfile.ZipFile(jar) as archive:
        prefix = 'toolchains/' + name + '/'
        manifest = json.loads(archive.read(prefix + 'manifest.json'))
        missing = required_commands(name) - manifest.get('commands', {}).keys()
        if missing:
            raise ValueError('JAR omits required tools: ' + ', '.join(sorted(missing)))
        for command in required_commands(name):
            if manifest['commands'][command] not in manifest['files']:
                raise ValueError('Unchecked command in JAR: ' + command)
        for entry in manifest['files']:
            if entry.startswith('/') or '..' in entry.replace('\\', '/').split('/'):
                raise ValueError('Unsafe tool entry: ' + entry)
            h = hashlib.sha256()
            with archive.open(prefix + entry) as stream:
                for data in iter(lambda: stream.read(8 << 20), b''):
                    h.update(data)
            if h.hexdigest() != manifest.get('sha256', {}).get(entry):
                raise ValueError('Missing checksum or corrupt JAR payload: ' + entry)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--jar', type=Path, required=True)
    parser.add_argument('--platform', default=host_platform())
    parser.add_argument('--java', default='java')
    args = parser.parse_args()
    verify_archive(args.jar, args.platform)
    env = dict(os.environ)
    for key in tuple(env):
        if key.startswith('HICT_') and any(s in key for s in ('TOOLCHAIN', '_BIN', 'NATIVE_LIBRARY')):
            env.pop(key)
    # A fresh Java home directory forces classpath extraction, rather than a
    # developer's external native cache. Keep evidence if the command fails.
    home = tempfile.mkdtemp(prefix='hict-packaged-tool-check-')
    try:
        subprocess.run([args.java, '-Duser.home=' + home, '-Djava.io.tmpdir=' + home, '-jar', str(args.jar.resolve()),
                        'check-toolchains', '--require-hictk', '--require-dotplot',
                        '--require-sequence-tools', '--check-available-natives'], env=env, check=True)
    except subprocess.CalledProcessError:
        # The Java CLI reports exit codes; retain the native loader's actual
        # stderr too (especially PyInstaller DLL/path failures on Windows).
        name = 'hict-moddotplot.exe' if os.name == 'nt' else 'hict-moddotplot'
        for command in Path(home).rglob(name):
            if command.is_file():
                print('Native diagnostic:', command, flush=True)
                result = subprocess.run([str(command), '--help'], env=env,
                                        capture_output=True, text=True, errors='replace', timeout=30)
                print(result.stdout, result.stderr, 'exit:', result.returncode, flush=True)
        raise
    print('Packaged tools: hashes and startup checks passed for', args.platform)


if __name__ == '__main__':
    main()
