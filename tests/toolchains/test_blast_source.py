"""Exercise source/prebuilt routing and the macOS shell build contract."""
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts/toolchains'))
import add_blast as blast
import blast_prebuilt


class BlastSourceTest(unittest.TestCase):
    def test_macos_builds_verified_source_and_keeps_provenance(self):
        data = io.BytesIO()
        with tarfile.open(fileobj=data, mode='w:gz') as archive:
            member = tarfile.TarInfo('ncbi-blast-2.16.0+-src/c++/LICENSE')
            member.size = 7
            archive.addfile(member, io.BytesIO(b'fixture'))
        contents = data.getvalue()
        import hashlib
        with tempfile.TemporaryDirectory() as temporary:
            work = Path(temporary)
            payload = work / 'payload'
            (payload / 'bin').mkdir(parents=True)

            def build(args, **kwargs):
                self.assertTrue(args[1].endswith('build_blast_darwin.sh'))
                output = Path(args[3])
                (output / 'bin').mkdir(parents=True)
                (output / 'bin/blastn').write_bytes(b'fixture')
                (output / 'share/doc').mkdir(parents=True)
                (output / 'share/doc/blast-macos-compatibility.json').write_text('{}')
                (output / 'share/doc/blast-source-patches.json').write_text('{}')

            with patch.object(blast.sys, 'platform', 'darwin'), \
                 patch.object(blast.urllib.request, 'urlopen', return_value=io.BytesIO(contents)), \
                 patch.object(blast, 'SOURCE_SHA256', hashlib.sha256(contents).hexdigest()), \
                 patch.object(blast.subprocess, 'run', side_effect=build), \
                 patch.object(blast, 'smoke', return_value={'passed': True}) as smoke, \
                 patch.object(blast_prebuilt, 'add_prebuilt') as prebuilt:
                self.assertEqual(blast.add_blast(payload, work, False), 'bin/blastn')
            prebuilt.assert_not_called()
            smoke.assert_called_once()
            report = json.loads((payload / 'share/doc/sequence-analysis/blast-smoke.json').read_text())
            self.assertEqual(report['sourceSha256'], hashlib.sha256(contents).hexdigest())
            self.assertIsNone(report['linuxAbi'])
            self.assertTrue((payload / 'share/doc/sequence-analysis/blast-macos-compatibility.json').is_file())
            self.assertTrue((payload / 'share/doc/sequence-analysis/blast-source-patches.json').is_file())

    def test_windows_keeps_official_binary_route(self):
        with patch.object(blast.sys, 'platform', 'win32'), \
             patch.object(blast_prebuilt, 'add_prebuilt', return_value='bin/blast/blastn.exe') as prebuilt:
            self.assertEqual(blast.add_blast(Path('payload'), Path('work'), False), 'bin/blast/blastn.exe')
            prebuilt.assert_called_once()

    @unittest.skipUnless(os.name == 'posix', 'POSIX shell builder')
    def test_macos_builder_passes_target_to_compilation_and_audit(self):
        # Invoke the actual shell script, with recording tools instead of an
        # unavailable Apple compiler on Linux. Native builds run in macOS CI.
        with tempfile.TemporaryDirectory() as temporary:
            work = Path(temporary)
            source = work / 'source/c++'
            source.mkdir(parents=True)
            (source / 'configure').write_text('''#!/usr/bin/env bash
printf '%s\\n' "$@" > "$RECORD/configure-args"
printf '%s\\n' "$CFLAGS" "$CXXFLAGS" "$LDFLAGS" "$MACOSX_DEPLOYMENT_TARGET" > "$RECORD/compiler-flags"
mkdir -p HiCTBlast/build HiCTBlast/bin
printf '#!/usr/bin/env bash\\nexit 0\\n' > HiCTBlast/bin/blastn
chmod +x HiCTBlast/bin/blastn
''')
            tools = work / 'tools'
            tools.mkdir()
            scripts = {
                'xcrun': 'if [[ "$1" == --find ]]; then command -v mock-cc; else printf "/fake/sdk\\n"; fi',
                'mock-cc': 'printf "%s\\n" "$@" > "$RECORD/preflight-args"; exit "${MOCK_CC_STATUS:-0}"',
                'make': 'printf "%s\\n" "$@" > "$RECORD/make-args"',
                'codesign': 'printf "%s\\n" "$@" > "$RECORD/codesign-args"',
                'python3': 'printf "%s\\n" "$@" > "$RECORD/audit-args"',
            }
            for name, script in scripts.items():
                path = tools / name
                path.write_text('#!/usr/bin/env bash\n' + script + '\n')
                path.chmod(0o755)
            for arch in ('arm64', 'x86_64'):
                record = work / arch
                record.mkdir()
                env = dict(os.environ, PATH=str(tools) + os.pathsep + os.environ['PATH'],
                           RECORD=str(record), HICT_DARWIN_ARCH=arch, HICT_BUILD_JOBS='2',
                           MACOSX_DEPLOYMENT_TARGET='12.3')
                env.pop('MACOS_DEPLOYMENT_TARGET', None)
                subprocess.run(['bash', str(blast.ROOT / 'scripts/toolchains/build_blast_darwin.sh'),
                                str(source.parent), str(work / ('output-' + arch))],
                               env=env, check=True, capture_output=True, text=True)
                args = (record / 'configure-args').read_text().splitlines()
                self.assertIn('--without-openmp', args)
                self.assertIn('--with-mt', args)
                self.assertIn('--with-sqlite3=/usr', args)
                self.assertIn('--without-boost', args)
                flags = (record / 'compiler-flags').read_text().splitlines()
                for value in flags[:3]:
                    self.assertIn('-arch ' + arch, value)
                    self.assertIn('-mmacosx-version-min=12.3', value)
                audit_args = (record / 'audit-args').read_text().splitlines()
                self.assertEqual(audit_args[audit_args.index('--arch') + 1], arch)
                self.assertEqual(audit_args[audit_args.index('--target') + 1], '12.3')
                self.assertIn('2', (record / 'make-args').read_text().splitlines())
                preflight = (record / 'preflight-args').read_text().splitlines()
                self.assertIn('-DTARGET_OS_MAC=1', preflight)
                self.assertIn('-fsyntax-only', preflight)
                self.assertTrue(preflight[-1].endswith('/src/util/compress/zlib/zutil.c'))

            # NCBI can return success after skipping unmet requirements.
            with (source / 'configure').open('a') as configure:
                configure.write('rm HiCTBlast/bin/blastn\n')
            result = subprocess.run(['bash', str(blast.ROOT / 'scripts/toolchains/build_blast_darwin.sh'),
                                     str(source.parent), str(work / 'missing-binary')],
                                    env=env, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('did not produce blastn', result.stderr)

            (record / 'make-args').unlink()
            result = subprocess.run(['bash', str(blast.ROOT / 'scripts/toolchains/build_blast_darwin.sh'),
                                     str(source.parent), str(work / 'failed-preflight')],
                                    env=dict(env, MOCK_CC_STATUS='7'), capture_output=True, text=True)
            self.assertEqual(result.returncode, 7)
            self.assertFalse((record / 'make-args').exists())


if __name__ == '__main__':
    unittest.main()
