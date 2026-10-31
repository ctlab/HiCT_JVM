import importlib.util
import io
import os
from pathlib import Path
import platform
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch
import zipfile

ROOT = Path(__file__).resolve().parents[2]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


audit = load('macos_audit', 'scripts/ci/check_macos_compatibility.py')
build = load('macos_sequence_build', 'scripts/toolchains/build_sequence_tools.py')


class MacosCompatibilityTest(unittest.TestCase):
    def test_modern_and_legacy_headers_use_minimum_not_build_sdk(self):
        self.assertEqual(audit.minimum_versions('''Load command 7
          cmd LC_BUILD_VERSION
          platform 1
          minos 12.0
          sdk 26.0
        Load command 8
          cmd LC_SOURCE_VERSION
          version 99.0
        '''), ['12.0'])
        self.assertEqual(audit.minimum_versions('''Load command 1
          cmd LC_VERSION_MIN_MACOSX
          version 10.13
          sdk 15.0
        '''), ['10.13'])
        self.assertEqual(audit.version('12.0'), audit.version('12.0.0'))

    def test_missing_and_wrong_platform_headers_fail(self):
        for data in ('', 'Load command 0\ncmd LC_BUILD_VERSION\nplatform IOS\nminos 12.0'):
            with self.assertRaises(ValueError):
                audit.minimum_versions(data)

    def test_rejects_newer_dependency_and_absolute_build_host_link(self):
        auditor = audit.Auditor('12.0', 'arm64')
        outputs = ['arm64', 'Load command 0\ncmd LC_BUILD_VERSION\nplatform MACOS\nminos 14.0',
                   'library:\n\t/opt/homebrew/lib/libexample.dylib (compatibility version 1.0)']
        with patch.object(audit.subprocess, 'check_output', side_effect=outputs):
            auditor.inspect_binary(Path('library'), 'library')
        self.assertEqual(len(auditor.errors), 2)
        self.assertIn('14.0', auditor.errors[0])
        self.assertIn('/opt/homebrew/', auditor.errors[1])

    def test_other_architecture_does_not_raise_minimum(self):
        auditor = audit.Auditor('12.0', 'arm64')
        with patch.object(audit.subprocess, 'check_output', return_value='x86_64') as command:
            auditor.inspect_binary(Path('intel-library'), 'intel-library')
        self.assertEqual(command.call_count, 1)
        self.assertEqual(auditor.errors, [])

    def test_scans_nested_native_tar_inside_jar_without_extracting_paths(self):
        native = bytes.fromhex('cffaedfe') + b'fixture'
        tar = io.BytesIO()
        with tarfile.open(fileobj=tar, mode='w:gz') as archive:
            member = tarfile.TarInfo('../native.dylib')
            member.size = len(native)
            archive.addfile(member, io.BytesIO(native))
        jar = io.BytesIO()
        with zipfile.ZipFile(jar, 'w') as archive:
            archive.writestr('natives.tar.gz', tar.getvalue())
            archive.writestr('Ignored.class', bytes.fromhex('cafebabe') + b'java')
        jar.seek(0)
        auditor = audit.Auditor('12.0', 'arm64')
        with patch.object(auditor, 'inspect_binary') as inspect:
            auditor.scan(jar, 'app.jar')
        self.assertEqual(inspect.call_count, 1)
        self.assertEqual(inspect.call_args.args[1], 'app.jar!natives.tar.gz!../native.dylib')

    def test_python_wheels_are_selected_for_target_not_host_os(self):
        with patch.dict(os.environ, {}, clear=True), patch.object(build.sys, 'platform', 'darwin'), \
             patch.object(build.platform, 'machine', return_value='arm64'), patch.object(build, 'run') as run:
            build.install_requirements('python', Path('/work'), Path('/docs'))
            download, install = (call.args[0] for call in run.call_args_list)
            self.assertIn('macosx_12_3_arm64', download)
            self.assertIn('--only-binary=:all:', download)
            self.assertIn('--no-index', install)
            self.assertEqual(os.environ['MACOSX_DEPLOYMENT_TARGET'], '12.3')

    @unittest.skipUnless(os.name == 'posix', 'POSIX shell entrypoints')
    def test_shell_default_alias_override_and_conflicts(self):
        script = ROOT / 'scripts/portable/macos_target.sh'
        for variables, expected in (({}, '12.3'), ({'MACOS_DEPLOYMENT_TARGET': '13.0'}, '13.0'),
                ({'MACOSX_DEPLOYMENT_TARGET': '12.3'}, '12.3'),
                ({'MACOSX_DEPLOYMENT_TARGET': '12.0'}, None),
                ({'MACOSX_DEPLOYMENT_TARGET': '11.0'}, None),
                ({'MACOSX_DEPLOYMENT_TARGET': 'bad'}, None),
                ({'MACOSX_DEPLOYMENT_TARGET': '12.0', 'MACOS_DEPLOYMENT_TARGET': '13.0'}, None)):
            env = {k: v for k, v in os.environ.items() if k not in
                   ('MACOSX_DEPLOYMENT_TARGET', 'MACOS_DEPLOYMENT_TARGET')}
            result = subprocess.run(['bash', '-ec', 'source "$1"; printf "%s" "$MACOSX_DEPLOYMENT_TARGET"',
                                     'test', str(script)], env=dict(env, **variables), capture_output=True, text=True)
            if expected:
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout, expected)
            else:
                self.assertNotEqual(result.returncode, 0)

    @unittest.skipUnless(sys.platform == 'darwin', 'Requires Apple compiler/lipo/otool')
    def test_real_binary_on_latest_sdk_targets_monterey(self):
        with tempfile.TemporaryDirectory() as work:
            binary = Path(work) / 'sample'
            subprocess.run(['clang', '-mmacosx-version-min=12.0', '-x', 'c', '-', '-o', str(binary)],
                           input='int main(void) { return 0; }', text=True, check=True)
            auditor = audit.Auditor('12.0', platform.machine())
            auditor.check_tree(Path(work))
            self.assertEqual(auditor.errors, [])
            self.assertEqual(len(auditor.binaries), 1)
            older = audit.Auditor('11.0', platform.machine())
            older.check_tree(Path(work))
            self.assertTrue(older.errors)


if __name__ == '__main__':
    unittest.main()
