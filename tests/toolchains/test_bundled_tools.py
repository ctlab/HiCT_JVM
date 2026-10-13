import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts/toolchains'))
import prepare_bundled_tools as tools
from verify_packaged_tools import verify_archive


class BundledToolsTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        commands = {k: 'bin/' + k for k in tools.required_commands('linux_x86_64')}
        for name in commands.values():
            p = self.root / name; p.parent.mkdir(exist_ok=True); p.write_bytes(b'fixture'); p.chmod(0o755)
        self.manifest = dict(commands=commands, files=list(commands.values()),
                             sha256={v: hashlib.sha256(b'fixture').hexdigest() for v in commands.values()})
        self.write_manifest()

    def write_manifest(self):
        (self.root / 'manifest.json').write_text(json.dumps(self.manifest))

    def test_required_tools_include_blast_on_linux_and_seqkit_everywhere(self):
        for name in ('linux_x86_64', 'darwin_arm64', 'darwin_x86_64', 'windows_x86_64'):
            self.assertIn('seqkit', tools.required_commands(name))
            self.assertIn('hict-moddotplot', tools.required_commands(name))
        self.assertIn('blastn', tools.required_commands('linux_x86_64'))
        self.assertNotIn('mm2plus_avx512', tools.required_commands('darwin_arm64'))

    def test_source_change_invalidates_cache(self):
        with patch.object(tools, 'source_key', return_value='original'):
            tools.seal(self.root, 'linux_x86_64')
            self.assertTrue(tools.cached(self.root, 'linux_x86_64'))
        with patch.object(tools, 'source_key', return_value='changed'):
            self.assertFalse(tools.cached(self.root, 'linux_x86_64'))

    def test_corruption_invalidates_cache(self):
        tools.seal(self.root, 'linux_x86_64')
        (self.root / 'bin/seqkit').write_bytes(b'changed')
        self.assertFalse(tools.cached(self.root, 'linux_x86_64'))
        with self.assertRaisesRegex(ValueError, 'corrupt'):
            tools.verify(self.root, 'linux_x86_64')

    def test_missing_command_is_rejected_even_with_valid_hashes(self):
        self.manifest['commands'].pop('blastn'); self.write_manifest()
        with self.assertRaisesRegex(ValueError, 'blastn'):
            tools.verify(self.root, 'linux_x86_64')

    def test_unlisted_command_is_rejected(self):
        self.manifest['commands']['seqkit'] = 'bin/unlisted'; self.write_manifest()
        with self.assertRaisesRegex(ValueError, 'checksummed'):
            tools.verify(self.root, 'linux_x86_64')

    def test_packaged_contents_checked_independently_of_build_directory(self):
        jar = self.root / 'test.jar'
        with zipfile.ZipFile(jar, 'w') as z:
            z.write(self.root / 'manifest.json', 'toolchains/linux_x86_64/manifest.json')
            for name in self.manifest['files']:
                z.writestr('toolchains/linux_x86_64/' + name, b'fixture' if name != 'bin/seqkit' else b'wrong packaged bytes')
        with self.assertRaisesRegex(ValueError, 'corrupt JAR'):
            verify_archive(jar, 'linux_x86_64')

    def test_path_escape_rejected(self):
        self.manifest['files'].append('../outside'); self.write_manifest()
        with self.assertRaisesRegex(ValueError, 'Unsafe'):
            tools.verify(self.root, 'linux_x86_64')


if __name__ == '__main__':
    unittest.main()
