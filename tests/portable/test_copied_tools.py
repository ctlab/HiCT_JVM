import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('copied_tools', ROOT / 'scripts/portable/verify_copied_tools.py')
copied = importlib.util.module_from_spec(spec)
spec.loader.exec_module(copied)
from prepare_bundled_tools import required_commands


class CopiedToolsTest(unittest.TestCase):
    def test_checksums_preserve_signed_bytes_and_reject_mutation(self):
        for platform in ('darwin_arm64', 'darwin_x86_64'):
            with self.subTest(platform=platform), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                commands = {c: 'bin/' + c for c in required_commands(platform)}
                for entry in commands.values():
                    file = root / entry
                    file.parent.mkdir(exist_ok=True)
                    file.write_bytes(b'signed binary fixture')
                    file.chmod(0o755)
                manifest = dict(commands=commands, files=list(commands.values()),
                                sha256={p: hashlib.sha256((root / p).read_bytes()).hexdigest() for p in commands.values()})
                (root / 'manifest.json').write_text(json.dumps(manifest))
                before = {p: p.read_bytes() for p in root.rglob('*') if p.is_file()}
                with patch.object(sys, 'argv', ['verify_copied_tools.py', '--directory', tmp, '--platform', platform]):
                    copied.main()
                    self.assertEqual(before, {p: p.read_bytes() for p in before})
                    (root / 'bin/seqkit').write_bytes(b'changed signature')
                    with self.assertRaisesRegex(ValueError, 'corrupt tool file'):
                        copied.main()
