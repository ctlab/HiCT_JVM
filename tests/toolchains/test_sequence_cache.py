import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('sequence_cache', Path(__file__).resolve().parents[2] / 'scripts/ci/verify_sequence_cache.py')
cache = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cache)


class CacheTests(unittest.TestCase):
    def test_inventory_and_corruption(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'tool').write_bytes(b'fixture')
            manifest = dict(files=['tool'], sha256={'tool': hashlib.sha256(b'fixture').hexdigest()},
                            commands={key: 'tool' for key in ('seqkit', 'moddotplot', 'hict-moddotplot', 'blastn')})
            (root / 'manifest.json').write_text(json.dumps(manifest))
            cache.verify(root)
            (root / 'extra').write_text('unexpected')
            with self.assertRaises(ValueError): cache.verify(root)
            (root / 'extra').unlink()
            (root / 'tool').write_bytes(b'modified')
            with self.assertRaises(ValueError): cache.verify(root)
            (root / 'tool').unlink()
            with self.assertRaises(ValueError): cache.verify(root)


class ComponentTests(unittest.TestCase):
    @staticmethod
    def component(target, kind, files=None):
        root = Path(str(target) + '-' + kind)
        root.mkdir()
        commands = ('blastn',) if kind == 'blast' else ('seqkit', 'moddotplot', 'hict-moddotplot')
        files = files or list(commands)
        for name in files:
            (root / name).write_bytes(b'fixture')
        manifest = dict(platform='linux', commands={c: files[0] for c in commands}, files=files,
                        sha256={f: hashlib.sha256(b'fixture').hexdigest() for f in files})
        (root / 'manifest.json').write_text(json.dumps(manifest))
        return root

    @staticmethod
    def assembler():
        import sys
        ci = str(Path(__file__).resolve().parents[2] / 'scripts/ci')
        sys.path.insert(0, ci)
        try:
            from assemble_sequence_components import assemble
            return assemble
        finally:
            sys.path.remove(ci)

    def test_verified_components_combine_and_cannot_overwrite_existing_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / 'merged'
            self.component(target, 'sequence')
            self.component(target, 'blast')
            self.assembler()(target)
            cache.verify(target)
            with self.assertRaises(FileExistsError):
                self.assembler()(target)

    def test_overlapping_files_are_rejected_before_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / 'merged'
            self.component(target, 'sequence', ['collision'])
            self.component(target, 'blast', ['collision'])
            with self.assertRaisesRegex(ValueError, 'collision'):
                self.assembler()(target)
            self.assertFalse(target.exists())

    def test_wrong_platform_or_corrupt_component_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / 'merged'
            self.component(target, 'sequence')
            root = self.component(target, 'blast')
            path = root / 'manifest.json'
            manifest = json.loads(path.read_text())
            manifest['platform'] = 'win32'
            path.write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError, 'platform'):
                self.assembler()(target)
            (root / 'blastn').write_bytes(b'corrupt')
            with self.assertRaisesRegex(ValueError, 'Checksum'):
                self.assembler()(target)
            self.assertFalse(target.exists())
